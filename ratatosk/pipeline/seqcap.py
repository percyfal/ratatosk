# Copyright (c) 2013 Per Unneberg
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
import luigi
import os
import glob
import logging
import ratatosk
from ratatosk import backend
from ratatosk.job import PipelineTask, JobTask, JobWrapperTask
from ratatosk.lib.tools.picard import PicardMetrics
from ratatosk.lib.tools.fastqc import FastQCJobTask
from ratatosk.lib.tools.gatk import UnifiedGenotyper, CombineVariants, SelectSnpVariants, SelectIndelVariants, VariantSnpRecalibrator, VariantIndelRecalibrator, VariantSnpRecalibrator, VariantIndelRecalibrator, VariantSnpFiltrationExp, VariantIndelFiltrationExp
from ratatosk.utils import rreplace, fullclassname, make_fastq_links
from ratatosk.lib.annotation.snpeff import snpEff

logger = logging.getLogger('luigi-interface')

class SeqCapReadBackedPhasing(ratatosk.lib.tools.gatk.ReadBackedPhasing):
    # TODO: fix this mess where we have a dependency on a bam file
    # generated waaay back, lacking -variants-combined label. See
    # ratatosk.lib.tools.gatk.ReadBackedPhasing
    parent_task = luigi.Parameter(default="ratatosk.pipeline.seqcap.CombineFilteredVariants")
    source_suffix = ".vcf"
    label = "-phased"

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        base = source.split("-")[0]
        ext = ratatosk.lib.tools.gatk.PrintReads(target=source).target_suffix
        return [ratatosk.lib.tools.gatk.PrintReads(target=base + ext), cls(target=source)]

class CombineFilteredVariants(CombineVariants):
    parent_task = luigi.Parameter(default="ratatosk.pipeline.seqcap.FiltrationWrapper")
    label = "-combined"
    source_suffix = ""

    # Need to override since CombineVariants expects splitting on chromosome
    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source)]

    def args(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        retval = []
        for x in cls(target=source).input():
            retval += ["-V", x]
        retval += ["-o", self.output()]
        if not self.ref:
            raise Exception("need reference for CombineVariants")
        retval += ["-R", self.ref]
        return retval

class snpEffWrapper(JobWrapperTask):
    label = "-effects"
    def requires(self):
        return [snpEff(target=self.target + ".txt", target_suffix=".txt"), snpEff(target=self.target + ".vcf", target_suffix=".vcf")]


class FiltrationWrapper(JobWrapperTask):
    _config_section = "SeqCapPipeline"
    _config_subsection = "FiltrationWrapper"
    label = None

    # TODO: this does not take care of the freebayes case
    def requires(self):
        if backend.__global_vars__["cov_interval"] == "regional":
            # Use JEXL filtering
            return [VariantSnpFiltrationExp(target=self.target + SelectSnpVariants().label + VariantSnpFiltrationExp().label + VariantSnpFiltrationExp().target_suffix),
                    VariantIndelFiltrationExp(target=self.target + SelectIndelVariants().label + VariantIndelFiltrationExp().label + VariantIndelFiltrationExp().target_suffix)]
        else:
            if backend.__global_vars__["cov_interval"] == "exome":
                # Use recalibrator for exome
                return [VariantSnpRecalibratorExome(target=self.target + SelectSnpVariants().label + VariantSnpRecalibratorExome().label + VariantSnpRecalibratorExome().target_suffix[0]),
                        VariantIndelRecalibrator(target=self.target + SelectIndelVariants().label + VariantIndelRecalibrator().label + VariantIndelRecalibrator().target_suffix[0])]
            else:
                # use whole-genome recalibration
                return [VariantSnpRecalibrator(target=self.target + SelectSnpVariants().label + VariantSnpRecalibrator().label + VariantSnpRecalibrator().target_suffix[0]),
                        VariantIndelRecalibrator(target=self.target + SelectIndelVariants().label + VariantIndelRecalibrator().label + VariantIndelRecalibrator().target_suffix[0])]

    def output(self):
        return self.input()

class SelectVariantsWrapper(JobWrapperTask):
    def requires(self):
        return [SelectSnpVariants(target=self.target + SelectSnpVariants().label + SelectSnpVariants().target_suffix),
                SelectIndelVariants(target=self.target + SelectIndelVariants().label + SelectIndelVariants().target_suffix)]

class SeqCapPipeline(PipelineTask):
    _config_section = "pipeline"
    _config_subsection = "SeqCap"
    indir = luigi.Parameter(description="Where raw data lives", default=None)
    outdir = luigi.Parameter(description="Where analysis takes place", default=None)
    sample = luigi.Parameter(default=[], description="Samples to process.", is_list=True)
    flowcell = luigi.Parameter(default=[], description="Flowcells to process.", is_list=True)
    lane = luigi.Parameter(default=[], description="Lanes to process.", is_list=True)
    cov_interval = luigi.Parameter(default=None, description="If set, determines whether VQSR is run, and governs filtering rules. One of 'exome' or 'regional'. 'exome' uses a modified version of VQSR for small-sample exome sets, 'regional' uses JEXL-based filtering and does not run VQSR.")
    # Hard-code this for now - would like to calculate on the fly so that
    # implicit naming is unnecessary
    final_target_suffix = ".sort.merge.dup.realign.recal.bam"#realign.recal.filtered.eval_metrics"    
    targets = []

    def _setup(self):
        # List requirements for completion, consisting of classes above
        if self.indir is None:
            logger.error("Need input directory to run")
            self.targets = []
        if self.outdir is None:
            self.outdir = self.indir
        self.targets = [tgt for tgt in self.target_iterator()]
        if self.outdir != self.indir:
            self.targets = make_fastq_links(self.targets, self.indir, self.outdir)
        # Finally register targets in backend
        backend.__global_vars__["targets"] = self.targets
        backend.__global_vars__["cov_interval"] = self.cov_interval


class SeqCap(SeqCapPipeline):
    def requires(self):
        self._setup()
        reads = ["{}_R1_001.fastq.gz".format(x[2]) for x in self.targets] +  ["{}_R2_001.fastq.gz".format(x[2]) for x in self.targets]
        picard_metrics_targets = ["{}.{}".format(x[1], "sort.merge.dup") for x in self.targets]
        out_targets = ["{}.{}".format(x[1], "sort.merge.dup.realign.recal-variants-combined-phased.vcf") for x in self.targets]
        #out_targets = ["{}.{}".format(x[1], "sort.merge.dup.realign.recal.vcf") for x in self.targets]
        print reads
        print picard_metrics_targets
        print out_targets
        return [PicardMetrics(target=tgt) for tgt in picard_metrics_targets]  + [FastQCJobTask(target=tgt) for tgt in reads] + [SeqCapReadBackedPhasing(target=tgt) for tgt in out_targets]

    # + [snpEffWrapper]

    # + [CombineFilteredVariants(target=tgt) for tgt in out_targets]

# + [FiltrationWrapper(target=tgt, cov_interval=self.cov_interval) for tgt in out_targets]

    # + [SelectVariantsWrapper(target=tgt) for tgt in out_targets]

    #+ [UnifiedGenotyper(target=tgt) for tgt in out_targets]

class SeqCapSummary(SeqCapPipeline):
    def requires(self):
        self._setup()
        
