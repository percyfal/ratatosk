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
import csv
import logging
import ratatosk
from ratatosk import backend
from ratatosk.job import PipelineTask, JobTask, JobWrapperTask, PrintConfig
from ratatosk.utils import rreplace, fullclassname, make_fastq_links
from ratatosk.lib.align.bwa import BwaSampe, BwaAln
from ratatosk.lib.tools.gatk import VariantEval, UnifiedGenotyper, RealignerTargetCreator, IndelRealigner, VariantFiltration
from ratatosk.lib.tools.picard import PicardMetrics, MergeSamFiles
from ratatosk.lib.tools.fastqc import FastQCJobTask
from ratatosk.lib.variation.htslib import VcfMerge
from ratatosk.lib.variation.tabix import Bgzip

logger = logging.getLogger('luigi-interface')

# Dirty fix: standard bwa sampe calculates the wrong target names when
# trimming and syncing has been done. This once again shows the
# importance of collecting all labels preceding a given node in order
# to calculate the target name
class HaloBwaSampe(BwaSampe):
    _config_subsection = "HaloBwaSampe"
    parent_task = luigi.Parameter(default="ratatosk.lib.align.bwa.BwaAln")

    def requires(self):
        # From target name, generate sai1, sai2, fastq1, fastq2
        sai1 = rreplace(rreplace(rreplace(self._make_source_file_name(), self.source_suffix, self.read1_suffix + self.source_suffix, 1), ".trimmed.sync", "", 1), ".sai", ".trimmed.sync.sai", 1)
        sai2 = rreplace(rreplace(rreplace(self._make_source_file_name(), self.source_suffix, self.read2_suffix + self.source_suffix, 1), ".trimmed.sync", "", 1), ".sai", ".trimmed.sync.sai", 1)
        return [BwaAln(target=sai1), BwaAln(target=sai2)]

# Raw variant calling class done on merged data to generate a list of
# raw candidates around which realignment is done.
class RawUnifiedGenotyper(UnifiedGenotyper):
    _config_subsection = "RawUnifiedGenotyper"
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.picard.MergeSamFiles")
    options = luigi.Parameter(default=("-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH",), is_list=True)
    label = ".BOTH.raw"

# Override RealignerTargetCreator and IndelRealigner to use
# RawUnifiedGenotyper vcf as input for known sites
class RawRealignerTargetCreator(RealignerTargetCreator):
    _config_subsection = "RawRealignerTargetCreator"
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.picard.MergeSamFiles")
    target_suffix = luigi.Parameter(default=".intervals")
    
    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), ratatosk.lib.tools.samtools.IndexBam(target=rreplace(source, self.source_suffix, ".bai", 1), parent_task=fullclassname(cls)), ratatosk.pipeline.haloplex.RawUnifiedGenotyper(target=rreplace(source, ".bam", ".BOTH.raw.vcf", 1))]

    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output(), "-known", self.input()[2]]
        if not self.ref:
            raise Exception("need reference for Realignment")
        retval.append(" -R {}".format(self.ref))
        return retval

# NOTE: Here I redefine target dependencies for IndelRealigner, the way I believe it should be
class RawIndelRealigner(IndelRealigner):
    _config_subsection = "RawIndelRealigner"
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.picard.MergeSamFiles")
    source_suffix = luigi.Parameter(default=".bam")
    
    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), 
                ratatosk.lib.tools.samtools.IndexBam(target=rreplace(source, self.source_suffix, ".bai", 1), parent_task="ratatosk.lib.tools.picard.MergeSamFiles"), 
                ratatosk.pipeline.haloplex.RawRealignerTargetCreator(target=rreplace(source, ".bam", ".intervals", 1)),
                ratatosk.pipeline.haloplex.RawUnifiedGenotyper(target=rreplace(source, ".bam", ".BOTH.raw.vcf", 1))]
    
    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output(),
                  "--targetIntervals", self.input()[2],
                  "-known", self.input()[3]]
        if not self.ref:
            raise Exception("need reference for Realignment")
        retval.append(" -R {}".format(self.ref))
        return retval

class VariantHaloFiltration(VariantFiltration):
    """Settings for filtering haloplex variant calls"""
    _config_subsection = "VariantHaloFiltration"
    # Options from Halo
    options = luigi.Parameter(default=('--clusterWindowSize 10 --clusterSize 3 --filterExpression "MQ0 >= 4 && ((MQ0 / (1.0 * DP)) > 0.1)" --filterName "HARD_TO_VALIDATE" --filterExpression "DP < 10" --filterName "LowCoverage" --filterExpression "QUAL < 30.0" --filterName "VeryLowQual" --filterExpression "QUAL > 30.0 && QUAL < 50.0" --filterName "LowQual" --filterExpression "QD < 1.5" --filterName "LowQD"',), is_list=True)


class HaloPipeline(PipelineTask):
    _config_section = "pipeline"
    _config_subsection = "HaloPlex"
    # Weird: after subclassing job.PipelineTask, not having a default
    # here throws an incomprehensible error
    indir = luigi.Parameter(description="Where raw data lives", default=None)
    outdir = luigi.Parameter(description="Where analysis takes place", default=None)
    sample = luigi.Parameter(default=[], description="Samples to process.", is_list=True)
    flowcell = luigi.Parameter(default=[], description="Flowcells to process.", is_list=True)
    lane = luigi.Parameter(default=[], description="Lanes to process.", is_list=True)
    # Hard-code this for now - would like to calculate on the fly so that
    # implicit naming is unnecessary
    final_target_suffix = "trimmed.sync.sort.merge.realign.recal.clip.filtered.eval_metrics"    
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

class HaloPlex(HaloPipeline):
    def requires(self):
        self._setup()
        reads = ["{}_R1_001.fastq.gz".format(x[2]) for x in self.targets] +  ["{}_R2_001.fastq.gz".format(x[2]) for x in self.targets]
        variant_targets = ["{}.{}".format(x[1], self.final_target_suffix) for x in self.targets]
        picard_metrics_targets = ["{}.{}".format(x[1], "trimmed.sync.sort.merge") for x in self.targets]

        return [VariantEval(target=tgt) for tgt in variant_targets] + [PicardMetrics(target=tgt2) for tgt2 in picard_metrics_targets] + [PrintConfig()] + [FastQCJobTask(target=tgt) for tgt in reads]

class HaloBgzip(Bgzip):
    _config_subsection = "HaloBgzip"
    parent_task = luigi.Parameter(default="ratatosk.lib.variation.htslib.VcfMerge")

class HaloPlexSummary(HaloPipeline):
    def requires(self):
        self._setup()
        return [HaloBgzip(target=os.path.join(self.outdir, "all.vcfmerge.vcf.gz"))]
