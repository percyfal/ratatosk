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
"""
Ratatosk seqcap pipeline module.

The sequence capture pipeline implements the `best practice
recommended by BROAD
<http://www.broadinstitute.org/gatk/guide/topic?name=best-practices>`_.

It adds the following custom tasks:

1. CombineFilteredVariants
2. FiltrationWrapper
3. SelectVariantsWrapper

The main pipeline tasks are 

1. SeqCap
2. SeqCapSummary

They should be run in this order.

Calling via ratatosk_run.py
----------------------------

.. code-block:: text

   ratatosk_run.py SeqCap --indir inputdir --custom-config custom_config_file.yaml
   ratatosk_run.py SeqCapSummary --indir inputdir --custom-config custom_config_file.yaml


Classes
-------
"""
import luigi
import logging
from ratatosk import backend
from ratatosk.job import PipelineTask, JobWrapperTask
from ratatosk.lib.tools.gatk import  CombineVariants, SelectSnpVariants, SelectIndelVariants, VariantSnpRecalibrator, VariantIndelRecalibrator, VariantSnpRecalibrator, VariantIndelRecalibrator, VariantSnpFiltrationExp, VariantIndelFiltrationExp, VariantSnpEffAnnotator
from ratatosk.utils import make_fastq_links

logger = logging.getLogger('luigi-interface')

class CombineFilteredVariants(CombineVariants):
    """
    CombineVariants is called elsewhere in pipeline so this class is
    needed to get its own namespace in the configuration file.
    """
    _config_section = "ratatosk.lib.tools.gatk"

    parent_task = luigi.Parameter(default=("ratatosk.pipeline.seqcap.FiltrationWrapper",
                                           "ratatosk.lib.tools.gatk.InputBamFile",), is_list=True)
    label = "-combined"
    suffix = ".vcf"
    split_by = None

    def args(self):
        cls = self.parent()[0]
        retval = []
        for x in cls(target=self.source()).input():
            retval += ["-V", x]
        retval += ["-o", self.output()]
        if not self.ref:
            raise Exception("need reference for CombineVariants")
        retval += ["-R", self.ref]
        return retval

class FiltrationWrapper(JobWrapperTask):
    """
    The FiltrationWrapper wraps snp and indel variant filtration
    tasks. It sets up different filtration tasks depending on the
    *cov_interval* setting, which can be one of "regional", "exome",
    or None. The effects of the different choices are as follows:

    regional
      Do filtering based on JEXL-expressions. See `section 3, subtitle Recommendations for very small data sets <http://www.broadinstitute.org/gatk/guide/topic?name=best-practices>`_

    exome
      Use VQSR with modified argument settings (`--maxGaussians 4 --percentBad 0.05`) as recommended in  `3. Notes about small whole exome projects <http://www.broadinstitute.org/gatk/guide/topic?name=best-practices>`_

    None
      Perform "standard" VQSR

    
    """
    _config_section = "ratatosk.pipeline.seqcap"
    label = None
    suffix = ""

    # TODO: this does not take care of the freebayes case
    def requires(self):
        target = self.target
        if isinstance(self.target, tuple) or isinstance(self.target, list):
            target = self.target[0]
        if backend.__global_vars__["cov_interval"] == "regional":
            # Use JEXL filtering
            return [VariantSnpFiltrationExp(target=target + SelectSnpVariants().label + VariantSnpFiltrationExp().label + VariantSnpFiltrationExp().parent()[0]().sfx()),
                    VariantIndelFiltrationExp(target=target + SelectIndelVariants().label + VariantIndelFiltrationExp().label + VariantIndelFiltrationExp().parent()[0]().sfx())]
        else:
            if backend.__global_vars__["cov_interval"] == "exome":
                # Use recalibrator for exome
                return [VariantSnpRecalibratorExome(target=target + SelectSnpVariants().label + VariantSnpRecalibratorExome().label + VariantSnpRecalibratorExome().parent()[0]().sfx()),
                        VariantIndelRecalibrator(target=target + SelectIndelVariants().label + VariantIndelRecalibrator().label + VariantIndelRecalibrator().parent()[0]().sfx())]
            else:
                # use whole-genome recalibration
                return [VariantSnpRecalibrator(target=target + SelectSnpVariants().label + VariantSnpRecalibrator().label + VariantSnpRecalibrator().parent()[0]().sfx()),
                        VariantIndelRecalibrator(target=target + SelectIndelVariants().label + VariantIndelRecalibrator().label + VariantIndelRecalibrator().parent()[0]().sfx())]

    def output(self):
        return self.input()

class SelectVariantsWrapper(JobWrapperTask):
    def requires(self):
        return [SelectSnpVariants(target=self.target + SelectSnpVariants().label + SelectSnpVariants().parent()[0]().sfx()),
                SelectIndelVariants(target=self.target + SelectIndelVariants().label + SelectIndelVariants().parent()[0]().sfx())]

class SeqCapPipeline(PipelineTask):
    indir = luigi.Parameter(description="Where raw data lives", default=None)
    outdir = luigi.Parameter(description="Where analysis takes place", default=None)
    sample = luigi.Parameter(default=[], description="Samples to process.", is_list=True)
    flowcell = luigi.Parameter(default=[], description="Flowcells to process.", is_list=True)
    lane = luigi.Parameter(default=[], description="Lanes to process.", is_list=True)
    cov_interval = luigi.Parameter(default="regional", description="If set, determines whether VQSR is run, and governs filtering rules. One of 'exome' or 'regional'. 'exome' uses a modified version of VQSR for small-sample exome sets, 'regional' uses JEXL-based filtering and does not run VQSR.")
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
        out_targets = ["{}.{}".format(x[1], "sort.merge.dup.realign.recal-variants-combined-phased-effects") for x in self.targets]
        out_targets = ["{}.{}".format(x[1], "sort.merge.dup.realign.recal-variants-combined-phased-annotated.vcf") for x in self.targets]
        return [VariantSnpEffAnnotator(target=tgt) for tgt in out_targets]

class SeqCapSummary(SeqCapPipeline):
    def requires(self):
        self._setup()
        
