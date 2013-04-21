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
The haloplex pipeline implements a workflow recommended by Agilent technologies. 

It adds the following custom tasks:

1. RawUnifiedGenotyper
2. VariantHaloFiltration

The main pipeline tasks are 

1. HaloPlex
2. HaloPlexSummary

They should be run in this order.

Calling via ratatosk_run.py
----------------------------

.. code-block:: text

   ratatosk_run.py Halo --indir inputdir --custom-config custom_config_file.yaml
   ratatosk_run.py HaloPlexSummary --indir inputdir --custom-config custom_config_file.yaml


Classes
-------
"""

import luigi
import os
from ratatosk import backend
from ratatosk.job import PipelineTask, JobTask, JobWrapperTask, PrintConfig
from ratatosk.utils import make_fastq_links
from ratatosk.lib.tools.gatk import VariantEval, UnifiedGenotyper, VariantFiltration
from ratatosk.lib.variation.tabix import Bgzip
from ratatosk.log import get_logger

logger = get_logger()

class RawUnifiedGenotyper(UnifiedGenotyper):
    """
    RawUnifiedGenotyper is a variant calling class done on merged data
    to generate a list of raw candidates around which realignment is
    done.
    """
    _config_section = "ratatosk.lib.tools.gatk"
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.picard.MergeSamFiles", "ratatosk.lib.tools.picard.PicardMetrics"), is_list=True)
    options = luigi.Parameter(default=("-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH",), is_list=True)
    label = luigi.Parameter(default=".BOTH.raw")

class VariantHaloFiltration(VariantFiltration):
    """VariantHaloFiltration implementes settings tailored for
    filtering haloplex variant calls

    """
    _config_section = "ratatosk.lib.tools.gatk"
    # Options from Halo
    options = luigi.Parameter(default=('--clusterWindowSize 10 --clusterSize 3 --filterExpression "MQ0 >= 4 && ((MQ0 / (1.0 * DP)) > 0.1)" --filterName "HARD_TO_VALIDATE" --filterExpression "DP < 10" --filterName "LowCoverage" --filterExpression "QUAL < 30.0" --filterName "VeryLowQual" --filterExpression "QUAL > 30.0 && QUAL < 50.0" --filterName "LowQual" --filterExpression "QD < 1.5" --filterName "LowQD"',), is_list=True)


class HaloPipeline(PipelineTask):
    # Weird: after subclassing job.PipelineTask, not having a default
    # here throws an incomprehensible error
    indir = luigi.Parameter(description="Where raw data lives", default=None)
    outdir = luigi.Parameter(description="Where analysis takes place", default=None)
    target_generator_file = luigi.Parameter(description="Target generator file name", default=None, is_global=True)
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
        if self.outdir != self.indir and self.targets:
            self.targets = make_fastq_links(self.targets, self.indir, self.outdir)
        # Finally register targets in backend
        backend.__global_vars__["targets"] = self.targets

class HaloPlex(HaloPipeline):
    def requires(self):
        self._setup()
        if not self.targets:
            return []
        variant_targets = ["{}.{}".format(x.prefix("sample"), self.final_target_suffix) for x in self.targets]
        return [VariantEval(target=tgt) for tgt in variant_targets]

class HaloBgzip(Bgzip):
    _config_section = "ratatosk.lib.variation.tabix"
    parent_task = luigi.Parameter(default=("ratatosk.lib.variation.htslib.VcfMerge", ), is_list=True)

class HaloPlexSummary(HaloPipeline):
    def requires(self):
        self._setup()
        return [HaloBgzip(target=os.path.join(self.outdir, "all.vcfmerge.vcf.gz"))]
