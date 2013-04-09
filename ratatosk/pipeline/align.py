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
The align pipeline implements an alignment pipeline that aligns reads,
merges samples and generates picard quality statistics.

Calling via ratatosk_run.py
----------------------------

.. code:: bash

   ratatosk_run.py Align --indir inputdir --custom-config custom_config_file.yaml
   ratatosk_run.py AlignSummary --indir inputdir --custom-config custom_config_file.yaml


Classes
-------
"""
import luigi
import os
import glob
import logging
from ratatosk import backend
from ratatosk.job import PipelineTask, JobTask, JobWrapperTask
from ratatosk.lib.tools.picard import PicardMetrics, SortSam
from ratatosk.lib.tools.fastqc import FastQCJobTask
from ratatosk.lib.files.fastq import FastqFileLink
from ratatosk.utils import make_fastq_links

logger = logging.getLogger('luigi-interface')

class AlignPipeline(PipelineTask):
    _config_section = "pipeline"
    _config_subsection = "Align"
    sample = luigi.Parameter(default=[], is_list=True, description="Samples to process")
    flowcell = luigi.Parameter(default=[], is_list=True, description = "flowcells to process")
    lane = luigi.Parameter(default=[], description="Lanes to process.", is_list=True)
    indir = luigi.Parameter(description="Where raw data lives", default=None)
    outdir = luigi.Parameter(description="Where analysis takes place", default=None)
    final_target_suffix = ".sort.merge.dup.bam"

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



class Align(AlignPipeline):
    def requires(self):
        self._setup()
        fastqc_targets = ["{}_R1_001_fastqc".format(x[2]) for x in self.targets] +  ["{}_R2_001_fastqc".format(x[2]) for x in self.targets]
        picard_metrics_targets = ["{}.{}".format(x[1], "sort.merge.dup") for x in self.targets]
        return [PicardMetrics(target=tgt) for tgt in picard_metrics_targets]  + [FastQCJobTask(target=tgt) for tgt in fastqc_targets]


class AlignSummary(AlignPipeline):
    def requires(self):
        self._setup()
