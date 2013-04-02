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
from ratatosk.job import PipelineTask, JobTask, JobWrapperTask
from ratatosk.lib.tools.picard import PicardMetrics, SortSam
from ratatosk.lib.files.fastq import FastqFileLink
from ratatosk.utils import make_fastq_links

logger = logging.getLogger('luigi-interface')

class AlignSeqcap(PipelineTask):
    _config_section = "pipeline"
    _config_subsection = "AlignSeqcap"
    sample = luigi.Parameter(default=[], is_list=True, description="Samples to process")
    flowcell = luigi.Parameter(default=[], is_list=True, description = "flowcells to process")
    lane = luigi.Parameter(default=[], description="Lanes to process.", is_list=True)
    indir = luigi.Parameter(description="Where raw data lives", default=None)
    outdir = luigi.Parameter(description="Where analysis takes place", default=None)
    final_target_suffix = ".sort.merge.dup.bam"

    def requires(self):
        if not self.indir:
            return
        if self.outdir is None:
            self.outdir = self.indir
        targets = [tgt for tgt in self.target_iterator()]
        if self.outdir != self.indir:
            targets = make_fastq_links(targets, self.indir, self.outdir)

        picard_metrics_targets = ["{}.{}".format(x[1], "sort.merge.dup") for x in targets]
        return [PicardMetrics(target=tgt) for tgt in picard_metrics_targets]


