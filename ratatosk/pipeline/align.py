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

logger = logging.getLogger('luigi-interface')

class AlignSeqcap(PipelineTask):
    _config_section = "pipeline"
    _config_subsection = "AlignSeqcap"
    sample = luigi.Parameter(default=[], is_list=True, description="Samples to process")
    flowcell = luigi.Parameter(default=[], is_list=True, description = "flowcells to process")
    project = luigi.Parameter(description="Project name", default=None)
    projectdir = luigi.Parameter(description="Where projects live", default=os.curdir)
    final_target_suffix = ".sort.merge.bam"

    def requires(self):
        if not self.project:
            return
        tgt_fun = self.set_target_generator_function()
        if not tgt_fun:
            return []
        target_list = tgt_fun(self)
        print target_list
        picard_metrics_target_list = ["{}.{}".format(x, "sort.merge") for x in target_list]
        return [PicardMetrics(target=tgt) for tgt in picard_metrics_target_list]


