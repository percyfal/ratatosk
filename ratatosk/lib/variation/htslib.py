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
import os
import luigi
import logging
import ratatosk.lib.files.external
import ratatosk.lib.variation.tabix
from ratatosk.handler import RatatoskHandler, register_task_handler
from ratatosk import backend
from ratatosk.job import InputJobTask, JobTask, DefaultShellJobRunner

class HtslibJobRunner(DefaultShellJobRunner):
    pass

class InputVcfFile(InputJobTask):
    _config_section = "htslib"
    _config_subsection = "InputVcfFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.VcfFile")
    target_suffix = luigi.Parameter(default=".vcf")

class HtslibVcfJobTask(JobTask):
    _config_section = "htslib"
    executable = luigi.Parameter(default="vcf")
    parent_task = luigi.Parameter(default="ratatosk.lib.variation.tabix.TabixTabixJobTask")

    def job_runner(self):
        return HtslibJobRunner()

class HtslibVcfMergeJobTask(HtslibVcfJobTask):
    _config_subsection = "vcfmerge"
    sub_executable = luigi.Parameter(default="merge")
    target_generator_handler = luigi.Parameter(default=None)
    label = luigi.Parameter(default=".vcfmerge")
    
    def args(self):
        return [x for x in self.input()]
    
    def requires(self):
        cls = self.set_parent_task()
        sources = []
        if self.target_generator_handler and "target_generator_handler" not in self._handlers.keys():
            tgf = RatatoskHandler(label="target_generator_handler", mod=self.target_generator_handler)
            register_task_handler(self, tgf)
        if not "target_generator_handler" in self._handlers.keys():
            logging.warn("vcf merge requires a target generator handler; no defaults are as of yet implemented")
            return []
        sources = self._handlers["target_generator_handler"](self)
        return [cls(target=src) for src in sources]

