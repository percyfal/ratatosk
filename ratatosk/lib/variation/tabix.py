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
from ratatosk.job import InputJobTask, JobTask
from ratatosk.jobrunner import DefaultShellJobRunner

class TabixJobRunner(DefaultShellJobRunner):
    pass

class InputVcfFile(InputJobTask):
    _config_section = "tabix"
    _config_subsection = "InputVcfFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.VcfFile")
    suffix = luigi.Parameter(default=(".vcf", ), is_list=True)

class TabixJobTask(JobTask):
    _config_section = "tabix"
    executable = ""

    def job_runner(self):
        return TabixJobRunner()

    def exe(self):
        return self.sub_executable
    
    def main(self):
        return None

class Bgzip(TabixJobTask):
    _config_subsection = "Bgzip"
    sub_executable = luigi.Parameter(default="bgzip")
    parent_task = luigi.Parameter(default="ratatosk.lib.variation.tabix.InputVcfFile")
    suffix = luigi.Parameter(default=".vcf.gz")

    def args(self):
        return [self.input()[0]]

# Since this is such a common operation, add the task here
class BgUnzip(TabixJobTask):
    _config_subsection = "BgUnzip"
    sub_executable = luigi.Parameter(default="bgzip")
    parent_task = luigi.Parameter(default="ratatosk.lib.variation.tabix.Bgzip")
    suffix = luigi.Parameter(default=".vcf")

    def opts(self):
        retval = list(self.options)
        if not "-d" in retval:
            retval += ["-d"]
        return retval

    def args(self):
        return [self.input()[0]]

class Tabix(TabixJobTask):
    _config_subsection = "Tabix"
    sub_executable = luigi.Parameter(default="tabix")
    parent_task = luigi.Parameter(default="ratatosk.lib.variation.tabix.Bgzip")
    suffix = luigi.Parameter(default=".vcf.gz.tbi")

    def args(self):
        return [self.input()[0]]
    
