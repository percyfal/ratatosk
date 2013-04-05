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
from ratatosk.job import InputJobTask, JobTask, DefaultShellJobRunner

class TabixJobRunner(DefaultShellJobRunner):
    pass

class InputVcfFile(InputJobTask):
    _config_section = "tabix"
    _config_subsection = "InputVcfFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.VcfFile")
    target_suffix = luigi.Parameter(default=".vcf")

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
    target_suffix = luigi.Parameter(default=".gz")
    source_suffix = luigi.Parameter(default="")

    def args(self):
        return [self.input()]

# Since this is such a common operation, add the task here
class BgUnzip(TabixJobTask):
    _config_subsection = "BgUnzip"
    sub_executable = luigi.Parameter(default="bgzip")
    parent_task = luigi.Parameter(default="ratatosk.lib.variation.tabix.InputVcfFile")
    target_suffix = luigi.Parameter(default=".vcf")
    source_suffix = luigi.Parameter(default=".vcf.gz")

    def opts(self):
        retval = list(self.options)
        if not "-d" in retval:
            retval += ["-d"]
        return retval

    def args(self):
        return [self.input()]

    def post_run_hook(self):
        pass
        # #luigi.build(TabixTabixJobTask(target=rreplace(self.target, TabixTabixJobTask.source_suffix, TabixTabixJobTask.target_suffix, 1))
        # print "running post_run_hook"
        # print TabixTabixJobTask(target=rreplace(self.target, TabixTabixJobTask.source_suffix, TabixTabixJobTask.target_suffix, 1))

class Tabix(TabixJobTask):
    _config_subsection = "Tabix"
    sub_executable = luigi.Parameter(default="tabix")
    parent_task = luigi.Parameter(default="ratatosk.lib.variation.tabix.Bgzip")
    target_suffix = luigi.Parameter(default=".gz.tbi")
    source_suffix = luigi.Parameter(default=".gz")

    def args(self):
        return [self.input()]
    
