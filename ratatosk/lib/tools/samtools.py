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

logger = logging.getLogger('luigi-interface')

class SamtoolsJobRunner(DefaultShellJobRunner):
    pass

class InputSamFile(InputJobTask):
    """Wrapper task that serves as entry point for samtools tasks that take sam file as input"""
    _config_section = "samtools"
    _config_subsection = "InputSamFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.SamFile")

class InputBamFile(InputJobTask):
    """Wrapper task that serves as entry point for samtools tasks that take bam file as input"""
    _config_section = "samtools"
    _config_subsection = "InputBamFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.BamFile")
    
class SamtoolsJobTask(JobTask):
    """Main samtools job task"""
    _config_section = "samtools"
    executable = luigi.Parameter(default="samtools")
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.samtools.InputSamFile")
    target_suffix = luigi.Parameter(default=".bam")
    source_suffix = luigi.Parameter(default=".bam")

    def job_runner(self):
        return SamtoolsJobRunner()

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)

class SamToBam(SamtoolsJobTask):
    _config_subsection = "SamToBam"
    sub_executable = "view"
    options = luigi.Parameter(default=("-bSh",), is_list=True)
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.samtools.InputSamFile")
    target_suffix = luigi.Parameter(default=".bam")
    source_suffix = luigi.Parameter(default=".sam")

    def args(self):
        retval = [self.input(), ">", self.output()]
        if self.pipe:
            return retval + ["-"]
        return retval

class SortBam(SamtoolsJobTask):
    _config_subsection = "sortbam"
    sub_executable = "sort"
    target_suffix = luigi.Parameter(default=".bam")
    source_suffix = luigi.Parameter(default=".bam")
    label = luigi.Parameter(default=".sort")
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.samtools.SamToBam")

    def add_suffix(self):
        """samtools sort generates its output based on a prefix, hence
        we need to add a suffix here"""
        return self.target_suffix

    def args(self):
        output_prefix = luigi.LocalTarget(self.output().fn.replace(".bam", ""))
        return [self.input(), output_prefix]

class IndexBam(SamtoolsJobTask):
    _config_subsection = "indexbam"
    sub_executable = "index"
    target_suffix = luigi.Parameter(default=".bai")
    source_suffix = luigi.Parameter(default=".bam")
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.samtools.InputBamFile")

    def args(self):
        return [self.input(), self.output()]

# "Connection" tasks
import ratatosk.lib.align.bwa 
class SampeToSamtools(SamToBam):
    def requires(self):
        source = self._make_source_file_name()
        return ratatosk.lib.align.bwa.BwaSampe(target=source)
