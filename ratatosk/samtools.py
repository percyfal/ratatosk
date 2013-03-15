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
import ratatosk.external
import ratatosk.bwa
from ratatosk.job import JobTask, DefaultShellJobRunner

logger = logging.getLogger('luigi-interface')

class SamtoolsJobRunner(DefaultShellJobRunner):
    pass

class InputSamFile(JobTask):
    """Wrapper task that serves as entry point for samtools tasks that take sam file as input"""
    _config_section = "samtools"
    _config_subsection = "InputSamFile"
    target = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.SamFile")

    def requires(self):
        cls = self.set_parent_task()
        return cls(target=self.target)
    def output(self):
        return luigi.LocalTarget(self.target)

class InputBamFile(JobTask):
    """Wrapper task that serves as entry point for samtools tasks that take bam file as input"""
    _config_section = "samtools"
    _config_subsection = "InputBamFile"
    target = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.BamFile")

    def requires(self):
        cls = self.set_parent_task()
        return cls(target=self.target)
    def output(self):
        return luigi.LocalTarget(self.target)
    
class SamtoolsJobTask(JobTask):
    """Main samtools job task"""
    _config_section = "samtools"
    target = luigi.Parameter(default=None)
    samtools = luigi.Parameter(default="samtools")
    parent_task = luigi.Parameter(default="ratatosk.samtools.InputSamFile")
    target_suffix = luigi.Parameter(default=".bam")
    source_suffix = luigi.Parameter(default=".bam")

    def exe(self):
        """Executable"""
        return self.samtools

    def job_runner(self):
        return SamtoolsJobRunner()

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()

class SamToBam(SamtoolsJobTask):
    _config_subsection = "SamToBam"
    options = luigi.Parameter(default="-bSh")
    parent_task = luigi.Parameter(default="ratatosk.samtools.InputSamFile")
    target_suffix = luigi.Parameter(default=".bam")
    source_suffix = luigi.Parameter(default=".sam")

    def main(self):
        return "view"

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)

    def output(self):
        return luigi.LocalTarget(self.target)

    def args(self):
        return [self.input(), ">", self.output()]

class SortBam(SamtoolsJobTask):
    _config_subsection = "sortbam"
    target = luigi.Parameter(default=None)
    target_suffix = luigi.Parameter(default=".bam")
    source_suffix = luigi.Parameter(default=".bam")
    label = luigi.Parameter(default=".sort")
    options = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.samtools.SamToBam")
    source = None

    def requires(self):
        cls = self.set_parent_task()
        self.source = self._make_source_file_name()
        return SamToBam(target=self.source)

    def output(self):
        return luigi.LocalTarget(self.target)

    def main(self):
        return "sort"

    def add_suffix(self):
        return ".bam"

    def args(self):
        output_prefix = luigi.LocalTarget(self.output().fn.replace(".bam", ""))
        return [self.source, output_prefix]

class IndexBam(SamtoolsJobTask):
    _config_subsection = "indexbam"
    target = luigi.Parameter(default=None)
    target_suffix = luigi.Parameter(default=".bai")
    source_suffix = luigi.Parameter(default=".bam")
    options = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.samtools.InputBamFile")
    source = None
    def requires(self):
        cls = self.set_parent_task()
        self.source = self._make_source_file_name()
        return cls(target=self.source)

    def output(self):
        return luigi.LocalTarget(self.target)

    def main(self):
        return "index"

    def args(self):
        return [self.source, self.output()]

# "Connection" tasks
import ratatosk.bwa as bwa
class SampeToSamtools(SamToBam):
    def requires(self):
        source = self._make_source_file_name()
        return bwa.BwaSampe(target=source)
