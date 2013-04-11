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
Provide wrappers for  `samtools <http://samtools.sourceforge.net/>`_


Classes
-------
"""
import os
import luigi
import logging
import ratatosk.lib.files.external
from ratatosk.job import InputJobTask, JobTask
from ratatosk.utils import rreplace
from ratatosk.jobrunner import DefaultShellJobRunner

logger = logging.getLogger('luigi-interface')

class SamtoolsJobRunner(DefaultShellJobRunner):
    pass

class InputSamFile(InputJobTask):
    """Wrapper task that serves as entry point for samtools tasks that take sam file as input"""
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.SamFile")
    suffix = luigi.Parameter(default=".sam")

class InputBamFile(InputJobTask):
    """Wrapper task that serves as entry point for samtools tasks that take bam file as input"""
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.BamFile")
    suffix = luigi.Parameter(default=".bam")
    
class SamtoolsJobTask(JobTask):
    """Main samtools job task"""
    executable = luigi.Parameter(default="samtools")
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.samtools.InputSamFile", ), is_list=True)
    suffix = luigi.Parameter(default=".bam")

    def job_runner(self):
        return SamtoolsJobRunner()

class SamToBam(SamtoolsJobTask):
    sub_executable = "view"
    options = luigi.Parameter(default=("-bSh",), is_list=True)
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.samtools.InputSamFile", ), is_list=True)
    suffix = luigi.Parameter(default=".bam")

    def args(self):
        retval = [self.input()[0], ">", self.output()]
        if self.pipe:
            return retval + ["-"]
        return retval

class SortBam(SamtoolsJobTask):
    sub_executable = "sort"
    suffix = luigi.Parameter(default=".bam")
    label = luigi.Parameter(default=".sort")
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.samtools.SamToBam", ), is_list=True)

    def add_suffix(self):
        """samtools sort generates its output based on a prefix, hence
        we need to add a suffix here"""
        return self.suffix

    def args(self):
        output_prefix = luigi.LocalTarget(rreplace(self.output().path, self.suffix, "", 1))
        return [self.input()[0], output_prefix]

class Index(SamtoolsJobTask):
    sub_executable = "index"
    suffix = luigi.Parameter(default=".bai")
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.samtools.InputBamFile")

    def args(self):
        return [self.input()[0], self.output()]

# "Connection" tasks
# import ratatosk.lib.align.bwa 
# class SampeToSamtools(SamToBam):
#     def requires(self):
#         source = self._make_source_file_name()
#         return ratatosk.lib.align.bwa.BwaSampe(target=source)
