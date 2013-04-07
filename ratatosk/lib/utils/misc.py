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
import time
import shutil
import logging
from itertools import izip
from ratatosk.job import InputJobTask, JobTask
from ratatosk.jobrunner import DefaultShellJobRunner, DefaultGzShellJobRunner
from ratatosk.utils import rreplace

logger = logging.getLogger('luigi-interface')

class MiscJobRunner(DefaultShellJobRunner):
        pass

class ResyncMatesJobRunner(DefaultGzShellJobRunner):
        pass

class InputFastqFile(InputJobTask):
    _config_section = "misc"
    _config_subsection = "InputFastqFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.FastqFile")
    suffix = luigi.Parameter(default=".fastq.gz")

class ResyncMatesJobTask(JobTask):
    _config_section = "misc"
    _config_subsection = "ResyncMates"
    executable = luigi.Parameter(default="resyncMates.pl")
    label = luigi.Parameter(default=".sync")
    target = luigi.Parameter(default=(), is_list=True)
    parent_task = luigi.Parameter(default=("ratatosk.lib.utils.misc.InputFastqFile", "ratatosk.lib.utils.misc.InputFastqFile", ), is_list=True)
    suffix = luigi.Parameter(default=(".fastq.gz", ".fastq.gz", ), is_list=True)

    def job_runner(self):
		return ResyncMatesJobRunner()

    def output(self):
        return [luigi.LocalTarget(self.target[0]), luigi.LocalTarget(self.target[1])]

    def args(self):
        return ["-i", self.input()[0], "-j", self.input()[1], "-o", self.output()[0], "-p", self.output()[1]]
