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
Provide miscallaneous wrappers.


Classes
-------
"""

import os
import luigi
import time
import shutil
from itertools import izip
import ratatosk.lib.files.input
from ratatosk.job import JobTask
from ratatosk.jobrunner import DefaultShellJobRunner, DefaultGzShellJobRunner
from ratatosk.utils import rreplace
from ratatosk.log import get_logger

logger = get_logger()

class InputFastqFile(ratatosk.lib.files.input.InputFastqFile):
    pass

class MiscJobRunner(DefaultShellJobRunner):
    pass

class ResyncMatesJobRunner(DefaultGzShellJobRunner):
    pass

class ResyncMates(JobTask):
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
