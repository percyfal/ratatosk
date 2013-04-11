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
Provide wrappers for `cutadapt <http://code.google.com/p/cutadapt/>`_ 

Install with `pip install cutadapt`


Classes
-------
"""

import os
import luigi
import time
import shutil
import random
import logging
from ratatosk.job import InputJobTask, JobTask
from ratatosk.jobrunner import DefaultShellJobRunner, DefaultGzShellJobRunner
from ratatosk.utils import rreplace

logger = logging.getLogger('luigi-interface')

# NB: Now subclass DefaultGzShellJobRunner
class CutadaptJobRunner(DefaultGzShellJobRunner):
    pass

class InputFastqFile(InputJobTask):
    _config_section = "cutadapt"
    _config_subsection = "InputFastqFile"
    parent_task = luigi.Parameter(default=("ratatosk.lib.files.external.FastqFile", ), is_list=True)
    suffix = luigi.Parameter(default=(".fastq.gz", ), is_list=True)

# NB: cutadapt is a non-hiearchical tool. Group under, say, utils?
class CutadaptJobTask(JobTask):
    _config_section = "cutadapt"
    _config_subsection = "Cutadapt"
    label = luigi.Parameter(default=".trimmed")
    executable = luigi.Parameter(default="cutadapt")
    parent_task = luigi.Parameter(default=("ratatosk.lib.utils.cutadapt.InputFastqFile", ),is_list=True)
    # Use Illumina TruSeq adapter sequences as default
    threeprime = luigi.Parameter(default="AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC")
    fiveprime = luigi.Parameter(default="AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC")
    read1_suffix = luigi.Parameter(default="_R1_001")
    read2_suffix = luigi.Parameter(default="_R2_001")
    suffix = luigi.Parameter(default=(".fastq.gz", ".fastq.cutadapt_metrics"), is_list=True)

    def read1(self):
        # Assume read 2 if no match...
        return self.input()[0].path.find(self.read1_suffix) > 0

    def job_runner(self):
        return CutadaptJobRunner()

    def args(self):
        cls = self.parent()[0]
        seq = self.threeprime if self.read1() else self.fiveprime
        return ["-a", seq, self.input()[0], "-o", self.output(), ">", rreplace(self.input()[0].path, str(cls().suffix[0]), self.label + self.suffix[1], 1)]
