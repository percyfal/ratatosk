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
from ratatosk.job import JobTask, DefaultShellJobRunner
from cement.utils import shell

class CutadaptJobRunner(DefaultShellJobRunner):
    pass

class InputFastqFile(JobTask):
    _config_section = "cutadapt"
    _config_subsection = "input_fastq_file"
    target = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.FastqFile")
    
    def requires(self):
        cls = self.set_parent_task()
        return cls(target=self.target)
    def output(self):
        return luigi.LocalTarget(self.target)
    def run(self):
        pass

# NB: cutadapt is a non-hiearchical tool. Group under, say, utils?
class CutadaptJobTask(JobTask):
    _config_section = "cutadapt"
    options = luigi.Parameter(default=None)
    label = luigi.Parameter(default=".trimmed")
    cutadapt = luigi.Parameter(default="cutadapt")
    parent_task = luigi.Parameter(default="ratatosk.cutadapt.InputFastqFile")
    # Use Illumina TruSeq adapter sequences as default
    threeprime = luigi.Parameter(default="AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC")
    fiveprime = luigi.Parameter(default="AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC")
    read1_suffix = luigi.Parameter(default="_R1_001")
    read2_suffix = luigi.Parameter(default="_R2_001")

    def read1(self):
        # Assume read 2 if no match...
        return self.input().fn.find(self.read1_suffix) > 0

    def exe(self):
        return self.cutadapt

    def job_runner(self):
        return CutadaptJobRunner()

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)

    def output(self):
        return luigi.LocalTarget(self.target)

    def args(self):
        seq = self.threeprime if self.read1() else self.fiveprime
        return ["-a", seq, self.input(), "-o", self.output(), ">", self.input().fn.replace(".fastq.gz", ".trimmed.fastq.cutadapt_metrics")]
