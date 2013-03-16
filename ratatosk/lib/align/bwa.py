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
import re
import luigi
import time
import shutil
from ratatosk.job import JobTask, DefaultShellJobRunner
from ratatosk.utils import rreplace, fullclassname
from cement.utils import shell


class BwaJobRunner(DefaultShellJobRunner):
    pass

class InputFastqFile(JobTask):
    _config_section = "bwa"
    _config_subsection = "InputFastqFile"
    target = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.FastqFile")
    
    def requires(self):
        cls = self.set_parent_task()
        return cls(target=self.target)
    def output(self):
        return luigi.LocalTarget(self.target)
    def run(self):
        pass

class BwaJobTask(JobTask):
    """Main bwa class with parameters necessary for all bwa classes"""
    _config_section = "bwa"
    bwa = luigi.Parameter(default="bwa")
    bwaref = luigi.Parameter(default=None)
    num_threads = luigi.Parameter(default=1)

    def exe(self):
        """Executable of this task"""
        return self.bwa

    def job_runner(self):
        return BwaJobRunner()


class BwaAln(BwaJobTask):
    _config_subsection = "aln"
    options = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.lib.align.bwa.InputFastqFile")
    target_suffix = luigi.Parameter(default=".sai")
    source_suffix = luigi.Parameter(default=".fastq.gz")
    read1_suffix = luigi.Parameter(default="_R1_001")
    read2_suffix = luigi.Parameter(default="_R2_001")
    is_read1 = True
    can_multi_thread = True

    def main(self):
        return "aln"
    
    def opts(self):
        return '-t {} {}'.format(str(self.threads()), self.options if self.options else "")

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        # Ugly hack for 1 -> 2 dependency: works but should be dealt with otherwise
        if str(fullclassname(cls)) in ["ratatosk.misc.ResyncMatesJobTask"]:
            if re.search(self.read1_suffix, source):
                self.is_read1 = True
                fq1 = source
                fq2 = rreplace(source, self.read1_suffix, self.read2_suffix, 1)
            else:
                self.is_read1 = False
                fq1 = rreplace(source, self.read2_suffix, self.read1_suffix, 1)
                fq2 = source
            return cls(target=[fq1, fq2])
        else:
            return cls(target=source)
    
    def output(self):
        return luigi.LocalTarget(self.target)

    def args(self):
        # bwa aln "-f" option seems to be broken!?!
        if len(self.input()) > 1:
            if self.is_read1:
                return [self.bwaref, self.input()[0], ">", self.output()]
            else:
                return [self.bwaref, self.input()[1], ">", self.output()]
        else:
            return [self.bwaref, self.input(), ">", self.output()]

class BwaAlnWrapperTask(luigi.WrapperTask):
    fastqfiles = luigi.Parameter(default=[], is_list=True)
    def requires(self):
        return [BwaAln(target=x) for x in self.fastqfiles]

class BwaSampe(BwaJobTask):
    _config_subsection = "sampe"
    # Get these with static methods
    read1_suffix = luigi.Parameter(default="_R1_001")
    read2_suffix = luigi.Parameter(default="_R2_001")
    source_suffix = luigi.Parameter(default=".sai")
    target_suffix = luigi.Parameter(default=".sam")
    read_group = luigi.Parameter(default=None)
    can_multi_thread = False
    max_memory_gb = 5 # bwa documentation says ~5.4 for human genome

    def main(self):
        return "sampe"

    def requires(self):
        # From target name, generate sai1, sai2, fastq1, fastq2
        sai1 = rreplace(self._make_source_file_name(), self.source_suffix, self.read1_suffix + self.source_suffix, 1)
        sai2 = rreplace(self._make_source_file_name(), self.source_suffix, self.read2_suffix + self.source_suffix, 1)
        return [BwaAln(target=sai1), BwaAln(target=sai2)]

    def output(self):
        return luigi.LocalTarget(self.target)

    def args(self):
        sai1 = self.input()[0]
        sai2 = self.input()[1]
        fastq1 = luigi.LocalTarget(rreplace(sai1.fn, self.source_suffix, ".fastq.gz", 1))
        fastq2 = luigi.LocalTarget(rreplace(sai2.fn, self.source_suffix, ".fastq.gz", 1))
        if not self.read_group:
            foo = sai1.fn.replace(".sai", "")
            # The platform should be configured elsewhere
            self.read_group = "-r \"{}\"".format("\t".join(["@RG", "ID:{}".format(foo), "SM:{}".format(foo), "PL:Illumina"]))
        return [self.read_group, self.bwaref, sai1, sai2, fastq1, fastq2, ">", self.output()]

    
