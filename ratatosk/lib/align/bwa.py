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
from ratatosk.job import InputJobTask, JobTask, JobWrapperTask, DefaultShellJobRunner
from ratatosk.utils import rreplace, fullclassname
from cement.utils import shell


class BwaJobRunner(DefaultShellJobRunner):
    pass

class InputFastqFile(InputJobTask):
    _config_section = "bwa"
    _config_subsection = "InputFastqFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.FastqFile")
    target_suffix = luigi.Parameter(default=".fastq")

class InputFastaFile(InputJobTask):
    _config_section = "bwa"
    _config_subsection = "InputFastaFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.FastaFile")
    target_suffix = luigi.Parameter(default=".fa")

class BwaJobTask(JobTask):
    """Main bwa class with parameters necessary for all bwa classes"""
    _config_section = "bwa"
    executable = luigi.Parameter(default="bwa")
    bwaref = luigi.Parameter(default=None)
    num_threads = luigi.Parameter(default=1)

    def job_runner(self):
        return BwaJobRunner()

    def output(self):
        return luigi.LocalTarget(self.target)


class BwaAln(BwaJobTask):
    _config_subsection = "aln"
    sub_executable = "aln"
    parent_task = luigi.Parameter(default="ratatosk.lib.align.bwa.InputFastqFile")
    target_suffix = luigi.Parameter(default=".sai")
    source_suffix = luigi.Parameter(default=".fastq.gz")
    read1_suffix = luigi.Parameter(default="_R1_001")
    read2_suffix = luigi.Parameter(default="_R2_001")
    is_read1 = True
    can_multi_thread = True

    def opts(self):
        # Threads is an option so handle it here
        retval = list(self.options)
        return retval + ['-t {}'.format(str(self.threads()))]

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        # Ugly hack for 1 -> 2 dependency: works but should be dealt with otherwise
        if str(fullclassname(cls)) in ["ratatosk.lib.utils.misc.ResyncMatesJobTask"]:
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

    def args(self):
        # bwa aln "-f" option seems to be broken!?!
        if isinstance(self.input(), list):
            if self.is_read1:
                return [self.bwaref, self.input()[0], ">", self.output()]
            else:
                return [self.bwaref, self.input()[1], ">", self.output()]
        else:
            return [self.bwaref, self.input(), ">", self.output()]

class BwaAlnWrapperTask(JobWrapperTask):
    fastqfiles = luigi.Parameter(default=[], is_list=True)
    def requires(self):
        return [BwaAln(target=x) for x in self.fastqfiles]

class BwaSampe(BwaJobTask):
    _config_subsection = "sampe"
    sub_executable = "sampe"
    # Get these with static methods
    read1_suffix = luigi.Parameter(default="_R1_001")
    read2_suffix = luigi.Parameter(default="_R2_001")
    source_suffix = luigi.Parameter(default=".sai")
    target_suffix = luigi.Parameter(default=".sam")
    read_group = luigi.Parameter(default=None)
    platform = luigi.Parameter(default="Illumina")
    can_multi_thread = False
    max_memory_gb = 6 # bwa documentation says ~5.4 for human genome

    def requires(self):
        # From target name, generate sai1, sai2, fastq1, fastq2
        sai1 = rreplace(self._make_source_file_name(), self.source_suffix, self.read1_suffix + self.source_suffix, 1)
        sai2 = rreplace(self._make_source_file_name(), self.source_suffix, self.read2_suffix + self.source_suffix, 1)
        return [BwaAln(target=sai1), BwaAln(target=sai2)]

    def _get_read_group(self):
        if not self.read_group:
            sai1 = self.input()[0]
            rgid = sai1.fn.replace(".sai", "")
            smid = rgid
            # Get sample information if present. Note that this
            # requires the
            # backend.__handlers__["target_generator_handler"] be set
            for tgt in self.target_iterator():
                if smid.startswith(tgt[2]):
                    smid = tgt[0]
                    break
            # The platform should be configured elsewhere
            return "-r \"{}\"".format("\t".join(["@RG", "ID:{}".format(rgid), "SM:{}".format(smid), "PL:{}".format(self.platform)]))
        else:
            return self.read_group

    def args(self):
        sai1 = self.input()[0]
        sai2 = self.input()[1]
        fastq1 = luigi.LocalTarget(rreplace(sai1.fn, self.source_suffix, ".fastq.gz", 1))
        fastq2 = luigi.LocalTarget(rreplace(sai2.fn, self.source_suffix, ".fastq.gz", 1))
        return [self._get_read_group(), self.bwaref, sai1, sai2, fastq1, fastq2, ">", self.output()]

    
class BwaIndex(BwaJobTask):
    _config_subsection = "index"
    sub_executable = "index"
    source_suffix = luigi.Parameter(default=".fa")
    target_suffix = luigi.Parameter(default=".fa.bwt")
    parent_task = luigi.Parameter(default="ratatosk.lib.align.bwa.InputFastaFile")
    
    def args(self):
        return [self.input()]
