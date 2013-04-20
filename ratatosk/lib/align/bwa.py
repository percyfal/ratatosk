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
Wrapper library for `bwa <http://bio-bwa.sourceforge.net/>`_.


Classes
--------
"""
import re
import luigi
from itertools import izip
import ratatosk.lib.files.input
from ratatosk.job import JobTask, JobWrapperTask, DefaultShellJobRunner, PipedTask
from ratatosk.lib.tools.samtools import SamToBam
from ratatosk.utils import rreplace, fullclassname

class InputFastqFile(ratatosk.lib.files.input.InputFastqFile):
    pass

class InputFastaFile(ratatosk.lib.files.input.InputFastaFile):
    suffix = luigi.Parameter(default=".fa")

class BwaJobRunner(DefaultShellJobRunner):
    pass

class BwaJobTask(JobTask):
    """Main bwa class with parameters necessary for all bwa classes"""
    executable = luigi.Parameter(default="bwa")
    bwaref = luigi.Parameter(default=None)
    num_threads = luigi.Parameter(default=1)

    def job_runner(self):
        return BwaJobRunner()

    def output(self):
        return luigi.LocalTarget(self.target)


class Aln(BwaJobTask):
    sub_executable = "aln"
    parent_task = luigi.Parameter(default=("ratatosk.lib.align.bwa.InputFastqFile",))
    suffix = luigi.Parameter(default=".sai")
    read1_suffix = luigi.Parameter(default="_R1_001")
    read2_suffix = luigi.Parameter(default="_R2_001")
    is_read1 = True
    can_multi_thread = True

    def opts(self):
        # Threads is an option so handle it here
        retval = list(self.options)
        return retval + ['-t {}'.format(str(self.threads()))]

    def requires(self):
        cls = self.parent()[0]
        source = self.source()[0]
        # Ugly hack for 1 -> 2 dependency: works but should be dealt with otherwise
        if str(fullclassname(cls)) in ["ratatosk.lib.utils.misc.ResyncMates"]:
            if re.search(self.read1_suffix, source):
                self.is_read1 = True
                fq1 = source
                fq2 = rreplace(source, self.read1_suffix, self.read2_suffix, 1)
            else:
                self.is_read1 = False
                fq1 = rreplace(source, self.read2_suffix, self.read1_suffix, 1)
                fq2 = source
            retval = [cls(target=[fq1, fq2])]
        else:
            retval = [cls(target=source)]
        if len(self.parent()) > 1:
            retval += [cls(target=source) for cls, source in izip(self.parent()[1:], self.source()[1:])]
        return retval

    def args(self):
        # bwa aln "-f" option seems to be broken!?!
        if isinstance(self.input()[0], list):
            if self.is_read1:
                return [self.bwaref, self.input()[0][0], ">", self.output()]
            else:
                return [self.bwaref, self.input()[0][1], ">", self.output()]
        else:
            return [self.bwaref, self.input()[0], ">", self.output()]

class BwaAlnWrapperTask(JobWrapperTask):
    fastqfiles = luigi.Parameter(default=[], is_list=True)
    def requires(self):
        return [Aln(target=x) for x in self.fastqfiles]

class Sampe(BwaJobTask):
    sub_executable = "sampe"
    add_label = luigi.Parameter(default=("_R1_001", "_R2_001"), is_list=True)
    suffix = luigi.Parameter(default=".sam")
    read_group = luigi.Parameter(default=None)
    platform = luigi.Parameter(default="Illumina")
    parent_task = luigi.Parameter(default=("ratatosk.lib.align.bwa.Aln", "ratatosk.lib.align.bwa.Aln"), is_list=True)
    can_multi_thread = False
    max_memory_gb = 6 # bwa documentation says ~5.4 for human genome

    def _get_read_group(self):
        if not self.read_group:
            from ratatosk import backend
            cls = self.parent()[0]
            sai1 = self.input()[0]
            rgid = rreplace(rreplace(sai1.path, cls().sfx(), "", 1), self.add_label[0], "", 1)
            smid = rgid
            # Get sample information if present in global vars. Note
            # that this requires the
            # backend.__global_vars__["targets"] be set
            # This is not perfect but works for now
            for tgt in backend.__global_vars__.get("targets", []):
                if smid.startswith(tgt[2]):
                    smid = tgt[0]
                    break
            # The platform should be configured elsewhere
            rg = "\"{}\"".format("\t".join(["@RG", "ID:{}".format(rgid), "SM:{}".format(smid), "PL:{}".format(self.platform)]))
            if self.pipe:
                return rg.replace("\t", "\\t")
            else:
                return rg
        else:
            return self.read_group

    def args(self):
        cls = self.parent()[0]
        parent_cls = cls().parent()[0]
        (fastq1, fastq2) = [luigi.LocalTarget(rreplace(sai.path, cls().suffix, parent_cls().sfx(), 1)) for sai in self.input()]
        return ["-r", self._get_read_group(), self.bwaref, self.input()[0].path, self.input()[1].path, fastq1, fastq2, ">", self.output()]

class Bampe(PipedTask):
    add_label = luigi.Parameter(default=("_R1_001", "_R2_001"), is_list=True)
    parent_task = luigi.Parameter(default=("ratatosk.lib.align.bwa.Aln", "ratatosk.lib.align.bwa.Aln"), is_list=True)
    suffix = luigi.Parameter(default=".bam")
    read_group = luigi.Parameter(default=None)
    platform = luigi.Parameter(default="Illumina")
    can_multi_thread = False
    max_memory_gb = 6 # bwa documentation says ~5.4 for human genome

    def args(self):
        return [Sampe(target=self.target.replace(".bam", ".sam"), pipe=True), SamToBam(target=self.target, pipe=True)]

class Index(BwaJobTask):
    sub_executable = "index"
    suffix = luigi.Parameter(default=".fa.bwt")
    parent_task = luigi.Parameter(default=("ratatosk.lib.align.bwa.InputFastaFile", ), is_list=True)
    
    def args(self):
        return [self.input()[0]]
