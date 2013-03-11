import os
import luigi
import time
import shutil
from ratatosk.job import JobTask, DefaultShellJobRunner
from cement.utils import shell

class BwaJobRunner(DefaultShellJobRunner):
    pass

class InputFastqFile(JobTask):
    _config_section = "bwa"
    _config_subsection = "input_fastq_file"
    fastq = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.FastqFile")
    
    def requires(self):
        cls = self.set_parent_task()
        return cls(fastq=self.fastq)
    def output(self):
        return luigi.LocalTarget(os.path.abspath(self.input().fn))
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
    fastq = luigi.Parameter(default=None)
    options = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.bwa.InputFastqFile")
    can_multi_thread = True

    def main(self):
        return "aln"
    
    def opts(self):
        return '-t {} {}'.format(str(self.threads()), self.options if self.options else "")

    def requires(self):
        cls = self.set_parent_task()
        return cls(fastq=self.fastq)
    
    def output(self):
        return luigi.LocalTarget(os.path.abspath(self.input().fn).replace(".gz", "").replace(".fastq", ".sai"))

    def args(self):
        # bwa aln "-f" option seems to be broken!?!
        return [self.bwaref, self.input(), ">", self.output()]

class BwaAlnWrapperTask(luigi.WrapperTask):
    fastqfiles = luigi.Parameter(default=[], is_list=True)
    def requires(self):
        return [BwaAln(fastq=x) for x in self.fastqfiles]

class BwaSampe(BwaJobTask):
    _config_subsection = "sampe"
    sai1 = luigi.Parameter(default=None)
    sai2 = luigi.Parameter(default=None)
    # Get these with static methods
    read1_suffix = luigi.Parameter(default="_R1_001")
    read2_suffix = luigi.Parameter(default="_R2_001")
    read_group = luigi.Parameter(default=None)
    can_multi_thread = False
    max_memory_gb = 5 # bwa documentation says ~5.4 for human genome

    def main(self):
        return "sampe"

    def requires(self):
        return [BwaAln(fastq=self.sai1.replace(".sai", ".fastq.gz")),
                BwaAln(fastq=self.sai2.replace(".sai", ".fastq.gz"))]

    def output(self):
        return luigi.LocalTarget(os.path.abspath(self.sai1).replace(self.read1_suffix, "").replace(".sai", ".sam"))

    def args(self):
        sai1 = self.input()[0]
        sai2 = self.input()[1]
        fastq1 = luigi.LocalTarget(sai1.fn.replace(".sai", ".fastq.gz"))
        fastq2 = luigi.LocalTarget(sai2.fn.replace(".sai", ".fastq.gz"))
        if not self.read_group:
            foo = sai1.fn.replace(".sai", "")
            # The platform should be configured elsewhere
            self.read_group = "-r \"{}\"".format("\t".join(["@RG", "ID:{}".format(foo), "SM:{}".format(foo), "PL:Illumina"]))
        return [self.read_group, self.bwaref, sai1, sai2, fastq1, fastq2, ">", self.output()]

    
