import os
import luigi
import time
import shutil
from pm.luigi.job import JobTask, DefaultShellJobRunner
from cement.utils import shell

class CutadaptJobRunner(DefaultShellJobRunner):
    pass

class InputFastqFile(JobTask):
    _config_section = "cutadapt"
    _config_subsection = "input_fastq_file"
    fastq = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="pm.luigi.external.FastqFile")
    
    def requires(self):
        cls = self.set_parent_task()
        return cls(fastq=self.fastq)
    def output(self):
        return luigi.LocalTarget(os.path.abspath(self.input().fn))
    def run(self):
        pass

# NB: cutadapt is a non-hiearchical tool. Group under, say, utils?
class CutadaptJobTask(JobTask):
    _config_section = "cutadapt"
    options = luigi.Parameter(default=None)
    fastq = luigi.Parameter(default=None)
    cutadapt = luigi.Parameter(default="cutadapt")
    parent_task = luigi.Parameter(default="pm.luigi.cutadapt.InputFastqFile")
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
        return cls(fastq=self.fastq)

    def output(self):
        return luigi.LocalTarget(os.path.abspath(self.input().fn).replace(".fastq.gz", ".trimmed.fastq.gz"))

    def args(self):
        seq = self.threeprime if self.read1() else self.fiveprime
        return ["-a", seq, self.input(), "-o", self.output(), ">", self.input().fn.replace(".fastq.gz", ".trimmed.fastq.cutadapt_metrics")]
