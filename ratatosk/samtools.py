import os
import luigi
import logging
import ratatosk.external
import ratatosk.bwa
from ratatosk.job import JobTask, DefaultShellJobRunner

logger = logging.getLogger('luigi-interface')

class SamtoolsJobRunner(DefaultShellJobRunner):
    pass

class InputSamFile(JobTask):
    """Wrapper task that serves as entry point for samtools tasks that take sam file as input"""
    _config_section = "samtools"
    _config_subsection = "input_sam_file"
    sam = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.SamFile")

    def requires(self):
        cls = self.set_parent_task()
        return cls(sam=self.sam)
    def output(self):
        return luigi.LocalTarget(self.sam)

class InputBamFile(JobTask):
    """Wrapper task that serves as entry point for samtools tasks that take bam file as input"""
    _config_section = "samtools"
    _config_subsection = "input_bam_file"
    bam = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.BamFile")

    def requires(self):
        cls = self.set_parent_task()
        return cls(bam=self.bam)
    def output(self):
        return luigi.LocalTarget(self.bam)
    
class SamtoolsJobTask(JobTask):
    """Main samtools job task"""
    _config_section = "samtools"
    sam = luigi.Parameter(default=None)
    bam = luigi.Parameter(default=None)
    samtools = luigi.Parameter(default="samtools")
    parent_task = luigi.Parameter(default="ratatosk.samtools.InputSamFile")

    #    def __init__(self, *args, **kwargs):
    #         super(SamtoolsJobTask, self).__init__(*args, **kwargs)
    # if self.bam and not self.sam:
    #     self.sam = self.bam.replace(".bam", ".sam")
    # if self.sam and not self.bam:
    #     self.bam = self.bam.replace(".sam", ".sam")

    def exe(self):
        """Executable"""
        return self.samtools

    def job_runner(self):
        return SamtoolsJobRunner()

    def requires(self):
        cls = self.set_parent_task()
        if self.sam and not self.bam:
            return cls(sam=self.sam)
        elif self.bam and not self.sam:
            return cls(bam=self.bam)
        else:
            logger.warn("Both sam file ('{0}') and bam file ('{1}') options set: using sam file argument".format(self.sam, self.bam))
            return cls(sam=self.sam)

class SamToBam(SamtoolsJobTask):
    _config_subsection = "samtobam"
    options = luigi.Parameter(default="-bSh")
    parent_task = luigi.Parameter(default="ratatosk.samtools.InputSamFile")
    sam = luigi.Parameter(default=None)

    def main(self):
        return "view"

    def requires(self):
        cls = self.set_parent_task()
        return cls(sam=self.sam)

    def output(self):
        if self.bam:
            self.sam = self.bam.replace(".bam", ".sam")
        return luigi.LocalTarget(os.path.abspath(self.sam).replace(".sam", ".bam"))

    def args(self):
        return [self.sam, ">", self.output()]

class SortBam(SamtoolsJobTask):
    _config_subsection = "sortbam"
    bam = luigi.Parameter(default=None)
    options = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.samtools.SamToBam")

    def requires(self):
        cls = self.set_parent_task()
        return SamToBam(sam=os.path.abspath(self.bam).replace(".bam", ".sam"))

    def output(self):
        return luigi.LocalTarget(os.path.abspath(self.bam).replace(".bam", ".sort.bam"))

    def main(self):
        return "sort"

    def add_suffix(self):
        return ".bam"

    def args(self):
        output_prefix = luigi.LocalTarget(self.output().fn.replace(".bam", ""))
        return [self.bam, output_prefix]

class IndexBam(SamtoolsJobTask):
    _config_subsection = "indexbam"
    bam = luigi.Parameter(default=None)
    options = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.samtools.InputBamFile")

    def requires(self):
        cls = self.set_parent_task()
        return cls(bam=self.bam)

    def output(self):
        return luigi.LocalTarget(os.path.abspath(self.bam).replace(".bam", ".bam.bai"))

    def main(self):
        return "index"

    def args(self):
        return [self.bam, self.output()]

# "Connection" tasks
import ratatosk.bwa as bwa
class SampeToSamtools(SamToBam):
    def requires(self):
        return bwa.BwaSampe(sai1=os.path.join(self.sam.replace(".sam", bwa.BwaSampe().read1_suffix + ".sai")),
                            sai2=os.path.join(self.sam.replace(".sam", bwa.BwaSampe().read2_suffix + ".sai")))
