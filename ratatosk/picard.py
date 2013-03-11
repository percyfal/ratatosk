import os
import luigi
import logging
import time
import glob
import ratatosk.external
from ratatosk.job import JobTask, DefaultShellJobRunner
from cement.utils import shell

# TODO: make these configurable 
JAVA="java"
JAVA_OPTS="-Xmx2g"
PICARD_HOME=os.getenv("PICARD_HOME")

logger = logging.getLogger('luigi-interface')

class PicardJobRunner(DefaultShellJobRunner):
    path = PICARD_HOME
    def run_job(self, job):
        if not job.jar() or not os.path.exists(os.path.join(self.path,job.jar())):
            logger.error("Can't find jar: {0}, full path {1}".format(job.jar(),
                                                                     os.path.abspath(job.jar())))
            raise Exception("job jar does not exist")
        arglist = ['java', job.java_opt(), '-jar', os.path.join(self.path, job.jar())]
        if job.main():
            arglist.append(job.main())
        if job.opts():
            arglist.append(job.opts())
        (tmp_files, job_args) = DefaultShellJobRunner._fix_paths(job)

        arglist += job_args
        cmd = ' '.join(arglist)        
        logger.info(cmd)
        (stdout, stderr, returncode) = shell.exec_cmd(cmd.replace("= ", "="), shell=True)

        if returncode == 0:
            logger.info("Shell job completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                a.move(os.path.join(os.curdir, b.path))
        else:
            raise Exception("Job '{}' failed: \n{}".format(cmd.replace("= ", "="), " ".join([stderr])))

class InputBamFile(JobTask):
    _config_section = "picard"
    _config_subsection = "input_bam_file"
    bam = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.BamFile")
    def requires(self):
        cls = self.set_parent_task()
        return cls(bam=self.bam)
    def output(self):
        return luigi.LocalTarget(os.path.relpath(self.input().fn))
    def run(self):
        pass

class PicardJobTask(JobTask):
    _config_section = "picard"
    java_options = "-Xmx2g"
    label = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.picard.InputBamFile")

    def jar(self):
        """Path to the jar for this Picard job"""
        return None

    def java_opt(self):
        return self.java_options

    def exe(self):
        return self.jar()

    def job_runner(self):
        return PicardJobRunner()

    def requires(self):
        cls = self.set_parent_task()
        return cls(bam=self.bam)

class SortSam(PicardJobTask):
    _config_subsection = "SortSam"
    bam = luigi.Parameter(default=None)
    options = luigi.Parameter(default="SO=coordinate MAX_RECORDS_IN_RAM=750000")
    label = luigi.Parameter(default=".sort")

    def jar(self):
        return "SortSam.jar"
    def requires(self):
        cls = self.set_parent_task()
        return cls(bam=self.bam.replace("{}.bam".format(self.label), ".bam"))
    def output(self):
        return luigi.LocalTarget(os.path.relpath(self.input().fn).replace(".bam", "{}.bam".format(self.label)))
    def args(self):
        return ["INPUT=", self.input(), "OUTPUT=", self.output()]

class MergeSamFiles(PicardJobTask):
    _config_subsection = "MergeSamFiles"
    # bam is the target, i.e. is *one* file
    bam = luigi.Parameter(default=None)
    sample = luigi.Parameter(default=None)
    label = luigi.Parameter(default=".merge")
    read1_suffix = luigi.Parameter(default="_R1_001")
    # FIXME: TMP_DIR should not be hard-coded
    options = luigi.Parameter(default="SO=coordinate TMP_DIR=./tmp")
    def jar(self):
        return "MergeSamFiles.jar"
    def requires(self):
        cls = self.set_parent_task()
        bam_list = self.organize_sample_runs(cls)
        return [cls(bam=x.replace("{}.bam".format(self.label), ".bam")) for x in bam_list]
    def output(self):
        fn = self.input()[0].fn
        return luigi.LocalTarget(os.path.join(os.path.dirname(os.path.relpath(fn)), os.pardir, os.path.basename(fn)).replace(".bam", "{}.bam".format(self.label)))
    def args(self):
        return ["OUTPUT=", self.output()] + [item for sublist in [["INPUT=", x] for x in self.input()] for item in sublist]
    def organize_sample_runs(self, cls):
        # This currently relies on the folder structure sample/fc1,
        # sample/fc2 etc... This should possibly also be a
        # configurable function?
        flowcells = os.listdir(os.path.dirname(self.bam))
        bam_list = []
        for fc in flowcells:
            sample_runs = list(set([x.replace(".fastq.gz", "").replace(self.read1_suffix, "") for x in sorted(glob.glob(os.path.join(os.path.dirname(self.bam), fc, "*{}.fastq.gz".format(self.read1_suffix))))]))
            bam_list.extend(["{}{}.bam".format(x, cls().label) for x in sample_runs])
        return bam_list
    
class AlignmentMetrics(PicardJobTask):
    _config_subsection = "AlignmentMetrics"
    bam = luigi.Parameter(default=None)
    options = luigi.Parameter(default=None)
    
    def jar(self):
        return "CollectAlignmentSummaryMetrics.jar"
    def output(self):
        return luigi.LocalTarget(os.path.relpath(self.input().fn).replace(".bam", ".align_metrics"))
    def args(self):
        return ["INPUT=", self.input(), "OUTPUT=", self.output()]

class InsertMetrics(PicardJobTask):
    _config_subsection = "InsertMetrics"
    bam = luigi.Parameter(default=None)
    options = luigi.Parameter(default=None)
    
    def jar(self):
        return "CollectInsertSizeMetrics.jar"
    def output(self):
        return [luigi.LocalTarget(os.path.relpath(self.input().fn).replace(".bam", ".insert_metrics")), 
                luigi.LocalTarget(os.path.relpath(self.input().fn).replace(".bam", ".insert_hist"))]
    def args(self):
        return ["INPUT=", self.input(), "OUTPUT=", self.output()[0], "HISTOGRAM_FILE=", self.output()[1]]

class DuplicationMetrics(PicardJobTask):
    _config_subsection = "DuplicationMetrics"
    bam = luigi.Parameter(default=None)
    options = luigi.Parameter(default=None)

    def jar(self):
        return "MarkDuplicates.jar"
    def requires(self):
        # FIXME: the suffix calculations here and in output are wrong
        cls = self.set_parent_task()
        return cls(bam=self.bam.replace(".dup.bam", ".bam"))
    def output(self):
        return luigi.LocalTarget(os.path.relpath(self.bam).replace(".bam", ".dup.bam"))
    def args(self):
        return ["INPUT=", self.input(), "OUTPUT=", self.output(), "METRICS_FILE=", self.output().fn.replace(".bam", ".dup_metrics")]

class HsMetrics(PicardJobTask):
    _config_subsection = "HsMetrics"
    bam = luigi.Parameter(default=None)
    options = luigi.Parameter(default=None)
    baits = luigi.Parameter(default=None)
    targets = luigi.Parameter(default=None)
    
    def jar(self):
        return "CalculateHsMetrics.jar"
    def output(self):
        print "Input to hsmetrics: " + str(self.input().fn)
        return luigi.LocalTarget(os.path.relpath(self.input().fn).replace(".bam", ".hs_metrics"))
    def args(self):
        return ["INPUT=", self.input(), "OUTPUT=", self.output(), "BAIT_INTERVALS=", self.baits, "TARGET_INTERVALS=", self.targets]

class PicardMetrics(luigi.WrapperTask):
    bam = luigi.Parameter(default=None)
    def requires(self):
        return [DuplicationMetrics(bam=self.bam.replace(".bam", ".dup.bam")), HsMetrics(bam=self.bam),
                InsertMetrics(bam=self.bam), AlignmentMetrics(bam=self.bam)]
