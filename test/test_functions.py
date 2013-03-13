import os
import glob
import sys
import unittest
import luigi
import time
import logging
from ratatosk.job import JobTask
import ratatosk.bwa as BWA
import ratatosk.samtools as SAM
import ratatosk.fastq as FASTQ
import ratatosk.picard as PICARD
import ratatosk.gatk as GATK
import ratatosk.cutadapt as CUTADAPT
import ratatosk.fastqc as FASTQC
import ratatosk.external
import ngstestdata as ntd

logging.basicConfig(level=logging.DEBUG)
sample = "P001_101_index3_TGACCA_L001"
bam = os.path.join(sample + ".bam")
localconf = "pipeconf.yaml"
local_scheduler = '--local-scheduler'
# Don't really want to run luigi but rather access the functions
# directly for testing
def _luigi_args(args):
    if local_scheduler:
        return [local_scheduler] + args
    return args

class TestGeneralFunctions(unittest.TestCase):
    def test_prefix_generation(self):
        # See if it is possible from a task to construct a prefix that
        # is unique and resolved for a particular task
        localconf = "../config/ratatosk.yaml"


        class gatk1(ratatosk.gatk.RealignerTargetCreator):
            target = luigi.Parameter(default="dummy")

        class gatk2(ratatosk.gatk.IndelRealigner):
            target = luigi.Parameter(default="dummy")
            def requires(self):
                return gatk1(target=self.target)

        class gatk3(ratatosk.gatk.BaseRecalibrator):
            target = luigi.Parameter(default="dummy")
            def requires(self):
                return gatk2(target=self.target)
        
        class gatk4(ratatosk.gatk.PrintReads):
            target = luigi.Parameter(default="dummy")
            def requires(self):
                return gatk3(target=self.target)
            
        # gatk4._update_config(localconf)
        # gatk4.parent_class = gatk3
        # gatk4.target = "dummy"

        # gatk5 = ratatosk.gatk.RealignerTargetCreator()
        # gatk5._update_config(localconf)
        # gatk5.parent_class = gatk4
        # gatk5.target = "dummy"
        print "Parent class " + str(gatk4.parent_task)
        gatk4().name_prefix()
