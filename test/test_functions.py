import os
import glob
import sys
import unittest
import luigi
import time
import logging
from ratatosk.job import JobTask
import ratatosk.lib.align.bwa as BWA
import ratatosk.lib.tools.samtools as SAM
import ratatosk.lib.files.fastq as FASTQ
import ratatosk.lib.tools.picard as PICARD
import ratatosk.lib.tools.gatk as GATK
import ratatosk.lib.utils.cutadapt as CUTADAPT
import ratatosk.lib.tools.fastqc as FASTQC
import ratatosk.lib.files.external
import ngstestdata as ntd

logging.basicConfig(level=logging.DEBUG)
sample = "P001_101_index3_TGACCA_L001"
bam = os.path.join(sample + ".bam")

class TestGeneralFunctions(unittest.TestCase):
    def test_prefix_generation(self):
        # See if it is possible from a task to construct a prefix that
        # is unique and resolved for a particular task
        localconf = "../config/ratatosk.yaml"

        class gatk1(ratatosk.lib.tools.gatk.RealignerTargetCreator):
            target = luigi.Parameter(default="dummy")

        class gatk2(ratatosk.lib.tools.gatk.IndelRealigner):
            target = luigi.Parameter(default="dummy")
            def requires(self):
                return gatk1(target=self.target)

        class gatk3(ratatosk.lib.tools.gatk.BaseRecalibrator):
            target = luigi.Parameter(default="dummy")
            def requires(self):
                return gatk2(target=self.target)
        
        class gatk4(ratatosk.lib.tools.gatk.PrintReads):
            target = luigi.Parameter(default="dummy")
            def requires(self):
                return gatk3(target=self.target)

        gatk4().name_prefix()
