import os
import glob
import sys
import shutil
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
from ratatosk.utils import make_fastq_links
import ngstestdata as ntd
from site_functions import target_generator

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

        # gatk4().name_prefix()


class TestFunctions(unittest.TestCase):
    def setUp(self):
        self.project = os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "projects", "J.Doe_00_01"))

    def tearDown(self):
        if os.path.exists("tmp"):
            shutil.rmtree("tmp")

    def test_make_fastq_links(self):
        """Test making fastq links"""
        # Assume Illumina/SciLife data structure
        tl = target_generator(indir=self.project)
        fql = make_fastq_links(tl, indir=self.project, outdir="tmp")
        self.assertTrue(os.path.lexists(os.path.join("tmp", os.path.relpath(tl[0][2], self.project) + "_R1_001.fastq.gz")))
        self.assertTrue(os.path.lexists(os.path.join("tmp", os.path.dirname(os.path.relpath(tl[0][2], self.project)),
                                                     "SampleSheet.csv")))
