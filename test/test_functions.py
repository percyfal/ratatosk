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
      #luigi.run(_luigi_args(['--target', bam, '--config-file', localconf, '--parent-task', 'ratatosk.bwa.BwaSampe']), main_task_cls=SAM.SamToBam)
      tmp = SAM.SamToBam()
      print tmp.parent_task
      print tmp.config_file
      tmp.config_file = localconf
      print tmp.config_file
      tmp._update_config(localconf)
      print tmp.name_prefix()

