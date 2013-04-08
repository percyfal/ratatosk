import os
import glob
import sys
import shutil
import unittest
import luigi
import ratatosk.job
import ratatosk.lib.tools.gatk
import ratatosk.lib.align.bwa
from ratatosk.config import get_config

localconf = "pipeconf.yaml"
local_scheduler = '--local-scheduler'
process = os.popen("ps x -o pid,args | grep ratatoskd | grep -v grep").read() #sometimes have to use grep -v grep

if process:
    local_scheduler = None

def _luigi_args(args):
    if local_scheduler:
        return [local_scheduler] + args
    return args

class TestWrapper(unittest.TestCase):
    def test_generic_wrapper_luigi(self):
        """Test Generic wrapper called from luigi. The idea is to pass a task name that is called from GenericWrapperTask."""
        luigi.run(_luigi_args(['--config-file', localconf, '--parent-task', 'ratatosk.lib.tools.gatk.IndelRealigner']), main_task_cls=ratatosk.job.GenericWrapperTask)

class TestJobTask(unittest.TestCase):
    def test_job_init(self):
        """Test initialization of job"""
        cnf = get_config()
        cnf.add_config_path(localconf)
        task = ratatosk.lib.align.bwa.Aln(target="data/sample1_1.sai", parent_task=('ratatosk.lib.align.bwa.InputFastqFile', ))
        task = ratatosk.lib.tools.gatk.UnifiedGenotyper(target="data/sample1_1.sai")
        
