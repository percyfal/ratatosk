import os
import glob
import sys
import shutil
import unittest
import luigi
import ratatosk.job

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
        """Test Generic wrapper called from luigi"""
        luigi.run(_luigi_args(['--config-file', localconf, '--parent-task', 'ratatosk.lib.tools.gatk.IndelRealigner']), main_task_cls=ratatosk.job.GenericWrapperTask)
