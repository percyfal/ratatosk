"""
pm.luigi.sample

Organize tasks by sample. Setup global parameters for use 
"""
import os
import luigi
import logging
import pm.luigi.external
from pm.luigi.job import JobTask, DefaultShellJobRunner
from cement.utils import shell

logger = logging.getLogger('luigi-interface')

class SampleJobRunner(DefaultShellJobRunner):
    pass


