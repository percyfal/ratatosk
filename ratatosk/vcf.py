import os
import luigi
import logging
import ratatosk.external
from ratatosk.job import JobTask, DefaultShellJobRunner

class VcfJobRunner(DefaultShellJobRunner):
    pass

class VcfJobTask(JobTask):
    _config_section = "vcftools"
    
