import os
import luigi
import logging
import pm.luigi.external
from pm.luigi.job import JobTask, DefaultShellJobRunner

class VcfJobRunner(DefaultShellJobRunner):
    pass

class VcfJobTask(JobTask):
    _config_section = "vcftools"
    
