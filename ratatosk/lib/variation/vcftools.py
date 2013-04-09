# Copyright (c) 2013 Per Unneberg
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
"""
Provide wrappers for `vcftools <http://vcftools.sourceforge.net/perl_module.html>`_

Classes
-------
"""



import os
import luigi
import logging
import ratatosk.lib.files.external
from ratatosk.job import InputJobTask, JobTask, DefaultShellJobRunner

class VcfJobRunner(DefaultShellJobRunner):
    pass

class InputVcfFile(InputJobTask):
    _config_section = "vcftools"
    _config_subsection = "InputVcfFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.VcfFile")
    target_suffix = luigi.Parameter(default=".vcf")

class VcfJobTask(JobTask):
    _config_section = "vcftools"
    
    def job_runner(self):
        return VcfJobRunner()

class Vcf(JobTask):
    pass

