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
import os
import luigi
import logging
import ratatosk.lib.files.external
from ratatosk.utils import rreplace, fullclassname
from ratatosk.job import InputJobTask, JobTask, DefaultShellJobRunner
import subprocess

logger = logging.getLogger('luigi-interface')

class AnnovarJobRunner(DefaultShellJobRunner):
    pass

class InputVcfFile(InputJobTask):
    _config_section = "annovar"
    _config_subsection = "InputVcfFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.VcfFile")
    target_suffix = luigi.Parameter(default=".vcf")

class AnnovarJobTask(JobTask):
    _config_section = "annovar"
    genome = luigi.Parameter(default="hg19")
    dbsnp = luigi.Parameter(default=130)

    def exe(self):
        # Annovar has no main executable
        return ""
    
# FIXME: setup databases task should be requirement for downstream
# calls
class AnnovarDownDb(AnnovarJobTask):
    pass

class AnnovarSummarizeAnnovar(AnnovarJobTask):
    _config_subsection = "summarize_annovar"
    
