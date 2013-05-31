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
Provide wrappers for `angsd <http://popgen.dk/wiki/index.php/Main_Page>`_

Classes
-------
"""

import luigi
import ratatosk.lib.files.input
import ratatosk.lib.variation.tabix
from ratatosk.handler import RatatoskHandler, register_task_handler
from ratatosk.job import JobTask
from ratatosk.jobrunner import  DefaultShellJobRunner
from ratatosk.utils import rreplace, fullclassname
from ratatosk.log import get_logger

logger = get_logger()

class InputTxtFile(ratatosk.lib.files.input.InputTxtFile):
    pass

class AngsdJobRunner(DefaultShellJobRunner):
    pass

class AngsdJobTask(JobTask):
    executable = luigi.Parameter(default="angsd")
    parent_task = luigi.Parameter(default=("ratatosk.lib.popgen.angsd.InputTxtFile", ), is_list=True)
    
    def job_runner(self):
        return AngsdJobRunner()

class AngsdBamJobTask(AngsdJobTask):
    """Use bam input"""
    options = luigi.Parameter(default=())
    bamlist = luigi.Parameter(default=("bamlist.txt",))

    def source(self):
        return self.bamlist

    def args(self):
        retval = ["-bam", self.input()[0], "-out", self.output()]
        return retval
