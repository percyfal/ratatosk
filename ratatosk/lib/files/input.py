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

Input file types/tasks. Class representations of file types used in
analyses. Each file type has as default :attr:`parent_task
<ratatosk.job.BaseJobTask.parent_task>` one of the :mod:`.external`
tasks.

The use of these classes is mainly intended for two cases:

1. Input file tasks can be used to connect different modules by changing
   the :attr:`parent_task` to depend on a task from another module
2. (Future work) The input file tasks implicitly defines a file type
   and could therefore be used to validate input/output

Classes
-------
"""
import luigi
from ratatosk.job import InputJobTask

class InputBamFile(InputJobTask):
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.BamFile")
    suffix = luigi.Parameter(default=".bam")

class InputVcfFile(InputJobTask):
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.VcfFile")
    suffix = luigi.Parameter(default=".vcf")
