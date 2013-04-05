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
import ratatosk.shell as shell

logger = logging.getLogger('luigi-interface')

class MutectJobRunner(DefaultShellJobRunner):
    @staticmethod
    def _get_main(job):
        return "-T {}".format(job.main())

    def _make_arglist(self, job):
        if not job.jar() or not os.path.exists(os.path.join(job.path(),job.jar())):
            logger.error("Can't find jar: {0}, full path {1}".format(job.jar(),
                                                                     os.path.abspath(job.jar())))
            raise Exception("job jar does not exist")
        arglist = [job.java()] + job.java_opt() + ['-jar', os.path.join(job.path(), job.jar())]
        if job.main():
            arglist.append(self._get_main(job))
        if job.opts():
            arglist += job.opts()
        (tmp_files, job_args) = DefaultShellJobRunner._fix_paths(job)
        arglist += job_args
        return (arglist, tmp_files)


class InputBamFile(InputJobTask):
    _config_section = "mutect"
    _config_subsection = "InputBamFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.BamFile")
    target_suffix = luigi.Parameter(default=".bam")

class MutectJobTask(JobTask):
    _config_section = "mutect"
    exe_path = luigi.Parameter(default=os.getenv("MUTECT_HOME") if os.getenv("MUTECT_HOME") else os.curdir)
    executable = luigi.Parameter(default="muTect.jar")
    source_suffix = luigi.Parameter(default=".bam")
    target_suffix = luigi.Parameter(default=".bam")
    java_exe = luigi.Parameter(default="java")
    java_options = luigi.Parameter(default=("-Xmx2g",), description="Java options", is_list=True)
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.gatk.InputBamFile")
    ref = luigi.Parameter(default=None)
    can_multi_thread = True

    def jar(self):
        return self.executable

    def exe(self):
        return self.jar()

    def java_opt(self):
        return list(self.java_options)

    def java(self):
        return self.java_exe

    def job_runner(self):
        return MutectJobRunner()

