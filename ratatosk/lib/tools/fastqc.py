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
from ratatosk.job import InputJobTask, JobTask, DefaultShellJobRunner
logger = logging.getLogger('luigi-interface')
import ratatosk.shell as shell

# This was a nightmare to get right. Temporary output is a directory,
# so would need custom _fix_paths for cases like this
class FastQCJobRunner(DefaultShellJobRunner):
    """This job runner must take into account that there is no default
    output file but rather an output directory"""
    def _make_arglist(self, job):
        arglist = [job.exe()]
        if job.opts():
            arglist += job.opts()
        (tmp_files, job_args) = DefaultShellJobRunner._fix_paths(job)
        (tmpdir, outdir) = tmp_files[0]
        arglist += ['-o', tmpdir.fn]
        arglist += [job_args[0]]
        return (arglist, tmp_files)

    def run_job(self, job):
        (arglist, tmp_files) = self._make_arglist(job)
        (tmpdir, outdir) = tmp_files[0]
        os.makedirs(os.path.join(os.curdir, tmpdir.fn))
        # Need to send output to temporary *directory*, not file
        cmd = ' '.join(arglist)        
        logger.info("Job runner '{0}'; running command '{1}'".format(self.__class__, cmd))
        (stdout, stderr, returncode) = shell.exec_cmd(cmd, shell=True)

        if returncode == 0:
            logger.info("Shell job completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                a.move(os.path.join(os.curdir, b.path))
        else:
            raise Exception("Job '{}' failed: \n{}".format(cmd.replace("= ", "="), " ".join([stderr])))

class InputBamFile(InputJobTask):
    _config_section = "fastqc"
    _config_subsection = "InputBamFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.BamFile")

class InputFastqFile(InputJobTask):
    _config_section = "fastqc"
    _config_subsection = "input_fastq_file"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.FastqFile")

class FastQCJobTask(JobTask):
    _config_section = "fastqc"
    executable = luigi.Parameter(default="fastqc")
    parent_task = luigi.Parameter(default = "ratatosk.lib.tools.fastqc.InputFastqFile")
    # fastqc has many outputs (targets) - arbitrarily use the
    # "summary.txt" output. Or use the --noextract option?
    target_file_name = luigi.Parameter(default = "summary.txt")

    def job_runner(self):
        return FastQCJobRunner()

    def output(self):
        outdir = "{}_fastqc".format(os.path.splitext(self.input().fn.replace(".gz", ""))[0])
        return luigi.LocalTarget(outdir)

    def args(self):
        return [self.input(), self.output()]
