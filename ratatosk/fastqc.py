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
from ratatosk.job import JobTask, DefaultShellJobRunner
logger = logging.getLogger('luigi-interface')
from cement.utils import shell

# This was a nightmare to get right. Temporary output is a directory,
# so would need custom _fix_paths for cases like this
class FastQCJobRunner(DefaultShellJobRunner):
    """This job runner must take into account that there is no default
    output file but rather an output directory"""
    def run_job(self, job):
        arglist = [job.exe()]
        if job.opts():
            arglist.append(job.opts())
        (tmp_files, job_args) = DefaultShellJobRunner._fix_paths(job)
        (tmpdir, outdir) = tmp_files[0]
        arglist += ['-o', tmpdir.fn]
        arglist += [job_args[0]]
        os.makedirs(os.path.join(os.curdir, tmpdir.fn))
        # Need to send output to temporary *directory*, not file
        cmd = ' '.join(arglist)        
        (stdout, stderr, returncode) = shell.exec_cmd(cmd, shell=True)

        if returncode == 0:
            logger.info("Shell job completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                a.move(os.path.join(os.curdir, b.path))
        else:
            raise Exception("Job '{}' failed: \n{}".format(cmd.replace("= ", "="), " ".join([stderr])))

class InputBamFile(JobTask):
    _config_section = "fastqc"
    _config_subsection = "InputBamFile"
    bam = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.BamFile")
    def requires(self):
        cls = self.set_parent_task()
        return cls(bam=self.bam)
    def output(self):
        return luigi.LocalTarget(os.path.relpath(self.input().fn))
    def run(self):
        pass

class InputFastqFile(JobTask):
    _config_section = "fastqc"
    _config_subsection = "input_fastq_file"
    fastq = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.FastqFile")
    def requires(self):
        cls = self.set_parent_task()
        return cls(fastq=self.fastq)
    def output(self):
        return luigi.LocalTarget(os.path.relpath(self.input().fn))
    def run(self):
        pass

class FastQCJobTask(JobTask):
    _config_section = "fastqc"
    fastqc = luigi.Parameter(default="fastqc")
    seqfile = luigi.Parameter(default=None)
    label = luigi.Parameter(default = ".label")
    options = luigi.Parameter(default = None)
    parent_task = luigi.Parameter(default = "ratatosk.fastqc.InputFastqFile")
    # fastqc has many outputs (targets) - arbitrarily use the
    # "summary.txt" output. Or use the --noextract option?
    target_file_name = luigi.Parameter(default = "summary.txt")

    def job_runner(self):
        return FastQCJobRunner()
    
    def exe(self):
        """Path to executable"""
        return self.fastqc
    def requires(self):
        # Here is the problem of many inputs; fastqc accepts sam, bam,
        # fastq. Maybe each class should have a generic parameter to
        # fall back on?
        cls = self.set_parent_task()
        return InputFastqFile(fastq=self.seqfile)
    def output(self):
        # Luigi authors advise against multiple outputs, but here goes
        # anyways... 
        outdir = "{}_fastqc".format(os.path.splitext(self.input().fn.replace(".gz", ""))[0].replace(self.label, ""))
        return luigi.LocalTarget(outdir)
    def args(self):
        return [self.input(), self.output()]
