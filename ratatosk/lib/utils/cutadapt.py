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
import time
import shutil
import random
import logging
from ratatosk.job import InputJobTask, JobTask, DefaultShellJobRunner
from cement.utils import shell

logger = logging.getLogger('luigi-interface')

# Aaarrgh - it doesn't get uglier than this. cutadapt "seamlessly"
# reads and writes gzipped files using the '-o' option for output. In
# the job runner we work with temp files, so the output is not
# compressed... Took me a while to figure out. Solution for now
#  is to add suffix 'gz' to the temp file. Obviously this won't work
#  for uncompressed input (SIGH), but as I discuss in issues, maybe
#  this is a good thing.
class CutadaptJobRunner(DefaultShellJobRunner):

    @staticmethod
    def _fix_paths(job):
        """Modelled after hadoop_jar.HadoopJarJobRunner._fix_paths.
        """
        tmp_files = []
        args = []
        for x in job.args():
            if isinstance(x, luigi.LocalTarget): # input/output
                if x.exists(): # input
                    args.append(x.path)
                else: # output
                    # Note the ugly ".gz" fix - cutadapt needs this to determine outfile type...
                    y = luigi.LocalTarget(x.path + \
                                              '-luigi-tmp-%09d' % random.randrange(0, 1e10) + '.gz')
                    logger.info("Using temp path: {0} for path {1}".format(y.path, x.path))
                    args.append(y.path)
                    tmp_files.append((y, x))
            else:
                args.append(str(x))
        return (tmp_files, args)

    def run_job(self, job):
        if not job.exe():# or not os.path.exists(job.exe()):
            logger.error("Can't find executable: {0}, full path {1}".format(job.exe(),
                                                                            os.path.abspath(job.exe())))
            raise Exception("executable does not exist")
        arglist = [job.exe()]
        if job.main():
            arglist.append(self._get_main(job))
        if job.opts():
            arglist.append(job.opts())
        (tmp_files, job_args) = CutadaptJobRunner._fix_paths(job)
        arglist += job_args
        cmd = ' '.join(arglist)
        logger.info(cmd)
        (stdout, stderr, returncode) = shell.exec_cmd(cmd, shell=True)
        if returncode == 0:
            logger.info("Shell job completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                a.move(os.path.join(os.curdir, b.path))
        else:
            raise Exception("Job '{}' failed: \n{}".format(' '.join(arglist), " ".join([stderr])))


class InputFastqFile(InputJobTask):
    _config_section = "cutadapt"
    _config_subsection = "input_fastq_file"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.FastqFile")
    

# NB: cutadapt is a non-hiearchical tool. Group under, say, utils?
class CutadaptJobTask(JobTask):
    _config_section = "cutadapt"
    label = luigi.Parameter(default=".trimmed")
    executable = luigi.Parameter(default="cutadapt")
    parent_task = luigi.Parameter(default="ratatosk.lib.utils.cutadapt.InputFastqFile")
    # Use Illumina TruSeq adapter sequences as default
    threeprime = luigi.Parameter(default="AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC")
    fiveprime = luigi.Parameter(default="AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC")
    read1_suffix = luigi.Parameter(default="_R1_001")
    read2_suffix = luigi.Parameter(default="_R2_001")

    def read1(self):
        # Assume read 2 if no match...
        return self.input().fn.find(self.read1_suffix) > 0

    def job_runner(self):
        return CutadaptJobRunner()

    def args(self):
        seq = self.threeprime if self.read1() else self.fiveprime
        return ["-a", seq, self.input(), "-o", self.output(), ">", self.input().fn.replace(".fastq.gz", ".trimmed.fastq.cutadapt_metrics")]
