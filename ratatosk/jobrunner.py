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
import random
import sys
import os
import yaml
from datetime import datetime
import subprocess
import logging
import warnings
import luigi
from subprocess import Popen, PIPE
from itertools import izip
from luigi.task import flatten
import ratatosk.shell as shell
import ratatosk
from ratatosk import backend
from ratatosk.handler import RatatoskHandler, register_attr
from ratatosk.config import get_config, get_custom_config
from ratatosk.utils import rreplace, update, config_to_dict

# Use luigi-interface for now
logger = logging.getLogger('luigi-interface')

class JobRunner(object):
    run_job = NotImplemented

class DefaultShellJobRunner(JobRunner):
    """Default job runner to use for shell jobs."""
    def __init__(self):
        pass

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
                    ypath = x.path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
                    y = luigi.LocalTarget(ypath)
                    logger.info("Using temp path: {0} for path {1}".format(y.path, x.path))
                    args.append(y.path)
                    if job.add_suffix():
                        x = luigi.LocalTarget(x.path + job.add_suffix())
                        y = luigi.LocalTarget(y.path + job.add_suffix())
                    tmp_files.append((y, x))
            else:
                args.append(str(x))
        return (tmp_files, args)

    @staticmethod
    def _get_main(job):
        """Add main program to command. Override this for programs
        where main is given as a parameter, e.g. for GATK"""
        return job.main()

    def _make_arglist(self, job):
        """Make and return the arglist"""
        if job.path():
            exe = os.path.join(job.path(), job.exe())
        else:
            exe = job.exe()
        if not exe:# or not os.path.exists(exe):
            logger.error("Can't find executable: {0}, full path {1}".format(exe,
                                                                            os.path.abspath(exe)))
            raise Exception("executable does not exist")
        arglist = [exe]
        if job.main():
            arglist.append(self._get_main(job))
        if job.opts():
            arglist += job.opts()
        # Need to call self.__class__ since fix_paths overridden in
        # DefaultGzShellJobRunner
        (tmp_files, job_args) = self.__class__._fix_paths(job)
        if not job.pipe:
            arglist += job_args
        return (arglist, tmp_files)
        
    def run_job(self, job):
        (arglist, tmp_files) = self._make_arglist(job)
        if job.pipe:
            return (arglist, tmp_files)
        cmd = ' '.join(arglist)
        logger.info("\nJob runner '{0}';\n\trunning command '{1}'\n".format(self.__class__, cmd))
        (stdout, stderr, returncode) = shell.exec_cmd(cmd, shell=True)
        if returncode == 0:
            logger.info("Shell job completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                # This is weird; using the example in luigi
                # (a.move(b)) doesn't work, and using just b.path
                # fails unless it contains a directory (e.g. './file'
                # works, 'file' doesn't)
                a.move(os.path.join(os.curdir, b.path))
        else:
            raise Exception("Job '{}' failed: \n{}".format(' '.join(arglist), " ".join([stderr])))
                

# Aaarrgh - it doesn't get uglier than this. Some programs
# "seamlessly" read and write gzipped files. In the job runner we work
# with temp files that lack the .gz suffix, so the output is not
# compressed... Took me a while to figure out. Solution for now
#  is to add suffix 'gz' to the temp file. Obviously this won't work
#  for uncompressed input (SIGH), but as I discuss in issues, maybe
#  this is a good thing.
class DefaultGzShellJobRunner(DefaultShellJobRunner):
    """Temporary fix for programs that determine if output is zipped
    based on the suffix of the file name. When working with tmp files
    this will not work so an extra .gz suffix needs to be added. This
    should be taken care of in a cleaner way."""
    def __init__(self):
        pass

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
                    ypath = x.path + '-luigi-tmp-%09d' % random.randrange(0, 1e10) + '.gz'
                    y = luigi.LocalTarget(ypath)
                    logger.info("Using temp path: {0} for path {1}".format(y.path, x.path))
                    args.append(y.path)
                    if job.add_suffix():
                        x = luigi.LocalTarget(x.path + job.add_suffix())
                        y = luigi.LocalTarget(y.path + job.add_suffix())
                    tmp_files.append((y, x))
            else:
                args.append(str(x))
        return (tmp_files, args)

class PipedJobRunner(DefaultShellJobRunner):
    @staticmethod
    def _strip_output(job):
        tmp_files = []
        args = []
        for x in job.args():
            if isinstance(x, luigi.LocalTarget): # input/output
                if x.exists(): # input
                    args.append(x.path)
                else: # output
                    pass
            else:
                # Strip out any options that have to do with outputs.
                # Should be defined in task
                if str(x) not in ["-o", ">", "OUTPUT=", "O="]:
                    args.append(str(x))
        return (tmp_files, args)
        
    def run_job(self, job):
        cmdlist = []
        tmp_files = []
        for j in job.args():
            arglist = j.job_runner()._make_arglist(j)[0] + self._strip_output(j)[1]
            cmdlist.append(arglist)

        plist = []
        # This is extremely annoying. The bwa -r "@RG\tID:foo" etc
        # screws up the regular call to Popen, forcing me to use
        # shell=True. Some escape character that needs correcting;
        # throws error [bwa_sai2sam_pe] malformated @RG line
        plist.append(Popen(" ".join(cmdlist[0]), stdout=PIPE, shell=True))
        #plist.append(Popen(cmdlist[0], stdout=PIPE))
        for i in xrange(1, len(cmdlist)):
            #plist.append(Popen(cmdlist[i], stdin=plist[i-1].stdout, stdout=PIPE))
            plist.append(Popen(" ".join(cmdlist[1]), stdin=plist[i-1].stdout, stdout=PIPE, shell=True))
            plist[i-1].stdout.close()
        pipe = Popen("cat > {}".format(job.target), stdin=plist[-1].stdout, shell=True)
        out, err = pipe.communicate()
