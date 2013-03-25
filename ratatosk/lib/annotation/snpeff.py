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

JAVA="java"
JAVA_OPTS="-Xmx2g"
SNPEFF_HOME=os.getenv("SNPEFF_HOME") if os.getenv("SNPEFF_HOME") else os.curdir
SNPEFF_JAR="snpEff.jar"

class snpEffJobRunner(DefaultShellJobRunner):
    path = SNPEFF_HOME

    def run_job(self, job):
        if not job.jar() or not os.path.exists(os.path.join(self.path,job.jar())):
            logger.error("Can't find jar: {0}, full path {1}".format(job.jar(),
                                                                     os.path.abspath(job.jar())))
            raise Exception("job jar does not exist")
        arglist = [JAVA] + job.java_opt() + ['-jar', os.path.join(self.path, job.jar())]
        if job.main():
            arglist.append(self._get_main(job))
        if job.opts():
            arglist += job.opts()
        (tmp_files, job_args) = DefaultShellJobRunner._fix_paths(job)
        arglist += job_args
        cmd = ' '.join(arglist)        
        logger.info("\nJob runner '{0}';\n\trunning command '{1}'".format(self.__class__, cmd))
        (stdout, stderr, returncode) = shell.exec_cmd(cmd, shell=True)
        if returncode == 0:
            logger.info("Shell job completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                # TODO : this should be relpath?
                a.move(os.path.join(os.curdir, b.path))
                # Some GATK programs generate bai or idx files on the fly...
                if os.path.exists(a.path + ".bai"):
                    logger.info("Saw {} file".format(a.path + ".bai"))
                    os.rename(a.path + ".bai", b.path.replace(".bam", ".bai"))
                if os.path.exists(a.path + ".idx"):
                    logger.info("Saw {} file".format(a.path + ".idx"))
                    os.rename(a.path + ".idx", b.path + ".idx")
        else:
            raise Exception("Job '{}' failed: \n{}".format(cmd, " ".join([stderr])))


class InputVcfFile(InputJobTask):
    _config_section = "snpeff"
    _config_subsection = "InputVcfFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.VcfFile")
    target_suffix = luigi.Parameter(default=".vcf")

class snpEffJobTask(JobTask):
    _config_section = "snpeff"
    executable = luigi.Parameter(default=SNPEFF_JAR)
    source_suffix = luigi.Parameter(default=".vcf")
    target_suffix = luigi.Parameter(default=".vcf")
    config = luigi.Parameter(default=os.path.join(SNPEFF_HOME, "snpEff.config"))
    genome = luigi.Parameter(default="GRCh37.64")
    java_options = luigi.Parameter(default=("-Xmx2g",), description="Java options", is_list=True)

    def jar(self):
        return self.executable

    def exe(self):
        return self.jar()

    def java_opt(self):
        return list(self.java_options)

    def job_runner(self):
        return snpEffJobRunner()


# TODO: add download requirement to snpEff in case database doesn't
# exist
class snpEffDownload(snpEffJobTask):
    pass

    
class snpEff(snpEffJobTask):
    _config_subsection = "eff"
    sub_executable = "eff"
    label = luigi.Parameter(default="-effects")
    options = luigi.Parameter(default=("-1",))
    parent_task = luigi.Parameter(default="ratatosk.lib.annotation.snpeff.InputVcfFile")
        
    def args(self):
        retval = ["-i", self.source_suffix.strip("."),
                  "-o", self.target_suffix.strip("."),
                  "-c", self.config,
                  self.genome,
                  self.input(),
                  ">", self.output()
                  ]
        return retval
