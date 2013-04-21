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
Provide wrappers for `annovar <http://www.openbioinformatics.org/annovar/>`_


Classes
-------
"""

import os
import luigi
import logging
import ratatosk.lib.files.input
from ratatosk.utils import rreplace, fullclassname
from ratatosk.job import JobTask
from ratatosk.jobrunner import DefaultShellJobRunner
from ratatosk.log import get_logger
import subprocess

logger = get_logger()

class InputVcfFile(ratatosk.lib.files.input.InputVcfFile):
    pass

class InputTxtFile(ratatosk.lib.files.input.InputTxtFile):
    pass

class InputPath(ratatosk.lib.files.input.InputPath):
    pass

class AnnovarJobRunner(DefaultShellJobRunner):
    pass

class AnnovarJobTask(JobTask):
    exe_path = luigi.Parameter(default=os.getenv("ANNOVAR_HOME") if os.getenv("ANNOVAR_HOME") else os.curdir)
    genome = luigi.Parameter(default="hg19")
    dbsnp = luigi.Parameter(default=130)

    def exe(self):
        # Annovar has no main executable so return the sub_executable instead
        return self.sub_executable

    def main(self):
        return None

    def job_runner(self):
        return AnnovarJobRunner()
    
class AnnovarDownDb(AnnovarJobTask):
    sub_executable = luigi.Parameter(default="annotate_variation.pl")
    dbdest = luigi.Parameter(default="humandb/", description="Database destination directory.")
    dbpath = luigi.Parameter(default=None, description="Database root path. Defaults to annovar path.")
    buildver = luigi.Parameter(default="hg19", description="Database build version. Defaults to hg19.")
    verdbsnp = luigi.Parameter(default="132")
    downdb = luigi.Parameter(default=None, description="Database to download. Defaults to refGene.")
    ucsc_urldict = {"refGene" : ["http://hgdownload.cse.ucsc.edu/goldenPath/{buildver}/database/refGene.txt.gz",
                                 "http://hgdownload.cse.ucsc.edu/goldenPath/{buildver}/database/refLink.txt.gz"],
                    "genomicSuperDups" : ["http://hgdownload.cse.ucsc.edu/goldenPath/{buildver}/database/genomicSuperDups.txt.gz"],
                    "snp" : ["http://hgdownload.cse.ucsc.edu/goldenPath/{buildver}/database/snp{verdbsnp}.txt.gz"],
                    "phastConsElements46way": ["http://hgdownload.cse.ucsc.edu/goldenPath/hg19/database/phastConsElements46way.txt.gz"],
               }
    annovar_urldict = {
        "avsift" : ["http://www.openbioinformatics.org/annovar/download/{buildver}_avsift.txt.gz"],
        "ljb_all" : ["http://www.openbioinformatics.org/annovar/download/hg19_ljb_all.txt.gz"],
        "esp5400_all" : ["http://www.openbioinformatics.org/annovar/download/hg19_esp5400_all.txt.gz"],
        "ALL.sites.2011_05" : ["http://www.openbioinformatics.org/annovar/download/hg19_ALL.sites.2011_05.txt.gz"],
        }

    def _fulldbpath(self):
        if self.dbpath is None:
            dbpath = os.path.join(self.exe_path, self.dbdest)
        else:
            dbpath = os.path.join(self.dbpath, self.dbdest)
        return dbpath

    def requires(self):
        return InputPath(target=self._fulldbpath())

    def output(self):
        if self.downdb in self.annovar_urldict.keys():
            outfile = os.path.join(os.path.dirname(self._fulldbpath()), 
                                   os.path.basename(self.annovar_urldict[self.downdb][0].replace(".gz", "").format(buildver=self.buildver, verdbsnp=self.verdbsnp)))
        else:
            outfile = os.path.join(os.path.dirname(self._fulldbpath()), 
                                   "{}_{}".format(self.buildver, os.path.basename(self.ucsc_urldict[self.downdb][0].replace(".gz", "").format(buildver=self.buildver, verdbsnp=self.verdbsnp))))
        return luigi.LocalTarget(outfile)

    def args(self):
        downdb=self.downdb
        if self.downdb=="snp":
            downdb = self.downdb + self.verdbsnp
        retval = ["-buildver", self.buildver, "-downdb", downdb]
        if self.downdb in self.annovar_urldict.keys():
            retval += ["-webfrom", "annovar"]
        retval += [self._fulldbpath()]
        return retval

class Convert2Annovar(AnnovarJobTask):
    sub_executable = luigi.Parameter(default="convert2annovar.pl")
    label = luigi.Parameter(default="-avinput")
    suffix = luigi.Parameter(default=".txt")
    # Should be coupled to source_suffix somehow
    options = luigi.Parameter(default=("-format vcf4", ))
    parent_task = luigi.Parameter(default=("ratatosk.lib.annotation.annovar.InputVcfFile", ), is_list=True)

    def args(self):
        return [self.input()[0], "--outfile", self.output()]

class SummarizeAnnovar(AnnovarJobTask):
    sub_executable = luigi.Parameter(default="summarize_annovar.pl")
    # This variable would be used with AnnovarDownDb requirement
    db_requires = luigi.Parameter(default=("refGene", "genomicSuperDups", "snp", "avsift", "ljb_all", "esp5400_all",
                                           "phastConsElements46way", "ALL.sites.2011_05"), is_list=True)
    parent_task = luigi.Parameter(default=("ratatosk.lib.annotation.annovar.Convert2Annovar", ), is_list=True)
    suffix = luigi.Parameter(default=".txt.log")
    buildver = luigi.Parameter(default="hg19")
    verdbsnp = luigi.Parameter(default="132")
    ver1000g = luigi.Parameter(default="1000g2011may")
    options = luigi.Parameter(default=("-remove",))
    db = luigi.Parameter(default="humandb")
    dbpath = luigi.Parameter(default=None, description="Database search path. Annovar search path is used if no information provided.")
    
    def opts(self):
        retval = list(self.options) + ["-buildver {}".format(self.buildver), 
                                        "-verdbsnp {}".format(self.verdbsnp),
                                        "-ver1000g {}".format(self.ver1000g)]
        return retval

    def requires(self):
        cls = self.parent()[0]
        source = self.source()[0]
        return [cls(target=source)] + [AnnovarDownDb(downdb=x, verdbsnp=self.verdbsnp) for x in self.db_requires]

    def args(self):
        annovardb = os.path.join(self.dbpath if self.dbpath else self.path(), self.db)
        return [self.input()[0], annovardb]
