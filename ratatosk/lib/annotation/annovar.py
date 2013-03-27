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
    
# FIXME: setup databases task should be requirement for downstream
# calls
#
# NOTE: Leaving this for now. Problem is we don't know the target
# names, so it's up to the user to install the necessary databases.
# 
# One way to do this would be to make a dictionary of databases:

# grep hgdownload annotate_variation.pl 
#                 push @urlin, "http://hgdownload.cse.ucsc.edu/goldenPath/$buildver/database/refGene.txt.gz";
#                 push @urlin, "http://hgdownload.cse.ucsc.edu/goldenPath/$buildver/database/refLink.txt.gz";
#                 push @urlin, "http://hgdownload.cse.ucsc.edu/goldenPath/$buildver/database/knownGene.txt.gz";
#                 push @urlin, "http://hgdownload.cse.ucsc.edu/goldenPath/$buildver/database/kgXref.txt.gz";
#                 push @urlin, "http://hgdownload.cse.ucsc.edu/goldenPath/$buildver/database/ensGene.txt.gz";
#                 push @urlin, "http://hgdownload.cse.ucsc.edu/goldenPath/$buildver/bigZips/chromFa.zip";         #example: hg18, hg19
#                 push @urlin, "http://hgdownload.cse.ucsc.edu/goldenPath/$buildver/bigZips/chromFa.tar.gz";      #example: panTro2
#                 push @urlin, "http://hgdownload.cse.ucsc.edu/goldenPath/$buildver/bigZips/$buildver.fa.gz";     #example: bosTau4
#                 push @urlin, "ftp://hgdownload.cse.ucsc.edu/goldenPath/$buildver/database/phastConsElements$1.txt.gz";
#
# 
# Needed databases for summarize annovar:
#
# $ANNOVAR_HOME/annotate_variation.pl -buildver hg19 -downdb refGene $ANNOVAR_HOME/humandb
# $ANNOVAR_HOME/annotate_variation.pl -buildver hg19 -downdb snp130 $ANNOVAR_HOME/humandb
#
# class AnnovarDownDb(AnnovarJobTask):
#     _config_subsection = "downdb"
#     sub_executable = luigi.Parameter(default="annotate_variation.pl")
#     dbdest = luigi.Parameter(default="humandb/", description="Database destination directory.")
#     dbpath = luigi.Parameter(default=None, description="Database root path. Defaults to annovar path.")

class Convert2Annovar(AnnovarJobTask):
    _config_subsection = "convert2annovar"
    sub_executable = luigi.Parameter(default="convert2annovar.pl")
    label = luigi.Parameter(default="-avinput")
    target_suffix = luigi.Parameter(default=".txt")
    source_suffix = luigi.Parameter(default=".vcf")
    # Should be coupled to source_suffix somehow
    options = luigi.Parameter(default=("-format vcf4", ))
    parent_task = luigi.Parameter(default="ratatosk.lib.annotation.annovar.InputVcfFile")
    
    def args(self):
        return [self.input(), "--outfile", self.output()]


# Needed databases
# humandb/hg19_ALL.sites.2011_05.txt
# humandb/hg19_MT_GRCh37_ensGene.txt
# humandb/hg19_avsift.txt
# humandb/hg19_esp5400_all.txt
# humandb/hg19_genomicSuperDups.txt
# humandb/hg19_ljb_all.txt
# humandb/hg19_phastConsElements46way.txt
# humandb/hg19_refGene.txt
# humandb/hg19_refLink.txt
# humandb/hg19_snp132.txt
class SummarizeAnnovar(AnnovarJobTask):
    _config_subsection = "summarize_annovar"
    sub_executable = luigi.Parameter(default="summarize_annovar.pl")
    # This variable would be used with AnnovarDownDb requirement
    # db_requires = luigi.Parameter(default=("dbsnp",), is_list=True)
    parent_task = luigi.Parameter(default="ratatosk.lib.annotation.annovar.Convert2Annovar")
    target_suffix = luigi.Parameter(default=".log")
    source_suffix = luigi.Parameter(default="")
    options = luigi.Parameter(default=("-buildver hg19", "-verdbsnp 132", "-ver1000g 1000g2011may"))
    db = luigi.Parameter(default="humandb")
    dbpath = luigi.Parameter(default=None, description="Database search path. Annovar search path is used if no information provided.")

    def args(self):
        annovardb = os.path.join(self.dbpath if self.dbpath else self.path(), self.db)
        return [self.input(), annovardb]
