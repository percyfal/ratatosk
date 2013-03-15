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
import time
import glob
import ratatosk.external
from ratatosk.utils import rreplace
from ratatosk.job import JobTask, DefaultShellJobRunner
from cement.utils import shell

# TODO: make these configurable 
JAVA="java"
JAVA_OPTS="-Xmx2g"
PICARD_HOME=os.getenv("PICARD_HOME")

logger = logging.getLogger('luigi-interface')

class PicardJobRunner(DefaultShellJobRunner):
    path = PICARD_HOME
    def run_job(self, job):
        if not job.jar() or not os.path.exists(os.path.join(self.path,job.jar())):
            logger.error("Can't find jar: {0}, full path {1}".format(job.jar(),
                                                                     os.path.abspath(job.jar())))
            raise Exception("job jar does not exist")
        arglist = ['java', job.java_opt(), '-jar', os.path.join(self.path, job.jar())]
        if job.main():
            arglist.append(job.main())
        if job.opts():
            arglist.append(job.opts())
        (tmp_files, job_args) = DefaultShellJobRunner._fix_paths(job)

        arglist += job_args
        cmd = ' '.join(arglist)        
        logger.info(cmd.replace("= ", "="))
        (stdout, stderr, returncode) = shell.exec_cmd(cmd.replace("= ", "="), shell=True)

        if returncode == 0:
            logger.info("Shell job completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                a.move(os.path.join(os.curdir, b.path))
        else:
            raise Exception("Job '{}' failed: \n{}".format(cmd.replace("= ", "="), " ".join([stderr])))

class InputBamFile(JobTask):
    _config_section = "picard"
    _config_subsection = "InputBamFile"
    target = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.BamFile")
    def requires(self):
        cls = self.set_parent_task()
        return cls(target=self.target)
    def output(self):
        return luigi.LocalTarget(self.target)
    def run(self):
        pass

class PicardJobTask(JobTask):
    _config_section = "picard"
    java_options = "-Xmx2g"
    parent_task = luigi.Parameter(default="ratatosk.picard.InputBamFile")
    target_suffix = luigi.Parameter(default=".bam")
    source_suffix = luigi.Parameter(default=".bam")

    def jar(self):
        """Path to the jar for this Picard job"""
        return None

    def java_opt(self):
        return self.java_options

    def exe(self):
        return self.jar()

    def job_runner(self):
        return PicardJobRunner()

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)

class SortSam(PicardJobTask):
    _config_subsection = "SortSam"
    options = luigi.Parameter(default="SO=coordinate MAX_RECORDS_IN_RAM=750000")
    label = luigi.Parameter(default=".sort")

    def jar(self):
        return "SortSam.jar"
    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)
    def output(self):
        return luigi.LocalTarget(self.target)
    def args(self):
        return ["INPUT=", self.input(), "OUTPUT=", self.output()]

class MergeSamFiles(PicardJobTask):
    _config_subsection = "MergeSamFiles"
    label = luigi.Parameter(default=".merge")
    read1_suffix = luigi.Parameter(default="_R1_001")
    # FIXME: TMP_DIR should not be hard-coded
    options = luigi.Parameter(default="SO=coordinate TMP_DIR=./tmp")

    def jar(self):
        return "MergeSamFiles.jar"
    def requires(self):
        cls = self.set_parent_task()
        sources = self.organize_sample_runs(cls)
        return [cls(target=src) for src in sources]

    def output(self):
        return luigi.LocalTarget(self.target)

    def args(self):
        return ["OUTPUT=", self.output()] + [item for sublist in [["INPUT=", x] for x in self.input()] for item in sublist]
    
    def organize_sample_runs(self, cls):
        # This currently relies on the folder structure sample/fc1,
        # sample/fc2 etc... This should possibly also be a
        # configurable function?
        # NB: this is such a pain to get right I'm adding lots of debug right now
        logger.debug("Organizing samples for {}".format(self.target))
        targetdir = os.path.dirname(self.target)
        flowcells = os.listdir(targetdir)
        bam_list = []
        for fc in flowcells:
            fc_dir = os.path.join(targetdir, fc)
            if not os.path.isdir(fc_dir):
                continue
            if not fc_dir.endswith("XX"):
                continue
            logger.debug("Looking in directory {}".format(fc))
            # This assumes only one sample run per flowcell
            bam_list.append(os.path.join(fc_dir, os.path.basename(rreplace(self.target, "{}{}".format(self.label, self.target_suffix), self.source_suffix, 1))))
        logger.debug("Generated target bamfile list {}".format(bam_list))
        return bam_list
    
class AlignmentMetrics(PicardJobTask):
    _config_subsection = "AlignmentMetrics"
    options = luigi.Parameter(default=None)
    target_suffix = luigi.Parameter(default=".align_metrics")
    
    def jar(self):
        return "CollectAlignmentSummaryMetrics.jar"
    def output(self):
        return luigi.LocalTarget(self.target)
    def args(self):
        return ["INPUT=", self.input(), "OUTPUT=", self.output()]

class InsertMetrics(PicardJobTask):
    _config_subsection = "InsertMetrics"
    options = luigi.Parameter(default=None)
    target_suffix = luigi.Parameter(default=[".insert_metrics", ".insert_hist"], is_list=True)
    
    def jar(self):
        return "CollectInsertSizeMetrics.jar"
    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)
    def output(self):
        return [luigi.LocalTarget(self.target),
                luigi.LocalTarget(rreplace(self.target, self.target_suffix[0], self.target_suffix[1], 1))]
    def args(self):
        return ["INPUT=", self.input(), "OUTPUT=", self.output()[0], "HISTOGRAM_FILE=", self.output()[1]]

class DuplicationMetrics(PicardJobTask):
    _config_subsection = "DuplicationMetrics"
    options = luigi.Parameter(default=None)
    label = luigi.Parameter(default=".dup")
    target_suffix = luigi.Parameter(default=[".bam", ".dup_metrics"], is_list=True)

    def jar(self):
        return "MarkDuplicates.jar"
    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)
    def output(self):
        return luigi.LocalTarget(self.target)
    def args(self):
        return ["INPUT=", self.input(), "OUTPUT=", self.output(), "METRICS_FILE=", rreplace(self.output().fn, "{}{}".format(self.label, self.target_suffix[0]), self.target_suffix[1], 1)]

class HsMetrics(PicardJobTask):
    _config_subsection = "HsMetrics"
    bam = luigi.Parameter(default=None)
    options = luigi.Parameter(default=None)
    baits = luigi.Parameter(default=None)
    targets = luigi.Parameter(default=None)
    target_suffix = luigi.Parameter(default=".hs_metrics")
    
    def jar(self):
        return "CalculateHsMetrics.jar"
    def output(self):
        return luigi.LocalTarget(self.target)
    def args(self):
        return ["INPUT=", self.input(), "OUTPUT=", self.output(), "BAIT_INTERVALS=", self.baits, "TARGET_INTERVALS=", self.targets]

class PicardMetrics(luigi.WrapperTask):
    target = luigi.Parameter(default=None)
    def requires(self):
        return [InsertMetrics(target=self.target + str(InsertMetrics.target_suffix.default[0])),
                # This currently gives the wrong target
                # DuplicationMetrics(target= self.target + str(DuplicationMetrics.label.default[0]) + str(DuplicationMetrics.target_suffix.default[0])),
                HsMetrics(target=self.target + str(HsMetrics.target_suffix.default)),
                AlignmentMetrics(target=self.target + str(AlignmentMetrics.target_suffix.default))]
