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
Provide wrappers for `picard <http://picard.sourceforge.net/>`_


Classes
-------
"""

import os
import luigi
import re
import ratatosk.lib.files.input
from ratatosk.utils import rreplace
from ratatosk.config import get_config
from ratatosk.job import JobWrapperTask, JobTask
from ratatosk.jobrunner import JavaJobRunner
from ratatosk.handler import RatatoskHandler, register_task_handler
from ratatosk.log import get_logger

logger = get_logger()

class InputBamFile(ratatosk.lib.files.input.InputBamFile):
    pass

class InputFastaFile(ratatosk.lib.files.input.InputFastaFile):
    suffix = luigi.Parameter(default=".fa")

class PicardJobRunner(JavaJobRunner):
    pass

class PicardJobTask(JobTask):
    java_exe = "java"
    java_options = luigi.Parameter(default=("-Xmx2g",), is_list=True)
    exe_path = luigi.Parameter(default=os.getenv("PICARD_HOME") if os.getenv("PICARD_HOME") else os.curdir)
    executable = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.picard.InputBamFile", ), is_list=True)
    suffix = luigi.Parameter(default=".bam")
    ref = luigi.Parameter(default=None)
    validation_stringency = luigi.Parameter(default="SILENT", description="Validation stringency to use")

    def jar(self):
        """Path to the jar for this Picard job"""
        return self.executable
    
    def java_opt(self):
        return list(self.java_options)

    def exe(self):
        return self.jar()

    def job_runner(self):
        return PicardJobRunner()

    def java(self):
        return self.java_exe

    def opts(self):
        if not re.search("VALIDATION_STRINGENCY", " ".join(list(self.options))):
            return list(self.options) + ["VALIDATION_STRINGENCY={}".format(self.validation_stringency)]
        return list(self.options)

class CreateSequenceDictionary(PicardJobTask):
    executable = "CreateSequenceDictionary.jar"
    suffix = luigi.Parameter(default=".dict")
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.picard.InputFastaFile", ), is_list=True)

    def args(self):
        return ["REFERENCE=", self.input()[0], "OUTPUT=", self.output()]

class SortSam(PicardJobTask):
    executable = "SortSam.jar"
    options = luigi.Parameter(default=("SO=coordinate MAX_RECORDS_IN_RAM=750000",), is_list=True)
    label = luigi.Parameter(default=".sort")

    def args(self):
        return ["INPUT=", self.input()[0], "OUTPUT=", self.output()]

class MergeSamFiles(PicardJobTask):
    executable = "MergeSamFiles.jar"
    label = luigi.Parameter(default=".merge")
    read1_suffix = luigi.Parameter(default="_R1_001")
    target_generator_handler = luigi.Parameter(default=None)
    # FIXME: TMP_DIR should not be hard-coded
    options = luigi.Parameter(default=("SO=coordinate TMP_DIR=./tmp", ), is_list=True)

    def args(self):
        return ["OUTPUT=", self.output()] + [item for sublist in [["INPUT=", x] for x in self.input()] for item in sublist]

    def requires(self):
        cls = self.parent()[0]
        sources = []
        cnf = get_config()
        if self.target_generator_handler and "target_generator_handler" not in self._handlers.keys():
            tgf = RatatoskHandler(label="target_generator_handler", mod=self.target_generator_handler)
            register_task_handler(self, tgf)
        if not "target_generator_handler" in self._handlers.keys():
            logger.warn("MergeSamFiles requires a target generator handler; no defaults are as of yet implemented")
            return []
        sources = list(set(self._handlers["target_generator_handler"](self)))
        return [cls(target=src) for src in sources]    
    
class AlignmentMetrics(PicardJobTask):
    executable = "CollectAlignmentSummaryMetrics.jar"
    suffix = luigi.Parameter(default=".align_metrics")

    def opts(self):
        retval = list(self.options)
        if not re.search("VALIDATION_STRINGENCY", " ".join(list(self.options))):
            retval += ["VALIDATION_STRINGENCY={}".format(self.validation_stringency)]
        if self.ref:
            retval += [" REFERENCE_SEQUENCE={}".format(self.ref)]
        return retval

    def args(self):
        return ["INPUT=", self.input()[0], "OUTPUT=", self.output()]

class InsertMetrics(PicardJobTask):
    executable = "CollectInsertSizeMetrics.jar"
    suffix = luigi.Parameter(default=(".insert_metrics", ".insert_hist"), is_list=True)

    def opts(self):
        retval = list(self.options)
        if not re.search("VALIDATION_STRINGENCY", " ".join(list(self.options))):
            retval += ["VALIDATION_STRINGENCY={}".format(self.validation_stringency)]
        if self.ref:
            retval += [" REFERENCE_SEQUENCE={}".format(self.ref)]
        return retval
    
    def output(self):
        return [luigi.LocalTarget(self.target),
                luigi.LocalTarget(rreplace(self.target, self.suffix[0], self.suffix[1], 1))]

    def args(self):
        return ["INPUT=", self.input()[0], "OUTPUT=", self.output()[0], "HISTOGRAM_FILE=", self.output()[1]]

class DuplicationMetrics(PicardJobTask):
    executable = "MarkDuplicates.jar"
    label = luigi.Parameter(default=".dup")
    suffix = luigi.Parameter(default=(".bam", ".dup_metrics"), is_list=True)

    def args(self):
        return ["INPUT=", self.input()[0], "OUTPUT=", self.output(), "METRICS_FILE=", rreplace(self.output().path, "{}{}".format(self.label, self.suffix[0]), self.suffix[1], 1)]

class HsMetrics(PicardJobTask):
    executable = "CalculateHsMetrics.jar"
    bait_regions = luigi.Parameter(default=None)
    target_regions = luigi.Parameter(default=None)
    suffix = luigi.Parameter(default=".hs_metrics")
    
    def opts(self):
        retval = list(self.options)
        if not re.search("VALIDATION_STRINGENCY", " ".join(list(self.options))):
            retval += ["VALIDATION_STRINGENCY={}".format(self.validation_stringency)]
        if self.ref:
            retval += [" REFERENCE_SEQUENCE={}".format(self.ref)]
        return retval

    def args(self):
        if not self.bait_regions or not self.target_regions:
            raise Exception("need bait and target regions to run CalculateHsMetrics")
        return ["INPUT=", self.input()[0], "OUTPUT=", self.output(), "BAIT_INTERVALS=", os.path.expanduser(self.bait_regions), "TARGET_INTERVALS=", os.path.expanduser(self.target_regions)]

class HsMetricsNonDup(HsMetrics):
    """Run on non-deduplicated data"""
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.picard.MergeSamFiles", ), is_list=True)

class PicardMetrics(JobWrapperTask):
    suffix = luigi.Parameter(default=("", ), is_list=True)
    def requires(self):
        return [InsertMetrics(target=self.target + str(InsertMetrics().sfx())),
                HsMetrics(target=self.target + str(HsMetrics().sfx())),
                AlignmentMetrics(target=self.target + str(AlignmentMetrics().sfx()))]

class PicardMetricsNonDup(JobWrapperTask):
    """Runs hs metrics on both duplicated and de-duplicated data"""
    def requires(self):
        return [InsertMetrics(target=self.target + str(InsertMetrics().suffix)),
                HsMetrics(target=self.target + str(HsMetrics().suffix)),
                HsMetricsNonDup(target=rreplace(self.target, str(DuplicationMetrics().label), "", 1) + str(HsMetrics().suffix)),
                AlignmentMetrics(target=self.target + str(AlignmentMetrics().suffix))]

