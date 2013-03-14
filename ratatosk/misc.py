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
import logging
from ratatosk.job import JobTask, DefaultShellJobRunner
from ratatosk.utils import rreplace

logger = logging.getLogger('luigi-interface')

class MiscJobRunner(DefaultShellJobRunner):
        pass

class InputFastqFile(JobTask):
    _config_section = "misc"
    _config_subsection = "InputFastqFile"
    target = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.FastqFile")
    
    def requires(self):
        cls = self.set_parent_task()
        return cls(target=self.target)
    def output(self):
        return luigi.LocalTarget(self.target)
    def run(self):
        pass

class ResyncMatesJobTask(JobTask):
    _config_section = "misc"
    _config_subsection = "ResyncMates"
    resyncmates = luigi.Parameter(default="resyncMates.pl")
    label = luigi.Parameter(default=".sync")
    target = luigi.Parameter(default=[], is_list=True)
    parent_task = luigi.Parameter(default="ratatosk.misc.InputFastqFile")
    read1_suffix = luigi.Parameter(default="_R1_001")
    read2_suffix = luigi.Parameter(default="_R2_001")

    def exe(self):
        return self.resyncmates

    def job_runner(self):
        return MiscJobRunner()

    def requires(self):
        cls = self.set_parent_task()
        sources = self._make_paired_source_file_names()
        return [cls(target=x) for x in sources]

    def output(self):
        return [luigi.LocalTarget(self.target[0]), luigi.LocalTarget(self.target[1])]

    def args(self):
        return ["-i", self.input()[0], "-j", self.input()[1], "-o", self.output()[0], "-p", self.output()[1]]

    # Put this here for now since this is the first case I've sofar
    # encountered where there is a 2-2 target-source mapping
    def _make_paired_source_file_names(self):
        """Construct source file name from a target.
        """
        source_list = self.target
        for source in source_list:
            if isinstance(self.target_suffix, tuple):
                if self.target_suffix[0] and self.source_suffix:
                    source = rreplace(source, self.target_suffix[0], self.source_suffix, 1)
            else:
                if self.target_suffix and self.source_suffix:
                    source = rreplace(source, self.target_suffix, self.source_suffix, 1)
            if not self.label:
                source_list.append(source)
            if source.count(self.label) > 1:
                logger.warn("label '{}' found multiple times in target '{}'; this could be intentional".format(self.label, source))
            elif source.count(self.label) == 0:
                logger.warn("label '{}' not found in target '{}'; are you sure your target is correctly formatted?".format(self.label, source))
        return [rreplace(x, self.label, "", 1) for x in source_list]


