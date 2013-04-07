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
"""ratatosk.lib.files.external

External tasks. The outputs of these tasks have
been created by some external process and therefore
have no run method.

"""
import os
import luigi
import logging

logger = logging.getLogger('luigi-interface')

class BamFile(luigi.ExternalTask):
    target = luigi.Parameter(default=None)
    label = luigi.Parameter(default=None)

    def output(self):
        if not self.target:
            return None
        return luigi.LocalTarget(os.path.abspath(self.target))

class SamFile(luigi.ExternalTask):
    target = luigi.Parameter(default=None)
    label = luigi.Parameter(default="")

    def output(self):
        logger.debug("Got target '{}' in {}".format(self.target, self.__class__))
        if not self.target:
            return None
        return luigi.LocalTarget(os.path.abspath(self.target))
    
class FastqFile(luigi.ExternalTask):
    target = luigi.Parameter(default=None)
    label = luigi.Parameter(default=None)
    suffix = luigi.Parameter(default=".fastq.gz")

    def output(self):
        if not self.target:
            return None
        return luigi.LocalTarget(os.path.abspath(self.target))

class FastaFile(luigi.ExternalTask):
    target = luigi.Parameter(default=None)
    label = luigi.Parameter(default=None)

    def output(self):
        if not self.target:
            return None
        return luigi.LocalTarget(os.path.abspath(self.target))

class VcfFile(luigi.ExternalTask):
    target = luigi.Parameter(default=None)
    label = luigi.Parameter(default=None)

    def output(self):
        if not self.target:
            return None
        return luigi.LocalTarget(os.path.abspath(self.target))

class TxtFile(luigi.ExternalTask):
    target = luigi.Parameter(default=None)
    label = luigi.Parameter(default=None)

    def output(self):
        if not self.target:
            return None
        return luigi.LocalTarget(os.path.abspath(self.target))

class Path(luigi.ExternalTask):
    target = luigi.Parameter(default=None)
    label = luigi.Parameter(default=None)

    def output(self):
        if not self.target:
            return None
        return luigi.LocalTarget(os.path.abspath(self.target))
