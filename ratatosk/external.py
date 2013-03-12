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
"""ratatosk.external

External tasks. The outputs of these tasks have
been created by some external process and therefore
have no run method.

"""
import os
import luigi
import logging

class BamFile(luigi.ExternalTask):
    bam = luigi.Parameter(default=None)

    def output(self):
        if not self.bam:
            return None
        return luigi.LocalTarget(os.path.abspath(self.bam))

class SamFile(luigi.ExternalTask):
    sam = luigi.Parameter(default=None)

    def output(self):
        if not self.sam:
            return None
        return luigi.LocalTarget(os.path.abspath(self.sam))
    
class FastqFile(luigi.ExternalTask):
    fastq = luigi.Parameter(default=None)

    def output(self):
        if not self.fastq:
            return None
        return luigi.LocalTarget(os.path.abspath(self.fastq))

class VcfFile(luigi.ExternalTask):
    vcf = luigi.Parameter(default=None)

    def output(self):
        if not self.vcf:
            return None
        return luigi.LocalTarget(os.path.abspath(self.vcf))
