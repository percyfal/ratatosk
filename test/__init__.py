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
import logging
import time
import unittest
from mako.template import Template

logging.basicConfig(level=logging.INFO)
msg = "Not all required environment variables have been set; at least GATK_HOME and PICARD_HOME need to be set"
def has_environment_variables():
    # Currently need to check for GATK_HOME and PICARD_HOME
    has_gatk = not (os.getenv("GATK_HOME") is None or os.getenv("GATK_HOME") == "")
    has_picard = not (os.getenv("PICARD_HOME") is None or os.getenv("PICARD_HOME") == "")
    return all([has_gatk, has_picard])

ngsloadmsg = "No ngstestdata module; skipping test. Do a 'git clone https://github.com/percyfal/ngs.test.data' followed by 'python setup.py develop'"
has_data = False
try:
    import ngstestdata as ntd
    has_data = True
except:
    logging.warn(ngsloadmsg)
    time.sleep(3)

@unittest.skipIf(not has_data, ngsloadmsg)
@unittest.skipIf(not has_environment_variables(), msg)
def setUpModule():
    """Set up requirements for test suite"""
    config = {
        'bwaref' : os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "bwa", "chr11.fa")),
        'fastq' : os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "projects", "J.Doe_00_01", "P001_101_index3", "121015_BB002BBBXX")),
        'knownSites1' : os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "variation", "dbsnp132_chr11.vcf")),
        'knownSites2' : os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "variation", "1000G_omni2.5.vcf")),
        'ref' : os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "seq", "chr11.fa")),
        'dbsnp' : os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "variation", "dbsnp132_chr11.vcf")),
        'baits' : os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "seqcap", "chr11_baits.interval_list")),
        'targets' : os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "seqcap", "chr11_targets.interval_list"))
        }
    config_template = Template(filename=os.path.join(os.path.dirname(__file__), "pipeconf.yaml.mako"))
    with open("pipeconf.yaml", "w") as fh:
        fh.write(config_template.render(**config))
