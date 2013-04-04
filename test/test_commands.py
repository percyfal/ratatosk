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

# NOTE: this test suite only verifies that the commands are formatted
# correctly. No actual spawning of subprocesses is done.

import os
import unittest
import yaml
import luigi
import logging
import ratatosk
from ratatosk.job import JobTask
from ratatosk.lib.align.bwa import BwaIndex
import ratatosk.lib.tools.picard
from nose.plugins.attrib import attr

logging.basicConfig(level=logging.DEBUG)

# Check for central planner
local_scheduler = '--local-scheduler'
process = os.popen("ps x -o pid,args | grep ratatoskd | grep -v grep").read() #sometimes have to use grep -v grep

if process:
    local_scheduler = None

def _luigi_args(args):
    if local_scheduler:
        return [local_scheduler] + args
    return args

ref = "data/chr11.fa"
read1 = "data/read1.fastq.gz"
read2 = "data/read2.fastq.gz"
localconf = "mock.yaml"

def setUpModule():
    luigi.build([BwaIndex(target=ref + ".bwt")])
    with open(localconf, "w") as fp:
        fp.write(yaml.safe_dump({
                    'bwa' :{
                        'InputFastqFile': {'target_suffix':'.fastq.gz'},
                        'bwaref': 'data/chr11.fa',
                        'sampe':{'read1_suffix':"1",
                                 'read2_suffix':"2"},
                        },
                    'picard' : {
                        'InputBamFile' :
                            {'parent_task': 'ratatosk.lib.tools.samtools.SamToBam'},
                        'SortSam':
                            {'parent_task': 'ratatosk.lib.tools.samtools.SamToBam'},
                        'DuplicationMetrics':
                            {'parent_task': 'ratatosk.lib.tools.picard.SortSam'},
                        'AlignmentMetrics' :
                            {'parent_task': 'ratatosk.lib.tools.picard.DuplicationMetrics'},
                        'InsertMetrics' :
                            {'parent_task' : 'ratatosk.lib.tools.picard.DuplicationMetrics'},
                        'HsMetrics' :
                            {'parent_task' : 'ratatosk.lib.tools.picard.DuplicationMetrics',
                             'bait_regions' : 'data/chr11_baits.interval_list',
                             'target_regions' : 'data/chr11_targets.interval_list'},
                        }
                    },
                                default_flow_style=False))

def tearDownModule():
    if os.path.exists(localconf):
        os.unlink(localconf)
    files = os.listdir("data")
    [os.unlink(os.path.join("data", x)) for x in files if x.startswith("chr11.fa.")]

@attr("full")
class TestCommand(unittest.TestCase):
    def tearDown(self):
        files = os.listdir("data")
        [os.unlink(os.path.join("data", x)) for x in files if x.startswith("read.")]
        [os.unlink(os.path.join("data", x)) for x in files if x.endswith(".sai")]

    def test_bwaaln(self):
        luigi.run(_luigi_args(['--target', read1.replace(".fastq.gz", ".sai"), '--config-file', localconf]),
                  main_task_cls=ratatosk.lib.align.bwa.BwaAln)

    def test_bwasampe(self):
        luigi.run(_luigi_args(['--target', read1.replace("1.fastq.gz", ".sam"), '--config-file', localconf, '--use-long-names']), main_task_cls=ratatosk.lib.align.bwa.BwaSampe)

    def test_sortbam(self):
        luigi.run(_luigi_args(['--target', read1.replace("1.fastq.gz", ".sort.bam"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.SortSam)

    def test_picard_metrics(self):
        luigi.run(_luigi_args(['--target', read1.replace("1.fastq.gz", ".sort.dup"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.PicardMetrics)
