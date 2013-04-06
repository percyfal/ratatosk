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
from ratatosk.config import get_config
from subprocess import Popen, PIPE
from ratatosk.job import JobTask, DefaultShellJobRunner, InputJobTask
from ratatosk.lib.align.bwa import BwaIndex, Bampe
import ratatosk.lib.tools.picard
from nose.plugins.attrib import attr

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

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
    global cnf
    cnf = get_config()
    cnf.clear()
    with open(localconf, "w") as fp:
        fp.write(yaml.safe_dump({
                    'bwa' :{
                        'InputFastqFile': {'target_suffix':'.fastq.gz'},
                        'bwaref': 'data/chr11.fa',
                        'sampe':{'read1_suffix':"1",
                                 'read2_suffix':"2"},
                        'Bampe':{'read1_suffix':"1",
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
                        },
                    'gatk' : 
                    {
                        'UnifiedGenotyper' : {'ref': 'data/chr11.fa'},
                        'CombineVariants' : {'ref': 'data/chr11.fa'},
                        }
                    },
                                default_flow_style=False))
        
def tearDownModule():
    if os.path.exists(localconf):
        os.unlink(localconf)
    cnf.clear()

@attr("full")
class TestCommand(unittest.TestCase):
    @classmethod 
    def setUpClass(cls):
        luigi.build([BwaIndex(target=ref + ".bwt")])

    @classmethod
    def tearDownClass(cls):
        files = os.listdir("data")
        [os.unlink(os.path.join("data", x)) for x in files if x.startswith("chr11.fa.")]

    def tearDown(self):
        files = [x for x in os.listdir("data") if os.path.isfile(x)]
        [os.unlink(os.path.join("data", x)) for x in files if x.startswith("read.")]
        [os.unlink(os.path.join("data", x)) for x in files if x.endswith(".sai")]

    def test_bwaaln(self):
        luigi.run(_luigi_args(['--target', read1.replace(".fastq.gz", ".sai"), '--config-file', localconf]),
                  main_task_cls=ratatosk.lib.align.bwa.BwaAln)

    def test_bwasampe(self):
        from ratatosk.handler import register, RatatoskHandler
        key = "target_generator_handler"
        h = RatatoskHandler(label=key, mod="test.site_functions.target_generator")
        register(h)
        luigi.run(_luigi_args(['--target', read1.replace("1.fastq.gz", ".sam"), '--config-file', localconf, '--use-long-names']), main_task_cls=ratatosk.lib.align.bwa.BwaSampe)

    def test_sortbam(self):
        luigi.run(_luigi_args(['--target', read1.replace("1.fastq.gz", ".sort.bam"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.SortSam)

    def test_picard_metrics(self):
        luigi.run(_luigi_args(['--target', read1.replace("1.fastq.gz", ".sort.dup"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.PicardMetrics)



##############################
# Pipe tests
##############################
class TestJobRunner(DefaultShellJobRunner):
    def run_job(self, job):
        (arglist, tmp_files) = self._make_arglist(job)
        if job.pipe:
            return arglist
        cmd = ' '.join(arglist)
        logger.info("\nJob runner '{0}';\n\trunning command '{1}'\n".format(self.__class__, cmd))
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        (stdout, stderr) = proc.communicate()
        proc.wait()
        if proc.returncode == 0:
            logger.info("Shell job completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                a.move(os.path.join(os.curdir, b.path))
        else:
            raise Exception("Job '{}' failed: \n{}".format(' '.join(arglist), " ".join([stderr])))

class InputPath(InputJobTask):
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.Path")
    
class Task1(JobTask):
    """Task1 : list contents of current directory"""
    executable = luigi.Parameter(default="ls")

    def output(self):
        if self.pipe:
            return 
        else:
            return luigi.LocalTarget(self.target)

    def requires(self):
        return InputPath(target=os.curdir)

    def job_runner(self):
        return TestJobRunner()

    def args(self):
        if self.pipe:
            return [self.input()]
        else:
            return [self.input(), ">", self.output()]

class Task2(JobTask):
    """Task2 : reverse sort output from Task1"""
    executable = luigi.Parameter(default="sort")
    options = luigi.Parameter(default=("-r", ))
    parent_task = luigi.Parameter(default="test.test_commands.Task1")
    label = luigi.Parameter(default=".sort")

    def output(self):
        if self.pipe:
            return 
        else:
            return luigi.LocalTarget(self.target)

    def job_runner(self):
        return TestJobRunner()

    def args(self):
        if self.pipe:
            return [self.input()]
        else:
            return [self.input(), ">", self.output()]

class PipedJobRunner(DefaultShellJobRunner):
    def run_job(self, job):
        cmdlist = []
        tmp_files = []
        for j in job.args():
            cmdlist.append(j.job_runner()._make_arglist(j)[0])
        cmdlist[0]  += [job.args()[0].input().path]
        plist = []
        plist.append(Popen(cmdlist[0], stdout=PIPE))
        for i in xrange(1, len(cmdlist)):
            plist.append(Popen(cmdlist[1], stdin=plist[i-1].stdout, stdout=PIPE))
        pipe = Popen("cat > {}".format(job.target), stdin=plist[-1].stdout, shell=True)
        out, err = pipe.communicate()

class PipedTask(JobTask):
    tasks = luigi.Parameter(default=[], is_list=True)

    def requires(self):
        return InputPath(target=os.curdir)
    
    def output(self):
        return luigi.LocalTarget(self.target)

    def job_runner(self):
        return PipedJobRunner()

    def args(self):
        return self.tasks
    
@attr("full")
class TestPipedCommand(unittest.TestCase):
    def test_ls(self):
        luigi.build([Task1(target="output.txt")])
        luigi.run(['--pipe', '--target', "output.txt"], main_task_cls=Task1)

    def test_sort(self):
        luigi.build([Task2(target="output.sort.txt")])
        luigi.run(['--pipe', '--target', "output.sort.txt"], main_task_cls=Task2)

    def test_pipe(self):
        t1 = Task1(target="output.txt", pipe=True)
        t2 = Task2(target="output.sort.txt", pipe=True)
        pt = PipedTask(tasks=[t1, t2], target="output.sort.txt")
        luigi.build([pt])

    def test_bampe(self):
        luigi.run(['--target', "data/read.bam", '--config-file', localconf],main_task_cls=Bampe)
