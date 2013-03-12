import os
import sys
import yaml
import unittest
import luigi
import logging
import ratatosk
from ratatosk.interface import get_config
import ratatosk.bwa
import ratatosk.fastq
import ratatosk.gatk
import ratatosk.samtools
import ratatosk.picard
from luigi.mock import MockFile

logging.basicConfig(level=logging.DEBUG)

configfile = os.path.join(os.path.dirname(__file__), "pipeconf.yaml")
config = get_config(configfile)
    
class TestConfigParser(unittest.TestCase):
    yaml_config = None
    @classmethod
    def setUpClass(cls):
        with open(configfile) as fh:
            cls.yaml_config = yaml.load(fh)
            
        #print cls.yaml_config

    def test_get_config(self):
        local_config = get_config(configfile)
        self.assertIsInstance(local_config, ratatosk.yamlconfigparser.YAMLParserConfigHandler)
        
    def test_get_list(self):
        """Make sure list parsing ok"""
        self.assertIsInstance(config.get(section="gatk", option="knownSites"), list)
        self.assertListEqual(sorted(os.path.basename(x) for x in config.get(section="gatk", option="knownSites")), 
                             ['1000G_omni2.5.vcf', 'dbsnp132_chr11.vcf'])


# Setup Mock files to capture output from tasks
# Not quite sure how to capture stderr
class MockBwaAln(ratatosk.bwa.BwaAln):
    def output(self):
        return File('/tmp/test_jobtask_config', mirror_on_stderr=True)

class TestRatatoskConfig(unittest.TestCase):
    def setUp(self):
        global File
        File = MockFile
        MockFile._file_contents.clear()

    def test_dry_run(self):
        luigi.run(['--dry-run', '--bam', 'tabort.bam', '--config-file', '../config/ratatosk.yaml'], main_task_cls=ratatosk.gatk.UnifiedGenotyper)

    def test_jobtask_config(self):
        luigi.run(['--parent-task', 'new.module.path', '--fastq', 'tabort.fastq','--config-file', '../config/ratatosk.yaml'], main_task_cls=MockBwaAln)
