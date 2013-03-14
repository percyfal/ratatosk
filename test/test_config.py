import os
import sys
import yaml
import unittest
import luigi
import logging
import yaml
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
            
    def test_get_config(self):
        local_config = get_config(configfile)
        self.assertIsInstance(local_config, ratatosk.yamlconfigparser.YAMLParserConfigHandler)
        
    def test_get_list(self):
        """Make sure list parsing ok"""
        self.assertIsInstance(config.get(section="gatk", option="knownSites"), list)
        self.assertListEqual(sorted(os.path.basename(x) for x in config.get(section="gatk", option="knownSites")), 
                             ['1000G_omni2.5.vcf', 'dbsnp132_chr11.vcf'])

class TestConfigUpdate(unittest.TestCase):
    def setUp(self):
        global File
        File = MockFile
        MockFile._file_contents.clear()

    def test_config_update(self):
        """Test updating config with and without disable_parent_task_update"""
        # reading mock in _update_config doesn't work
        #mock_config = File("/tmp/mock.yaml")
        #fp = mock_config.open("w")
        #fp.close()
        with open("mock.yaml", "w") as fp:
            fp.write(yaml.safe_dump({'gatk':{'parent_task':'another.class', 'UnifiedGenotyper':{'parent_task': 'no.such.class'}}}, default_flow_style=False))

        # Main gatk task
        gatkjt = ratatosk.gatk.GATKJobTask()
        self.assertEqual(gatkjt.parent_task, "ratatosk.gatk.InputBamFile")
        kwargs = gatkjt._update_config("mock.yaml")
        self.assertEqual(kwargs, {'parent_task':'another.class'})
        kwargs = gatkjt._update_config("mock.yaml", disable_parent_task_update=True)
        self.assertEqual(kwargs, {})

        # UnifiedGenotyper
        #
        # Incidentally, this verifies that subsection key value 'no.such.class'
        # overrides section key 'another.class'
        ug = ratatosk.gatk.UnifiedGenotyper()
        self.assertEqual(ug.parent_task, "ratatosk.gatk.ClipReads")
        kwargs = ug._update_config("mock.yaml")
        self.assertEqual(kwargs, {'parent_task':'no.such.class'})
        kwargs = ug._update_config("mock.yaml", disable_parent_task_update=True)
        self.assertEqual(kwargs, {})
        os.unlink("mock.yaml")
        

