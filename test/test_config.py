import os
import sys
import yaml
import unittest
import luigi
import logging
import yaml
import ratatosk
from ratatosk.config import get_config, get_custom_config, RatatoskConfigParser
from ratatosk import interface, backend
import ratatosk.lib.align.bwa
import ratatosk.lib.files.fastq
import ratatosk.lib.tools.gatk
import ratatosk.lib.tools.samtools
import ratatosk.lib.tools.picard

logging.basicConfig(level=logging.DEBUG)

configfile = os.path.join(os.path.dirname(__file__), "pipeconf.yaml")
ratatosk_file = os.path.join(os.pardir, "config", "ratatosk.yaml")


def setUpModule():
    global cnf
    cnf = get_custom_config()
    cnf.clear()

def tearDownModule():
    cnf.clear()

# FIX ME: 
class TestConfigParser(unittest.TestCase):
    yaml_config = None
    @classmethod
    def setUpClass(cls):
        with open(configfile) as fh:
            cls.yaml_config = yaml.load(fh)
        os.environ["GATK_HOME_MOCK"] = os.path.abspath(os.curdir)
        os.environ["PICARD_HOME_MOCK"] = os.path.abspath(os.curdir)
        with open("mock.yaml", "w") as fp:
            fp.write(yaml.safe_dump({'gatk':{'java':'java', 'path': '$GATK_HOME_MOCK'},
                                     'picard':{'java':'java', 'path': '$PICARD_HOME_MOCK/test'}}, default_flow_style=False))

    @classmethod
    def tearDownClass(cls):
        if os.path.exists("mock.yaml"):
            os.unlink("mock.yaml")
        del os.environ["GATK_HOME_MOCK"]
        del os.environ["PICARD_HOME_MOCK"]


    def setUp(self):
        cnf.clear()
        self.assertEqual([], cnf._instance._custom_config_paths)

    def tearDown(self):
        cnf.clear()


    def test_get_config(self):
        """Test getting config instance"""
        cnf.add_config_path(configfile)
        self.assertIsInstance(cnf, ratatosk.config.RatatoskConfigParser)
        cnf.del_config_path(configfile)
        
    def test_get_list(self):
        """Make sure list parsing ok"""
        cnf.add_config_path(configfile)
        self.assertIsInstance(cnf.get(section="gatk", option="knownSites"), list)
        self.assertListEqual(sorted(os.path.basename(x) for x in cnf.get(section="gatk", option="knownSites")), 
                             ['knownSites1.vcf', 'knownSites2.vcf'])
        cnf.del_config_path(configfile)

    def test_add_config_path(self):
        """Test adding same config again"""
        cnf.add_config_path(configfile)
        self.assertEqual(1, len(cnf._instance._custom_config_paths))
        cnf.del_config_path(configfile)

    def test_del_config_path(self):
        """Test deleting config path"""
        cnf.add_config_path(configfile)
        cnf.del_config_path(configfile)
        self.assertEqual([], cnf._instance._custom_config_paths)
        
    def test_expand_vars(self):
        cnf = get_config()
        cnf.add_config_path("mock.yaml")
        self.assertEqual(os.getenv("GATK_HOME_MOCK"), cnf._sections['gatk']['path'])
        self.assertEqual(os.path.join(os.getenv("PICARD_HOME_MOCK"), "test"), cnf._sections['picard']['path'])
        cnf.del_config_path("mock.yaml")

class TestConfigUpdate(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        with open("mock.yaml", "w") as fp:
            fp.write(yaml.safe_dump({'gatk':{'parent_task':'another.class', 'UnifiedGenotyper':{'parent_task': 'no.such.class'}}}, default_flow_style=False))

    @classmethod
    def tearDownClass(self):
        if os.path.exists("mock.yaml"):
            os.unlink("mock.yaml")
        cnf.clear()

    def test_config_update(self):
        """Test updating config with and without disable_parent_task_update"""
        # Main gatk task
        #
        # Setting parent_task is necessary since the config file has
        # been set in test_command.py
        cnf.add_config_path(ratatosk_file)
        gatkjt = ratatosk.lib.tools.gatk.GATKJobTask()
        print "Parent task: " + str(gatkjt.parent_task)
        kwargs = gatkjt._update_config(cnf)

        cnf.del_config_path(ratatosk_file)
        self.assertEqual(gatkjt.parent_task, "ratatosk.lib.tools.gatk.InputBamFile")
        cnf.add_config_path("mock.yaml")
        kwargs = gatkjt._update_config(cnf)
        self.assertEqual(kwargs['parent_task'], 'another.class')
        kwargs = gatkjt._update_config(cnf, disable_parent_task_update=True)
        self.assertIsNone(kwargs.get('parent_task'))
        cnf.del_config_path("mock.yaml")
        cnf.clear()

    def test_config_update_main(self):
        """Test updating main subsection"""
        # UnifiedGenotyper
        #
        # Incidentally, this verifies that subsection key value 'no.such.class'
        # overrides section key 'another.class'
        ug = ratatosk.lib.tools.gatk.UnifiedGenotyper()
        self.assertEqual(ug.parent_task, "ratatosk.lib.tools.gatk.ClipReads")
        cnf.del_config_path(ratatosk_file)
        cnf.add_config_path("mock.yaml")
        kwargs = ug._update_config(cnf)
        self.assertEqual(kwargs.get('parent_task'), 'no.such.class')
        kwargs = ug._update_config(cnf, disable_parent_task_update=True)
        self.assertIsNone(kwargs.get('parent_task'))
        cnf.del_config_path("mock.yaml")

class TestGlobalConfig(unittest.TestCase):
    def setUp(self):
        with open(ratatosk_file) as fp:
            self.ratatosk = yaml.load(fp)

    def test_global_config(self):
        """Test that backend.__global_config__ is updated correctly when instantiating a task"""
        backend.__global_config__ = {}
        cnf.clear()
        self.assertEqual(backend.__global_config__, {})
        cnf.add_config_path(ratatosk_file)
        ug = ratatosk.lib.tools.gatk.UnifiedGenotyper()
        ug._update_config(cnf)
        self.assertEqual(backend.__global_config__['picard'], self.ratatosk['picard'])
        self.assertEqual(backend.__global_config__['gatk'].get('UnifiedGenotyper').get('options'),
                         ('-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH',))
        cnf.clear()
