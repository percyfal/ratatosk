import os
import sys
import yaml
import unittest
import luigi
import logging
import yaml
import ratatosk
from ratatosk.interface import get_config, get_custom_config, RatatoskConfigParser
from ratatosk import interface
import ratatosk.lib.align.bwa
import ratatosk.lib.files.fastq
import ratatosk.lib.tools.gatk
import ratatosk.lib.tools.samtools
import ratatosk.lib.tools.picard
from luigi.mock import MockFile

logging.basicConfig(level=logging.DEBUG)

configfile = os.path.join(os.path.dirname(__file__), "pipeconf.yaml")
ratatosk_file = os.path.join(os.pardir, "config", "ratatosk.yaml")

class TestConfigParser(unittest.TestCase):
    yaml_config = None
    @classmethod
    def setUpClass(cls):
        with open(configfile) as fh:
            cls.yaml_config = yaml.load(fh)

    def setUp(self):
        self.config = get_config()
        self.assertEqual([], self.config._config_paths)
        os.environ["GATK_HOME_MOCK"] = os.path.abspath(os.curdir)
        os.environ["PICARD_HOME_MOCK"] = os.path.abspath(os.curdir)
        with open("mock.yaml", "w") as fp:
            fp.write(yaml.safe_dump({'gatk':{'java':'java', 'path': '$GATK_HOME_MOCK'},
                                     'picard':{'java':'java', 'path': '$PICARD_HOME_MOCK/test'}}, default_flow_style=False))

    def tearDown(self):
        if os.path.exists("mock.yaml"):
            os.unlink("mock.yaml")
        self.config._config_paths = []
        del os.environ["GATK_HOME_MOCK"]
        del os.environ["PICARD_HOME_MOCK"]

    def test_get_config(self):
        """Test getting config instance"""
        cnf = get_config()
        cnf.add_config_path(configfile)
        self.assertIsInstance(cnf, ratatosk.interface.RatatoskConfigParser)
        cnf.del_config_path(configfile)
        
    def test_get_list(self):
        """Make sure list parsing ok"""
        cnf = get_config()
        cnf.add_config_path(configfile)
        self.assertIsInstance(cnf.get(section="gatk", option="knownSites"), list)
        self.assertListEqual(sorted(os.path.basename(x) for x in cnf.get(section="gatk", option="knownSites")), 
                             ['1000G_omni2.5.vcf', 'dbsnp132_chr11.vcf'])
        cnf.del_config_path(configfile)

    def test_add_config_path(self):
        """Test adding same config again"""
        cnf = get_config()
        cnf.add_config_path(configfile)
        self.assertEqual(1, len(cnf._config_paths))
        cnf.del_config_path(configfile)

    def test_del_config_path(self):
        """Test deleting config path"""
        cnf = get_config()
        cnf.del_config_path(configfile)
        self.assertEqual([], cnf._config_paths)
        
    def test_expand_vars(self):
        cnf = get_config()
        cnf.add_config_path("mock.yaml")
        self.assertEqual(os.getenv("GATK_HOME_MOCK"), cnf._sections['gatk']['path'])
        self.assertEqual(os.path.join(os.getenv("PICARD_HOME_MOCK"), "test"), cnf._sections['picard']['path'])

class TestConfigUpdate(unittest.TestCase):
    def setUp(self):
        global File
        File = MockFile
        MockFile._file_contents.clear()
        self.cnf = get_config()
        self.cnf.del_config_path("mock.yaml")
        with open("mock.yaml", "w") as fp:
            fp.write(yaml.safe_dump({'gatk':{'parent_task':'another.class', 'UnifiedGenotyper':{'parent_task': 'no.such.class'}}}, default_flow_style=False))

    def tearDown(self):
        if os.path.exists("mock.yaml"):
            os.unlink("mock.yaml")
        self.cnf._config_paths = []

    def test_config_update(self):
        """Test updating config with and without disable_parent_task_update"""
        # Main gatk task
        gatkjt = ratatosk.lib.tools.gatk.GATKJobTask()
        self.assertEqual(gatkjt.parent_task, "ratatosk.lib.tools.gatk.InputBamFile")
        self.cnf.add_config_path("mock.yaml")
        kwargs = gatkjt._update_config(self.cnf)
        self.assertEqual(kwargs['parent_task'], 'another.class')
        kwargs = gatkjt._update_config(self.cnf, disable_parent_task_update=True)
        self.assertIsNone(kwargs.get('parent_task'))
        self.cnf.del_config_path("mock.yaml")

    def test_config_update_main(self):
        """Test updating main subsection"""
        # UnifiedGenotyper
        #
        # Incidentally, this verifies that subsection key value 'no.such.class'
        # overrides section key 'another.class'
        ug = ratatosk.lib.tools.gatk.UnifiedGenotyper()
        self.assertEqual(ug.parent_task, "ratatosk.lib.tools.gatk.ClipReads")
        self.cnf.del_config_path(ratatosk_file)
        self.cnf.add_config_path("mock.yaml")
        kwargs = ug._update_config(self.cnf)
        self.assertEqual(kwargs.get('parent_task'), 'no.such.class')
        kwargs = ug._update_config(self.cnf, disable_parent_task_update=True)
        self.assertIsNone(kwargs.get('parent_task'))
        self.cnf.del_config_path("mock.yaml")

class TestGlobalConfig(unittest.TestCase):
    def setUp(self):
        self.cnf = get_config()
        with open(ratatosk_file) as fp:
            self.ratatosk = yaml.load(fp)
        self.cnf.del_config_path(ratatosk_file)

    def tearDown(self):
        self.cnf._config_paths = []

    def test_global_config(self):
        """Test that interface.global_config is updated correctly when instantiating a task"""
        # Is updated by other tests, so can't be sure it's empty
        # self.assertEqual(interface.global_config, {})
        interface.global_config = {}
        ug = ratatosk.lib.tools.gatk.UnifiedGenotyper()
        ug._update_config(self.cnf)
        self.assertEqual(interface.global_config['picard'], self.ratatosk['picard'])
        self.assertEqual(interface.global_config['gatk'].get('UnifiedGenotyper').get('options'),
                         ('-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH',))
