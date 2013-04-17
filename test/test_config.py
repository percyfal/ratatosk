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
            fp.write(yaml.safe_dump({'ratatosk.lib.tools.gatk':{'java':'java', 'path': '$GATK_HOME_MOCK'},
                                     'ratatosk.lib.tools.picard':{'java':'java', 'path': '$PICARD_HOME_MOCK/test'}}, default_flow_style=False))

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
        self.assertIsInstance(cnf.get(section="ratatosk.lib.tools.gatk", option="knownSites"), list)
        self.assertListEqual(sorted(os.path.basename(x) for x in cnf.get(section="ratatosk.lib.tools.gatk", option="knownSites")), 
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
        self.assertEqual(os.getenv("GATK_HOME_MOCK"), cnf._sections['ratatosk.lib.tools.gatk']['path'])
        self.assertEqual(os.path.join(os.getenv("PICARD_HOME_MOCK"), "test"), cnf._sections['ratatosk.lib.tools.picard']['path'])
        cnf.del_config_path("mock.yaml")

class TestConfigUpdate(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        with open("mock.yaml", "w") as fp:
            fp.write(yaml.safe_dump({'ratatosk.lib.tools.gatk':{'parent_task':'another.class', 'UnifiedGenotyper':{'parent_task': 'no.such.class', 'options':['-stand_call_conf 10.0', '-stand_emit_conf 3.0']}}}, default_flow_style=False))
        with open("custommock.yaml", "w") as fp:
            fp.write(yaml.safe_dump({'ratatosk.lib.tools.gatk':{'parent_task':'another.class', 'UnifiedGenotyper':{'parent_task': 'no.such.class', 'options':['-stand_call_conf 20.0', '-stand_emit_conf 30.0']}}}, default_flow_style=False))


    @classmethod
    def tearDownClass(self):
        if os.path.exists("mock.yaml"):
            os.unlink("mock.yaml")
        cnf.clear()

    def test_config_update(self):
        """Test updating config with and without disable_parent_task_update"""
        # Main gatk task
        luigi.run(['--config-file', ratatosk_file, '--target', 'mock.fastq.gz', '--dry-run'], main_task_cls=ratatosk.lib.files.fastq.FastqFileLink)
        gatkjt = ratatosk.lib.tools.gatk.GATKJobTask()
        self.assertEqual(gatkjt.parent_task, ("ratatosk.lib.tools.gatk.InputBamFile", ))
        cnf.add_config_path("mock.yaml")
        kwargs = gatkjt._update_config(cnf, {})
        self.assertEqual(kwargs['parent_task'], 'another.class')
        kwargs = gatkjt._update_config(cnf, {}, disable_parent_task_update=True)
        self.assertIsNone(kwargs.get('parent_task'))
        cnf.del_config_path("mock.yaml")
        cnf.clear()

    def test_config_update_main(self):
        """Test updating main subsection"""
        # UnifiedGenotyper
        #
        # Incidentally, this verifies that subsection key value 'no.such.class'
        # overrides section key 'another.class'
        luigi.run(['--config-file', ratatosk_file, '--target', 'mock.bam', '--dry-run'], main_task_cls=ratatosk.lib.files.fastq.FastqFileLink)
        ug = ratatosk.lib.tools.gatk.UnifiedGenotyper()
        self.assertEqual(ug.parent_task, "ratatosk.lib.tools.gatk.ClipReads")
        cnf.del_config_path(ratatosk_file)
        cnf.add_config_path("mock.yaml")
        kwargs = ug._update_config(cnf, {})
        self.assertEqual(kwargs.get('parent_task'), 'no.such.class')
        kwargs = ug._update_config(cnf, {}, disable_parent_task_update=True)
        self.assertIsNone(kwargs.get('parent_task'))
        cnf.del_config_path("mock.yaml")

    def test_config_update_only_default(self):
        """Test that default parameters are correct"""
        ug = ratatosk.lib.tools.gatk.UnifiedGenotyper()
        for key, value in  ug.get_param_values(ug.get_params(), [], {}):
            self.assertEqual(value, ug.get_param_default(key))

    def test_config_update_with_config(self):
        """Test that configuration file overrides default values"""
        ug = ratatosk.lib.tools.gatk.UnifiedGenotyper()
        param_values_dict = {x[0]:x[1] for x in ug.get_param_values(ug.get_params(), [], {})}
        cnf = get_config()
        cnf.clear()
        cnf.add_config_path("mock.yaml")
        kwargs = ug._update_config(cnf, param_values_dict)
        self.assertEqual(kwargs['options'], ['-stand_call_conf 10.0', '-stand_emit_conf 3.0'])

    def test_config_update_with_custom_config(self):
        """Test that custom configuration overrides configuration setting"""
        ug = ratatosk.lib.tools.gatk.UnifiedGenotyper()
        param_values_dict = {x[0]:x[1] for x in ug.get_param_values(ug.get_params(), [], {})}
        cnf = get_config()
        cnf.clear()
        cnf.add_config_path("mock.yaml")
        customcnf = get_custom_config()
        customcnf.clear()
        customcnf.add_config_path("custommock.yaml")
        kwargs = ug._update_config(cnf, param_values_dict)
        self.assertEqual(kwargs['options'], ['-stand_call_conf 10.0', '-stand_emit_conf 3.0'])
        kwargs = ug._update_config(customcnf, param_values_dict, disable_parent_task_update=True)
        self.assertEqual(kwargs['options'], ['-stand_call_conf 20.0', '-stand_emit_conf 30.0'])

    def test_config_update_with_command_line_parameter(self):
        """Test that command line parameter overrides configuration setting"""
        ug = ratatosk.lib.tools.gatk.UnifiedGenotyper(options='test')
        param_values_dict = {x[0]:x[1] for x in ug.get_param_values(ug.get_params(), [], {'options':'test'})}
        cnf = get_config()
        cnf.clear()
        cnf.add_config_path("mock.yaml")
        customcnf = get_custom_config()
        customcnf.clear()
        customcnf.add_config_path("custommock.yaml")
        kwargs = ug._update_config(cnf, param_values_dict)
        self.assertEqual(kwargs['options'], ['-stand_call_conf 10.0', '-stand_emit_conf 3.0'])
        kwargs = ug._update_config(customcnf, param_values_dict, disable_parent_task_update=True)
        self.assertEqual(kwargs['options'], ['-stand_call_conf 20.0', '-stand_emit_conf 30.0'])
        for key, value in ug.get_params():
            new_value = None
            # Got a command line option => override config file. Currently overriding parent_task *is* possible here (FIX ME?)
            if value.default != param_values_dict.get(key, None):
                new_value = param_values_dict.get(key, None)
                kwargs[key] = new_value
        self.assertEqual(kwargs['options'], 'test')


class TestGlobalConfig(unittest.TestCase):
    def setUp(self):
        with open(ratatosk_file) as fp:
            self.ratatosk = yaml.load(fp)

    def test_global_config(self):
        """Test that backend.__global_config__ is updated correctly when instantiating a task.

        FIXME: currently not working, see :func:`ratatosk.job.BaseJobTask.__init__`"""
        backend.__global_config__ = {}
        cnf.clear()
        self.assertEqual(backend.__global_config__, {})
        cnf.add_config_path(ratatosk_file)
        ug = ratatosk.lib.tools.gatk.UnifiedGenotyper()
        ug._update_config(cnf, {})
        # self.assertEqual(backend.__global_config__['ratatosk.lib.tools.picard'], self.ratatosk['ratatosk.lib.tools.picard'])
        # self.assertEqual(backend.__global_config__['ratatosk.lib.tools.gatk'].get('UnifiedGenotyper').get('options'),
        #                  ('-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH',))
        cnf.clear()
