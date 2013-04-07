import os
import shutil
import unittest
import luigi
import logging
import yaml
from itertools import izip
import ratatosk.lib.align.bwa as BWA
import ratatosk.lib.tools.samtools as SAM
import ratatosk.lib.files.fastq as FASTQ
import ratatosk.lib.tools.picard as PICARD
import ratatosk.lib.tools.gatk as GATK
import ratatosk.lib.utils.cutadapt as CUTADAPT
import ratatosk.lib.tools.fastqc as FASTQC
import ratatosk.lib.files.external
from ratatosk.config import get_config
from ratatosk.utils import make_fastq_links, rreplace

logging.basicConfig(level=logging.DEBUG)
sample = "P001_101_index3_TGACCA_L001"
bam = os.path.join(sample + ".bam")
localconf = "mock.yaml"
ratatosk_conf = os.path.join(os.path.dirname(__file__), os.pardir, "config", "ratatosk.yaml")

def setUpModule():
    global cnf
    cnf = get_config()
    with open(localconf, "w") as fp:
        fp.write(yaml.safe_dump({
                    'picard' : {
                        'InsertMetrics' :
                            {'parent_task' : 'ratatosk.lib.tools.picard.DuplicationMetrics'},
                        },
                    'gatk' : 
                    {
                        'IndelRealigner' :
                            {'parent_task': ['ratatosk.lib.tools.picard.MergeSamFiles',
                                             'ratatosk.lib.tools.gatk.RealignerTargetCreator',
                                             'ratatosk.lib.tools.gatk.UnifiedGenotyper'],
                             'source_label': [None, None, 'BOTH.raw'],
                             'source_suffix' : ['.bam', '.intervals', '.vcf'],
                             },
                        'RealignerTargetCreator' :
                            {'parent_task' : 'ratatosk.lib.align.bwa.BwaAln'},
                        }
                    },
                                default_flow_style=False))
        # Need to add ratatosk first, then override with localconf
        cnf.add_config_path(ratatosk_conf)
        cnf.add_config_path(localconf)

def tearDownModule():
    # if os.path.exists(localconf):
    #     os.unlink(localconf)
    cnf.clear()

class TestGeneralFunctions(unittest.TestCase):
    def test_make_source_file_name_from_string(self):
        """Test generating source file names from strings only"""
        def _make_source_file_name(target, label, src_suffix, tgt_suffix, src_label=None):
            # If tgt_suffix is list, target suffix should always
            # correspond to tgt_suffix[0]
            source = target
            if isinstance(tgt_suffix, tuple) or isinstance(tgt_suffix, list):
                tgt_suffix = tgt_suffix[0]
            if tgt_suffix and not src_suffix is None:
                if src_label:
                    # Trick: remove src_label first if present since
                    # the source label addition here corresponds to a
                    # "diff" compared to target name
                    source = rreplace(rreplace(source, tgt_suffix, "", 1), src_label, "", 1) + src_label + src_suffix
                else:
                    source = rreplace(source, tgt_suffix, src_suffix, 1)
            if label:
                if source.count(label) > 1:
                    print "label '{}' found multiple times in target '{}'; this could be intentional".format(label, source)
                elif source.count(label) == 0:
                    print "label '{}' not found in target '{}'; are you sure your target is correctly formatted?".format(label, source)
                source = rreplace(source, label, "", 1)
            return source
        # Test IndelRealigner source name generation. IndelRealigner
        # takes as input at least a bam file and realign intervals,
        # and optionally vcf sources (and more...)
        target = ".merge.realign.bam"
        source_suffix = (".bam", ".intervals", ".vcf")
        source_label = (None, None, ".BOTH.raw")
        label = ".realign"
        out_fn = []
        for src_sfx, src_lab in izip(source_suffix, source_label):
            out_fn.append(_make_source_file_name(target, label, src_sfx, ".bam", src_lab))
        self.assertEqual(out_fn, [".merge.bam", ".merge.intervals", ".merge.BOTH.raw.vcf"])
        source_label = (".merge", ".merge", ".BOTH.raw")
        out_fn = []
        for src_sfx, src_lab in izip(source_suffix, source_label):
            out_fn.append(_make_source_file_name(target, label, src_sfx, ".bam", src_lab))
        self.assertEqual(out_fn, [".merge.bam", ".merge.intervals", ".merge.BOTH.raw.vcf"])

        # Test ReadBackedPhasing where the variant suffix can differ
        # much from the original bam file
        target = ".merge-variants-combined-phased.vcf"
        source_suffix = (".bam", ".vcf")
        source_label = (None, None)
        label = "-phased"
        out_fn = []
        for src_sfx, src_lab in izip(source_suffix, source_label):
            out_fn.append(_make_source_file_name(target, label, src_sfx, ".bam", src_lab))
        out_fn = []
        source_label = ("-variants-combined", None)
        for src_sfx, src_lab in izip(source_suffix, source_label):
            out_fn.append(_make_source_file_name(target, label, src_sfx, ".bam", src_lab))

    def test_make_source_file_name_from_class(self):
        """Test generating source file names from classes, utilizing
        the fact that the classes themselves contain the information
        we request (label and source_suffix). Problem is they are not
        instantiated.
        """
        def _make_source_file_name(target_cls, source_cls, diff_label=None):
            src_label = source_cls().label
            tgt_suffix = target_cls.target_suffix
            src_suffix = source_cls().target_suffix
            if isinstance(tgt_suffix, tuple) or isinstance(tgt_suffix, list):
                tgt_suffix = tgt_suffix[0]
            if isinstance(src_suffix, tuple) or isinstance(src_suffix, list):
                src_suffix = src_suffix[0]
            # Start by stripping tgt_suffix
            if tgt_suffix:
                source = rreplace(target_cls.target, tgt_suffix, "", 1)
            else:
                source = target_cls.target
            # Then remove the target label and diff_label
            source = rreplace(source, target_cls.label, "", 1)
            if diff_label:
                source = rreplace(source, str(diff_label), "", 1)
            if src_label:
                # Trick: remove src_label first if present since
                # the source label addition here corresponds to a
                # "diff" compared to target name
                source = rreplace(source, str(src_label), "", 1) + str(src_label) + str(src_suffix)
            else:
                source = source + str(src_suffix)
            if src_label:
                if source.count(str(src_label)) > 1:
                    print "label '{}' found multiple times in target '{}'; this could be intentional".format(src_label, source)
                elif source.count(src_label) == 0:
                    print "label '{}' not found in target '{}'; are you sure your target is correctly formatted?".format(src_label, source)

            return source
        # Test IndelRealigner source name generation. IndelRealigner
        # takes as input at least a bam file and realign intervals,
        # and optionally vcf sources (and more...)
        target = ".merge.realign.bam"
        s = ratatosk.lib.tools.gatk.IndelRealigner(target=target, 
                                                   parent_task=['ratatosk.lib.tools.picard.MergeSamFiles',
                                                                'ratatosk.lib.tools.gatk.RealignerTargetCreator',
                                                                'ratatosk.lib.tools.gatk.UnifiedGenotyper',])
        out_fn = []
        for p in s.parent():
            out_fn.append(_make_source_file_name(s, p))
        self.assertEqual(out_fn, [".merge.bam", ".merge.intervals", ".merge.vcf"])
        # Test ReadBackedPhasing where the variant suffix can differ
        # much from the original bam file
        target = ".merge-variants-combined-phased.vcf"
        out_fn = []
        s = ratatosk.lib.tools.gatk.ReadBackedPhasing(target=target)
        for p, dl in izip(s.parent(), s.diff_label):
            out_fn.append(_make_source_file_name(s, p, dl))
        self.assertEqual(out_fn, ['.merge.bam', '.merge-variants-combined.vcf'])

        # Test picard metrics with two output files
        target = ".merge.dup.insert_metrics"
        s = ratatosk.lib.tools.picard.InsertMetrics(target=target)
        self.assertEqual(_make_source_file_name(s, s.parent().pop()), ".merge.dup.bam")

class TestFunctions(unittest.TestCase):
    def setUp(self):
        pass

    def test_make_fastq_links(self):
        """Test making fastq links"""
        pass
        # tl = target_generator(indir=self.project)
        # fql = make_fastq_links(tl, indir=self.project, outdir="tmp")
        # self.assertTrue(os.path.lexists(os.path.join("tmp", os.path.relpath(tl[0][2], self.project) + "_R1_001.fastq.gz")))
        # self.assertTrue(os.path.lexists(os.path.join("tmp", os.path.dirname(os.path.relpath(tl[0][2], self.project)),
        #                                              "SampleSheet.csv")))
