import os
import glob
import shutil
import sys
import unittest
import luigi
import time
import yaml
import logging
import gzip
from luigi.mock import MockFile
from ratatosk.job import JobTask
import ratatosk.lib.files.external
import ratatosk.lib.files.fastq
import ratatosk.lib.tools.samtools
import ratatosk.lib.tools.fastqc
import ratatosk.lib.utils.cutadapt
import ratatosk.lib.utils.misc
import ngstestdata as ntd

logging.basicConfig(level=logging.DEBUG)

bwa = "bwa"
samtools = "samtools"
bwaref = os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "bwa", "chr11.fa"))
bwaseqref = os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "seq", "chr11.fa"))
baitregions = os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "seqcap", "chr11_baits.interval_list"))
targetregions = os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "seqcap", "chr11_targets.interval_list"))
indir = os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "projects", "J.Doe_00_01", "P001_101_index3", "121015_BB002BBBXX"))
projectdir = os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "projects", "J.Doe_00_01"))
sample = "P001_101_index3_TGACCA_L001"
fastq1 = os.path.join(os.curdir, sample + "_R1_001.fastq.gz")
fastq2 = os.path.join(os.curdir, sample + "_R2_001.fastq.gz")

sai1 = os.path.join(sample + "_R1_001.sai")
sai2 = os.path.join(sample + "_R2_001.sai")

sam = os.path.join(sample + ".sam")
bam = os.path.join(sample + ".bam")
sortbam = os.path.join(sample + ".sort.bam")
realignbam = os.path.join(sample + ".sort.realign.bam")
recalbam = os.path.join(sample + ".sort.realign.recal.bam")
clipbam = os.path.join(sample + ".sort.realign.recal.clip.bam")
clipvcf = os.path.join(sample + ".sort.realign.recal.clip.vcf")
filteredvcf = os.path.join(sample + ".sort.realign.recal.clip.filtered.vcf")

localconf = "pipeconf.yaml"
local_scheduler = '--local-scheduler'
process = os.popen("ps x -o pid,args | grep ratatoskd | grep -v grep").read() #sometimes have to use grep -v grep

if process:
    local_scheduler = None

def _luigi_args(args):
    if local_scheduler:
        return [local_scheduler] + args
    return args

class TestSamtoolsWrappers(unittest.TestCase):
    def setUp(self):
        global File
        File = MockFile
        MockFile._file_contents.clear()

    def tearDown(self):
        Pfiles = glob.glob("P*")
        [os.unlink(x) for x in Pfiles if os.path.isfile(x)]

    def test_samtools_view(self):
        luigi.run(_luigi_args(['--target', bam, '--config-file', localconf, '--parent-task', 'ratatosk.lib.align.bwa.BwaSampe']), main_task_cls=ratatosk.lib.tools.samtools.SamToBam)
        self.assertTrue(os.path.exists(bam))
        with open(sam) as fp:
            h2 = fp.readlines()[0:2]
            self.assertEqual(h2, ['@SQ\tSN:chr11\tLN:2000000\n', '@RG\tID:P001_101_index3_TGACCA_L001_R1_001\tSM:P001_101_index3_TGACCA_L001_R1_001\tPL:Illumina\n'])
            

class TestMiscWrappers(unittest.TestCase):
    def tearDown(self):
        Pfiles = glob.glob("P*")
        [os.unlink(x) for x in Pfiles if os.path.isfile(x)]
        [shutil.rmtree(x) for x in Pfiles if os.path.isdir(x)]
        
    def test_luigihelp(self):
        try:
            luigi.run(['-h'], main_task_cls=ratatosk.lib.files.fastq.FastqFileLink)
        except:
            pass

    def test_fastqln(self):
        luigi.run(_luigi_args(['--target', fastq1, '--config-file', localconf, '--use-long-names']), main_task_cls=ratatosk.lib.files.fastq.FastqFileLink)
        self.assertTrue(os.path.exists(fastq1))
        with gzip.open(fastq1) as fp:
            h2 = fp.readlines()[0:2]
            self.assertEqual(h2, ['@SRR362778.2383730/1\n', 'GCATTACCCTGATACCAAAACCAGAGAAGGACACTATAATAAAAATAAATTGCAGACCAATACTCCTGATGAACTT\n'])
 
    def test_cutadapt(self):
        luigi.run(_luigi_args(['--target', os.path.basename(fastq1.replace(".fastq.gz", ".trimmed.fastq.gz")), '--config-file', localconf, '--parent-task', 'ratatosk.lib.files.fastq.FastqFileLink']), main_task_cls=ratatosk.lib.utils.cutadapt.CutadaptJobTask)
        luigi.run(_luigi_args(['--target', os.path.basename(fastq2.replace(".fastq.gz", ".trimmed.fastq.gz")), '--config-file', localconf, '--parent-task', 'ratatosk.lib.files.fastq.FastqFileLink']), main_task_cls=ratatosk.lib.utils.cutadapt.CutadaptJobTask)
        mfile = "P001_101_index3_TGACCA_L001_R1_001.trimmed.fastq.cutadapt_metrics"
        self.assertTrue(os.path.exists(mfile))
        with open(mfile) as fp:
            metrics = fp.readlines()
            self.assertIn("Adapter 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC', length 34, was trimmed 31 times.\n", metrics)
       
    def test_fastqc(self):
        luigi.run(_luigi_args(['--target', os.path.basename(fastq1), '--config-file', localconf, '--parent-task', 'ratatosk.lib.files.fastq.FastqFileLink']), main_task_cls=ratatosk.lib.tools.fastqc.FastQCJobTask)
        fqc = os.path.join(os.curdir, os.path.basename(fastq1).replace(".fastq.gz", "_fastqc"), os.path.basename(fastq1).replace(".fastq.gz", "_fastqc"), "summary.txt")
        self.assertTrue(os.path.exists(fqc))
        with open(fqc) as fh:
            summary = fh.readlines()
            self.assertEqual('PASS\tBasic Statistics\tP001_101_index3_TGACCA_L001_R1_001.fastq.gz\n', summary[0])

    def test_resyncmates_after_trim(self):
        luigi.run(_luigi_args(['--target', fastq1.replace(".fastq.gz", ".trimmed.sync.fastq.gz"),
                               '--target', fastq2.replace(".fastq.gz", ".trimmed.sync.fastq.gz"),
                               '--parent-task', 'ratatosk.lib.utils.cutadapt.CutadaptJobTask',
                               '--config-file', localconf]), main_task_cls=ratatosk.lib.utils.misc.ResyncMatesJobTask)

    def test_bwaaln_after_trim_resyncmates(self):
        with open("mock.yaml", "w") as fp:
            fp.write(yaml.safe_dump({
                        'cutadapt':{'InputFastqFile':{'parent_task': 'ratatosk.lib.files.fastq.FastqFileLink'}},
                        'misc':{'ResyncMates':{'parent_task': 'ratatosk.lib.utils.cutadapt.CutadaptJobTask'}},
                        'fastq':{'link':{'indir': indir}},
                        'bwa' :{
                            'bwaref': bwaref,
                            'aln':{'parent_task':'ratatosk.lib.utils.misc.ResyncMatesJobTask'}}}, default_flow_style=False))
        luigi.run(_luigi_args(['--target', sai1.replace(".sai", ".trimmed.sync.sai"),
                               '--config-file', "mock.yaml"]), main_task_cls=ratatosk.lib.align.bwa.BwaAln)
        luigi.run(_luigi_args(['--target', sai2.replace(".sai", ".trimmed.sync.sai"),
                               '--config-file', "mock.yaml"]), main_task_cls=ratatosk.lib.align.bwa.BwaAln)
        os.unlink("mock.yaml")
        self.assertTrue(os.path.exists(sai1.replace(".sai", ".trimmed.sync.sai")))

class TestBwaWrappers(unittest.TestCase):
    def tearDown(self):
        Pfiles = glob.glob("P*")
        [os.unlink(x) for x in Pfiles if os.path.isfile(x)]

    def test_bwaaln(self):
        luigi.run(_luigi_args(['--target', sai1, '--config-file', localconf, '--use-long-names']), main_task_cls=ratatosk.lib.align.bwa.BwaAln)
        luigi.run(_luigi_args(['--target', sai2, '--config-file', localconf, '--use-long-names']), main_task_cls=ratatosk.lib.align.bwa.BwaAln)
      
    def test_bwasampe(self):
        luigi.run(_luigi_args(['--target', sam, '--config-file', localconf, '--use-long-names']), main_task_cls=ratatosk.lib.align.bwa.BwaSampe)

    def test_sortbam(self):
        luigi.run(_luigi_args(['--target', sortbam, '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.samtools.SortBam)

class TestPicardWrappers(unittest.TestCase):
    def tearDown(self):
        Pfiles = glob.glob("P*")
        [os.unlink(x) for x in Pfiles if os.path.isfile(x)]

    def test_picard_sortbam(self):
        luigi.run(_luigi_args(['--target', sortbam, '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.SortSam)

    def test_picard_alignmentmetrics(self):
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".align_metrics"),'--options', 'REFERENCE_SEQUENCE={}'.format(bwaseqref), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.AlignmentMetrics)

    def test_picard_insertmetrics(self):
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".insert_metrics"), '--options', 'REFERENCE_SEQUENCE={}'.format(bwaseqref), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.InsertMetrics)

    def test_picard_dupmetrics(self):
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".dup.bam"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.DuplicationMetrics)

    def test_picard_hsmetrics(self):
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".hs_metrics"), '--config-file', localconf, '--target-regions', targetregions, '--bait-regions', baitregions]), main_task_cls=ratatosk.lib.tools.picard.HsMetrics)

    def test_picard_metrics(self):
       luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ""), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.PicardMetrics)

class TestGATKWrappers(unittest.TestCase):
    def tearDown(self):
        Pfiles = glob.glob("P*")
        [os.unlink(x) for x in Pfiles if os.path.isfile(x)]

    # Depends on previous tasks (sortbam) - bam must be present
    def test_realigner_target_creator(self):
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".intervals"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.RealignerTargetCreator)
        
    def test_indel_realigner(self):
        luigi.run(_luigi_args(['--target', realignbam, '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.IndelRealigner)

    def test_base_recalibrator(self):
        luigi.run(_luigi_args(['--target', realignbam.replace(".bam", ".recal_data.grp"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.BaseRecalibrator)

    def test_print_reads(self):
        luigi.run(_luigi_args(['--target', recalbam, '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.PrintReads)

    def test_clip_reads(self):
        luigi.run(_luigi_args(['--target', clipbam, '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.ClipReads)

    # TODO: Test vcf outputs
    def test_unified_genotyper(self):
        luigi.run(_luigi_args(['--target', clipvcf, '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.UnifiedGenotyper)

    def test_variant_filtration(self):
        luigi.run(_luigi_args(['--target', filteredvcf, '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.VariantFiltration)

    def test_variant_evaluation(self):
        luigi.run(_luigi_args(['--target', filteredvcf.replace(".vcf", ".eval_metrics"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.VariantEval)

