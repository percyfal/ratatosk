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
import pysam
from ratatosk.job import JobTask
import ratatosk.lib.files.external
import ratatosk.lib.files.fastq
import ratatosk.lib.tools.samtools
import ratatosk.lib.tools.picard
import ratatosk.lib.tools.gatk
import ratatosk.lib.tools.fastqc
import ratatosk.lib.utils.cutadapt
import ratatosk.lib.utils.misc
import ratatosk.lib.annotation.snpeff
import ratatosk.lib.annotation.annovar
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
samplename = "P001_101_index3"
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
        """Setup samtools wrappers by linking fastq files from ngs_test_data"""
        for f in [fastq1, fastq2]:
            if not os.path.exists(f):
                os.symlink(os.path.join(indir, os.path.basename(f)), f)

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
    def setUp(self):
        """Setup misc wrappers by linking fastq files from ngs_test_data"""
        for f in [fastq1, fastq2]:
            if not os.path.exists(f):
                os.symlink(os.path.join(indir, os.path.basename(f)), f)

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
        if os.path.exists(fastq1):
            os.unlink(fastq1)
        luigi.run(_luigi_args(['--target', fastq1, '--config-file', localconf, '--use-long-names']), main_task_cls=ratatosk.lib.files.fastq.FastqFileLink)
        self.assertTrue(os.path.exists(fastq1))
        with gzip.open(fastq1) as fp:
            h2 = fp.readlines()[0:3]
            self.assertTrue(h2[0].startswith("@SRR"))
            self.assertEqual(h2[2].rstrip(), "+")

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
    def setUp(self):
        """Setup bwa wrappers by linking fastq files from ngs_test_data"""
        for f in [fastq1, fastq2]:
            if not os.path.exists(f):
                os.symlink(os.path.join(indir, os.path.basename(f)), f)

    def tearDown(self):
        Pfiles = glob.glob("P*")
        [os.unlink(x) for x in Pfiles if os.path.isfile(x)]

    def test_bwaaln(self):
        luigi.run(_luigi_args(['--target', sai1, '--config-file', localconf, '--use-long-names']), main_task_cls=ratatosk.lib.align.bwa.BwaAln)
        luigi.run(_luigi_args(['--target', sai2, '--config-file', localconf, '--use-long-names']), main_task_cls=ratatosk.lib.align.bwa.BwaAln)
        self.assertTrue(os.path.exists(sai1))
        self.assertTrue(os.path.exists(sai2))

    def test_bwasampe(self):
        luigi.run(_luigi_args(['--target', sam, '--config-file', localconf, '--use-long-names']), main_task_cls=ratatosk.lib.align.bwa.BwaSampe)
        self.assertTrue(os.path.exists(sam))
        with open(sam) as fp:
            h2 = fp.readlines()[0:2]
            self.assertEqual(h2, ['@SQ\tSN:chr11\tLN:2000000\n', '@RG\tID:P001_101_index3_TGACCA_L001_R1_001\tSM:P001_101_index3_TGACCA_L001_R1_001\tPL:Illumina\n'])

    def test_sortbam(self):
        luigi.run(_luigi_args(['--target', sortbam, '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.samtools.SortBam)
        self.assertTrue(os.path.exists(sortbam))
        fp = pysam.Samfile(sortbam)
        pos = [x.pos for x in fp if x.pos>0]
        self.assertListEqual(pos[0:10], sorted(pos[0:10]))

class TestPicardWrappers(unittest.TestCase):
    def setUp(self):
        """Setup picard wrappers by linking fastq files from ngs_test_data"""
        for f in [fastq1, fastq2]:
            if not os.path.exists(f):
                os.symlink(os.path.join(indir, os.path.basename(f)), f)

    def tearDown(self):
        Pfiles = glob.glob("P*")
        [os.unlink(x) for x in Pfiles if os.path.isfile(x)]

    def test_picard_sortbam(self):
        luigi.run(_luigi_args(['--target', sortbam, '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.SortSam)
        self.assertTrue(os.path.exists(sortbam))
        fp = pysam.Samfile(sortbam)
        pos = [x.pos for x in fp if x.pos>0]
        self.assertListEqual(pos[0:10], sorted(pos[0:10]))

    def test_picard_alignmentmetrics(self):
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".align_metrics"),'--options', 'REFERENCE_SEQUENCE={}'.format(bwaseqref), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.AlignmentMetrics)
        self.assertTrue(os.path.exists(sortbam.replace(".bam", ".align_metrics")))
        with open(sortbam.replace(".bam", ".align_metrics")) as fh:
            metrics = fh.readlines()
            self.assertTrue(metrics[7].startswith("FIRST_OF_PAIR\t1001"))
            self.assertTrue(metrics[9].startswith("PAIR\t2002"))

    # NB: for insertmetrics, dupmetrics, hsmetrics we only assert
    # existence of file since contents are likely to change depending
    # on what version of the reference is used
    def test_picard_insertmetrics(self):
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".insert_metrics"), '--options', 'REFERENCE_SEQUENCE={}'.format(bwaseqref), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.InsertMetrics)
        self.assertTrue(os.path.exists(sortbam.replace(".bam", ".insert_metrics")))
        self.assertTrue(os.path.exists(sortbam.replace(".bam", ".insert_hist")))

    def test_picard_dupmetrics(self):
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".dup.bam"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.DuplicationMetrics)
        self.assertTrue(os.path.exists(sortbam.replace(".bam", ".dup_metrics")))

    def test_picard_hsmetrics(self):
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".hs_metrics"), '--config-file', localconf, '--target-regions', targetregions, '--bait-regions', baitregions]), main_task_cls=ratatosk.lib.tools.picard.HsMetrics)
        self.assertTrue(os.path.exists(sortbam.replace(".bam", ".hs_metrics")))

    def test_picard_metrics(self):
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ""), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.PicardMetrics)
        self.assertTrue(os.path.exists(sortbam.replace(".bam", ".insert_metrics")))
        self.assertTrue(os.path.exists(sortbam.replace(".bam", ".insert_hist")))
        self.assertTrue(os.path.exists(sortbam.replace(".bam", ".align_metrics")))
        self.assertTrue(os.path.exists(sortbam.replace(".bam", ".hs_metrics")))


# The following tests are different in that they require the entire
# project directory. Since so many GATK programs depend on previous
# output I have grouped most function calls in one test.
#
# TODO: make single tests of functions that only require the mergebam
# output. This should be in setUp
class TestGATKWrappers(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        fastqfiles = []
        ssheetfiles = []
        for root, dirs, files in os.walk(projectdir):
            fastq = [os.path.join(root, x) for x in files if x.endswith(".fastq.gz")]
            if fastq:
                fastqfiles.extend(fastq)
        for fq in fastqfiles:
            outdir = os.path.join(os.curdir, os.path.relpath(os.path.dirname(fq), projectdir))
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            fqlink = os.path.join(outdir, os.path.basename(fq))
            if not os.path.lexists(fqlink):
                os.symlink(os.path.join(os.path.relpath(os.path.dirname(fq), os.path.dirname(fqlink)), os.path.basename(fq)), fqlink)
            if os.path.exists(os.path.join(os.path.dirname(fq), "SampleSheet.csv")):
                if not os.path.lexists(os.path.join(outdir, "SampleSheet.csv")):
                    os.symlink(os.path.join(os.path.relpath(os.path.dirname(fq), os.path.dirname(fqlink)), "SampleSheet.csv"), os.path.join(outdir, "SampleSheet.csv"))
        self.mergebam = os.path.join(os.curdir, samplename, "{}.sort.merge.bam".format(samplename))

    def tearDown(self):
        Pfiles = glob.glob("P*")
        [os.unlink(x) for x in Pfiles if os.path.isfile(x)]
        [shutil.rmtree(x) for x in Pfiles if os.path.isdir(x)]

    def test_gatk(self):
        """Test running all steps for gatk from merging samples.
        By running all steps we don't need to redo alignment, sorting etc for each test.
        """
        luigi.run(_luigi_args(['--target', self.mergebam, '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.picard.MergeSamFiles)
        self.assertTrue(os.path.exists(self.mergebam))
        fp = pysam.Samfile(self.mergebam)
        read_groups = [x.tags[0][1] for x in fp]
        qlen = [x.qlen for x in fp]
        rg_set = set(read_groups)
        # Should have 4004 reads in total, 2002 from each read group.
        # Note that this could change if the test data set size is
        # changed.
        self.assertEqual(len(read_groups), 4004)
        self.assertEqual(sum([x == list(rg_set)[0] for x in read_groups]), 2002)
        self.assertEqual(sum([x == list(rg_set)[1] for x in read_groups]), 2002)

        # Run realignment
        luigi.run(_luigi_args(['--target', self.mergebam.replace(".bam", ".intervals"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.RealignerTargetCreator)
        self.assertTrue(os.path.exists, self.mergebam.replace(".bam", ".intervals"))
        with open(self.mergebam.replace(".bam", ".intervals")) as fh:
            intervals = [x.strip() for x in fh.readlines()]
            self.assertIn("chr11:75977-76011", intervals)

        luigi.run(_luigi_args(['--target', self.mergebam.replace(".bam", ".realign.bam"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.IndelRealigner)
        self.assertTrue(os.path.exists, self.mergebam.replace(".bam", ".realign.bam"))
        fp = pysam.Samfile(self.mergebam.replace(".bam", ".realign.bam"))
        self.assertIn("GATK IndelRealigner", [x['ID'] for x in fp.header['PG']])

        luigi.run(_luigi_args(['--target', self.mergebam.replace(".bam", ".realign.recal_data.grp"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.BaseRecalibrator)
        self.assertTrue(os.path.exists, self.mergebam.replace(".bam", ".realign.recal_data.grp"))
        with open(self.mergebam.replace(".bam", ".realign.recal_data.grp")) as fh:
            recaldata = [x.strip() for x in fh.readlines()[0:3]]
            self.assertTrue(recaldata[0].startswith("#:GATKReport"))
    
        luigi.run(_luigi_args(['--target', self.mergebam.replace(".bam", ".realign.recal.bam"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.PrintReads)
        self.assertTrue(os.path.exists(self.mergebam.replace(".bam", ".realign.recal.bam")))
        fp = pysam.Samfile(self.mergebam.replace(".bam", ".realign.recal.bam"))
        tagkeys = [x[0] for x in fp.next().tags]
        self.assertIn("BI", tagkeys)
        self.assertIn("BD", tagkeys)
        
        luigi.run(_luigi_args(['--target', self.mergebam.replace(".bam", ".realign.recal.clip.bam"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.ClipReads)
        self.assertTrue(os.path.exists(self.mergebam.replace(".bam", ".realign.recal.clip.bam")))
        fp = pysam.Samfile(self.mergebam.replace(".bam", ".realign.recal.clip.bam"))
        qlen_clip = [x.qlen for x in fp]
        fp = pysam.Samfile(self.mergebam)
        qlen = [x.qlen for x in fp]
        # Some clipping should have been done
        self.assertLessEqual(sum(qlen_clip), sum(qlen))
        
        luigi.run(_luigi_args(['--target', self.mergebam.replace(".bam", ".realign.recal.clip.vcf"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.UnifiedGenotyper)
        self.assertTrue(os.path.exists(self.mergebam.replace(".bam", ".realign.recal.clip.vcf")))
        self.assertTrue(os.path.exists(self.mergebam.replace(".bam", ".realign.recal.clip.vcf.idx")))

        luigi.run(_luigi_args(['--target', self.mergebam.replace(".bam", ".realign.recal.clip.filtered.vcf"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.VariantFiltration)
        self.assertTrue(os.path.exists(self.mergebam.replace(".bam", ".realign.recal.clip.filtered.vcf")))
        self.assertTrue(os.path.exists(self.mergebam.replace(".bam", ".realign.recal.clip.filtered.vcf.idx")))

        luigi.run(_luigi_args(['--target', self.mergebam.replace(".bam", ".realign.recal.clip.filtered.eval_metrics"), '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.gatk.VariantEval)
        self.assertTrue(os.path.exists(self.mergebam.replace(".bam", ".realign.recal.clip.filtered.eval_metrics")))


has_annovar = not (os.getenv("ANNOVAR_HOME") is None or os.getenv("ANNOVAR_HOME") == "")
has_snpeff = not (os.getenv("SNPEFF_HOME") is None or os.getenv("SNPEFF_HOME") == "")
class TestAnnotationWrapper(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        """Setup bwa wrappers by linking fastq files from ngs_test_data"""
        for f in [fastq1, fastq2]:
            if not os.path.exists(f):
                os.symlink(os.path.join(indir, os.path.basename(f)), f)
        luigi.run(_luigi_args(['--target', sortbam, '--config-file', localconf]), main_task_cls=ratatosk.lib.tools.samtools.SortBam)
        self.bam = sortbam
        luigi.run(_luigi_args(['--target', self.bam.replace(".bam", ".vcf")]), main_task_cls=ratatosk.lib.tools.gatk.UnifiedGenotyper)

    def tearDown(self):
        Pfiles = glob.glob("P*")
        [os.unlink(x) for x in Pfiles if os.path.isfile(x)]

    @unittest.skipIf(not has_snpeff, "No SNPEFF_HOME set; skipping")   
    def test_snpeff(self):
        luigi.run(_luigi_args(['--target', self.bam.replace(".bam", "-effects.vcf"), '--config-file', localconf]), main_task_cls=ratatosk.lib.annotation.snpeff.snpEff)
        with open(self.bam.replace(".bam", "-effects.vcf")) as fh:
            lines = [x.strip() for x in fh.readlines()]
        self.assertTrue(lines[0].startswith("##fileformat=VCF"))

    @unittest.skipIf(not has_snpeff, "No SNPEFF_HOME set; skipping")  
    def test_snpeff_txt(self):
        luigi.run(_luigi_args(['--target', self.bam.replace(".bam", "-effects.txt"), '--target-suffix', '.txt', '--config-file', localconf]), main_task_cls=ratatosk.lib.annotation.snpeff.snpEff)
        with open(self.bam.replace(".bam", "-effects.txt")) as fh:
            lines = [x.strip() for x in fh.readlines()]
        self.assertTrue(lines[0].startswith("# SnpEff version"))
        self.assertTrue(lines[2].startswith("# Chromo"))

    def test_GATK_variant_annotator(self):
        luigi.run(_luigi_args(['--target', self.bam.replace(".bam", "-gatkann.vcf"), '--config-file', localconf, '--parent-task', 'ratatosk.lib.tools.gatk.UnifiedGenotyper']), main_task_cls=ratatosk.lib.tools.gatk.VariantAnnotator)
        with open(self.bam.replace(".bam", "-gatkann.vcf")) as fh:
            lines = [x.strip() for x in fh.readlines()]
        self.assertTrue(any([x.startswith('##INFO=<ID=MQRankSum') for x in lines]))
        
    @unittest.skipIf(not has_snpeff, "No SNPEFF_HOME set; skipping")  
    def test_GATK_snpeff_variant_annotator(self):
        luigi.run(_luigi_args(['--target', self.bam.replace(".bam", "-annotated.vcf"), '--config-file', localconf, '--parent-task', 'ratatosk.lib.tools.gatk.UnifiedGenotyper']), main_task_cls=ratatosk.lib.tools.gatk.VariantSnpEffAnnotator)
        with open(self.bam.replace(".bam", "-annotated.vcf")) as fh:
            lines = [x.strip() for x in fh.readlines()]
        self.assertTrue(any([x.startswith('##OriginalSnpEffVersion="2.0.5') for x in lines]))

    @unittest.skipIf(not has_annovar, "No ANNOVAR_HOME set; skipping")
    def test_convert_annovar(self):
        luigi.run(_luigi_args(['--target', self.bam.replace(".bam", "-avinput.txt"), '--config-file', localconf, '--parent-task', 'ratatosk.lib.tools.gatk.UnifiedGenotyper']), main_task_cls=ratatosk.lib.annotation.annovar.Convert2Annovar)
        self.assertTrue(os.path.exists(self.bam.replace(".bam", "-avinput.txt")))
        with open(self.bam.replace(".bam", "-avinput.txt")) as fh:
            data = fh.readlines()
        self.assertEqual("chr11", data[0].split()[0])

    @unittest.skipIf(not has_annovar, "No ANNOVAR_HOME set; skipping")
    def test_summarize_annovar(self):
        luigi.run(_luigi_args(['--target', self.bam.replace(".bam", "-avinput.txt.log"), '--config-file', localconf]), main_task_cls=ratatosk.lib.annotation.annovar.SummarizeAnnovar)
        self.assertTrue(os.path.exists(self.bam.replace(".bam", "-avinput.txt.exome_summary.csv")))
        self.assertTrue(os.path.exists(self.bam.replace(".bam", "-avinput.txt.genome_summary.csv")))
