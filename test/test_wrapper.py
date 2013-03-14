import os
import glob
import sys
import unittest
import luigi
import time
import yaml
import logging
from ratatosk.job import JobTask
import ratatosk.bwa as BWA
import ratatosk.samtools as SAM
import ratatosk.fastq as FASTQ
import ratatosk.picard as PICARD
import ratatosk.gatk as GATK
import ratatosk.cutadapt as CUTADAPT
import ratatosk.fastqc as FASTQC
import ratatosk.misc as MISC
import ratatosk.external
import ngstestdata as ntd

logging.basicConfig(level=logging.DEBUG)

bwa = "bwa"
samtools = "samtools"
bwaref = os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "bwa", "chr11.fa"))
bwaseqref = os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "genomes", "Hsapiens", "hg19", "seq", "chr11.fa"))
indir = os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "projects", "J.Doe_00_01", "P001_101_index3", "121015_BB002BBBXX"))
projectdir = os.path.relpath(os.path.join(ntd.__path__[0], os.pardir, "data", "projects", "J.Doe_00_01"))
sample = "P001_101_index3_TGACCA_L001"
fastq1 = os.path.join(indir, sample + "_R1_001.fastq.gz")
fastq2 = os.path.join(indir, sample + "_R2_001.fastq.gz")

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

def _make_file_links():
    if not os.path.lexists(os.path.join(os.curdir, os.path.basename(fastq1))):
        os.symlink(fastq1, os.path.join(os.curdir, os.path.basename(fastq1)))
    if not os.path.lexists(os.path.join(os.curdir, os.path.basename(fastq2))):
        os.symlink(fastq2, os.path.join(os.curdir, os.path.basename(fastq2)))

class TestSamtoolsWrappers(unittest.TestCase):
   def test_samtools_view(self):
      luigi.run(_luigi_args(['--target', bam, '--config-file', localconf, '--parent-task', 'ratatosk.bwa.BwaSampe']), main_task_cls=SAM.SamToBam)


class TestMiscWrappers(unittest.TestCase):
    def test_luigihelp(self):
        try:
            luigi.run(['-h'], main_task_cls=FASTQ.FastqFileLink)
        except:
            pass

    def test_fastqln(self):
        luigi.run(_luigi_args(['--target', fastq1, '--config-file', localconf]), main_task_cls=FASTQ.FastqFileLink)

    def test_cutadapt(self):
        _make_file_links()
        luigi.run(_luigi_args(['--target', os.path.basename(fastq1.replace(".fastq.gz", ".trimmed.fastq.gz")), '--config-file', localconf]), main_task_cls=CUTADAPT.CutadaptJobTask)
        
    def test_fastqc(self):
        _make_file_links()
        luigi.run(_luigi_args(['--target', os.path.basename(fastq1), '--config-file', localconf]), main_task_cls=FASTQC.FastQCJobTask)

    # Here test trimming, resyncing, and finally bwa aln
    def test_resyncmates(self):
       luigi.run(_luigi_args(['--target', fastq1.replace(".fastq.gz", ".sync.fastq.gz"),
                              '--target', fastq2.replace(".fastq.gz", ".sync.fastq.gz")]), main_task_cls=MISC.ResyncMatesJobTask)

    def test_resyncmates_after_trim(self):
       luigi.run(_luigi_args(['--target', fastq1.replace(".fastq.gz", ".trimmed.sync.fastq.gz"),
                              '--target', fastq2.replace(".fastq.gz", ".trimmed.sync.fastq.gz"),
                              '--parent-task', 'ratatosk.cutadapt.CutadaptJobTask']), main_task_cls=MISC.ResyncMatesJobTask)
    def test_bwaaln_after_trim_resyncmates(self):
       with open("mock.yaml", "w") as fp:
          fp.write(yaml.safe_dump({
                   'misc':{'ResyncMates':{'parent_task': 'ratatosk.cutadapt.CutadaptJobTask'}},
                   'bwa' :{
                         'bwaref': bwaref,
                         'aln':{'parent_task':'ratatosk.misc.ResyncMatesJobTask'}}}, default_flow_style=False))
       luigi.run(_luigi_args(['--target', sai1.replace(".sai", ".trimmed.sync.sai"),
                                     '--config-file', 'mock.yaml']), main_task_cls=BWA.BwaAln)
       luigi.run(_luigi_args(['--target', sai2.replace(".sai", ".trimmed.sync.sai"),
                                     '--config-file', 'mock.yaml']), main_task_cls=BWA.BwaAln)
       os.unlink("mock.yaml")

    

class TestBwaWrappers(unittest.TestCase):
    def test_bwaaln(self):
        luigi.run(_luigi_args(['--target', sai1, '--config-file', localconf]), main_task_cls=BWA.BwaAln)
        luigi.run(_luigi_args(['--target', sai2, '--config-file', localconf]), main_task_cls=BWA.BwaAln)

    # Will currently fail if links aren't present since it doesn't
    # know where the links come from (hence _make_file_links function)
    def test_bwasampe(self):
        _make_file_links()
        luigi.run(_luigi_args(['--target', sam, '--config-file', localconf]), main_task_cls=BWA.BwaSampe)

    # Also fails; depends on InputSamFile, which only exists if
    # BWA.BwaSampe has been run. See below for putting different
    # modules together.
    def test_sortbam(self):
        _make_file_links()
        luigi.run(_luigi_args(['--target', sortbam, '--config-file', localconf]), main_task_cls=SAM.SortBam)


class TestPicardWrappers(unittest.TestCase):
    def test_picard_sortbam(self):
        _make_file_links()
        luigi.run(_luigi_args(['--target', sortbam, '--config-file', localconf]), main_task_cls=PICARD.SortSam)

    def test_picard_alignmentmetrics(self):
        _make_file_links()
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".align_metrics"),'--options', 'REFERENCE_SEQUENCE={}'.format(bwaseqref), '--config-file', localconf]), main_task_cls=PICARD.AlignmentMetrics)

    def test_picard_insertmetrics(self):
        _make_file_links()
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".insert_metrics"), '--options', 'REFERENCE_SEQUENCE={}'.format(bwaseqref), '--config-file', localconf]), main_task_cls=PICARD.InsertMetrics)

    def test_picard_dupmetrics(self):
        _make_file_links()
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".dup.bam"), '--config-file', localconf]), main_task_cls=PICARD.DuplicationMetrics)

    def test_picard_hsmetrics(self):
        _make_file_links()
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".hs_metrics"), '--config-file', localconf]), main_task_cls=PICARD.HsMetrics)

    def test_picard_metrics(self):
       _make_file_links()
       luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ""), '--config-file', localconf]), main_task_cls=PICARD.PicardMetrics)

    def test_picard_merge(self):
       pass
 

class TestGATKWrappers(unittest.TestCase):
    # Depends on previous tasks (sortbam) - bam must be present
    def test_realigner_target_creator(self):
        luigi.run(_luigi_args(['--target', sortbam.replace(".bam", ".intervals"), '--config-file', localconf]), main_task_cls=GATK.RealignerTargetCreator)
        
    def test_indel_realigner(self):
        luigi.run(_luigi_args(['--target', realignbam, '--config-file', localconf]), main_task_cls=GATK.IndelRealigner)

    def test_base_recalibrator(self):
        luigi.run(_luigi_args(['--target', realignbam.replace(".bam", ".recal_data.grp"), '--config-file', localconf]), main_task_cls=GATK.BaseRecalibrator)

    def test_print_reads(self):
        luigi.run(_luigi_args(['--target', recalbam, '--config-file', localconf]), main_task_cls=GATK.PrintReads)

    def test_clip_reads(self):
        luigi.run(_luigi_args(['--target', clipbam, '--config-file', localconf]), main_task_cls=GATK.ClipReads)

    # TODO: Test vcf outputs
    def test_unified_genotyper(self):
       print clipvcf
       #luigi.run(_luigi_args(['--target', clipvcf, '--config-file', localconf]), main_task_cls=GATK.UnifiedGenotyper)

    def test_variant_filtration(self):
        luigi.run(_luigi_args(['--target', filteredvcf, '--config-file', localconf]), main_task_cls=GATK.VariantFiltration)

    def test_variant_evaluation(self):
        luigi.run(_luigi_args(['--target', filteredvcf.replace(".vcf", ".eval_metrics"), '--config-file', localconf]), main_task_cls=GATK.VariantEval)

class TestLuigiParallel(unittest.TestCase):
    def test_bwa_samples(self):
        pass

class SampeToSamtools(SAM.SamToBam):
    def requires(self):
        source = self._make_source_file_name()
        return BWA.BwaSampe(target=source)

class TestLuigiPipelines(unittest.TestCase):
    def test_sampe_to_samtools(self):
        luigi.run(_luigi_args(['--sam', sam, '--config-file', localconf]), main_task_cls=SampeToSamtools)

    def test_sampe_to_samtools_sort(self):
        luigi.run(_luigi_args(['--bam', bam, '--config-file', localconf]), main_task_cls=SAM.SortBam)

    def test_sampe_to_picard_sort(self):
        luigi.run(_luigi_args(['--bam', bam, '--config-file', localconf]), main_task_cls=PICARD.SortSam)


# Small test of workflow
class Task1(JobTask):
    parent_task = luigi.Parameter("ratatosk.external.Fastqfile")
    def requires(self):
        return ratatosk.external.Fastqfile()

class TestLuigiInputOutput(unittest.TestCase):
    def test_luigi_input_output(self):
        #luigi.run(_luigi_args([]), main_task_cls=Task2)
        pass

    
