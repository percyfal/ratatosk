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
import re
import unittest
import luigi
import logging
from luigi.mock import MockFile
import ratatosk.lib.files.fastq
import ratatosk.lib.tools.samtools
import ratatosk.lib.tools.picard
import ratatosk.lib.tools.gatk
import ratatosk.lib.tools.fastqc
import ratatosk.lib.utils.cutadapt
import ratatosk.lib.utils.misc
import ratatosk.lib.annotation.snpeff
import ratatosk.lib.annotation.annovar
from ratatosk.config import get_config
File = MockFile

logging.basicConfig(level=logging.DEBUG)

bwa = "bwa"
samtools = "samtools"
samplename = "P001_101_index3"
sample = "P001_101_index3_TGACCA_L001"
fastq1 = os.path.join(os.curdir, sample + "_R1_001.fastq.gz")
fastq2 = os.path.join(os.curdir, sample + "_R2_001.fastq.gz")
ref = "reference.fa"

sai1 = os.path.join(sample + "_R1_001.sai")
sai2 = os.path.join(sample + "_R2_001.sai")

sam = os.path.join(sample + ".sam")
bam = os.path.join(sample + ".bam")
sortbam = os.path.join(sample + ".sort.bam")

localconf = "pipeconf.yaml"
local_scheduler = '--local-scheduler'
process = os.popen("ps x -o pid,args | grep ratatoskd | grep -v grep").read() #sometimes have to use grep -v grep

if process:
    local_scheduler = None

def _luigi_args(args):
    if local_scheduler:
        return [local_scheduler] + args
    return args

def _prune_luigi_tmp(args):
    """Remove luigi tmp string from file names"""
    return [re.sub(r'-luigi-tmp-[0-9]+(\.gz)?', '', x) for x in args]

def setUpModule():
    global config
    config = get_config()
    config.add_config_path(localconf)
    config.reload()
    
class TestSamtoolsWrappers(unittest.TestCase):
    def test_samtools_view(self):
        task = ratatosk.lib.tools.samtools.SamToBam(target=bam)
        self.assertEqual(['samtools', 'view', '-bSh', 'P001_101_index3_TGACCA_L001.sam', '>', 'P001_101_index3_TGACCA_L001.bam'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

class TestMiscWrappers(unittest.TestCase):
    def setUp(self):
        MockFile._file_contents = {}
        os.makedirs('indir')
        with open('indir/file.fastq.gz', 'w') as fh:
            fh.write("")

    def tearDown(self):
        os.unlink('indir/file.fastq.gz')
        os.rmdir('indir')

    def test_luigihelp(self):
        try:
            luigi.run(['-h'], main_task_cls=ratatosk.lib.files.fastq.FastqFileLink)
        except:
            pass

    def test_fastqln(self):
        outfile = 'file.fastq.gz'
        fql = ratatosk.lib.files.fastq.FastqFileLink(target=outfile, outdir=os.curdir, indir="indir")
        fql.run()
        self.assertTrue(os.path.exists(outfile))
        self.assertTrue(os.path.exists(os.readlink(outfile)))
        os.unlink('./file.fastq.gz')

    def test_cutadapt(self):
        task = ratatosk.lib.utils.cutadapt.CutadaptJobTask(target=fastq1.replace(".fastq.gz", ".trimmed.fastq.gz"), parent_task='ratatosk.lib.files.fastq.FastqFileLink')
        self.assertEqual(['cutadapt', '-a', 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC', './P001_101_index3_TGACCA_L001_R1_001.fastq.gz', '-o', './P001_101_index3_TGACCA_L001_R1_001.trimmed.fastq.gz', '>', './P001_101_index3_TGACCA_L001_R1_001.trimmed.fastq.cutadapt_metrics'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))
       
    def test_fastqc(self):
        task = ratatosk.lib.tools.fastqc.FastQCJobTask(target=os.path.basename(fastq1), parent_task='ratatosk.lib.files.fastq.FastqFileLink')
        self.assertEqual(['fastqc', '-o', './P001_101_index3_TGACCA_L001_R1_001.fastq.gz', './P001_101_index3_TGACCA_L001_R1_001.fastq.gz'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))           

    def test_resyncmates_after_trim(self):
        task = ratatosk.lib.utils.misc.ResyncMatesJobTask(target=[fastq1.replace(".fastq.gz", ".trimmed.sync.fastq.gz"),
                                                          fastq2.replace(".fastq.gz", ".trimmed.sync.fastq.gz")],
                                                          parent_task='ratatosk.lib.utils.cutadapt.CutadaptJobTask',
                                                          executable="resyncMates.pl")
        self.assertEqual(['resyncMates.pl', '-i', './P001_101_index3_TGACCA_L001_R1_001.trimmed.fastq.gz', '-j', './P001_101_index3_TGACCA_L001_R2_001.trimmed.fastq.gz', '-o', './P001_101_index3_TGACCA_L001_R1_001.trimmed.sync.fastq.gz', '-p', './P001_101_index3_TGACCA_L001_R2_001.trimmed.sync.fastq.gz'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))


class TestBwaWrappers(unittest.TestCase):
    def test_bwaaln(self):
        task = ratatosk.lib.align.bwa.BwaAln(target=sai1)
        self.assertEqual(['bwa', 'aln', '-t 1', 'bwa/hg19.fa', 'P001_101_index3_TGACCA_L001_R1_001.fastq.gz', '>', 'P001_101_index3_TGACCA_L001_R1_001.sai'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_bwasampe(self):
        task = ratatosk.lib.align.bwa.BwaSampe(target=sam)
        self.assertEqual(['bwa', 'sampe', '-r "@RG\tID:P001_101_index3_TGACCA_L001_R1_001\tSM:P001_101_index3_TGACCA_L001_R1_001\tPL:Illumina"', 'bwa/hg19.fa', 'P001_101_index3_TGACCA_L001_R1_001.sai', 'P001_101_index3_TGACCA_L001_R2_001.sai', 'P001_101_index3_TGACCA_L001_R1_001.fastq.gz', 'P001_101_index3_TGACCA_L001_R2_001.fastq.gz', '>', 'P001_101_index3_TGACCA_L001.sam'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_sortbam(self):
        task = ratatosk.lib.tools.samtools.SortBam(target=sortbam)
        self.assertEqual(['samtools', 'sort', 'P001_101_index3_TGACCA_L001.bam', 'P001_101_index3_TGACCA_L001.sort'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

@unittest.skipIf((os.getenv("PICARD_HOME") is None or os.getenv("PICARD_HOME") == ""), "No Environment PICARD_HOME set; skipping")
class TestPicardWrappers(unittest.TestCase):
    def _path(self, exe):
        return os.path.join(os.environ["PICARD_HOME"], exe)
    def test_picard_sortbam(self):
        task = ratatosk.lib.tools.picard.SortSam(target=sortbam)
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('SortSam.jar'), 'SO=coordinate MAX_RECORDS_IN_RAM=750000', 'INPUT=', 'P001_101_index3_TGACCA_L001.bam', 'OUTPUT=', 'P001_101_index3_TGACCA_L001.sort.bam'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))


    def test_picard_alignmentmetrics(self):
        task = ratatosk.lib.tools.picard.AlignmentMetrics(target=sortbam.replace(".bam", ".align_metrics"), options=['REFERENCE_SEQUENCE={}'.format(ref)])
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('CollectAlignmentSummaryMetrics.jar'), 'REFERENCE_SEQUENCE=reference.fa', 'INPUT=', 'P001_101_index3_TGACCA_L001.sort.bam', 'OUTPUT=', 'P001_101_index3_TGACCA_L001.sort.align_metrics'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_picard_insertmetrics(self):
        task = ratatosk.lib.tools.picard.InsertMetrics(target=sortbam.replace(".bam", ".insert_metrics"), options=['REFERENCE_SEQUENCE={}'.format(ref)])
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('CollectInsertSizeMetrics.jar'), 'REFERENCE_SEQUENCE=reference.fa', 'INPUT=', 'P001_101_index3_TGACCA_L001.sort.bam', 'OUTPUT=', 'P001_101_index3_TGACCA_L001.sort.insert_metrics', 'HISTOGRAM_FILE=', 'P001_101_index3_TGACCA_L001.sort.insert_hist'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_picard_dupmetrics(self):
        task = ratatosk.lib.tools.picard.DuplicationMetrics(target=sortbam.replace(".bam", ".dup.bam"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('MarkDuplicates.jar'), 'INPUT=', 'P001_101_index3_TGACCA_L001.sort.bam', 'OUTPUT=', 'P001_101_index3_TGACCA_L001.sort.dup.bam', 'METRICS_FILE=', 'P001_101_index3_TGACCA_L001.sort.dup_metrics'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_picard_hsmetrics(self):
        task = ratatosk.lib.tools.picard.HsMetrics(target=sortbam.replace(".bam", ".hs_metrics"),  target_regions="targets.interval_list", bait_regions="baits.interval_list")
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('CalculateHsMetrics.jar'), 'INPUT=', 'P001_101_index3_TGACCA_L001.sort.bam', 'OUTPUT=', 'P001_101_index3_TGACCA_L001.sort.hs_metrics', 'BAIT_INTERVALS=', 'baits.interval_list', 'TARGET_INTERVALS=', 'targets.interval_list'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_picard_metrics(self):
        task = ratatosk.lib.tools.picard.PicardMetrics(target=sortbam.replace(".bam", ""))
        metrics = [ratatosk.lib.tools.picard.InsertMetrics(target=sortbam.replace(".bam", ".insert_metrics")),
                   ratatosk.lib.tools.picard.HsMetrics(target=sortbam.replace(".bam", ".hs_metrics")),
                   ratatosk.lib.tools.picard.AlignmentMetrics(target=sortbam.replace(".bam", ".align_metrics"))]
        self.assertEqual(task.requires(), metrics)

    # NOTE: if no target_generator exists will be impossible to check formatting of input
    def test_merge_sam_files(self):
        mergebam = os.path.join(os.curdir, samplename, "{}.sort.merge.bam".format(samplename))
        task = ratatosk.lib.tools.picard.MergeSamFiles(target=mergebam)
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('MergeSamFiles.jar'), 'SO=coordinate TMP_DIR=./tmp', 'OUTPUT=', './P001_101_index3/P001_101_index3.sort.merge.bam'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

@unittest.skipIf((os.getenv("GATK_HOME") is None or os.getenv("GATK_HOME") == ""), "No environment GATK_HOME set; skipping")
class TestGATKWrappers(unittest.TestCase):
    def setUp(self):
        self.mergebam = os.path.join(os.curdir, samplename, "{}.sort.merge.bam".format(samplename))
        self.gatk = os.path.join(os.environ["GATK_HOME"], 'GenomeAnalysisTK.jar')

    def test_realigner_target_creator(self):
        task = ratatosk.lib.tools.gatk.RealignerTargetCreator(target=self.mergebam.replace(".bam", ".intervals"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T RealignerTargetCreator', '-nt 1', '', '-I', './P001_101_index3/P001_101_index3.sort.merge.bam', '-o', './P001_101_index3/P001_101_index3.sort.merge.intervals', ' -R reference.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))
                         
    def test_indelrealigner(self):
        task = ratatosk.lib.tools.gatk.IndelRealigner(target=self.mergebam.replace(".bam", ".realign.bam"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T IndelRealigner', '', '-I', './P001_101_index3/P001_101_index3.sort.merge.bam', '-o', './P001_101_index3/P001_101_index3.sort.merge.realign.bam', '--targetIntervals', './P001_101_index3/P001_101_index3.sort.merge.intervals', ' -R reference.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_base_recalibrator(self):
        task = ratatosk.lib.tools.gatk.BaseRecalibrator(target=self.mergebam.replace(".bam", ".realign.recal_data.grp"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T BaseRecalibrator', '-I', './P001_101_index3/P001_101_index3.sort.merge.realign.bam', '-o', './P001_101_index3/P001_101_index3.sort.merge.realign.recal_data.grp', ' -R reference.fa', ' -knownSites knownSites1.vcf  -knownSites knownSites2.vcf'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_printreads(self):
        task = ratatosk.lib.tools.gatk.PrintReads(target=self.mergebam.replace(".bam", ".realign.recal.bam"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T PrintReads', '-BQSR', './P001_101_index3/P001_101_index3.sort.merge.realign.recal_data.grp', '-o', './P001_101_index3/P001_101_index3.sort.merge.realign.recal.bam', '-I', './P001_101_index3/P001_101_index3.sort.merge.realign.bam', ' -R reference.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_clipreads(self):
        task = ratatosk.lib.tools.gatk.ClipReads(target=self.mergebam.replace(".bam", ".realign.recal.clip.bam"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T ClipReads', '--cyclesToTrim 1-5 --clipRepresentation WRITE_NS', '-I', './P001_101_index3/P001_101_index3.sort.merge.realign.recal.bam', '-o', './P001_101_index3/P001_101_index3.sort.merge.realign.recal.clip.bam', ' -R reference.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_unifiedgenotyper(self):
        task = ratatosk.lib.tools.gatk.UnifiedGenotyper(target=self.mergebam.replace(".bam", ".realign.recal.clip.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T UnifiedGenotyper', '-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH', '-nt 1', ' --dbsnp dbsnp132.vcf', '-I', './P001_101_index3/P001_101_index3.sort.merge.realign.recal.clip.bam', '-o', './P001_101_index3/P001_101_index3.sort.merge.realign.recal.clip.vcf', ' -R reference.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_variantfiltration(self):
        task = ratatosk.lib.tools.gatk.VariantFiltration(target=self.mergebam.replace(".bam", ".realign.recal.clip.filtered.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantFiltration', '--clusterWindowSize 10 --clusterSize 3 --filterExpression "MQ0 >= 4 && ((MQ0 / (1.0 * DP)) > 0.1)" --filterName "HARD_TO_VALIDATE" --filterExpression "DP < 10" --filterName "LowCoverage" --filterExpression "QUAL < 30.0" --filterName "VeryLowQual" --filterExpression "QUAL > 30.0 && QUAL < 50.0" --filterName "LowQual" --filterExpression "QD < 1.5" --filterName "LowQD"', '--variant', './P001_101_index3/P001_101_index3.sort.merge.realign.recal.clip.vcf', '-o', './P001_101_index3/P001_101_index3.sort.merge.realign.recal.clip.filtered.vcf', ' -R reference.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_varianteval(self):
        task = ratatosk.lib.tools.gatk.VariantEval(target=self.mergebam.replace(".bam", ".realign.recal.clip.filtered.eval_metrics"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantEval', '-ST Filter -l INFO --doNotUseAllStandardModules --evalModule CompOverlap --evalModule CountVariants --evalModule GenotypeConcordance --evalModule TiTvVariantEvaluator --evalModule ValidationReport --stratificationModule Filter', ' --dbsnp dbsnp132.vcf', '--eval', './P001_101_index3/P001_101_index3.sort.merge.realign.recal.clip.filtered.vcf', '-o', './P001_101_index3/P001_101_index3.sort.merge.realign.recal.clip.filtered.eval_metrics', ' -R reference.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_variant_annotator(self):
        task = ratatosk.lib.tools.gatk.VariantAnnotator(target=self.mergebam.replace(".bam", "-gatkann.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantAnnotator', '', '--variant', './P001_101_index3/P001_101_index3.sort.merge.vcf', '--out', './P001_101_index3/P001_101_index3.sort.merge-gatkann.vcf', ' -R reference.fa', '-A', 'BaseQualityRankSumTest', '-A', 'DepthOfCoverage', '-A', 'FisherStrand', '-A', 'GCContent', '-A', 'HaplotypeScore', '-A', 'HomopolymerRun', '-A', 'MappingQualityRankSumTest', '-A', 'MappingQualityZero', '-A', 'QualByDepth', '-A', 'ReadPosRankSumTest', '-A', 'RMSMappingQuality'], _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_GATK_snpeff_variant_annotator(self):
        task = ratatosk.lib.tools.gatk.VariantSnpEffAnnotator(target=self.mergebam.replace(".bam", "-annotated.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantAnnotator', '', '--variant', './P001_101_index3/P001_101_index3.sort.merge.vcf', '--out', './P001_101_index3/P001_101_index3.sort.merge-annotated.vcf', '--snpEffFile', './P001_101_index3/P001_101_index3.sort.merge-effects.vcf', ' -R reference.fa', '-A', 'SnpEff'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))


@unittest.skipIf((os.getenv("SNPEFF_HOME") is None or os.getenv("SNPEFF_HOME") == ""), "No environment SNPEFF_HOME set; skipping")
class TestSnpEffWrappers(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.bam = sortbam
        self.snpeff = os.path.join(os.environ["SNPEFF_HOME"], 'snpEff.jar')
        self.config = os.path.join(os.environ["SNPEFF_HOME"], 'snpEff.config')

    def test_snpeff(self):
        task = ratatosk.lib.annotation.snpeff.snpEff(target=self.bam.replace(".bam", "-effects.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.snpeff, 'eff', '-1', '-i', 'vcf', '-o', 'vcf', '-c', self.config, 'GRCh37.64', 'P001_101_index3_TGACCA_L001.sort.vcf', '>', 'P001_101_index3_TGACCA_L001.sort-effects.vcf'], _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_snpeff_txt(self):
        task = ratatosk.lib.annotation.snpeff.snpEff(target=self.bam.replace(".bam", "-effects.txt"),  target_suffix='.txt')
        self.assertEqual(['java', '-Xmx2g', '-jar', self.snpeff, 'eff', '-1', '-i', 'vcf', '-o', 'txt', '-c', self.config, 'GRCh37.64', 'P001_101_index3_TGACCA_L001.sort.vcf', '>', 'P001_101_index3_TGACCA_L001.sort-effects.txt'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))


@unittest.skipIf((os.getenv("ANNOVAR_HOME") is None or os.getenv("ANNOVAR_HOME") == ""), "No environment ANNOVAR_HOME set; skipping")
class TestAnnovarWrappers(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.bam = sortbam

    def _path(self, exe):
        return os.path.join(os.environ["ANNOVAR_HOME"], exe)

    def test_convert_annovar(self):
        task = ratatosk.lib.annotation.annovar.Convert2Annovar(target=self.bam.replace(".bam", "-avinput.txt"))
        self.assertEqual([self._path('convert2annovar.pl'), '-format vcf4', 'P001_101_index3_TGACCA_L001.sort.vcf', '--outfile', 'P001_101_index3_TGACCA_L001.sort-avinput.txt'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))

    def test_summarize_annovar(self):
        task = ratatosk.lib.annotation.annovar.SummarizeAnnovar(target=self.bam.replace(".bam", "-avinput.txt.log"))
        self.assertEqual([self._path('summarize_annovar.pl'), '-remove', '-buildver hg19', '-verdbsnp 132', '-ver1000g 1000g2011may', 'P001_101_index3_TGACCA_L001.sort-avinput.txt', self._path('humandb')],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)))


