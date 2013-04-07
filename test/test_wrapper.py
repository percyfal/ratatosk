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
from ratatosk.config import get_config, get_custom_config

File = MockFile

logging.basicConfig(level=logging.DEBUG)

sample = "sample1"
sample2 = "sample2"
indir = "data"
read1_suffix = "_1"
read2_suffix = "_2"

fastq1 = os.path.join(indir, sample + read1_suffix + ".fastq.gz")
fastq2 = os.path.join(indir, sample + read2_suffix + ".fastq.gz")
ref = os.path.join(indir, "chr11.fa")

sai1 = os.path.join(indir, sample + read1_suffix + ".sai")
sai2 = os.path.join(indir, sample + read2_suffix + ".sai")

sam = os.path.join(indir, sample + ".sam")
bam = os.path.join(indir, sample + ".bam")
sortbam = os.path.join(indir, sample + ".sort.bam")

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
    global cnf, custom_cnf
    cnf = get_config()
    cnf.clear()
    cnf.add_config_path(os.path.join(os.path.dirname(__file__), os.pardir, "config", "ratatosk.yaml"))
    cnf.add_config_path(localconf)
    custom_cnf = get_custom_config()
    custom_cnf.clear()
    custom_cnf.add_config_path(localconf)
    custom_cnf.reload()
    
def tearDownModule():
    cnf.clear()
    custom_cnf.clear()

class TestSamtoolsWrappers(unittest.TestCase):
    def test_samtools_view(self):
        task = ratatosk.lib.tools.samtools.SamToBam(target=bam)
        self.assertEqual(['samtools', 'view', '-bSh', 'data/sample1.sam', '>', 'data/sample1.bam'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_sortbam(self):
        task = ratatosk.lib.tools.samtools.SortBam(target=sortbam)
        self.assertEqual(['samtools', 'sort', 'data/sample1.bam', 'data/sample1.sort'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))


class TestMiscWrappers(unittest.TestCase):
    def setUp(self):
        self.fastqfile = os.path.relpath(os.path.join(os.path.dirname(__file__), 'indir', 'file.fastq.gz'), os.curdir)
        MockFile._file_contents = {}
        os.makedirs('indir')
        with open(self.fastqfile, 'w') as fh:
            fh.write("")

    def tearDown(self):
        if os.path.exists(self.fastqfile):
            os.unlink(self.fastqfile)
            os.rmdir(os.path.dirname(self.fastqfile))

    def test_luigihelp(self):
        try:
            luigi.run(['-h'], main_task_cls=ratatosk.lib.files.fastq.FastqFileLink)
        except:
            pass

    def test_fastqln(self):
        outfile = os.path.join(os.path.dirname(self.fastqfile), os.pardir, 'file.fastq.gz')
        luigi.run(_luigi_args(['--use-long-names', '--target', outfile, '--outdir', os.path.dirname(outfile), '--indir', os.path.dirname(self.fastqfile), '--config-file', os.path.relpath(os.path.join(os.path.dirname(__file__), os.pardir, "config", "ratatosk.yaml"), os.curdir)]), main_task_cls=ratatosk.lib.files.fastq.FastqFileLink)
        self.assertTrue(os.path.exists(outfile))
        self.assertTrue(os.path.exists(os.readlink(outfile)))
        os.unlink(outfile)

    def test_cutadapt(self):
        task = ratatosk.lib.utils.cutadapt.CutadaptJobTask(target=fastq1.replace(".fastq.gz", ".trimmed.fastq.gz"), read1_suffix="_1")
        # Needed in order to override pipeconf.yaml. This is a bug;
        # setting it in class instantiation should override config
        # file settings
        task.parent_task="ratatosk.lib.utils.cutadapt.InputFastqFile"
        self.assertEqual(['cutadapt', '-a', 'AGATCGGAAGAGCACACGTCTGAACTCCAGTCAC', 'data/sample1_1.fastq.gz', '-o', 'data/sample1_1.trimmed.fastq.gz', '>', 'data/sample1_1.trimmed.fastq.cutadapt_metrics'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    # NB: If fastqc is actually run this test will fail since
    # DefaultShellJobRunner._fix_paths won't return any tmp_files
    # since the output directory exists. This shouldn't affect the
    # behaviour of fastqc job tasks though
    def test_fastqc(self):
        task = ratatosk.lib.tools.fastqc.FastQCJobTask(target='data/sample1_1_fastqc')
        self.assertEqual(['fastqc', '-o', 'data/sample1_1_fastqc', 'data/sample1_1.fastq.gz'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))
        
    def test_resyncmates_after_trim(self):
        task = ratatosk.lib.utils.misc.ResyncMatesJobTask(target=[fastq1.replace(".fastq.gz", ".trimmed.sync.fastq.gz"),
                                                          fastq2.replace(".fastq.gz", ".trimmed.sync.fastq.gz")],
                                                          parent_task='ratatosk.lib.utils.cutadapt.CutadaptJobTask',
                                                          executable="resyncMates.pl")
        self.assertEqual(['resyncMates.pl', '-i', 'data/sample1_1.trimmed.fastq.gz', '-j', 'data/sample1_2.trimmed.fastq.gz', '-o', 'data/sample1_1.trimmed.sync.fastq.gz', '-p', 'data/sample1_2.trimmed.sync.fastq.gz'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))


class TestBwaWrappers(unittest.TestCase):
    def test_bwaaln(self):
        task = ratatosk.lib.align.bwa.BwaAln(target=sai1)
        self.assertEqual(['bwa', 'aln', '-t 1', 'data/chr11.fa', 'data/sample1_1.fastq.gz', '>', 'data/sample1_1.sai'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_bwasampe(self):
        task = ratatosk.lib.align.bwa.BwaSampe(target=sam, read1_suffix=read1_suffix, read2_suffix=read2_suffix)
        self.assertEqual(
            ['bwa', 'sampe', '-r', '"@RG\tID:data/sample1\tSM:data/sample1\tPL:Illumina"', 'data/chr11.fa', 'data/sample1_1.sai', 'data/sample1_2.sai', 'data/sample1_1.fastq.gz', 'data/sample1_2.fastq.gz', '>', 'data/sample1.sam'],
            _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0])
        )

    def test_bwaindex(self):
        task = ratatosk.lib.align.bwa.BwaIndex(target=ref + ".bwt")
        self.assertEqual(['bwa', 'index', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

# Temporary target generator
def merge_bam_generator(task):
    return ["data/sample1.sort.bam", "data/sample2.sort.bam"]

@unittest.skipIf((os.getenv("PICARD_HOME") is None or os.getenv("PICARD_HOME") == ""), "No Environment PICARD_HOME set; skipping")
class TestPicardWrappers(unittest.TestCase):
    def _path(self, exe):
        return os.path.join(os.environ["PICARD_HOME"], exe)
    def test_picard_sortbam(self):
        task = ratatosk.lib.tools.picard.SortSam(target=sortbam)
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('SortSam.jar'), 'SO=coordinate MAX_RECORDS_IN_RAM=750000', 'INPUT=', 'data/sample1.bam', 'OUTPUT=', 'data/sample1.sort.bam'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_picard_create_sequence_dictionary(self):
        task = ratatosk.lib.tools.picard.CreateSequenceDictionary(target="data/chr11.dict")
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('CreateSequenceDictionary.jar'), 'REFERENCE=', 'data/chr11.fa', 'OUTPUT=', 'data/chr11.dict'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_picard_alignmentmetrics(self):
        task = ratatosk.lib.tools.picard.AlignmentMetrics(target=sortbam.replace(".bam", ".align_metrics"), options=['REFERENCE_SEQUENCE={}'.format(ref)])
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('CollectAlignmentSummaryMetrics.jar'), 'REFERENCE_SEQUENCE=data/chr11.fa', 'INPUT=', 'data/sample1.sort.bam', 'OUTPUT=', 'data/sample1.sort.align_metrics'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_picard_insertmetrics(self):
        task = ratatosk.lib.tools.picard.InsertMetrics(target=sortbam.replace(".bam", ".insert_metrics"), options=['REFERENCE_SEQUENCE={}'.format(ref)])
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('CollectInsertSizeMetrics.jar'), 'REFERENCE_SEQUENCE=data/chr11.fa', 'INPUT=', 'data/sample1.sort.bam', 'OUTPUT=', 'data/sample1.sort.insert_metrics', 'HISTOGRAM_FILE=', 'data/sample1.sort.insert_hist'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_picard_dupmetrics(self):
        task = ratatosk.lib.tools.picard.DuplicationMetrics(target=sortbam.replace(".bam", ".dup.bam"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('MarkDuplicates.jar'), 'INPUT=', 'data/sample1.sort.bam', 'OUTPUT=', 'data/sample1.sort.dup.bam', 'METRICS_FILE=', 'data/sample1.sort.dup_metrics'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_picard_hsmetrics(self):
        task = ratatosk.lib.tools.picard.HsMetrics(target=sortbam.replace(".bam", ".hs_metrics"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('CalculateHsMetrics.jar'), 'INPUT=', 'data/sample1.sort.bam', 'OUTPUT=', 'data/sample1.sort.hs_metrics', 'BAIT_INTERVALS=', 'data/chr11_baits.interval_list', 'TARGET_INTERVALS=', 'data/chr11_targets.interval_list'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_picard_metrics(self):
        task = ratatosk.lib.tools.picard.PicardMetrics(target=sortbam.replace(".bam", ""))
        metrics = [ratatosk.lib.tools.picard.InsertMetrics(target=sortbam.replace(".bam", ".insert_metrics")),
                   ratatosk.lib.tools.picard.HsMetrics(target=sortbam.replace(".bam", ".hs_metrics")),
                   ratatosk.lib.tools.picard.AlignmentMetrics(target=sortbam.replace(".bam", ".align_metrics"))]
        self.assertEqual(task.requires(), metrics)

    # NOTE: if no target_generator exists will be impossible to check
    # formatting of input. Here create dummy file names
    def test_merge_sam_files(self):
        mergebam = "data/sample.sort.merge.bam"
        task = ratatosk.lib.tools.picard.MergeSamFiles(target=mergebam, target_generator_handler='test.test_wrapper.merge_bam_generator')
        self.assertEqual(['java', '-Xmx2g', '-jar', self._path('MergeSamFiles.jar'), 'SO=coordinate TMP_DIR=./tmp', 'OUTPUT=', 'data/sample.sort.merge.bam', 'INPUT=', 'data/sample1.sort.bam', 'INPUT=', 'data/sample2.sort.bam'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

@unittest.skipIf((os.getenv("GATK_HOME") is None or os.getenv("GATK_HOME") == ""), "No environment GATK_HOME set; skipping")
class TestGATKWrappers(unittest.TestCase):
    def setUp(self):
        self.mergebam = os.path.join(indir, "sample.sort.merge.bam")
        self.gatk = os.path.join(os.environ["GATK_HOME"], 'GenomeAnalysisTK.jar')

    def test_realigner_target_creator(self):
        task = ratatosk.lib.tools.gatk.RealignerTargetCreator(target=self.mergebam.replace(".bam", ".intervals"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T RealignerTargetCreator', '-nt 1', '', '-I', 'data/sample.sort.merge.bam', '-o', 'data/sample.sort.merge.intervals', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))
                         
    def test_indelrealigner(self):
        task = ratatosk.lib.tools.gatk.IndelRealigner(target=self.mergebam.replace(".bam", ".realign.bam"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T IndelRealigner', '', '-I', 'data/sample.sort.merge.bam', '-o', 'data/sample.sort.merge.realign.bam', '--targetIntervals', 'data/sample.sort.merge.intervals', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_base_recalibrator(self):
        task = ratatosk.lib.tools.gatk.BaseRecalibrator(target=self.mergebam.replace(".bam", ".realign.recal_data.grp"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T BaseRecalibrator', '-I', 'data/sample.sort.merge.realign.bam', '-o', 'data/sample.sort.merge.realign.recal_data.grp', '-R', 'data/chr11.fa', ' -knownSites knownSites1.vcf  -knownSites knownSites2.vcf'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_printreads(self):
        task = ratatosk.lib.tools.gatk.PrintReads(target=self.mergebam.replace(".bam", ".realign.recal.bam"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T PrintReads', '-BQSR', 'data/sample.sort.merge.realign.recal_data.grp', '-o', 'data/sample.sort.merge.realign.recal.bam', '-I', 'data/sample.sort.merge.realign.bam', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_clipreads(self):
        task = ratatosk.lib.tools.gatk.ClipReads(target=self.mergebam.replace(".bam", ".realign.recal.clip.bam"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T ClipReads', '--cyclesToTrim 1-5 --clipRepresentation WRITE_NS', '-I', 'data/sample.sort.merge.realign.recal.bam', '-o', 'data/sample.sort.merge.realign.recal.clip.bam', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_unifiedgenotyper(self):
        task = ratatosk.lib.tools.gatk.UnifiedGenotyper(target=self.mergebam.replace(".bam", ".realign.recal.clip.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T UnifiedGenotyper', '-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH', '-nt 1', '--dbsnp',  'data/dbsnp132_chr11.vcf', '-I', 'data/sample.sort.merge.realign.recal.clip.bam', '-o', 'data/sample.sort.merge.realign.recal.clip.vcf', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_variantfiltration(self):
        task = ratatosk.lib.tools.gatk.VariantFiltration(target=self.mergebam.replace(".bam", ".realign.recal.clip.filtered.vcf"),
                                                         options=['--clusterWindowSize 10 --clusterSize 3 --filterExpression "MQ0 >= 4 && ((MQ0 / (1.0 * DP)) > 0.1)" --filterName "HARD_TO_VALIDATE" --filterExpression "DP < 10" --filterName "LowCoverage" --filterExpression "QUAL < 30.0" --filterName "VeryLowQual" --filterExpression "QUAL > 30.0 && QUAL < 50.0" --filterName "LowQual" --filterExpression "QD < 1.5" --filterName "LowQD"', '--variant', 'data/sample.sort.merge.realign.recal.clip.vcf'])
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantFiltration','--clusterWindowSize 10 --clusterSize 3 --filterExpression "MQ0 >= 4 && ((MQ0 / (1.0 * DP)) > 0.1)" --filterName "HARD_TO_VALIDATE" --filterExpression "DP < 10" --filterName "LowCoverage" --filterExpression "QUAL < 30.0" --filterName "VeryLowQual" --filterExpression "QUAL > 30.0 && QUAL < 50.0" --filterName "LowQual" --filterExpression "QD < 1.5" --filterName "LowQD"', '--variant', 'data/sample.sort.merge.realign.recal.clip.vcf', '--variant', 'data/sample.sort.merge.realign.recal.clip.vcf', '--out', 'data/sample.sort.merge.realign.recal.clip.filtered.vcf', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_varianteval(self):
        task = ratatosk.lib.tools.gatk.VariantEval(target=self.mergebam.replace(".bam", ".realign.recal.clip.filtered.eval_metrics"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantEval', '-ST Filter -l INFO --doNotUseAllStandardModules --evalModule CompOverlap --evalModule CountVariants --evalModule GenotypeConcordance --evalModule TiTvVariantEvaluator --evalModule ValidationReport --stratificationModule Filter', '--dbsnp',  'data/dbsnp132_chr11.vcf', '--eval', 'data/sample.sort.merge.realign.recal.clip.filtered.vcf', '-o', 'data/sample.sort.merge.realign.recal.clip.filtered.eval_metrics', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))


    def test_variant_annotator(self):
        task = ratatosk.lib.tools.gatk.VariantAnnotator(target=self.mergebam.replace(".bam", "-gatkann.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantAnnotator', '', '--variant', 'data/sample.sort.merge.vcf', '--out', 'data/sample.sort.merge-gatkann.vcf', '-R', 'data/chr11.fa', '-A', 'BaseQualityRankSumTest', '-A', 'DepthOfCoverage', '-A', 'FisherStrand', '-A', 'GCContent', '-A', 'HaplotypeScore', '-A', 'HomopolymerRun', '-A', 'MappingQualityRankSumTest', '-A', 'MappingQualityZero', '-A', 'QualByDepth', '-A', 'ReadPosRankSumTest', '-A', 'RMSMappingQuality'], _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_GATK_snpeff_variant_annotator(self):
        task = ratatosk.lib.tools.gatk.VariantSnpEffAnnotator(target=self.mergebam.replace(".bam", "-annotated.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantAnnotator', '', '--variant', 'data/sample.sort.merge.vcf', '--out', 'data/sample.sort.merge-annotated.vcf', '--snpEffFile', 'data/sample.sort.merge-effects.vcf', '-R', 'data/chr11.fa', '-A', 'SnpEff'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_combine_variants(self):
        task = ratatosk.lib.tools.gatk.CombineVariants(target=self.mergebam.replace(".bam", "-variants-combined.vcf"), ref='data/chr11.fa')
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T CombineVariants', '-V', 'data/sample.sort.merge-variants-combined-split/sample.sort.merge-variants-combined-chr11.vcf', '-o', 'data/sample.sort.merge-variants-combined.vcf', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_select_variants(self):
        task = ratatosk.lib.tools.gatk.SelectVariants(target=self.mergebam.replace(".bam", "-snp-all.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T SelectVariants', '--selectTypeToInclude', 'SNP', '--selectTypeToInclude', 'INDEL', '--selectTypeToInclude', 'MIXED', '--selectTypeToInclude', 'MNP', '--selectTypeToInclude', 'SYMBOLIC', '--selectTypeToInclude', 'NO_VARIATION', '--variant', 'data/sample.sort.merge-snp.vcf', '--out', 'data/sample.sort.merge-snp-all.vcf', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_select_snp_variants(self):
        task = ratatosk.lib.tools.gatk.SelectSnpVariants(target=self.mergebam.replace(".bam", "-snp.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T SelectVariants', '--selectTypeToInclude', 'SNP', '--variant', 'data/sample.sort.merge.vcf', '--out', 'data/sample.sort.merge-snp.vcf', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_select_indel_variants(self):
        task = ratatosk.lib.tools.gatk.SelectIndelVariants(target=self.mergebam.replace(".bam", "-indel.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T SelectVariants', '--selectTypeToInclude', 'INDEL', '--selectTypeToInclude', 'MIXED', '--selectTypeToInclude', 'MNP', '--selectTypeToInclude', 'SYMBOLIC', '--selectTypeToInclude', 'NO_VARIATION', '--variant', 'data/sample.sort.merge.vcf', '--out', 'data/sample.sort.merge-indel.vcf', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_variant_recalibrator(self):
        """Test variant recalibation. Note that commands that require
        training data will not work; only JEXL filtering is
        applicable"""
        task = ratatosk.lib.tools.gatk.VariantRecalibrator(target=self.mergebam.replace(".bam", ".tranches"), ref="data/chr11.fa", 
                                                           options=["-an", "QD", "-resource:hapmap,VCF,known=false,training=true,truth=true,prior=15.0", "data/hapmap_3.3.vcf"])
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantRecalibrator', '-an', 'QD', '-resource:hapmap,VCF,known=false,training=true,truth=true,prior=15.0', 'data/hapmap_3.3.vcf', '--input', 'data/sample.sort.merge.vcf', '--tranches_file', 'data/sample.sort.merge.tranches', '--mode', 'BOTH', '--recal_file', 'data/sample.sort.merge.recal', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_variant_snp_recalibrator(self):
        task = ratatosk.lib.tools.gatk.VariantSnpRecalibrator(target=self.mergebam.replace(".bam", ".tranches"), 
                                                              train_hapmap="data/hapmap_3.3.vcf",
                                                              ref="data/chr11.fa", dbsnp="data/dbsnp132_chr11.vcf")
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantRecalibrator', '-an', 'QD', '-an', 'HaplotypeScore', '-an', 'MQRankSum', '-an', 'ReadPosRankSum', '-an', 'FS', '-an', 'MQ', '-an', 'DP', '-resource:hapmap,VCF,known=false,training=true,truth=true,prior=15.0', 'data/hapmap_3.3.vcf', '-resource:dbsnp,VCF,known=true,training=false,truth=false,prior=8.0', 'data/dbsnp132_chr11.vcf', '--input', 'data/sample.sort.merge.vcf', '--tranches_file', 'data/sample.sort.merge.tranches', '--mode', 'SNP', '--recal_file', 'data/sample.sort.merge.recal', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_variant_snp_recalibrator_exome(self):
        """Modified settings for exome. Should contain --maxGaussians"""
        task = ratatosk.lib.tools.gatk.VariantSnpRecalibratorExome(target=self.mergebam.replace(".bam", ".tranches"), 
                                                                   train_hapmap="data/hapmap_3.3.vcf",
                                                                   ref="data/chr11.fa", dbsnp="data/dbsnp132_chr11.vcf")
        arglist = _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0])
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantRecalibrator', '-an', 'QD', '-an', 'HaplotypeScore', '-an', 'MQRankSum', '-an', 'ReadPosRankSum', '-an', 'FS', '-an', 'MQ', '--maxGaussians', '4', '--percentBadVariants', '0.05', '-resource:hapmap,VCF,known=false,training=true,truth=true,prior=15.0', 'data/hapmap_3.3.vcf', '-resource:dbsnp,VCF,known=true,training=false,truth=false,prior=8.0', 'data/dbsnp132_chr11.vcf', '--input', 'data/sample.sort.merge.vcf', '--tranches_file', 'data/sample.sort.merge.tranches', '--mode', 'SNP', '--recal_file', 'data/sample.sort.merge.recal', '-R', 'data/chr11.fa'],
                         arglist)
        self.assertIn('--maxGaussians', arglist)

    def test_variant_indel_recalibrator(self):
        task = ratatosk.lib.tools.gatk.VariantIndelRecalibrator(target=self.mergebam.replace(".bam", ".tranches"), 
                                                                train_indels="data/Mills_Devine_2hit.indels.vcf",
                                                                ref="data/chr11.fa")
        self.assertEqual(['java', '-Xmx2g', '-jar', self.gatk, '-T VariantRecalibrator', '-an', 'QD', '-an', 'FS', '-an', 'HaplotypeScore', '-an', 'ReadPosRankSum', '-resource:mills,VCF,known=true,training=true,truth=true,prior=12.0', 'data/Mills_Devine_2hit.indels.vcf', '--input', 'data/sample.sort.merge.vcf', '--tranches_file', 'data/sample.sort.merge.tranches', '--mode', 'INDEL', '--recal_file', 'data/sample.sort.merge.recal', '-R', 'data/chr11.fa'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))
 
    # TODO: need to test the command on real data 
    def test_apply_recalibration(self):
        task = ratatosk.lib.tools.gatk.ApplyRecalibration(target=self.mergebam.replace(".bam","-filter.vcf"), ref="data/chr11.fa")
        print _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0])
        print " ".join(_prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    # TODO: test case for multiple parent classes
    # Need to set bam and vcf input
    def test_readbackedphasing(self):
        task = ratatosk.lib.tools.gatk.ReadBackedPhasing(target=self.mergebam.replace(".bam", "-indel-filter.vcf"))
        print _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0])

@unittest.skipIf((os.getenv("SNPEFF_HOME") is None or os.getenv("SNPEFF_HOME") == ""), "No environment SNPEFF_HOME set; skipping")
class TestSnpEffWrappers(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.bam = os.path.join(indir, "sample.sort.bam")
        self.snpeff = os.path.join(os.environ["SNPEFF_HOME"], 'snpEff.jar')
        self.config = os.path.join(os.environ["SNPEFF_HOME"], 'snpEff.config')

    def test_snpeff(self):
        task = ratatosk.lib.annotation.snpeff.snpEff(target=self.bam.replace(".bam", "-effects.vcf"))
        self.assertEqual(['java', '-Xmx2g', '-jar', self.snpeff, 'eff', '-1', '-i', 'vcf', '-o', 'vcf', '-c', self.config, 'GRCh37.64', 'data/sample.sort.vcf', '>', 'data/sample.sort-effects.vcf'], _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_snpeff_txt(self):
        task = ratatosk.lib.annotation.snpeff.snpEff(target=self.bam.replace(".bam", "-effects.txt"),  target_suffix='.txt')
        self.assertEqual(['java', '-Xmx2g', '-jar', self.snpeff, 'eff', '-1', '-i', 'vcf', '-o', 'txt', '-c', self.config, 'GRCh37.64', 'data/sample.sort.vcf', '>', 'data/sample.sort-effects.txt'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))


@unittest.skipIf((os.getenv("ANNOVAR_HOME") is None or os.getenv("ANNOVAR_HOME") == ""), "No environment ANNOVAR_HOME set; skipping")
class TestAnnovarWrappers(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.bam = os.path.join(indir, "sample.sort.bam")

    def _path(self, exe):
        return os.path.join(os.environ["ANNOVAR_HOME"], exe)

    def test_convert_annovar(self):
        task = ratatosk.lib.annotation.annovar.Convert2Annovar(target=self.bam.replace(".bam", "-avinput.txt"))
        self.assertEqual([self._path('convert2annovar.pl'), '-format vcf4', 'data/sample.sort.vcf', '--outfile', 'data/sample.sort-avinput.txt'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_summarize_annovar(self):
        task = ratatosk.lib.annotation.annovar.SummarizeAnnovar(target=self.bam.replace(".bam", "-avinput.txt.log"))
        self.assertEqual([self._path('summarize_annovar.pl'), '-remove', '-buildver hg19', '-verdbsnp 132', '-ver1000g 1000g2011may', 'data/sample.sort-avinput.txt', self._path('humandb')],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))



class TestVcfToolsWrappers(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.bam = os.path.join(indir, "sample.sort.bam")


# Temporary target generator
def vcf_generator(task):
    return ["vcf1.vcf.gz", "vcf2.vcf.gz"]
class TestHtslibWrappers(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        import ratatosk.lib.variation.htslib
        self.bam = os.path.join(indir, "sample.sort.bam")

    def test_vcf_merge(self):
        task = ratatosk.lib.variation.htslib.VcfMerge(target="out.vcfmerge.vcf.gz", target_generator_handler='test.test_wrapper.vcf_generator')
        self.assertEqual(['vcf', 'merge', 'vcf1.vcf.gz', 'vcf2.vcf.gz', '>', 'out.vcfmerge.vcf.gz'],
                         _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))
    
class TestTabixWrappers(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        import ratatosk.lib.variation.tabix
        self.bam = os.path.join(indir, "sample.sort.bam")

    def test_bgzip(self):
        task = ratatosk.lib.variation.tabix.Bgzip(target=self.bam.replace(".bam", ".vcf.gz"))
        self.assertEqual(['bgzip', 'data/sample.sort.vcf'], _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_bgunzip(self):
        """Test bgunzip via three different function calls"""
        task = ratatosk.lib.variation.tabix.Bgzip(target=self.bam.replace(".bam", ".vcf"), target_suffix=".vcf", source_suffix=".vcf.gz", options=["-d"])
        self.assertEqual(['bgzip', '-d', 'data/sample.sort.vcf.gz'], _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))
        task = ratatosk.lib.variation.tabix.BgUnzip(target=self.bam.replace(".bam", ".vcf"))
        self.assertEqual(['bgzip', '-d', 'data/sample.sort.vcf.gz'], _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))
        task = ratatosk.lib.variation.tabix.BgUnzip(target=self.bam.replace(".bam", ".vcf"), options=["-d"])
        self.assertEqual(['bgzip', '-d', 'data/sample.sort.vcf.gz'], _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))

    def test_tabix(self):
        task = ratatosk.lib.variation.tabix.Tabix(target=self.bam.replace(".bam", ".vcf.gz.tbi"))
        self.assertEqual(['tabix', 'data/sample.sort.vcf.gz'], _prune_luigi_tmp(task.job_runner()._make_arglist(task)[0]))
