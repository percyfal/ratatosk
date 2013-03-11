import os
import glob
import sys
import unittest
import luigi
import time
import logging
import ratatosk.bwa as BWA
import ratatosk.samtools as SAM
import ratatosk.fastq as FASTQ
import ratatosk.picard as PICARD
import ratatosk.gatk as GATK
import ratatosk.cutadapt as CUTADAPT
import ratatosk.fastqc as FASTQC
import ratatosk.external

# FIXME: Want to get rid of these in the application
gatk_msg = "No GATK_HOME variable set; export and set it to GATK root directory"
picard_msg = "No PICARD_HOME variable set; export and set it to PICARD root directory"

# Check for ngstestdata
ngsloadmsg = "No ngstestdata module; skipping test. Do a 'git clone https://github.com/percyfal/ngs.test.data' followed by 'python setup.py install'"
has_ngstestdata = False
try:
    import ngstestdata as ntd
    has_ngstestdata = True
except:
    pass

# Currently need to check for GATK_HOME and PICARD_HOME
has_gatk = os.getenv("GATK_HOME") is not None
has_picard = os.getenv("PICARD_HOME") is not None

logger = logging.getLogger('luigi-interface')

bwa = "bwa"
samtools = "samtools"

if has_ngstestdata:
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
process = os.popen("ps x -o pid,args | grep luigid | grep -v grep").read() #sometimes have to use grep -v grep
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
    
@unittest.skipIf(not has_ngstestdata, ngsloadmsg)
@unittest.skipIf(not has_gatk, gatk_msg)
@unittest.skipIf(not has_picard, picard_msg)
class TestLuigiWrappers(unittest.TestCase):
    # @classmethod
    # def tearDownClass(cls):
    #     for f in [sai1, sai2, sam, bam, sortbam]:
    #         if os.path.exists(f):
    #             os.unlink(f)

    def test_luigihelp(self):
        try:
            luigi.run(['-h'], main_task_cls=FASTQ.FastqFileLink)
        except:
            pass

    def test_fastqln(self):
        luigi.run(_luigi_args(['--fastq', fastq1, '--config-file', localconf]), main_task_cls=FASTQ.FastqFileLink)

    def test_bwaaln(self):
        luigi.run(_luigi_args(['--fastq', fastq1, '--config-file', localconf]), main_task_cls=BWA.BwaAln)
        luigi.run(_luigi_args(['--fastq', fastq2, '--config-file', localconf]), main_task_cls=BWA.BwaAln)

    # Will currently fail if links aren't present since it doesn't
    # know where the links come from (hence _make_file_links function)
    def test_bwasampe(self):
        _make_file_links()
        luigi.run(_luigi_args(['--sai1', sai1, '--sai2', sai2, '--config-file', localconf]), main_task_cls=BWA.BwaSampe)

    # Also fails; depends on InputSamFile, which only exists if
    # BWA.BwaSampe has been run. See below for putting different
    # modules together.
    def test_sortbam(self):
        _make_file_links()
        luigi.run(_luigi_args(['--bam', bam, '--config-file', localconf]), main_task_cls=SAM.SortBam)

    def test_picard_sortbam(self):
        _make_file_links()
        luigi.run(_luigi_args(['--bam', bam, '--config-file', localconf]), main_task_cls=PICARD.SortSam)

    def test_picard_alignmentmetrics(self):
        _make_file_links()
        luigi.run(_luigi_args(['--bam', bam,'--options', 'REFERENCE_SEQUENCE={}'.format(bwaseqref), '--config-file', localconf]), main_task_cls=PICARD.AlignmentMetrics)

    def test_picard_insertmetrics(self):
        _make_file_links()
        luigi.run(_luigi_args(['--bam', bam,'--options', 'REFERENCE_SEQUENCE={}'.format(bwaseqref), '--config-file', localconf]), main_task_cls=PICARD.InsertMetrics)

    def test_picard_dupmetrics(self):
        _make_file_links()
        luigi.run(_luigi_args(['--bam', sortbam, '--config-file', localconf]), main_task_cls=PICARD.DuplicationMetrics)

    def test_picard_hsmetrics(self):
        _make_file_links()
        luigi.run(_luigi_args(['--bam', sortbam, '--config-file', localconf]), main_task_cls=PICARD.HsMetrics)

    def test_gatk_ug(self):
        _make_file_links()
        luigi.run(_luigi_args(['--bam', sortbam, '--config-file', localconf]), main_task_cls=GATK.UnifiedGenotyper)

    def test_picard_metrics(self):
        _make_file_links()
        luigi.run(_luigi_args(['--bam', sortbam, '--config-file', localconf]), main_task_cls=PICARD.PicardMetrics)

    def test_cutadapt(self):
        _make_file_links()
        luigi.run(_luigi_args(['--fastq', os.path.basename(fastq1), '--config-file', localconf]), main_task_cls=CUTADAPT.CutadaptJobTask)
        
    def test_fastqc(self):
        _make_file_links()
        luigi.run(_luigi_args(['--seqfile', os.path.basename(fastq1), '--config-file', localconf]), main_task_cls=FASTQC.FastQCJobTask)

    # Depends on previous tasks (sortbam) - bam must be present
    def test_realignment_target_creator(self):
        luigi.run(_luigi_args(['--bam', os.path.basename(sortbam), '--config-file', localconf]), main_task_cls=GATK.RealignmentTargetCreator)

    def test_indel_realigner(self):
        luigi.run(_luigi_args(['--bam', os.path.basename(sortbam), '--config-file', localconf]), main_task_cls=GATK.IndelRealigner)

    def test_base_recalibrator(self):
        luigi.run(_luigi_args(['--bam', os.path.basename(realignbam), '--config-file', localconf]), main_task_cls=GATK.BaseRecalibrator)

    def test_print_reads(self):
        luigi.run(_luigi_args(['--bam', os.path.basename(realignbam), '--config-file', localconf]), main_task_cls=GATK.PrintReads)

    def test_clip_reads(self):
        luigi.run(_luigi_args(['--bam', os.path.basename(recalbam), '--config-file', localconf]), main_task_cls=GATK.ClipReads)

    # TODO: Test vcf outputs
    def test_unified_genotyper(self):
        luigi.run(_luigi_args(['--bam', os.path.basename(clipbam), '--config-file', localconf]), main_task_cls=GATK.UnifiedGenotyper)

    def test_variant_filtration(self):
        luigi.run(_luigi_args(['--vcf', os.path.basename(clipvcf), '--config-file', localconf]), main_task_cls=GATK.VariantFiltration)

    def test_variant_evaluation(self):
        luigi.run(_luigi_args(['--vcf', os.path.basename(filteredvcf), '--config-file', localconf]), main_task_cls=GATK.VariantEval)



@unittest.skipIf(not has_ngstestdata, ngsloadmsg)        
@unittest.skipIf(not has_gatk, gatk_msg)
@unittest.skipIf(not has_picard, picard_msg)
class TestLuigiParallel(unittest.TestCase):
    def test_bwa_samples(self):
        pass

    def test_sample_list(self):
        class BwaAlnSamples(BWA.BwaJobTask):
            samples = luigi.Parameter(default=[], is_list=True)

            def main(self):
                return "aln"

            def requires(self):
                indir = FASTQ.FastqFileLink().indir
                fastq = []
                for s in self.samples:
                    print "setting up requirements for sample {}".format(s)
                    if not os.path.exists(os.path.join(indir, s)):
                        print("No such sample {0} found in input directory {1}; skipping".format(s, indir))
                        continue
                    for fc in os.listdir(os.path.join(indir, s)):
                        fcdir = os.path.join(indir, s, fc)
                        if not os.path.isdir(fcdir):
                            print("{0} not a directory; skipping".format(fcdir))
                            continue
                        glob_str = os.path.join(fcdir, "{}*.fastq.gz".format(s))
                        print("looking in flowcell directory {} with glob {}".format(fcdir, glob_str))
                        fastqfiles = glob.glob(glob_str)
                        logging.info("found fastq files {}".format(fastqfiles))
                        fastq += fastqfiles
                print ("Found {} fastq files".format(len(fastq)))
                #print ("Found {}".format(self.fastq))

                return [FASTQ.FastqFileLink(x) for x in fastq]

            def args(self):
                return []

            def run(self):
                print "Found {} fastq files".format(len(self.input()))

                
            def output(self):
                return luigi.LocalTarget("tabort.txt")
        luigi.run(_luigi_args(['--samples', "P001_101_index3", '--config-file', localconf]), main_task_cls=BwaAlnSamples)
        

class SampeToSamtools(SAM.SamToBam):
    def requires(self):
        return BWA.BwaSampe(sai1=os.path.join(self.sam.replace(".sam", BWA.BwaSampe().read1_suffix + ".sai")),
                            sai2=os.path.join(self.sam.replace(".sam", BWA.BwaSampe().read2_suffix + ".sai")))

@unittest.skipIf(not has_ngstestdata, ngsloadmsg)
@unittest.skipIf(not has_gatk, gatk_msg)
@unittest.skipIf(not has_picard, picard_msg)
class TestLuigiPipelines(unittest.TestCase):
    def test_sampe_to_samtools(self):
        luigi.run(_luigi_args(['--sam', sam, '--config-file', localconf]), main_task_cls=SampeToSamtools)

    def test_sampe_to_samtools_sort(self):
        luigi.run(_luigi_args(['--bam', bam, '--config-file', localconf]), main_task_cls=SAM.SortBam)

    def test_sampe_to_picard_sort(self):
        luigi.run(_luigi_args(['--bam', bam, '--config-file', localconf]), main_task_cls=PICARD.SortSam)
