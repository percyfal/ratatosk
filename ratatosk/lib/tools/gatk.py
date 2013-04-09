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
"""
Provide wrappers for `GATK <http://www.broadinstitute.org/gatk/>`_


Classes
-------
"""
import os
import re
import luigi
import logging
from itertools import izip
import ratatosk.lib.files.external
import ratatosk.lib.tools.samtools
from ratatosk.utils import rreplace, fullclassname
from ratatosk.job import InputJobTask, JobTask
from ratatosk.jobrunner import DefaultShellJobRunner
import ratatosk.shell as shell
import pysam

logger = logging.getLogger('luigi-interface')

class GATKJobRunner(DefaultShellJobRunner):
    @staticmethod
    def _get_main(job):
        return "-T {}".format(job.main())

    def _make_arglist(self, job):
        if not job.jar() or not os.path.exists(os.path.join(job.path(),job.jar())):
            logger.error("Can't find jar: {0}, full path {1}".format(job.jar(),
                                                                     os.path.abspath(job.jar())))
            raise Exception("job jar does not exist")
        arglist = [job.java()] + job.java_opt() + ['-jar', os.path.join(job.path(), job.jar())]
        if job.main():
            arglist.append(self._get_main(job))
        if job.opts():
            arglist += job.opts()
        (tmp_files, job_args) = DefaultShellJobRunner._fix_paths(job)
        arglist += job_args
        return (arglist, tmp_files)

    def run_job(self, job):
        (arglist, tmp_files) = self._make_arglist(job)
        cmd = ' '.join(arglist)        
        logger.info("\nJob runner '{0}';\n\trunning command '{1}'".format(self.__class__, cmd))
        (stdout, stderr, returncode) = shell.exec_cmd(cmd, shell=True)
        if returncode == 0:
            logger.info("Shell job completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                # TODO : this should be relpath?
                a.move(os.path.join(os.curdir, b.path))
                # Some GATK programs generate bai or idx files on the fly...
                if os.path.exists(a.path + ".bai"):
                    logger.info("Saw {} file".format(a.path + ".bai"))
                    os.rename(a.path + ".bai", b.path.replace(".bam", ".bai"))
                if os.path.exists(a.path + ".idx"):
                    logger.info("Saw {} file".format(a.path + ".idx"))
                    os.rename(a.path + ".idx", b.path + ".idx")
        else:
            raise Exception("Job '{}' failed: \n{}".format(cmd, " ".join([stderr])))

class InputBamFile(InputJobTask):
    _config_section = "gatk"
    _config_subsection = "InputBamFile"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.BamFile")
    suffix = luigi.Parameter(default=".bam")

class InputVcfFile(InputJobTask):
    _config_section = "gatk"
    _config_subsection = "input_vcf_file"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.VcfFile")
    suffix = luigi.Parameter(default=".vcf")

class GATKJobTask(JobTask):
    _config_section = "gatk"
    exe_path = luigi.Parameter(default=os.getenv("GATK_HOME") if os.getenv("GATK_HOME") else os.curdir)
    executable = luigi.Parameter(default="GenomeAnalysisTK.jar")
    suffix = luigi.Parameter(default=".bam")
    java_exe = luigi.Parameter(default="java")
    java_options = luigi.Parameter(default=("-Xmx2g",), description="Java options", is_list=True)
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.gatk.InputBamFile",), is_list=True)
    ref = luigi.Parameter(default=None)
    # Additional commonly used options
    target_region = luigi.Parameter(default=None)

    def jar(self):
        return self.executable

    def exe(self):
        return self.jar()

    def java_opt(self):
        return list(self.java_options)

    def java(self):
        return self.java_exe

    def job_runner(self):
        return GATKJobRunner()


class GATKIndexedJobTask(GATKJobTask):
    """Similar to GATKJobTask, with the only difference that these
    sub_executables require indexed bam as input.

    TODO: sort out which these programs are
    """
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.gatk.InputBamFile",), is_list=True)

    def requires(self):
        """Task requirements. In many cases this is a single source
        whose name can be generated following the code below, and
        therefore doesn't need reimplementation in the subclasses."""
        bamcls = self.parent()[0]
        indexcls = ratatosk.lib.tools.samtools.Index
        return [cls(target=source) for cls, source in izip(self.parent(), self.source())] + [indexcls(target=rreplace(self.source()[0], bamcls().sfx(), indexcls().sfx(), 1), parent_task=fullclassname(bamcls))]


class RealignerTargetCreator(GATKIndexedJobTask):
    _config_subsection = "RealignerTargetCreator"
    sub_executable = "RealignerTargetCreator"
    known = luigi.Parameter(default=(), is_list=True)
    suffix = luigi.Parameter(default=".intervals")
    can_multi_thread = True

    def opts(self):
        retval = list(self.options)
        retval.append("-nt {}".format(self.num_threads))
        if self.target_region:
            retval.append("-L {}".format(self.target_region))
        retval.append(" ".join(["-known {}".format(x) for x in self.known]))
        return retval

    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for Realignment")
        retval += ["-R", self.ref]
        return retval

class IndelRealigner(GATKIndexedJobTask):
    _config_subsection = "IndelRealigner"
    sub_executable = "IndelRealigner"
    known = luigi.Parameter(default=(), is_list=True)
    label = luigi.Parameter(default=".realign")
    parent_task = luigi.Parameter(default=('ratatosk.lib.tools.gatk.InputBamFile',
                                           'ratatosk.lib.tools.gatk.RealignerTargetCreator',
                                           'ratatosk.lib.tools.gatk.UnifiedGenotyper',
                                           ), is_list=True)

    def opts(self):
        retval = list(self.options)
        retval += ["{}".format(" ".join(["-known {}".format(x) for x in self.known]))]
        return retval

    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output(), "--targetIntervals", self.input()[1]]
        # Here, known can be provided via extra vcf arguments; skip
        # last in list which is the indexed bam requirement
        if len(self.input()) > 2:
            retval += ["{}".format(" ".join(["-known {}".format(x.path) for x in self.input()[2:-1]]))]
        if not self.ref:
            raise Exception("need reference for Realignment")
        retval += ["-R", self.ref]
        return retval

class BaseRecalibrator(GATKIndexedJobTask):
    _config_subsection = "BaseRecalibrator"
    sub_executable = "BaseRecalibrator"
    knownSites = luigi.Parameter(default=(), is_list=True)
    suffix = luigi.Parameter(default=".recal_data.grp")

    def opts(self):
        retval = list(self.options)
        if self.target_region:
            retval += ["-L {}".format(self.target_region)]
        return retval

    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for BaseRecalibrator")
        if not self.knownSites:
            raise Exception("need knownSites to run BaseRecalibrator")
        retval += ["-R", self.ref]
        retval += [" ".join([" -knownSites {}".format(x) for x in self.knownSites])]
        return retval

class PrintReads(GATKJobTask):
    _config_subsection = "PrintReads"
    sub_executable = "PrintReads"
    parent_task = luigi.Parameter(default=('ratatosk.lib.tools.gatk.InputBamFile',
                                           'ratatosk.lib.tools.gatk.BaseRecalibrator'), is_list=True)
    label = luigi.Parameter(default=".recal")
    suffix = luigi.Parameter(default=(".bam",), is_list=True)

    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output()]
        if len(self.input()) > 1:
            retval += ["-BQSR", self.input()[1]]
        if not self.ref:
            raise Exception("need reference for PrintReads")
        retval += ["-R", self.ref]
        return retval

class ClipReads(GATKJobTask):
    _config_subsection = "ClipReads"
    sub_executable = "ClipReads"
    # Tailored for HaloPlex
    options = luigi.Parameter(default=("--cyclesToTrim 1-5 --clipRepresentation WRITE_NS",), is_list=True)
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.gatk.InputBamFile", ), is_list=True)
    label = luigi.Parameter(default=".clip")

    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for ClipReads")
        retval += ["-R", self.ref]
        return retval

class VariantEval(GATKJobTask):
    _config_subsection = "VariantEval"
    sub_executable = "VariantEval"
    options = luigi.Parameter(default=("-ST Filter -l INFO --doNotUseAllStandardModules --evalModule CompOverlap --evalModule CountVariants --evalModule GenotypeConcordance --evalModule TiTvVariantEvaluator --evalModule ValidationReport --stratificationModule Filter",), is_list=True)
    dbsnp = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.gatk.InputVcfFile", ), is_list=True)
    suffix = luigi.Parameter(default=".eval_metrics")

    def opts(self):
        retval = list(self.options)
        # TODO: Sort this one out
        if not self.dbsnp:
            raise Exception("need dbsnp for VariantEval")
        retval += ["--dbsnp", self.dbsnp]
        # TODO: This too
        if self.target_region:
            retval += ["-L", self.target_region]
        return retval
    
    def args(self):
        retval = ["--eval", self.input()[0], "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for VariantEval")
        retval += ["-R", self.ref]
        return retval

class VariantAnnotator(GATKJobTask):
    _config_subsection = "VariantAnnotator"
    sub_executable = "VariantAnnotator"
    options = luigi.Parameter(default=("",), is_list=True)
    dbsnp = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.gatk.InputVcfFile", ), is_list=True)
    suffix = luigi.Parameter(default=".vcf")
    snpeff = luigi.Parameter(default=None)
    label = luigi.Parameter(default="-gatkann")

    annotations = ["BaseQualityRankSumTest", "DepthOfCoverage", "FisherStrand",
                   "GCContent", "HaplotypeScore", "HomopolymerRun",
                   "MappingQualityRankSumTest", "MappingQualityZero",
                   "QualByDepth", "ReadPosRankSumTest", "RMSMappingQuality"]

    def opts(self):
        retval = list(self.options)
        if self.target_region:
            retval += ["-L {}".format(self.target_region)]
        return retval
    
    def args(self):
        retval = ["--variant", self.input()[0], "--out", self.output()]
        if not self.ref:
            raise Exception("need reference for VariantAnnotator")
        retval += ["-R", self.ref]
        for x in self.annotations:
            retval += ["-A", x]
        return retval

# NB: GATK requires snpEff version 2.0.5, nothing else. Therefore, it
# would be convenient to "lock" this version for this task, meaning
# ratatosk.lib.annotation.snpeff.snpEff must be version 2.0.5
class VariantSnpEffAnnotator(VariantAnnotator):
    _config_subsection = "VariantSnpEffAnnotator"
    label = luigi.Parameter(default="-annotated")
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.gatk.InputVcfFile",
                                           "ratatosk.lib.annotation.snpeff.snpEff"), is_list=True)
    suffix = luigi.Parameter(default=(".vcf",), is_list=True)

    def args(self):
        retval = ["--variant", self.input()[0], "--out", self.output(), "--snpEffFile", self.input()[1]]
        if not self.ref:
            raise Exception("need reference for VariantAnnotator")
        retval += ["-R", self.ref]
        retval += ["-A", "SnpEff"]
        return retval
        
class UnifiedGenotyper(GATKIndexedJobTask):
    _config_subsection = "UnifiedGenotyper"
    sub_executable = "UnifiedGenotyper"
    options = luigi.Parameter(default=("-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH",), is_list=True)
    suffix = luigi.Parameter(default=".vcf")
    dbsnp = luigi.Parameter(default=None)
    #label = luigi.Parameter(default=".RAW")?
    can_multi_thread = True

    def opts(self):
        retval = list(self.options)
        retval += ["-nt {}".format(self.num_threads)]
        if self.target_region:
            retval += ["-L {}".format(self.target_region)]
        if self.dbsnp:
            retval += ["--dbsnp", self.dbsnp]
        return retval

    def args(self):
        retval =  ["-I", self.input()[0], "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for UnifiedGenotyper")
        retval += ["-R", self.ref]
        return retval

class SplitUnifiedGenotyper(UnifiedGenotyper):
    # Label should be same as calling function (often CombineVariants)
    label = luigi.Parameter(default="-variants")
    suffix = luigi.Parameter(default=(".vcf", ), is_list=True)
    
    def _make_source_file_name(self, parent_cls):
        """Assume pattern is {base}-split/{base}-{ref}{ext}, as in
        CombineVariants.

        FIX ME: well, generalize
        """
        base = rreplace(os.path.join(os.path.dirname(os.path.dirname(self.target)), os.path.basename(self.target)), self.label, "", 1).split("-")
        return "".join(base[0:-1]) + parent_cls().sfx()

class CombineVariants(GATKJobTask):
    _config_subsection = "CombineVariants"
    sub_executable = "CombineVariants"
    suffix = luigi.Parameter(default=".vcf")
    label = luigi.Parameter(default="-variants")
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.gatk.SplitUnifiedGenotyper", "ratatosk.lib.tools.gatk.InputBamFile",), is_list=True)
    split = luigi.BooleanParameter(default=True)
    by_chromosome = luigi.BooleanParameter(default=True)

    def requires(self):
        cls = self.parent()[0]
        bamcls = self.parent()[1]
        source = self.source()[0]
        if self.split:
            # Partition sources by chromosome. Need to get the
            # references from the source bam file, i.e. the source to
            # the parent task
            bamfile = rreplace(source, self.sfx(), bamcls().sfx(), 1)
            if os.path.exists(bamfile):
                samfile = pysam.Samfile(bamfile, "rb")
                refs = samfile.references
                samfile.close()
            elif os.path.exists(os.path.expanduser(self.ref)):
                dictfile = os.path.expanduser(os.path.splitext(self.ref)[0] + ".dict")
                with open(dictfile) as fh:
                    seqdict = [x for x in fh.readlines() if x.startswith("@SQ")]
                m = [re.search(r'SN:([a-zA-z0-9]+)', x) for x in seqdict]
                refs = [x.group(1) for x in m]
            else:
                return []
            outdir = "{base}-split".format(base=os.path.splitext(self.target)[0])
            if not os.path.exists(outdir):
                os.makedirs(outdir)
            split_targets = [os.path.join("{base}-split".format(base=os.path.splitext(self.target)[0]), 
                                          "{base}-{ref}{ext}".format(base=os.path.splitext(os.path.basename(self.target))[0], ref=chr_ref, ext=self.sfx())) for chr_ref in refs]
            return [cls(target=tgt, target_region=chr_ref) for tgt in split_targets]
        else:
            return [cls(target=source)]

    def args(self):
        retval = []
        for x in self.input():
            retval += ["-V", x]
        retval += ["-o", self.output()]
        if not self.ref:
            raise Exception("need reference for CombineVariants")
        retval += ["-R", self.ref]
        return retval

class SelectVariants(GATKJobTask):
    _config_subsection = "SelectVariants"
    sub_executable = "SelectVariants"
    suffix = luigi.Parameter(default=".vcf")
    label = luigi.Parameter(default="-all")
    selectType = luigi.Parameter(default=("--selectTypeToInclude", "SNP", 
                                          "--selectTypeToInclude", "INDEL",
                                          "--selectTypeToInclude", "MIXED",
                                          "--selectTypeToInclude", "MNP",
                                          "--selectTypeToInclude", "SYMBOLIC",
                                          "--selectTypeToInclude", "NO_VARIATION"), is_list=True)
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.gatk.UnifiedGenotyper", ), is_list=True)

    def args(self):
        retval = [x for x in self.selectType]
        retval += ['--variant', self.input()[0], '--out', self.output()]
        if not self.ref:
            raise Exception("need reference for SelectVariants")
        retval += ["-R", self.ref]
        return retval

class SelectSnpVariants(SelectVariants):
    _config_subsection = "SelectSnpVariants"
    label = luigi.Parameter(default="-snp")
    selectType = luigi.Parameter(default=("--selectTypeToInclude", "SNP"), is_list=True)

class SelectIndelVariants(SelectVariants):
    _config_subsection = "SelectIndelVariants"
    label = luigi.Parameter(default="-indel")
    selectType = luigi.Parameter(default=("--selectTypeToInclude", "INDEL",
                                          "--selectTypeToInclude", "MIXED",
                                          "--selectTypeToInclude", "MNP",
                                          "--selectTypeToInclude", "SYMBOLIC",
                                          "--selectTypeToInclude", "NO_VARIATION"), is_list=True)

# Variant recalibration
#
# This section has many different tasks, tailored for various best practice settings
#
class VariantRecalibrator(GATKJobTask):
    """Generic VariantRecalibrator task from which specialized
    recalibration tasks inherit"""
    _config_subsection = "VariantRecalibrator"
    sub_executable = "VariantRecalibrator"
    label = luigi.Parameter(default=None)
    mode = luigi.Parameter(default="BOTH")
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.gatk.InputVcfFile", ), is_list=True)
    suffix = luigi.Parameter(default=(".tranches", ".recal"), is_list=True)
    options = luigi.Parameter(default=())

    def output(self):
        if isinstance(self.suffix, tuple):
            return [luigi.LocalTarget(rreplace(self.target, self.suffix[0], x, 1)) for x in self.suffix]
        else:
            return [luigi.LocalTarget(self.target)]

    def args(self):
        retval = ["--input", self.input()[0], 
                   "--tranches_file", self.output()[0]]
        retval += ['--mode', self.mode]
        if len(self.output()) == 2:
            retval += ["--recal_file", self.output()[1]]
        if not self.ref:
            raise Exception("need reference for VariantRecalibration")
        retval += ["-R", self.ref]
        return retval

class VariantSnpRecalibrator(VariantRecalibrator):
    _config_subsection = "VariantSnpRecalibrator"
    label = luigi.Parameter(default=None)
    mode = luigi.Parameter(default="SNP")
    suffix = luigi.Parameter(default=(".tranches", ".recal"))
    train_hapmap = luigi.Parameter(default=None)
    train_1000g_omni = luigi.Parameter(default=None)
    dbsnp = luigi.Parameter(default=None)
    options = luigi.Parameter(default=( 
                              "-an", "QD",
                              "-an", "HaplotypeScore",
                              "-an", "MQRankSum",
                              "-an", "ReadPosRankSum",
                              "-an", "FS",
                              "-an", "MQ",
                              "-an", "DP"), is_list=True)
    def opts(self):
        retval = list(self.options)
        if not self.train_hapmap and not self.train_1000g_omni:
            raise Exception("need training file for VariantSnp")
        if self.train_hapmap:
            retval += ["-resource:hapmap,VCF,known=false,training=true,truth=true,prior=15.0",
                       self.train_hapmap]
        if self.train_1000g_omni:
            retval += ["-resource:omni,VCF,known=false,training=true,truth=false,prior=12.0",
                       self.train_1000g_omni]
        if self.dbsnp:
            retval += ["-resource:dbsnp,VCF,known=true,training=false,truth=false,prior=8.0",
                       self.dbsnp]
        return retval

class VariantSnpRecalibratorExome(VariantSnpRecalibrator):
    """Variant snp recalibration, smaller callsets. Recommendations
    are to use 30 samples for VQSR, adding additional samples, or
    using other settings, as those implemented here.

    From http://gatkforums.broadinstitute.org/discussion/1259/what-vqsr-training-sets-arguments-should-i-use-for-my-specific-project:

    2. Use the VQSR with the smaller SNP callset but experiment with the precise
    argument settings (try adding --maxGaussians 4 --percentBad 0.05
    to your command line, for example)
    """

    _config_subsection = "VariantSnpRecalibratorRegional"
    options = luigi.Parameter(default=( 
                              "-an", "QD",
                              "-an", "HaplotypeScore",
                              "-an", "MQRankSum",
                              "-an", "ReadPosRankSum",
                              "-an", "FS",
                              "-an", "MQ",
                              "--maxGaussians", "4", 
                              "--percentBadVariants", "0.05"), is_list=True)
    

class VariantIndelRecalibrator(VariantRecalibrator):
    """From
    http://gatkforums.broadinstitute.org/discussion/1259/what-vqsr-training-sets-arguments-should-i-use-for-my-specific-project:

    Note that achieving great results with indels may require even
    more than the recommended 30 samples in your exome sequencing
    project
    """
    _config_subsection = "VariantIndelRecalibrator"
    label = luigi.Parameter(default=None)
    mode = luigi.Parameter(default="INDEL")
    train_indels = luigi.Parameter(default=None)
    options = luigi.Parameter(default=(
            "-an", "QD",
            "-an", "FS",
            "-an", "HaplotypeScore",
            "-an", "ReadPosRankSum"), is_list=True)

    def opts(self):
        retval = list(self.options)
        if not self.train_indels:
            raise Exception("need indel training file for VariantIndelRecalibrator")
        retval += ["-resource:mills,VCF,known=true,training=true,truth=true,prior=12.0",
                   self.train_indels]
        return retval

# 
# VariantFiltration
#
class VariantFiltration(GATKJobTask):
    """Generic VariantFiltration class"""
    _config_subsection = "VariantFiltration"
    sub_executable = "VariantFiltration"
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.gatk.InputVcfFile")
    label = luigi.Parameter(default=".filtered")
    suffix = luigi.Parameter(default=".vcf")
        
    def args(self):
        retval = ["--variant", self.input()[0], "--out", self.output()]
        if not self.ref:
            raise Exception("need reference for VariantFiltration")
        retval += ["-R", self.ref]
        return retval

class VariantFiltrationExp(VariantFiltration):
    """Perform hard filtering using JEXL expressions"""
    _config_subsection = "VariantFiltrationExp"
    expressions = luigi.Parameter(default=(), is_list=True)

    def opts(self):
        retval = list(self.options)
        for exp in self.expressions:
            retval += ["--filterName", "GATKStandard{e}".format(e=exp.split()[0]),
                       "--filterExpression", "'{}'".format(exp)]
        return retval

class VariantSnpFiltrationExp(VariantFiltrationExp):
    """Perform hard filtering using JEXL expressions"""
    _config_subsection = "VariantSnpFiltrationExp"
    label = luigi.Parameter(default="-filterSNP")
    expressions = luigi.Parameter(default=("QD < 2.0", "MQ < 40.0", "FS > 60.0",
                                           "HaplotypeScore > 13.0",
                                           "MQRankSum < -12.5",
                                           "ReadPosRankSum < -8.0"), is_list=True)

class VariantIndelFiltrationExp(VariantFiltrationExp):
    """Perform hard filtering using JEXL expressions"""
    _config_subsection = "VariantIndelFiltrationExp"
    label = luigi.Parameter(default="-filterINDEL")
    expressions = luigi.Parameter(default=("QD < 2.0", "ReadPosRankSum < -20.0", "FS > 200.0"), is_list=True)


#
# ApplyRecalibration
#
class ApplyRecalibration(GATKJobTask):
    _config_subsection = "ApplyRecalibration"
    sub_executable = "ApplyRecalibration"
    label = luigi.Parameter(default="-filter")
    mode = luigi.Parameter(default="SNP")
    parent_task = luigi.Parameter(default=("ratatosk.lib.tools.gatk.InputVcfFile",
                                           "ratatosk.lib.tools.gatk.VariantRecalibrator", ), is_list=True)
    suffix = luigi.Parameter(default=".vcf")
    
    def opts(self):
        retval = list(self.options)
        retval += ["--mode", self.mode]
        return retval

    def args(self):
        retval = ["--input", self.input()[0], "--tranches_file", self.input()[1][0],
                  "--recal_file", self.input()[1][1],
                  '--out', self.output()]
        if not self.ref:
            raise Exception("need reference for ApplyRecalibration")
        retval += ["-R", self.ref]
        return retval
    
    
class ReadBackedPhasing(GATKJobTask):
    _config_subsection = "ReadBackedPhasing"
    sub_executable = "ReadBackedPhasing"
    label = luigi.Parameter(default="-phased")
    parent_task = luigi.Parameter(default=('ratatosk.lib.tools.gatk.InputBamFile',
                                           'ratatosk.lib.tools.gatk.UnifiedGenotyper'), is_list=True)
    suffix = luigi.Parameter(default=".vcf")

    def args(self):
        retval = ["-I", self.input()[0], '--variant', self.input()[1],
                  '--out', self.output()]
        if not self.ref:
            raise Exception("need reference for ReadBackedPhasing")
        retval += ["-R", self.ref]
        return retval
    
