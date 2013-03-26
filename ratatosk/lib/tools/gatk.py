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
import os
import luigi
import logging
import ratatosk.lib.files.external
from ratatosk.utils import rreplace, fullclassname
from ratatosk.job import InputJobTask, JobTask, DefaultShellJobRunner
import ratatosk.shell as shell

logger = logging.getLogger('luigi-interface')

JAVA="java"
JAVA_OPTS="-Xmx2g"
GATK_HOME=os.getenv("GATK_HOME")
GATK_JAR="GenomeAnalysisTK.jar"

class GATKJobRunner(DefaultShellJobRunner):
    @staticmethod
    def _get_main(job):
        return "-T {}".format(job.main())

    def run_job(self, job):
        if not job.jar() or not os.path.exists(os.path.join(job.path(),job.jar())):
            logger.error("Can't find jar: {0}, full path {1}".format(job.jar(),
                                                                     os.path.abspath(job.jar())))
            raise Exception("job jar does not exist")
        arglist = [JAVA] + job.java_opt() + ['-jar', os.path.join(job.path(), job.jar())]
        if job.main():
            arglist.append(self._get_main(job))
        if job.opts():
            arglist += job.opts()
        (tmp_files, job_args) = DefaultShellJobRunner._fix_paths(job)
        arglist += job_args
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
    target_suffix = luigi.Parameter(default=".bam")

class InputVcfFile(InputJobTask):
    _config_section = "gatk"
    _config_subsection = "input_vcf_file"
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.VcfFile")
    
class GATKJobTask(JobTask):
    _config_section = "gatk"
    exe_path = luigi.Parameter(default=GATK_HOME)
    executable = luigi.Parameter(default=GATK_JAR)
    source_suffix = luigi.Parameter(default=".bam")
    target_suffix = luigi.Parameter(default=".bam")
    java_options = luigi.Parameter(default=("-Xmx2g",), description="Java options", is_list=True)
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.gatk.InputBamFile")
    ref = luigi.Parameter(default=None)
    # Additional commonly used options
    target_region = luigi.Parameter(default=None)

    def jar(self):
        return self.executable

    def exe(self):
        return self.jar()

    def java_opt(self):
        return list(self.java_options)

    def job_runner(self):
        return GATKJobRunner()


class GATKIndexedJobTask(GATKJobTask):
    """Similar to GATKJobTask, with the only difference that these
    sub_executables require indexed bam as input.

    TODO: sort out which these programs are
    """

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), ratatosk.lib.tools.samtools.IndexBam(target=rreplace(source, self.source_suffix, ".bai", 1), parent_task=fullclassname(cls))]


class RealignerTargetCreator(GATKIndexedJobTask):
    _config_subsection = "RealignerTargetCreator"
    sub_executable = "RealignerTargetCreator"
    known = luigi.Parameter(default=(), is_list=True)
    target_suffix = luigi.Parameter(default=".intervals")

    def opts(self):
        retval = list(self.options)
        if self.target_region:
            retval.append("-L {}".format(self.target_region))
        retval.append(" ".join(["-known {}".format(x) for x in self.known]))
        return " ".join(retval)

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), ratatosk.lib.tools.samtools.IndexBam(target=rreplace(source, self.source_suffix, ".bai", 1), parent_task=fullclassname(cls))]

    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for Realignment")
        retval.append(" -R {}".format(self.ref))
        return retval

class IndelRealigner(GATKIndexedJobTask):
    _config_subsection = "IndelRealigner"
    sub_executable = "IndelRealigner"
    known = luigi.Parameter(default=(), is_list=True)
    label = luigi.Parameter(default=".realign")
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.gatk.InputBamFile")
    source_suffix = luigi.Parameter(default=".bam")
    source = None

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source),
                ratatosk.lib.tools.samtools.IndexBam(target=rreplace(source, self.source_suffix, ".bai", 1), parent_task="ratatosk.lib.tools.gatk.InputBamFile"), 
                ratatosk.lib.tools.gatk.RealignerTargetCreator(target=rreplace(source, ".bam", ".intervals", 1))]

    def opts(self):
        retval = list(self.options)
        retval += ["{}".format(" ".join(["-known {}".format(x) for x in self.known]))]
        return " ".join(retval)

    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output(), "--targetIntervals", self.input()[2]]
        if not self.ref:
            raise Exception("need reference for Realignment")
        retval += [" -R {}".format(self.ref)]
        return retval

class BaseRecalibrator(GATKIndexedJobTask):
    _config_subsection = "BaseRecalibrator"
    sub_executable = "BaseRecalibrator"
    # Setting default=[] doesn't work?!?
    knownSites = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.gatk.InputBamFile")
    target_suffix = luigi.Parameter(default=".recal_data.grp")

    def opts(self):
        retval = list(self.options)
        if self.target_region:
            retval += ["-L {}".format(self.target_region)]
        return retval

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), ratatosk.lib.tools.samtools.IndexBam(target=rreplace(source, self.source_suffix, ".bai", 1), parent_task=fullclassname(cls))]

    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for BaseRecalibrator")
        if not self.knownSites:
            raise Exception("need knownSites to run BaseRecalibrator")
        retval += [" -R {}".format(self.ref)]
        retval += [" ".join([" -knownSites {}".format(x) for x in self.knownSites])]
        return retval


class PrintReads(GATKJobTask):
    # NB: print reads does *not* require BaseRecalibrator. Still this
    # is usually the case so supply an option
    _config_subsection = "PrintReads"
    sub_executable = "PrintReads"
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.gatk.BaseRecalibrator")
    label = luigi.Parameter(default=".recal")
    recalibrate = luigi.Parameter(default=True, is_boolean=True)
    source_suffix = luigi.Parameter(default=".recal_data.grp")
    sourceBSQR = None

    def args(self):
        # This is plain daft and inconsistent. If we want PrintReads
        # to run on a bam file for which there is baserecalibrated
        # output, it does *not* work to set requirements to point both
        # to IndelRealigner and
        # BaseReacalibrator(parent_task=IndelRealigner) - the
        # dependencies break. This fix changes meaning of input option
        # (-I) depending on whether we do recalibrate or note
        # TODO: sort this out - is the above statement really true?
        if self.recalibrate:
            inputfile = rreplace(self.input().fn, self.source_suffix, InputBamFile.target_suffix.default, 1)
            retval = ["-BQSR", self.input(), "-o", self.output(), "-I", inputfile]
        else:
            retval = ["-I", self.input(), "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for PrintReads")
        retval += [" -R {}".format(self.ref)]
        return retval

class ClipReads(GATKJobTask):
    _config_subsection = "ClipReads"
    sub_executable = "ClipReads"
    # Tailored for HaloPlex
    options = luigi.Parameter(default=("--cyclesToTrim 1-5 --clipRepresentation WRITE_NS",), is_list=True)
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.gatk.InputBamFile")
    label = luigi.Parameter(default=".clip")

    def args(self):
        retval = ["-I", self.input(), "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for ClipReads")
        retval += [" -R {}".format(self.ref)]
        return retval

class VariantFiltration(GATKJobTask):
    _config_subsection = "VariantFiltration"
    sub_executable = "VariantFiltration"
    # Options from Halo
    options = luigi.Parameter(default=('--clusterWindowSize 10 --clusterSize 3 --filterExpression "MQ0 >= 4 && ((MQ0 / (1.0 * DP)) > 0.1)" --filterName "HARD_TO_VALIDATE" --filterExpression "DP < 10" --filterName "LowCoverage" --filterExpression "QUAL < 30.0" --filterName "VeryLowQual" --filterExpression "QUAL > 30.0 && QUAL < 50.0" --filterName "LowQual" --filterExpression "QD < 1.5" --filterName "LowQD"',), is_list=True)
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.gatk.InputVcfFile")
    label = luigi.Parameter(default=".filtered")
    target_suffix = luigi.Parameter(default=".vcf")
    source_suffix = luigi.Parameter(default=".vcf")

    def args(self):
        retval = ["--variant", self.input(), "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for VariantFiltration")
        retval += [" -R {}".format(self.ref)]
        return retval

class VariantEval(GATKJobTask):
    _config_subsection = "VariantEval"
    sub_executable = "VariantEval"
    options = luigi.Parameter(default=("-ST Filter -l INFO --doNotUseAllStandardModules --evalModule CompOverlap --evalModule CountVariants --evalModule GenotypeConcordance --evalModule TiTvVariantEvaluator --evalModule ValidationReport --stratificationModule Filter",), is_list=True)
    dbsnp = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.gatk.InputVcfFile")
    source_suffix = luigi.Parameter(default=".vcf")
    target_suffix = luigi.Parameter(default=".eval_metrics")

    def opts(self):
        retval = list(self.options)
        # TODO: Sort this one out
        if not self.dbsnp:
            raise Exception("need dbsnp for VariantEval")
        retval += [" --dbsnp {}".format(self.dbsnp)]
        # TODO: This too
        if self.target_region:
            retval += ["-L {}".format(self.target_region)]
        return retval
    
    def args(self):
        retval = ["--eval", self.input(), "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for VariantEval")
        retval += [" -R {}".format(self.ref)]
        return retval

class VariantAnnotator(GATKJobTask):
    _config_subsection = "VariantAnnotator"
    sub_executable = "VariantAnnotator"
    options = luigi.Parameter(default=("",), is_list=True)
    dbsnp = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.gatk.InputVcfFile")
    source_suffix = luigi.Parameter(default=".vcf")
    target_suffix = luigi.Parameter(default=".txt")
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
        retval = ["--variant", self.input(), "--out", self.output()]
        if not self.ref:
            raise Exception("need reference for VariantAnnotator")
        retval += [" -R {}".format(self.ref)]
        for x in self.annotations:
            retval += ["-A", x]
        return retval

# NB: GATK requires snpEff version 2.0.5, nothing else. Therefore, it
# would be convenient to "lock" this version for this task, meaning
# ratatosk.lib.annotation.snpeff.snpEff must be version 2.0.5
class VariantSnpEffAnnotator(VariantAnnotator):
    _config_subsection = "VariantSnpEffAnnotator"
    label = luigi.Parameter(default="-annotated")

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), 
                # Once again, this shows the necessity of multiple
                # parent_tasks. Here, we have to use the default value
                # for snpEff label.
                ratatosk.lib.annotation.snpeff.snpEff(target=rreplace(source, "{}".format(self.source_suffix), "{}{}".format(ratatosk.lib.annotation.snpeff.snpEff.label.default, self.source_suffix), 1))]

    def args(self):
        retval = ["--variant", self.input()[0], "--out", self.output(), "--snpEffFile", self.input()[1]]
        if not self.ref:
            raise Exception("need reference for VariantAnnotator")
        retval += [" -R {}".format(self.ref)]
        retval += ["-A", "SnpEff"]
        return retval

        
class UnifiedGenotyper(GATKIndexedJobTask):
    _config_subsection = "UnifiedGenotyper"
    sub_executable = "UnifiedGenotyper"
    options = luigi.Parameter(default=("-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH",), is_list=True)
    target_suffix = luigi.Parameter(default=".vcf")
    dbsnp = luigi.Parameter(default=None)
    #label = luigi.Parameter(default=".RAW")?

    def opts(self):
        retval = list(self.options)
        if self.target_region:
            retval += ["-L {}".format(self.target_region)]
        if self.dbsnp:
            retval += [" --dbsnp {}".format(self.dbsnp)]
        return retval

    def args(self):
        retval =  ["-I", self.input()[0], "-o", self.output()]
        if not self.ref:
            raise Exception("need reference for UnifiedGenotyper")
        retval += [" -R {}".format(self.ref)]
        return retval

