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
import ratatosk.external
from ratatosk.utils import rreplace
from ratatosk.job import JobTask, DefaultShellJobRunner
from cement.utils import shell

logger = logging.getLogger('luigi-interface')

JAVA="java"
JAVA_OPTS="-Xmx2g"
GATK_HOME=os.getenv("GATK_HOME")
GATK_JAR="GenomeAnalysisTK.jar"

class GATKJobRunner(DefaultShellJobRunner):
    # How configure this best way?
    path = GATK_HOME

    @staticmethod
    def _get_main(job):
        return "-T {}".format(job.main())

    def run_job(self, job):
        if not job.jar() or not os.path.exists(os.path.join(self.path,job.jar())):
            logger.error("Can't find jar: {0}, full path {1}".format(job.jar(),
                                                                     os.path.abspath(job.jar())))
            raise Exception("job jar does not exist")
        arglist = ['java', job.java_opt(), '-jar', os.path.join(self.path, job.jar())]
        if job.main():
            arglist.append(self._get_main(job))
        if job.opts():
            arglist.append(job.opts())
        (tmp_files, job_args) = DefaultShellJobRunner._fix_paths(job)
        arglist += job_args
        cmd = ' '.join(arglist)        
        logger.info(cmd)
        (stdout, stderr, returncode) = shell.exec_cmd(cmd, shell=True)

        if returncode == 0:
            logger.info("Shell job completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                # TODO : this should be relpath?
                a.move(os.path.join(os.curdir, b.path))
                # Some GATK programs generate bai files on the fly...
                if os.path.exists(a.path + ".bai"):
                    logger.info("Saw {} file".format(a.path + ".bai"))
                    os.rename(a.path + ".bai", b.path.replace(".bam", ".bai"))
        else:
            raise Exception("Job '{}' failed: \n{}".format(cmd, " ".join([stderr])))

class InputBamFile(JobTask):
    _config_section = "gatk"
    _config_subsection = "InputBamFile"
    target = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.BamFile")
    def requires(self):
        cls = self.set_parent_task()
        return cls(target=self.target)
    def output(self):
        return luigi.LocalTarget(self.target)

class InputVcfFile(JobTask):
    _config_section = "gatk"
    _config_subsection = "input_vcf_file"
    target = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.external.VcfFile")
    def requires(self):
        cls = self.set_parent_task()
        return cls(target=self.target)
    def output(self):
        return luigi.LocalTarget(self.target)
    
class GATKJobTask(JobTask):
    _config_section = "gatk"
    gatk = luigi.Parameter(default=GATK_JAR)
    target = luigi.Parameter(default=None)
    source_suffix = luigi.Parameter(default=".bam")
    target_suffix = luigi.Parameter(default=".bam")
    java_options = luigi.Parameter(default="-Xmx2g")
    parent_task = luigi.Parameter(default="ratatosk.gatk.InputBamFile")
    ref = luigi.Parameter(default=None)
    # Additional commonly used options
    target_region = luigi.Parameter(default=None)

    def jar(self):
        return self.gatk

    def exe(self):
        return self.jar()

    def java_opt(self):
        return self.java_options

    def job_runner(self):
        return GATKJobRunner()

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), ratatosk.samtools.IndexBam(target=rreplace(source, self.source_suffix, ".bai", 1))]

class RealignerTargetCreator(GATKJobTask):
    _config_subsection = "RealignerTargetCreator"
    target = luigi.Parameter(default=None)
    options = luigi.Parameter(default=None)
    known = luigi.Parameter(default=[], is_list=True)
    target_suffix = luigi.Parameter(default=".intervals")

    def opts(self):
        retval = self.options if self.options else ""
        if not self.ref:
            raise Exception("need reference for Realignment")
        retval += " -R {}".format(self.ref)
        if self.target_region:
            retval += "-L {}".format(self.target_region)
        retval += " ".join(["-known {}".format(x) for x in self.known])
        return retval

    def main(self):
        return "RealignerTargetCreator"

    def output(self):
        return luigi.LocalTarget(self.target)

    def args(self):
        return ["-I", self.input()[0], "-o", self.output()]

class IndelRealigner(GATKJobTask):
    _config_subsection = "IndelRealigner"
    bam = luigi.Parameter(default=None)
    known = luigi.Parameter(default=[], is_list=True)
    label = luigi.Parameter(default=".realign")
    parent_task = luigi.Parameter(default="ratatosk.gatk.InputBamFile")
    
    def main(self):
        return "IndelRealigner"

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), RealignerTargetCreator(target=rreplace(source, self.target_suffix, RealignerTargetCreator.target_suffix.default, 1))]

    def output(self):
        return luigi.LocalTarget(self.target)

    def opts(self):
        retval = self.options if self.options else ""
        if not self.ref:
            raise Exception("need reference for Realignment")
        retval += " -R {}".format(self.ref)
        retval += "{}".format(" ".join(["-known {}".format(x) for x in self.known]))
        return retval

    def args(self):
        return ["-I", self.input()[0], "-o", self.output(), "--targetIntervals", self.input()[1]]

class BaseRecalibrator(GATKJobTask):
    _config_subsection = "BaseRecalibrator"
    # Setting default=[] doesn't work?!?
    knownSites = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.gatk.InputBamFile")
    target_suffix = luigi.Parameter(default=".recal_data.grp")

    def main(self):
        return "BaseRecalibrator"

    def opts(self):
        retval = self.options if self.options else ""
        if not self.ref:
            raise Exception("need reference for BaseRecalibrator")
        if not self.knownSites:
            raise Exception("need knownSites to run BaseRecalibrator")
        retval += " -R {}".format(self.ref)
        if self.target_region:
            retval += "-L {}".format(self.target_region)
        retval += " ".join([" -knownSites {}".format(x) for x in self.knownSites])
        return retval

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), ratatosk.samtools.IndexBam(target=source)]

    def output(self):
        return luigi.LocalTarget(self.target)
    
    def args(self):
        return ["-I", self.input()[0], "-o", self.output()]


class PrintReads(GATKJobTask):
    _config_subsection = "PrintReads"
    parent_task = luigi.Parameter(default="ratatosk.gatk.InputBamFile")
    label = luigi.Parameter(default=".recal")

    def main(self):
        return "PrintReads"

    def opts(self):
        retval = self.options if self.options else ""
        if not self.ref:
            raise Exception("need reference for PrintReads")
        retval += " -R {}".format(self.ref)
        return retval

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)

    def output(self):
        return luigi.LocalTarget(self.target)
    
    def args(self):
        return ["-I", self.input(), "-BQSR", rreplace(self.input().fn, self.target_suffix, BaseRecalibrator.target_suffix.default, 1), "-o", self.output()]


class ClipReads(GATKJobTask):
    _config_subsection = "ClipReads"
    target = luigi.Parameter(default=None)
    # Tailored for HaloPlex
    options = luigi.Parameter(default="--cyclesToTrim 1-5 --clipRepresentation WRITE_NS")
    parent_task = luigi.Parameter(default="ratatosk.gatk.InputBamFile")
    label = luigi.Parameter(default=".clip")

    def main(self):
        return "ClipReads"

    def opts(self):
        retval = self.options if self.options else ""
        if not self.ref:
            raise Exception("need reference for ClipReads")
        retval += " -R {}".format(self.ref)
        return retval

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)

    def output(self):
        return luigi.LocalTarget(self.target)
    
    def args(self):
        return ["-I", self.input(), "-o", self.output()]

class VariantFiltration(GATKJobTask):
    _config_subsection = "VariantFiltration"
    target = luigi.Parameter(default=None)
    # Options from Halo
    options = luigi.Parameter(default='--clusterWindowSize 10 --clusterSize 3 --filterExpression "MQ0 >= 4 && ((MQ0 / (1.0 * DP)) > 0.1)" --filterName "HARD_TO_VALIDATE" --filterExpression "DP < 10" --filterName "LowCoverage" --filterExpression "QUAL < 30.0" --filterName "VeryLowQual" --filterExpression "QUAL > 30.0 && QUAL < 50.0" --filterName "LowQual" --filterExpression "QD < 1.5" --filterName "LowQD"')
    parent_task = luigi.Parameter(default="ratatosk.gatk.InputVcfFile")
    label = luigi.Parameter(default=".filtered")
    target_suffix = luigi.Parameter(default=".vcf")
    source_suffix = luigi.Parameter(default=".vcf")
    
    def main(self):
        return "VariantFiltration"

    def opts(self):
        retval = self.options if self.options else ""
        if not self.ref:
            raise Exception("need reference for VariantFiltration")
        retval += " -R {}".format(self.ref)
        return retval

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)

    def output(self):
        return luigi.LocalTarget(self.target)
    
    def args(self):
        return ["--variant", self.input(), "-o", self.output()]


class VariantEval(GATKJobTask):
    _config_subsection = "VariantEval"
    options = luigi.Parameter(default="-ST Filter -l INFO --doNotUseAllStandardModules --evalModule CompOverlap --evalModule CountVariants --evalModule GenotypeConcordance --evalModule TiTvVariantEvaluator --evalModule ValidationReport --stratificationModule Filter")
    dbsnp = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.gatk.InputVcfFile")
    source_suffix = luigi.Parameter(default=".vcf")
    target_suffix = luigi.Parameter(default=".eval_metrics")

    def main(self):
        return "VariantEval"

    def opts(self):
        retval = self.options if self.options else ""
        if not self.ref:
            raise Exception("need reference for VariantEval")
        # TODO: Sort this one out
        if not self.dbsnp:
            raise Exception("need dbsnp for VariantEval")
        retval += " -R {}".format(self.ref)
        # TODO: This too
        if self.target_region:
            retval += "-L {}".format(self.target_region)
        return retval

    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)

    def output(self):
        return luigi.LocalTarget(self.target)
    
    def args(self):
        return ["--eval", self.input(), "-o", self.output()]

        
class UnifiedGenotyper(GATKJobTask):
    _config_subsection = "UnifiedGenotyper"
    options = luigi.Parameter(default="-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH")
    target_suffix = luigi.Parameter(default=".vcf")
    #label = luigi.Parameter(default=".RAW")?

    def opts(self):
        retval = self.options if self.options else ""
        if not self.ref:
            raise Exception("need reference for UnifiedGenotyper")
        retval += " -R {}".format(self.ref)
        if self.target_region:
            retval += "-L {}".format(self.target_region)
        return retval

    def main(self):
        return "UnifiedGenotyper"

    def output(self):
        return luigi.LocalTarget(self.target)

    def args(self):
        return ["-I", self.input()[0], "-o", self.output()]

