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
import luigi
import os
import glob
import csv
import logging
import ratatosk
from ratatosk.job import PipelineTask, JobTask, JobWrapperTask
from ratatosk.utils import rreplace, fullclassname
from ratatosk.lib.align.bwa import BwaSampe, BwaAln
from ratatosk.lib.tools.gatk import VariantEval, UnifiedGenotyper, RealignerTargetCreator, IndelRealigner
from ratatosk.lib.tools.picard import PicardMetrics, MergeSamFiles
from ratatosk.lib.tools.fastqc import FastQCJobTask

logger = logging.getLogger('luigi-interface')

# Dirty fix: standard bwa sampe calculates the wrong target names when
# trimming and syncing has been done. This once again shows the
# importance of collecting all labels preceding a given node in order
# to calculate the target name
class HaloBwaSampe(BwaSampe):
    _config_subsection = "HaloBwaSampe"
    parent_task = luigi.Parameter(default="ratatosk.lib.align.bwa.BwaAln")

    def requires(self):
        # From target name, generate sai1, sai2, fastq1, fastq2
        sai1 = rreplace(rreplace(rreplace(self._make_source_file_name(), self.source_suffix, self.read1_suffix + self.source_suffix, 1), ".trimmed.sync", "", 1), ".sai", ".trimmed.sync.sai", 1)
        sai2 = rreplace(rreplace(rreplace(self._make_source_file_name(), self.source_suffix, self.read2_suffix + self.source_suffix, 1), ".trimmed.sync", "", 1), ".sai", ".trimmed.sync.sai", 1)
        return [BwaAln(target=sai1), BwaAln(target=sai2)]

# Raw variant calling class done on merged data to generate a list of
# raw candidates around which realignment is done.
class RawUnifiedGenotyper(UnifiedGenotyper):
    _config_subsection = "RawUnifiedGenotyper"
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.picard.MergeSamFiles")
    options = luigi.Parameter(default=["-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH"], is_list=True)
    label = ".BOTH.raw"

# Override RealignerTargetCreator and IndelRealigner to use
# RawUnifiedGenotyper vcf as input for known sites
class RawRealignerTargetCreator(RealignerTargetCreator):
    _config_subsection = "RawRealignerTargetCreator"
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.picard.MergeSamFiles")
    target_suffix = luigi.Parameter(default=".intervals")
    
    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), ratatosk.lib.tools.samtools.IndexBam(target=rreplace(source, self.source_suffix, ".bai", 1), parent_task=fullclassname(cls)), ratatosk.pipeline.haloplex.RawUnifiedGenotyper(target=rreplace(source, ".bam", ".BOTH.raw.vcf", 1))]

    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output(), "-known", self.input()[2]]
        if not self.ref:
            raise Exception("need reference for Realignment")
        retval.append(" -R {}".format(self.ref))
        return retval

# NOTE: Here I redefine target dependencies for IndelRealigner, the way I believe it should be
class RawIndelRealigner(IndelRealigner):
    _config_subsection = "RawIndelRealigner"
    parent_task = luigi.Parameter(default="ratatosk.lib.tools.picard.MergeSamFiles")
    source_suffix = luigi.Parameter(default=".bam")
    
    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), 
                ratatosk.lib.tools.samtools.IndexBam(target=rreplace(source, self.source_suffix, ".bai", 1), parent_task="ratatosk.lib.tools.picard.MergeSamFiles"), 
                ratatosk.pipeline.haloplex.RawRealignerTargetCreator(target=rreplace(source, ".bam", ".intervals", 1)),
                ratatosk.pipeline.haloplex.RawUnifiedGenotyper(target=rreplace(source, ".bam", ".BOTH.raw.vcf", 1))]
    
    def args(self):
        retval = ["-I", self.input()[0], "-o", self.output(),
                  "--targetIntervals", self.input()[2],
                  "-known", self.input()[3]]
        if not self.ref:
            raise Exception("need reference for Realignment")
        retval.append(" -R {}".format(self.ref))
        return retval

class HaloPlex(PipelineTask):
    # TODO: remove project, projectdir and just use indir?
    _config_section = "pipeline"
    _config_subsection = "HaloPlex"
    # Weird: after subclassing job.PipelineTask, not having a default
    # here throws an incomprehensible error
    project = luigi.Parameter(description="Project name (equals directory name", default=None)
    projectdir = luigi.Parameter(description="Where projects live", default=os.curdir)
    sample = luigi.Parameter(default=None, description="Sample directory.")
    # Hard-code this for now - would like to calculate on the fly so that
    # implicit naming is unnecessary
    final_target_suffix = "trimmed.sync.sort.merge.realign.recal.clip.filtered.eval_metrics"

    def requires(self):
        # List requirements for completion, consisting of classes above
        if self.project is None:
            return []
        tgt_fun = self.set_target_generator_function()
        if not tgt_fun:
            return []
        target_list = tgt_fun(self)
        # Hardcode read suffixes here for now
        # TODO: this is also a local modification so should be moved out of here. 
        reads = ["{}_R1_001.fastq.gz".format(x) for x in target_list] +  ["{}_R2_001.fastq.gz".format(x) for x in target_list]
        variant_target_list = ["{}.{}".format(x, self.final_target_suffix) for x in target_list]
        picard_metrics_target_list = ["{}.{}".format(x, "trimmed.sync.sort.merge") for x in target_list]
        return [VariantEval(target=tgt) for tgt in variant_target_list] + [PicardMetrics(target=tgt2) for tgt2 in picard_metrics_target_list]# + [FastQCJobTask(target=tgt) for tgt in reads]


