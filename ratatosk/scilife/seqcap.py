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
from ratatosk.utils import rreplace, fullclassname
from ratatosk.bwa import BwaSampe, BwaAln
from ratatosk.gatk import VariantEval, UnifiedGenotyper, RealignerTargetCreator, IndelRealigner
from ratatosk.picard import PicardMetrics

logger = logging.getLogger('luigi-interface')

# Dirty fix: standard bwa sampe calculates the wrong target names when
# trimming and syncing has been done. This once again shows the
# importance of collecting all labels preceding a given node in order
# to calculate the target name
class HaloBwaSampe(BwaSampe):
    _config_subsection = "HaloBwaSampe"
    parent_task = luigi.Parameter(default="ratatosk.bwa.BwaAln")

    def requires(self):
        # From target name, generate sai1, sai2, fastq1, fastq2
        sai1 = rreplace(rreplace(rreplace(self._make_source_file_name(), self.source_suffix, self.read1_suffix + self.source_suffix, 1), ".trimmed.sync", "", 1), ".sai", ".trimmed.sync.sai", 1)
        sai2 = rreplace(rreplace(rreplace(self._make_source_file_name(), self.source_suffix, self.read2_suffix + self.source_suffix, 1), ".trimmed.sync", "", 1), ".sai", ".trimmed.sync.sai", 1)
        return [BwaAln(target=sai1), BwaAln(target=sai2)]


# Raw variant calling class done on merged data to generate a list of
# raw candidates around which realignment is done.
class RawUnifiedGenotyper(UnifiedGenotyper):
    _config_subsection = "RawUnifiedGenotyper"
    parent_task = luigi.Parameter(default="ratatosk.picard.MergeSamFiles")
    options = luigi.Parameter(default="-stand_call_conf 30.0 -stand_emit_conf 10.0  --downsample_to_coverage 30 --output_mode EMIT_VARIANTS_ONLY -glm BOTH")
    label = ".BOTH.raw"

# Override RealignerTargetCreator and IndelRealigner to use
# RawUnifiedGenotyper vcf as input for known sites
class RawRealignerTargetCreator(RealignerTargetCreator):
    _config_subsection = "RawRealignerTargetCreator"
    parent_task = luigi.Parameter(default="ratatosk.picard.MergeSamFiles")
    target_suffix = luigi.Parameter(default=".intervals")
    
    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), ratatosk.samtools.IndexBam(target=rreplace(source, self.source_suffix, ".bai", 1), parent_task=fullclassname(cls)), ratatosk.scilife.seqcap.RawUnifiedGenotyper(target=rreplace(source, ".bam", ".BOTH.raw.vcf", 1))]

    def args(self):
        return ["-I", self.input()[0], "-o", self.output(), "-known", self.input()[2]]

# NOTE: Here I redefine target dependencies for IndelRealigner, the way I believe it should be
class RawIndelRealigner(IndelRealigner):
    _config_subsection = "RawIndelRealigner"
    parent_task = luigi.Parameter(default="ratatosk.picard.MergeSamFiles")
    source_suffix = luigi.Parameter(default=".bam")
    
    def requires(self):
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return [cls(target=source), 
                ratatosk.samtools.IndexBam(target=rreplace(source, self.source_suffix, ".bai", 1), parent_task="ratatosk.picard.MergeSamFiles"), 
                ratatosk.scilife.seqcap.RawRealignerTargetCreator(target=rreplace(source, ".bam", ".intervals", 1)),
                ratatosk.scilife.seqcap.RawUnifiedGenotyper(target=rreplace(source, ".bam", ".BOTH.raw.vcf", 1))]
    
    def args(self):
        return ["-I", self.input()[0], "-o", self.output(),
                "--targetIntervals", self.input()[2],
                "-known", self.input()[3]]


class HaloPlex(luigi.WrapperTask):
    # TODO: remove project, projectdir and just use indir?
    project = luigi.Parameter(description="Project name (equals directory name")
    projectdir = luigi.Parameter(description="Where projects live", default=os.curdir)
    sample = luigi.Parameter(default=None, description="Sample directory.")
    # Hard-code this for now - would like to calculate on the fly so that
    # implicit naming is unnecessary
    final_target_suffix = "trimmed.sync.sort.merge.realign.recal.clip.filtered.eval_metrics"

    def requires(self):
        # List requirements for completion, consisting of classes above
        target_list = self.target_generator()
        variant_target_list = ["{}.{}".format(x, self.final_target_suffix) for x in target_list]
        picard_metrics_target_list = ["{}.{}".format(x, "trimmed.sync.sort.merge") for x in target_list]
        return [VariantEval(target=tgt) for tgt in variant_target_list] + [PicardMetrics(target=tgt2) for tgt2 in picard_metrics_target_list]

    def target_generator(self):
        """Make all desired target output names based on the final target
        suffix.
        """
        target_list = []
        project_indir = os.path.join(self.projectdir, self.project)
        if not os.path.exists(project_indir):
            logger.warn("No such project '{}' found in project directory '{}'".format(self.project, self.projectdir))
            return target_list
        samples = os.listdir(project_indir)
        # Only run this sample if provided at command line.
        if self.sample:
            samples = self.sample
        for s in samples:
            sampledir = os.path.join(project_indir, s)
            if not os.path.isdir(sampledir):
                continue
            flowcells = os.listdir(sampledir)
            for fc in flowcells:
                if not fc.endswith("XX"):
                    continue
                fc_dir = os.path.join(sampledir, fc)
                # Yes folks, we also need to know the barcode and the lane...
                # Parse the flowcell config
                if not os.path.exists(os.path.join(fc_dir, "SampleSheet.csv")):
                    logger.warn("No sample sheet for sample '{}' in flowcell '{}';  skipping".format(s, fc))
                    continue
                ssheet = csv.DictReader(open(os.path.join(fc_dir, "SampleSheet.csv"), "r"))
                for line in ssheet:
                    logger.info("Adding sample '{0}' from flowcell '{1}' (barcode '{2}') to analysis".format(s, fc, line['Index']))
                    target_list.append(os.path.join(sampledir, "{}_{}_L00{}".format(s, line['Index'], line['Lane'] )))
        return target_list

