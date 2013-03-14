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
from ratatosk.gatk import VariantEval

logger = logging.getLogger('luigi-interface')

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
        target_list = ["{}.{}".format(x, self.final_target_suffix) for x in self.target_generator()]
        return [VariantEval(target=tgt) for tgt in target_list]

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

