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
import csv
import logging
from ratatosk.utils import rreplace

logging.basicConfig(level=logging.DEBUG)

def organize_sample_runs(task, cls):
    # This currently relies on the folder structure sample/fc1,
    # sample/fc2 etc... This should possibly also be a
    # configurable function?
    # NB: this is such a pain to get right I'm adding lots of debug right now
    logging.debug("Organizing samples for {}".format(task.target))
    targetdir = os.path.dirname(task.target)
    flowcells = os.listdir(targetdir)
    bam_list = []
    for fc in flowcells:
        fc_dir = os.path.join(targetdir, fc)
        if not os.path.isdir(fc_dir):
            continue
        if not fc_dir.endswith("XX"):
            continue
        logging.debug("Looking in directory {}".format(fc))
        # This assumes only one sample run per flowcell
        bam_list.append(os.path.join(fc_dir, os.path.basename(rreplace(task.target, "{}{}".format(task.label, task.target_suffix), task.source_suffix, 1))))
    logging.debug("Generated target bamfile list {}".format(bam_list))
    return bam_list


# Function for use with pipelines and data structure in ngs.test.data
def target_generator(task):
    """Make all desired target output names based on the final target
    suffix.
    """
    target_list = []
    project_indir = os.path.join(task.projectdir, task.project)
    if not os.path.exists(project_indir):
        logging.warn("No such project '{}' found in project directory '{}'".format(task.project, task.projectdir))
        return target_list
    samples = os.listdir(project_indir)
    # Only run this sample if provided at command line.
    if task.sample:
        samples = task.sample
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
                logging.warn("No sample sheet for sample '{}' in flowcell '{}';  skipping".format(s, fc))
                continue
            ssheet = csv.DictReader(open(os.path.join(fc_dir, "SampleSheet.csv"), "r"))
            for line in ssheet:
                logging.info("Adding sample '{0}' from flowcell '{1}' (barcode '{2}') to analysis".format(s, fc, line['Index']))
                target_list.append(os.path.join(sampledir, "{}_{}_L00{}".format(s, line['Index'], line['Lane'] )))
    return target_list
