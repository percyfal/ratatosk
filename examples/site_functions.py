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
import logging

def organize_sample_runs(self, cls):
    # This currently relies on the folder structure sample/fc1,
    # sample/fc2 etc... This should possibly also be a
    # configurable function?
    # NB: this is such a pain to get right I'm adding lots of debug right now
    print "In organize sample runs"
    logger.debug("Organizing samples for {}".format(self.target))
    targetdir = os.path.dirname(self.target)
    flowcells = os.listdir(targetdir)
    bam_list = []
    for fc in flowcells:
        fc_dir = os.path.join(targetdir, fc)
        if not os.path.isdir(fc_dir):
            continue
        if not fc_dir.endswith("XX"):
            continue
        logger.debug("Looking in directory {}".format(fc))
        # This assumes only one sample run per flowcell
        bam_list.append(os.path.join(fc_dir, os.path.basename(rreplace(self.target, "{}{}".format(self.label, self.target_suffix), self.source_suffix, 1))))
    logger.debug("Generated target bamfile list {}".format(bam_list))
    return bam_list
