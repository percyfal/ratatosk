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
import glob
import itertools
from collections import OrderedDict
from ratatosk.report.picard import PicardMetricsCollection

def group_samples(samples, grouping="sample"):
    """Group samples by sample or sample run.

    :param samples: list of :class:`ISample <ratatosk.experiment.ISample>` objects
    :param grouping: what to group by

    :returns: dictionary of grouped items
    """
    groups = OrderedDict()
    if grouping == "sample":
        for k,g in itertools.groupby(samples, key=lambda x:x.sample_id()):
            groups[k] = list(g)
    elif grouping == "samplerun":
        for k,g in itertools.groupby(samples, key=lambda x:x.prefix("samplerun")):
            groups[k] = list(g)
    return groups

def collect_metrics(grouped_samples, projroot, tgtdir, ext, grouping="sample"):
    """Collect metrics for a collection of samples.

    :param grouped_samples: samples grouped in some way
    :param projroot: project root directory
    :param tgtdir: documentation target directory 
    :param ext: metrics extension to search for
    :param grouping: what grouping to use

    :returns: list of (item_id, metrics file name)
    """
    metrics = []
    for item_id, itemlist in grouped_samples.items():
        item = itemlist[0]
        # FIXME: tgtdir should be docroot!
        pfx = os.path.relpath(itemlist[0].prefix(grouping), os.path.dirname(tgtdir))
        mfile = glob.glob(pfx + ".*" + ext)
        if mfile:
            metrics.append((item_id, mfile[0]))
    return PicardMetricsCollection(metrics)
