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
import re
import glob
import itertools
from collections import OrderedDict
from ratatosk.report.picard import PicardMetricsCollection
from texttable import Texttable

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

def collect_metrics(grouped_samples, projroot, tgtdir, ext, grouping="sample", use_curdir=False):
    """Collect metrics for a collection of samples.

    :param grouped_samples: samples grouped in some way
    :param projroot: project root directory
    :param tgtdir: documentation target directory 
    :param ext: metrics extension to search for
    :param grouping: what grouping to use
    :param use_curdir: use curdir as relative path

    :returns: list of (item_id, metrics file name)
    """
    metrics = []
    for item_id, itemlist in grouped_samples.items():
        item = itemlist[0]
        # FIXME: tgtdir should be docroot!
        pfx = os.path.relpath(itemlist[0].prefix(grouping), os.path.dirname(tgtdir))
        if use_curdir:
            relpath = os.path.relpath(os.curdir, tgtdir)
            pfx = os.path.relpath(pfx, relpath)
        mfile = glob.glob(pfx + ".*" + ext)
        if mfile:
            metrics.append((item_id, mfile[0]))
    return PicardMetricsCollection(metrics)

def array_to_texttable(data, align=None, precision=2):
    """Convert array to texttable. Sets column widths to the
    widest entry in each column."""
    if len(data)==0:
        return
    ttab = Texttable()
    ttab.set_precision(precision)
    if align:
        colWidths = [max(len(x), len(".. class:: {}".format(y))) for x,y in itertools.izip(data[0], align)]
    else:
        colWidths = [len(x) for x in data[0]]
    for row in data:
        for i in range(0, len(row)):
            if type(row[i]) == str:
                colWidths[i] = max([len(str(x)) for x in row[i].split("\n")] + [colWidths[i]])
            colWidths[i] = max(len(str(row[i])), colWidths[i])
    table_data = []
    if align:
        for row in data:
            table_row = []
            i = 0
            for col, aln in itertools.izip(row, align):
                table_row.append(".. class:: {}".format(aln) + " " * colWidths[i] + "{}".format(col))
                i = i + 1
            table_data.append(table_row)
    else:
        table_data = data
    ttab.add_rows(table_data)
    ttab.set_cols_width(colWidths)
    # Note: this does not affect the final pdf output
    ttab.set_cols_align(["r"] * len(colWidths))
    return ttab    

def indent_texttable_for_rst(ttab, indent=4, add_spacing=True):
    """Texttable needs to be indented for rst.

    :param ttab: texttable object
    :param indent: indentation (should be 4 *spaces* for rst documents)
    :param add_spacing_row: add additional empty row below class directives

    :returns: reformatted texttable object as string
    """
    output = ttab.draw()
    new_output = []
    for row in output.split("\n"):
        new_output.append(" " * indent + row)
        if re.search('.. class::', row):
            new_row = [" " if x != "|" else x for x in row]
            new_output.append(" " * indent + "".join(new_row))
    return "\n".join(new_output)
