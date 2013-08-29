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
from itertools import izip

METRICS_TYPES=['eval']

def _read_eval_metrics(f):
    tab = {}
    with open(f) as fh:
        data = [y for y in [x.rstrip() for x in fh.readlines()] if y]
        re_fmt = re.compile('^#:GATKTable:(\d+):(\d+):(.+):;')
        re_lab = re.compile('^#:GATKTable:(\S+):(.+)')
        seen_header = False
        for x in data:
            if x.startswith("INFO") or x.startswith("WARN") or x.startswith("#:GATKReport"):
                continue
            if re_fmt.match(x):
                m = re_fmt.match(x)
                fmt = m.group(3).split(":")
                fmt_list = ["{" + f.replace("%", ":") + "}" for f in fmt]
            elif re_lab.match(x):
                m = re_lab.match(x)
                lab = m.group(1)
                tab[lab] = []
                seen_header = False
                # print data 
            else:
                y = x.split()
                if seen_header:
                    row = [s_fmt.format(s) for s_fmt,s in izip(fmt_list, y)]
                else:
                    row = ["{}".format(s) for s in y]
                tab[lab].append(row)
    return tab


EXTENSIONS={'.eval_metrics':('eval', 'snp evaluation', _read_eval_metrics)}
    
def collect_metrics(grouped_samples, picklepath, ext, grouping="sample", use_curdir=False):
    """Collect metrics for a collection of samples.
    
    :param grouped_samples: samples grouped in some way
    :param picklepath: path to pickle file
    :param ext: metrics extension to search for
    :param grouping: what grouping to use
    :param use_curdir: use curdir as relative path
    
    :returns: list of (item_id, metrics file name)
    """
    metrics = []
    for item_id, itemlist in grouped_samples.items():
        relpath = os.path.relpath(os.curdir, picklepath)
        pfx = os.path.relpath(itemlist[0].prefix(grouping), relpath)
        # if use_curdir:
        #     relpath = os.path.relpath(os.curdir, tgtdir)
        #     pfx = os.path.relpath(pfx, relpath)
        mfile = glob.glob(pfx + ".*" + ext)
        if mfile:
            metrics.append((item_id, mfile[0]))
    return GATKMetricsCollection(metrics)
 


class GATKMetrics(object):
    """Class for reading/storing GATK table metrics"""

    def __init__(self, id, fn):
        self._metrics = {}
        self._file = fn
        self._id = id
        (_, self._metrics_type) = (os.path.splitext(fn))
        self._read_metrics()

    def _read_metrics(self):
        """Read metrics"""
        self._metrics = EXTENSIONS[self._metrics_type][2](self._file)

    def metrics(self, which=None, as_csv=False):
        if which:
            try:
                tab = {which : self._metrics[which]}
            except KeyError:
                print "no such key {}".format(which)
        else:
            tab = self._metrics
        if as_csv:
            tab_csv = {}
            for k in tab.keys():
               tab_csv[k] = [",".join([str(y) for y in x]) for x in tab[k]]
            return tab_csv
        return tab

    @property
    def id(self):
        return self._id

class GATKMetricsCollection(object):
    def __init__(self, mlist):
        self._metrics = []
        self._mlist = mlist
        self._collect_metrics()

    def _collect_metrics(self):
        for (sid, fn) in self._mlist:
            gatkm = GATKMetrics(sid, fn)
            self._metrics.append(gatkm)

    def metrics(self, which=None, as_csv=False):
        return [gatkm.metrics(which, as_csv=as_csv) for gatkm in self._metrics]

    def idlist(self):
        return [gatkm.id for gatkm in self._metrics]


        
