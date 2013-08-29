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

METRICS_TYPES=['align', 'hs', 'dup', 'insert']

# http://stackoverflow.com/questions/2170900/get-first-list-index-containing-sub-string-in-python
def index_containing_substring(the_list, substring):
    for i, s in enumerate(the_list):
        if substring in s:
            return i
    return -1

def _raw(x):
    return (x, None)

def _convert_input(x):
    if re.match("^[0-9]+$", x):
        return int(x)
    elif re.match("^[0-9,.]+$", x):
        return float(x.replace(",", "."))
    else:
        return str(x)

def _read_picard_metrics(f):
    with open(f) as fh:
        data = fh.readlines()
        # Find histogram line
        i_hist = index_containing_substring(data, "## HISTOGRAM")
        if i_hist == -1:
            i = len(data)
        else:
            i = i_hist
        metrics = [[_convert_input(y) for y in x.rstrip("\n").split("\t")] for x in data[0:i] if not re.match("^[ #\n]", x)]
        if i_hist == -1:
            return (metrics, None)
        hist = [[_convert_input(y) for y in x.rstrip("\n").split("\t")] for x in data[i_hist:len(data)] if not re.match("^[ #\n]", x)]
    return (metrics, hist)

# For now: extension maps to tuple (label, description). Label should
# be reused for analysis definitions
EXTENSIONS={'.align_metrics':('align', 'alignment', _read_picard_metrics),
            '.hs_metrics':('hs', 'hybrid selection', _read_picard_metrics),
            '.dup_metrics':('dup', 'duplication metrics', _read_picard_metrics),
            '.insert_metrics':('insert', 'insert size', _read_picard_metrics),
            }

class PicardMetrics(object):
    """class for reading/storing metrics"""
    def __init__(self, pmid, f):
        self._metrics = None
        self._hist = None
        self._file = f
        self._pmid = pmid
        (_, self._metrics_type) = (os.path.splitext(f))
        self._read_metrics()

    def _read_metrics(self):
        """Read metrics"""
        (self._metrics, self._hist) = EXTENSIONS[self._metrics_type][2](self._file)

    def metrics(self, as_csv=False):
        if as_csv:
            return [",".join([str(y) for y in x]) for x in self._metrics]
        return self._metrics

    def hist(self, as_csv=False):
        if as_csv:
            return [",".join([str(y) for y in x]) for x in self._hist]
        return self._hist

    def id(self):
        return self._pmid

class PicardMetricsCollection(object):
    def __init__(self, mlist):
        self._metrics = []
        self._mlist = mlist
        self._collect_metrics()

    def _collect_metrics(self):
        for (sid, fn) in self._mlist:
            pm = PicardMetrics(sid, fn)
            self._metrics.append(pm)
            
    def metrics(self, as_csv=False):
        return [pm.metrics(as_csv=as_csv) for pm in self._metrics]

    def hist(self, as_csv=False):
        return [pm.hist(as_csv=as_csv) for pm in self._metrics]

    def idlist(self):
        return [pm.id() for pm in self._metrics]


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
        #     relpath = os.path.relpath(os.curdir, picklepath)
        #     pfx = os.path.relpath(pfx, relpath)
        mfile = glob.glob(pfx + ".*" + ext)
        if mfile:
            metrics.append((item_id, mfile[0]))
    return PicardMetricsCollection(metrics)
