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
import csv
import glob
import itertools
from collections import OrderedDict
import cPickle as pickle
import ratatosk.report.picard as picard
import ratatosk.report.gatk as gatk
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

# This is too redundant
EXTENSIONS={'.align_metrics':('align', 'alignment', picard.collect_metrics),
            '.hs_metrics':('hs', 'hybrid selection', picard.collect_metrics),
            '.dup_metrics':('dup', 'duplication metrics', picard.collect_metrics),
            '.insert_metrics':('insert', 'insert size', picard.collect_metrics),
            '.eval_metrics':('eval', 'snp evaluation', gatk.collect_metrics),
            }

def collect_multi_metrics(pickled_samples, types=[".align_metrics", ".dup_metrics", ".insert_metrics", ".hs_metrics", ".eval_metrics"], use_curdir=True, **kw):
    """Collect metrics for multiple metrics types. 

    :param pickled_samples: pickled sample file
    :param docroot: documentation root directory
    :param types: metrics extension types to search for
    :param use_curdir: use current directory to calculate paths
    :param **kw: keyword arguments

    :returns: dictionary with mapping extension_key:metricscollection
    """
    data = {}
    samples = pickle.load(open(pickled_samples))
    grouped_samples = group_samples(samples)
    data["grouped_samples"] = grouped_samples
    for t in types:
        f = EXTENSIONS[t][2]
        metrics_collection = f(grouped_samples, os.path.dirname(pickled_samples), t, **kw)
        metrics_csv = metrics_collection.metrics(as_csv=True)
        data[t] = metrics_csv
    return data

def convert_metrics_to_best_practice(data):
    """Select only the relevant data for best practice reports."""
    bpmetrics = {}
    types = data.keys()
    # Get alignment metrics
    if ".align_metrics" in types:
        dataset = data[".align_metrics"]
        nseq = {'FIRST_OF_PAIR':[], 'SECOND_OF_PAIR':[], 'PAIR':[]}
        pct_aligned = {'FIRST_OF_PAIR':[], 'SECOND_OF_PAIR':[], 'PAIR':[]}
        for c in dataset:
            df = [row for row in csv.DictReader(c)]
            for row in df:
                nseq[row["CATEGORY"]].append(int(row["TOTAL_READS"]))
                pct_aligned[row["CATEGORY"]].append(100 * float(row["PCT_PF_READS_ALIGNED"]))
        bpmetrics[".align_metrics"] = {'pct_aligned':pct_aligned, 'nseq': nseq}

    # Get duplication metrics
    if ".dup_metrics" in types:
        dup = []
        dataset = data[".dup_metrics"]
        for c in dataset:
            df = [row for row in csv.DictReader(c)]
            dup.append(100 * float(df[0]["PERCENT_DUPLICATION"]))
        bpmetrics[".dup_metrics"] = dup

    # Get hybridization metrics
    if ".hs_metrics" in types:
        dataset = data[".hs_metrics"]
        hsmetrics = []
        headers = ["ZERO_CVG_TARGETS_PCT", "PCT_TARGET_BASES_2X", "PCT_TARGET_BASES_10X", "PCT_TARGET_BASES_20X", "PCT_TARGET_BASES_30X"]
        for c in dataset:
            df = [row for row in csv.DictReader(c)]
            hsmetrics.append([100 * float(df[0][x]) for x in headers] + [float(df[0]["MEAN_TARGET_COVERAGE"]),100*float(int(df[0]["ON_TARGET_BASES"]))/float(int(df[0]["PF_UQ_BASES_ALIGNED"])),  float(float(df[0]["FOLD_ENRICHMENT"]) / (float(df[0]["GENOME_SIZE"]) / float(df[0]["TARGET_TERRITORY"]))) * 100] )
        bpmetrics[".hs_metrics"] = hsmetrics

    # Insert size metrics
    if ".insert_metrics" in types:
        dataset = data[".insert_metrics"]
        insertmetrics = []
        for c in dataset:
            df = [row for row in csv.DictReader(c)]
            insertmetrics.append(df[0]['MEAN_INSERT_SIZE'])
        bpmetrics[".insert_metrics"] = insertmetrics

    # Evaluation metrics
    evalmetrics = []
    if ".eval_metrics" in types:
        for em in data[".eval_metrics"]:
            dataset = em["ValidationReport"]
            df = [row for row in csv.DictReader(dataset)]
            tmp = []
            tmp.append(df[0]["nComp"])
            tmp.append(df[0]["TP"])

            dataset = em["TiTvVariantEvaluator"]
            df = [row for row in csv.DictReader(dataset)]
            tmp.append(df[0]["tiTvRatio"])
            tmp.append(df[1]["tiTvRatio"])
            tmp.append(df[2]["tiTvRatio"])
            evalmetrics.append(tmp)
        bpmetrics[".eval_metrics"] = evalmetrics
    return bpmetrics


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
