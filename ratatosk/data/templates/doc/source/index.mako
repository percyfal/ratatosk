<%!
import os
import re
import csv
import cPickle as pickle
import ratatosk.report.picard as picard
import ratatosk.report.gatk as gatk
from ratatosk.report.utils import collect_multi_metrics, group_samples, array_to_texttable, indent_texttable_for_rst, convert_metrics_to_best_practice
%>

Project summary
=============================

:Project: ${project_name}
:Application: ${application}
:Date: ${date}

New summary stats
-----------------
<%
types = [".align_metrics", ".dup_metrics", ".insert_metrics", ".hs_metrics", ".eval_metrics"]
data = collect_multi_metrics(pickled_samples, types=types, use_curdir=True)
bpmetrics = convert_metrics_to_best_practice(data)
sampletab = []
header = ["sample", "total", "%aligned", "dup", "insert_size", "%ontarget", "meancov", "%10X cov", "0 cov", "TOTAL VARIATIONS", "IN dbSNP", "TI/TV ALL", "TI/TV dbSNP", "TI/TV NOVEL"]
sampletab.append(header)
i = 0
for s in data["grouped_samples"].iterkeys():
   row = [s, bpmetrics[".align_metrics"]["nseq"]["PAIR"][i], bpmetrics[".align_metrics"]["pct_aligned"]["PAIR"][i], bpmetrics[".dup_metrics"][i], bpmetrics[".insert_metrics"][i], bpmetrics[".hs_metrics"][i][6], bpmetrics[".hs_metrics"][i][5], bpmetrics[".hs_metrics"][i][2], bpmetrics[".hs_metrics"][i][0]] +  bpmetrics[".eval_metrics"][i]
   sampletab.append(row)
   i = i + 1

%>

${indent_texttable_for_rst(array_to_texttable(sampletab))}

QC Metrics
----------

Sequence statistics
^^^^^^^^^^^^^^^^^^^

.. plot::

   import os
   from pylab import *
   import matplotlib.pyplot as plt
   from ratatosk.report.utils import collect_multi_metrics, convert_metrics_to_best_practice

   types = [".align_metrics", ".dup_metrics", ".insert_metrics", ".hs_metrics", ".eval_metrics"]

   data = collect_multi_metrics("${pickled_samples}", types=types, use_curdir=True)
   bpmetrics = convert_metrics_to_best_practice(data)

   pct_aligned = bpmetrics[".align_metrics"]["pct_aligned"]
   nseq = bpmetrics[".align_metrics"]["nseq"]
   dup = bpmetrics[".dup_metrics"]

   if len(dup) == 0:
       sdup = [100 for i in range(0, n)]
   else: 
       sdup = [int(100 + 10 * x) for x in dup]
   plt.scatter(pct_aligned['PAIR'], nseq['PAIR'], s=sdup, alpha=0.75)
   plt.xlabel(r'Percent aligned', fontsize=14)
   plt.yscale('log', **{'basey':10})
   plt.ylabel(r'Read count', fontsize=14)
   plt.title("Sequence summary.\nPoint sizes correspond to duplication levels.", fontsize=14)
   plt.tight_layout()
   plt.show()
