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

Summary statistics
-------------------
<%
types = [".align_metrics", ".dup_metrics", ".insert_metrics", ".hs_metrics", ".eval_metrics"]
data = collect_multi_metrics(pickled_samples, types=types, use_curdir=True)
bpmetrics = convert_metrics_to_best_practice(data)

sampletab = []
header = ["sample", "total", "%aligned", "dup", "insert_size", "%ontarget", "meancov", "%10X cov", "0 cov", "TOTAL VARIATIONS", "IN dbSNP", "TI/TV ALL", "TI/TV dbSNP", "TI/TV NOVEL"]
sampletab.append(header)
i = 0

data["grouped_samples"].keys()
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
   n = len(data["grouped_samples"])

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

Alignment metrics
^^^^^^^^^^^^^^^^^

.. plot::

   import os
   import csv
   from pylab import *
   import matplotlib.pyplot as plt
   from ratatosk.report.utils import collect_multi_metrics, convert_metrics_to_best_practice

   types = [".align_metrics"]
   data = collect_multi_metrics("${pickled_samples}", types=types, use_curdir=True)
   bpmetrics = convert_metrics_to_best_practice(data)

   pmc = data[".align_metrics"]
   n = len(pmc.idlist())
   df = [row for c in pmc.metrics(as_csv=True) for row in csv.DictReader(c)]
   xticks(range(0,n), [x for x in pmc.idlist()], rotation=45)
   xlim(-.1, (n-1)*1.1)
   plt.plot(range(0,n), [100 * float(x["PCT_PF_READS_ALIGNED"]) for x in df if x["CATEGORY"]=="PAIR"], "o")
   plt.tight_layout()
   plt.show()

Duplication metrics
^^^^^^^^^^^^^^^^^^^^

.. plot::

   import os
   import csv
   from pylab import *
   import matplotlib.pyplot as plt
   from ratatosk.report.utils import collect_multi_metrics, convert_metrics_to_best_practice

   types = [".dup_metrics"]
   data = collect_multi_metrics("${pickled_samples}", types=types, use_curdir=True)
   bpmetrics = convert_metrics_to_best_practice(data)
   
   pmc = data[".dup_metrics"]
   n = len(pmc.idlist())
   df = [row for c in pmc.metrics(as_csv=True) for row in csv.DictReader(c)]
   pmccsv = pmc.metrics(as_csv=True)

   xticks(range(0,n), [x for x in pmc.idlist()], rotation=45)
   xlim(-.1, (n-1)*1.1)
   plt.plot(range(0,n), [float(x["PERCENT_DUPLICATION"]) for x in df], "o")
   plt.tight_layout()
   plt.show()

Hybridization metrics
^^^^^^^^^^^^^^^^^^^^^

.. plot::

   import os
   import csv
   from pylab import *
   import matplotlib.pyplot as plt
   from ratatosk.report.utils import collect_multi_metrics, convert_metrics_to_best_practice

   types = [".hs_metrics"]
   data = collect_multi_metrics("${pickled_samples}", types=types, use_curdir=True)
   bpmetrics = convert_metrics_to_best_practice(data)

   pmc = data[".hs_metrics"]
   df = [row for c in pmc.metrics(as_csv=True) for row in csv.DictReader(c)]
   hsmetrics = []
   headers = ["ZERO_CVG_TARGETS_PCT", "PCT_TARGET_BASES_2X", "PCT_TARGET_BASES_10X", "PCT_TARGET_BASES_20X", "PCT_TARGET_BASES_30X"]
   hticks = ["0X", "2X", "10X", "20X", "30X"]
   xticks(range(0,len(hticks)), [x for x in hticks])
   for row in df:
       hsmetrics.append([100 * float(row[x]) for x in headers])
   plt.boxplot(np.array(hsmetrics))
   plt.show()


.. plot::

   import os
   import csv
   from pylab import *
   import matplotlib.pyplot as plt
   from ratatosk.report.utils import collect_multi_metrics, convert_metrics_to_best_practice

   types = [".hs_metrics"]
   data = collect_multi_metrics("${pickled_samples}", types=types, use_curdir=True)
   bpmetrics = convert_metrics_to_best_practice(data)

   pmc = data[".hs_metrics"]
   df = [row for c in pmc.metrics(as_csv=True) for row in csv.DictReader(c)]
   hsmetrics = []
   headers = ["ZERO_CVG_TARGETS_PCT", "PCT_TARGET_BASES_2X", "PCT_TARGET_BASES_10X", "PCT_TARGET_BASES_20X", "PCT_TARGET_BASES_30X"]
   hticks = ["0X", "2X", "10X", "20X", "30X"]
   for row in df:
       hsmetrics.append([100 * float(row[x]) for x in headers])
   n = len(pmc.idlist())
   nsubplots = int(math.ceil(n/9))
   nrow = int(math.ceil(n/3))
   k = 0
   for i_subplot in range(0, nsubplots + 1):
      f, axarr = plt.subplots(3, 3, sharex='col', sharey='row')
      for i in range(0, 3):
      	  for j in range(0, 3):
       	      if k < n:
	      	  x = range(0, len(hticks))
               	  axarr[i,j].plot(x, hsmetrics[k], "o")
	       	  axarr[i,j].set_xticks(x)
	       	  axarr[i,j].set_title(pmc.idlist()[k])
	       	  axarr[i,j].set_xlim(-.1, (len(hticks)-1)*1.1)
	       	  axarr[i,j].set_ylim(-5, 105)
	       	  axarr[i,j].set_xticklabels(hticks)
   	      else:
		  axarr[i,j].axis('off')
              k += 1
   plt.show()
