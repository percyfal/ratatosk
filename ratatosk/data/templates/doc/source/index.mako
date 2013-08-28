<%!
import os
import csv
import cPickle as pickle
from ratatosk.report.picard import PicardMetricsCollection
from ratatosk.report.utils import collect_metrics, group_samples, array_to_texttable, indent_texttable_for_rst
%>

Project summary
=============================

:Project: ${project_name}
:Application: ${application}
:Date: ${date}

Samples
--------

.. toctree::
   :maxdepth: 2

   samples/index

QC Metrics
----------

Summary statistics
------------------

<%
samples = pickle.load(open(pickled_samples))
grouped_samples = group_samples(samples)
pmc = collect_metrics(grouped_samples, docroot, os.path.join(os.path.abspath(docroot), "source"), ".align_metrics", use_curdir=True)
pmccsv = pmc.metrics(as_csv=True)

# Get alignment metrics
nseq = {'FIRST_OF_PAIR':[], 'SECOND_OF_PAIR':[], 'PAIR':[]}
pct_aligned = {'FIRST_OF_PAIR':[], 'SECOND_OF_PAIR':[], 'PAIR':[]}
for c in pmccsv:
    df = [row for row in csv.DictReader(c)]
    for row in df:
        nseq[row["CATEGORY"]].append(int(row["TOTAL_READS"]))
    pct_aligned[row["CATEGORY"]].append(100 * float(row["PCT_PF_READS_ALIGNED"]))
    n = len(pmc.idlist())

# Get duplication metrics
pmc = collect_metrics(grouped_samples, docroot, os.path.join(os.path.abspath(docroot), "source"), ".dup_metrics", use_curdir=True)
pmccsv = pmc.metrics(as_csv=True)
dup = []
for c in pmccsv:
    df = [row for row in csv.DictReader(c)]
    dup.append(100 * float(df[0]["PERCENT_DUPLICATION"]))

if len(dup) == 0:
    sdup = [100 for i in range(0, n)]
else: 
    sdup = [int(100 + 10 * x) for x in dup]

# Get hybridization metrics
pmc = collect_metrics(grouped_samples, docroot, os.path.join(docroot, "source"), ".hs_metrics", use_curdir=True)
pmccsv = pmc.metrics(as_csv=True)
hsmetrics = []
headers = ["ZERO_CVG_TARGETS_PCT", "PCT_TARGET_BASES_2X", "PCT_TARGET_BASES_10X", "PCT_TARGET_BASES_20X", "PCT_TARGET_BASES_30X"]
for c in pmccsv:
    df = [row for row in csv.DictReader(c)]
    print df
    hsmetrics.append([100 * float(df[0][x]) for x in headers] + [float(df[0]["MEAN_TARGET_COVERAGE"]),100*float(int(df[0]["ON_TARGET_BASES"]))/float(int(df[0]["PF_UQ_BASES_ALIGNED"])),  float(float(df[0]["FOLD_ENRICHMENT"]) / (float(df[0]["GENOME_SIZE"]) / float(df[0]["TARGET_TERRITORY"]))) * 100])
                                                             

# Get insert size metrics
pmc = collect_metrics(grouped_samples, docroot, os.path.join(docroot, "source"), ".insert_metrics", use_curdir=True)
pmccsv = pmc.metrics(as_csv=True)
insertmetrics = []
for c in pmccsv:
    df = [row for row in csv.DictReader(c)]
    insertmetrics.append(df[0]['MEAN_INSERT_SIZE'])

sampletab = []
header = ["sample", "total", "aligned", "dup", "insert_size", "%ontarget", "meancov", "%10X cov", "0 cov"] # , "TOTAL VARIATIONS", "IN dbSNP", "TI/TV ALL", "TI/TV dbSNP", "TI/TV NOVEL"]
sampletab.append(header)

i = 0

for s in grouped_samples.iterkeys():
    sampletab.append([s, nseq['PAIR'][i] , pct_aligned['PAIR'][i], dup[i], insertmetrics[i], hsmetrics[i][6], hsmetrics[i][5], hsmetrics[i][2], hsmetrics[i][0] ])
    i = i + 1
%>

${indent_texttable_for_rst(array_to_texttable(sampletab))}

Sequence statistics
^^^^^^^^^^^^^^^^^^^

.. plot::

   import os
   import csv
   import cPickle as pickle
   from pylab import *
   import matplotlib.pyplot as plt
   from ratatosk.report.picard import PicardMetricsCollection
   from ratatosk.report.utils import collect_metrics, group_samples

   samples = pickle.load(open(os.path.relpath("${pickled_samples}", os.path.join("${docroot}", "source"))))
   grouped_samples = group_samples(samples)
   pmc = collect_metrics(grouped_samples, "${docroot}", os.path.join("${docroot}", "source"), ".align_metrics")
   pmccsv = pmc.metrics(as_csv=True)
   # Get alignment metrics
   nseq = {'FIRST_OF_PAIR':[], 'SECOND_OF_PAIR':[], 'PAIR':[]}
   pct_aligned = {'FIRST_OF_PAIR':[], 'SECOND_OF_PAIR':[], 'PAIR':[]}
   for c in pmccsv:
       df = [row for row in csv.DictReader(c)]
       for row in df:
           nseq[row["CATEGORY"]].append(int(row["TOTAL_READS"]))
       pct_aligned[row["CATEGORY"]].append(100 * float(row["PCT_PF_READS_ALIGNED"]))
       n = len(pmc.idlist())

   # Get duplication metrics
   pmc = collect_metrics(grouped_samples, "haloreport", os.path.join("haloreport", "source"), ".dup_metrics")
   pmccsv = pmc.metrics(as_csv=True)
   dup = []
   for c in pmccsv:
       df = [row for row in csv.DictReader(c)]
       dup.append(100 * float(df[0]["PERCENT_DUPLICATION"]))

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
   import cPickle as pickle
   from pylab import *
   import matplotlib.pyplot as plt
   from ratatosk.report.picard import PicardMetricsCollection
   from ratatosk.report.utils import collect_metrics, group_samples
   
   samples = pickle.load(open(os.path.relpath("${pickled_samples}", os.path.join("${docroot}", "source"))))
   grouped_samples = group_samples(samples)
   pmc = collect_metrics(grouped_samples, "${docroot}", os.path.join("${docroot}", "source"), ".align_metrics")
   pmccsv = pmc.metrics(as_csv=True)
   pct_aligned = {'FIRST_OF_PAIR':[], 'SECOND_OF_PAIR':[], 'PAIR':[]}
   for c in pmccsv:
       df = [row for row in csv.DictReader(c)]
       for row in df:
       	   pct_aligned[row["CATEGORY"]].append(100 * float(row["PCT_PF_READS_ALIGNED"]))

   n = len(pmc.idlist())
   xticks(range(0,n), [x for x in pmc.idlist()], rotation=45)
   xlim(-.1, (n-1)*1.1)
   plt.plot(range(0,n), pct_aligned['PAIR'], "o")
   plt.tight_layout()
   plt.show()




Duplication metrics
^^^^^^^^^^^^^^^^^^^^

.. plot::

   import os
   import csv
   import cPickle as pickle
   from pylab import *
   import matplotlib.pyplot as plt
   from ratatosk.report.picard import PicardMetricsCollection
   from ratatosk.report.utils import collect_metrics, group_samples
   
   samples = pickle.load(open(os.path.relpath("${pickled_samples}", os.path.join("${docroot}", "source"))))
   grouped_samples = group_samples(samples)
   pmc = collect_metrics(grouped_samples, "${docroot}", os.path.join("${docroot}", "source"), ".dup_metrics")
   pmccsv = pmc.metrics(as_csv=True)
   dup = []
   for c in pmccsv:
       df = [row for row in csv.DictReader(c)]
       dup.append(100 * float(df[0]["PERCENT_DUPLICATION"]))

   n = len(pmc.idlist())
   xticks(range(0,n), [x for x in pmc.idlist()], rotation=45)
   xlim(-.1, (n-1)*1.1)
   plt.plot(range(0,n), dup, "o")
   plt.tight_layout()
   plt.show()

Hybridization metrics
^^^^^^^^^^^^^^^^^^^^^

.. plot::

   import os
   import csv
   import cPickle as pickle
   import math
   from pylab import *
   import matplotlib.pyplot as plt
   import numpy as np
   from ratatosk.report.picard import PicardMetricsCollection
   from ratatosk.report.utils import collect_metrics, group_samples
   
   samples = pickle.load(open(os.path.relpath("${pickled_samples}", os.path.join("${docroot}", "source"))))
   grouped_samples = group_samples(samples)
   pmc = collect_metrics(grouped_samples, "${docroot}", os.path.join("${docroot}", "source"), ".hs_metrics")
   pmccsv = pmc.metrics(as_csv=True)
   hsmetrics = []
   headers = ["ZERO_CVG_TARGETS_PCT", "PCT_TARGET_BASES_2X", "PCT_TARGET_BASES_10X", "PCT_TARGET_BASES_20X", "PCT_TARGET_BASES_30X"]
   hticks = ["0X", "2X", "10X", "20X", "30X"]
   xticks(range(0,len(hticks)), [x for x in hticks])
   for c in pmccsv:
       df = [row for row in csv.DictReader(c)]
       hsmetrics.append([100 * float(df[0][x]) for x in headers])
   plt.boxplot(np.array(hsmetrics))
   plt.show()


.. plot::

   import os
   import csv
   import cPickle as pickle
   import math
   from pylab import *
   import matplotlib.pyplot as plt
   import numpy as np
   from ratatosk.report.picard import PicardMetricsCollection
   from ratatosk.report.utils import collect_metrics, group_samples
   
   samples = pickle.load(open(os.path.relpath("${pickled_samples}", os.path.join("${docroot}", "source"))))
   grouped_samples = group_samples(samples)
   pmc = collect_metrics(grouped_samples, "${docroot}", os.path.join("${docroot}", "source"), ".hs_metrics")
   pmccsv = pmc.metrics(as_csv=True)
   hsmetrics = []
   headers = ["ZERO_CVG_TARGETS_PCT", "PCT_TARGET_BASES_2X", "PCT_TARGET_BASES_10X", "PCT_TARGET_BASES_20X", "PCT_TARGET_BASES_30X"]
   hticks = ["0X", "2X", "10X", "20X", "30X"]
   for c in pmccsv:
       df = [row for row in csv.DictReader(c)]
       hsmetrics.append([100 * float(df[0][x]) for x in headers])
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
