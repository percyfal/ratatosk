Project summary
=============================

:Project: ${project_name}
:Application: ${application}
:Date: ${date}

Samples
--------

.. toctree::
   :maxdepth: 1

${samples}

QC Metrics
----------

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
       	   pct_aligned[row["CATEGORY"]].append(float(row["PCT_PF_READS_ALIGNED"]))

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
