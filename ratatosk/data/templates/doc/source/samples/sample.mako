<%!
import os
import cPickle as pickle
from ratatosk.report.utils import group_samples
%>

${sample_id}
=======================================

Metrics
--------

.. plot::

   import os
   import csv
   import cPickle as pickle
   from pylab import *
   import matplotlib.pyplot as plt
   from ratatosk.report.picard import PicardMetricsCollection
   from ratatosk.report.utils import collect_metrics, group_samples
   
   samples = pickle.load(open(os.path.relpath("${pickled_samples}", os.path.join("${docroot}", "source", "samples"))))
   grouped_samples = group_samples(samples)
   d = {"${sample_id}":grouped_samples.get("${sample_id}", None)}
   if not d is None:
       pmc = collect_metrics(d, "${docroot}", os.path.join("${docroot}", "source"), ".align_metrics")
       pmccsv = pmc.metrics(as_csv=True)
       nseq = {'FIRST_OF_PAIR':[], 'SECOND_OF_PAIR':[], 'PAIR':[]}
       for c in pmccsv:
           df = [row for row in csv.DictReader(c)]
           for row in df:
               nseq[row["CATEGORY"]].append(int(row["TOTAL_READS"]))

       n = len(pmc.idlist())
       xticks(range(0,n), [x for x in pmc.idlist()], rotation=45)
       xlim(-.1, (n-1)*1.1)
       plt.plot(range(0,n), nseq['PAIR'], "o")
       plt.tight_layout()
       plt.show()


Sample runs
------------

<%
samples = pickle.load(open(os.path.join(docroot, "samples.pickle")))
grouped_samples = group_samples(samples)
d = grouped_samples.get(sample_id, None)
%>

% for samplerun in d:
${samplerun.prefix("sample_run")}
${"^" * len(samplerun.prefix("sample_run"))}


:No. of reads:

% endfor
