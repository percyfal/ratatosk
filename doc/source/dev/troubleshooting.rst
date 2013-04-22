Troubleshooting
================

:mod:`luigi` and :mod:`ratatosk` print *a lot* of debugging
information, and it is not always easy (actually, it's really
difficult) to tell where something has gone wrong. This page
summarizes some of the most common problems, and where to look for
solutions. If you redirect the output to log files, a good place to
start is by running :program:`grep WARNING` on the log file.

Missing external dependency
----------------------------

.. code-block:: text

   WARNING:luigi-interface:Task FastqFile(target=target.fastq.gz,
   label=None, suffix=.fastq.gz) is not complete and run() is not
   implemented. Probably a missing external dependency.

This error means the file is missing, either because you forgot to put
it there (in case it's an :mod:`external
<ratatosk.lib.files.external>` task), or because the automatically
generated target names are incorrect. The latter case is by far the
commonest, and to make matters worse, extremely difficult to solve.
First, one should try to find the task that first generates an
erroneous name. Then, make sure that task and its parent tasks have
the appropriate :attr:`label <ratatosk.job.BaseJobTask.label>`,
:attr:`add_label <ratatosk.job.BaseJobTask.add_label>`, and
:attr:`diff_label <ratatosk.job.BaseJobTask.diff_label>` attributes. In
most cases, the error lies here. See also :ref:`Generating source
names <ratatosk.doc.configuration.source_names>`.

