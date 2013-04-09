Quickstart
==========


Examples in tests
-----------------

These examples are currently based on the tests in
`ratatosk.tests.test_commands` and `ratatosk.tests.test_wrappers`.

Creating file links
^^^^^^^^^^^^^^^^^^^^^^^^

The task :ref:`ratatosk.lib.files.fastq.FastqFileLink` creates a link
from source to a target. The source in this case depends on an
*external* task (:ref:`ratatosk.lib.files.external.FastqFile` meaning
this file was created by some outside process (e.g. sequencing
machine).

.. code:: bash

	nosetests -v -s test_wrapper.py:TestMiscWrappers.test_fastqln

.. figure:: ../../test_fastqln.png
   :alt: Fastq link task
   :scale: 50%
   :align: center

   **Figure 1.** Fastq link task

A couple of comments are warranted. First, the boxes shows tasks,
where the `FastqFile` is an external task. The file it points to must
exist for the task `FastqFileLink` executes. The color of the box
indicates status; here, green means the task has completed
successfully. Second, every task has its own set of options that can
be passed via the command line or in the code. In the `FastqFileLink`
task box we can see the options that were passed to the task. For
instance, the option `use_long_names=True` prints complete task names,
as shown above. 
	
Alignment with bwa sampe
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Here's a more useful example; paired-end alignment using `bwa`.

.. code:: bash

	nosetests -v -s test_commands.py:TestCommand.test_bwasampe

.. figure:: ../../test_bwasampe.png
   :alt: bwa sampe
   :scale: 50%
   :align: center

   **Figure 2.** Read alignment with bwa.

	
Wrapping up metrics tasks
^^^^^^^^^^^^^^^^^^^^^^^^^

The class :ref:`ratatosk.lib.tools.picard.PicardMetrics` subclasses
:ref:`ratatosk.job.JobWrapperTask` that can be used to require that
several tasks have completed. Here I've used it to group picard
metrics tasks:

.. code:: bash

	nosetests -v -s test_commands.py:TestCommand.test_picard_metrics

.. figure:: ../../test_picard_metrics.png
   :alt: picard metrics
   :scale: 50%
   :align: center

   **Figure 3.** Summarizing metrics with a wrapper task

Here, I've set the option `--use-long-names` to `False`, which changes
the output to show only the class names for each task. This example
utilizes a configuration file that links tasks together. More about
that in the next example.
