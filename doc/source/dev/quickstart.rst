Quickstart
==========

As outlined in the following section, :mod:`ratatosk` is a library of
:mod:`luigi` tasks. It is basically a collection of make targets,
based on a python framework.

Here follows some examples of how to run some basic tasks. The
commands are run in the test directory.

Running single tasks without configuration file
-----------------------------------------------

.. note:: The commands in this section need to be run sequentially

Sequence alignment with :program:`bwa`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note:: First index the reference with ``bwa index data/chr11.fa``

The following 

.. code-block:: text

   ratatosk_run.py Aln --target data/sample1_1.sai --bwaref data/chr11.fa
   ratatosk_run.py Aln --target data/sample1_2.sai --bwaref data/chr11.fa

Making sam files from alignments
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following command generates a sam file with :program:`bwa sampe`.
The option ``--add-label`` is used to generate the correct input file
names. Here, they correspond to read pair identifiers.

.. code-block:: text

   ratatosk_run.py Sampe --target data/sample1.sam --bwaref data/chr11.fa --add-label _1 --add-label _2

Making a sorted bam file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following command first runs :program:`samtools view -bSh`, then
:program:`SortSam.jar` to generate a sorted bam file.

.. code-block:: text

   ratatosk_run.py SortSam --target data/sample1.sort.bam

Working with a configuration file
---------------------------------

The following commands utilise a configuration file,
``pipeconf.yaml``. Start by removing all files generated in the
previous section:

.. code-block:: text

   rm -f data/*sai data/*sam data/*bam 

Running an alignment pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following command performs an alignment and converts the results
to a sorted bam file.

.. code-block:: text

   ratatosk_run.py SortSam --target data/sample1.sort.bam --custom-config pipeconf.yaml


The workflow is illustrated in the following image.

.. graphviz:: ../../grf/quickstart_alignpipe.dot


