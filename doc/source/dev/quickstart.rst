Quickstart
==========

*ratatosk* was initiated out of the need to write an analysis pipeline
for analysis of `HaloPlex
<http://www.genomics.agilent.com/GenericB.aspx?pagetype=Custom&subpagetype=Custom&pageid=3081>`_
data. Rather than writing yet another incomprehensible and
difficult-to-maintain shell script, I was basically looking for a more
modular approach to building complex pipelines of batch jobs. A
colleague pointed me to `luigi <https://github.com/spotify/luigi>`_,
which lets you do just that.

What is it?
----------------

Simply put, *ratatosk* is just a library of *luigi* tasks that are
implemented as wrappers for bioinformatics programs that primarily are
used for analysis of next-generation sequencing data. Therefore, there
is a one-to-one correspondence between a program (e.g. *bwa aln*) and
a wrapper task (actually there are exceptions, but I'll come to that
later). However, any program could could be wrapped up in a task, so
the framework is by no means limited to bioinformatics. For a complete
list of wrapper modules, see section :ref:`ratatosk.lib`.

In addition to providing a library of tasks, *ratatosk* adds a
framework for defining task dependencies via a simple configuration
file. The configuration file also allows for customizing program
options, output names, and more.

There is also a generic script, `ratatosk_run.py`, that can be used to
call a specific task in the library:

.. code:: bash

   ratatosk_run.py Task --config-file configuration.yaml

By configuring dependencies in the configuration file you are
configuring a workflow. In essence, this means you can create complex
workflows and modifying program options *without writing any code*.
Code reuse at its best.


Of targets and make
-------------------

Central to task processing is the `target` concept. As the `luigi`
authors point out, `luigi` is conceptually similar to GNU Make, so
it's probably best to introduce the target concept by recalling how
`make` does it. Let's assume you want to compress a file, `file.txt`,
with `gzip`. The command to run would then be ```gzip file.txt```,
producing an output file `file.txt.gz`, illustrated below.

.. figure:: ../../grf/WEB.png
   :scale: 50%
   :align: center
   :alt: WEB
   
   **Figure 1.** Zipping files with gzip

With Make, you can define a rule

.. code:: make

   %.txt.gz: %.txt
         gzip $<

which when you run the command ```make file.txt.gz``` will look at the
make rules to see if there is a rule defined for files with suffix
`.txt.gz`, and if so, run the command defined for that rule. The file
`file.txt.gz` is commonly called the *target*, and `file.txt` the
*source* (substituted by `$<` in the make command above). One
important thing to know is that if the target already exists, make
only runs a command if the source is newer than the target.

*ratatosk* revolves around the idea of a target, in that every task
accepts an option `--target`. The task dynamically generates the
*source* file name, and *luigi* resolves the underlying dependencies,
running the task if the source file exists. *luigi* does not, however,
rerun a task should the target exist and the source is newer than the
target. This is important to keep in mind, as it effects what tasks
are run. The call to *ratatosk_run.py* would actually be


.. code:: bash

   ratatosk_run.py Task --target target.out --config-file configuration.yaml

Basically, then, *ratatosk* is a collection of make targets, based on
a python framework.


Visualizing task dependencies
-----------------------------

One thing make doesn't do is visualize task dependencies (at least not
that I'm aware of). I chose to visualize the make tasks above in order
to connect to the way *luigi* visualizes tasks. *luigi* uses a
`central planner
<https://github.com/spotify/luigi#using-the-central-planner>`_ to
visualize the dependency graph. Below, I've shown an excerpt from one
of the implemented pipelines

.. figure:: ../../grf/dupmetrics_to_printreads_targets.png
   :scale: 70%
   :align: center
   :alt: dupmetrics_to_printreads_targets
   
   **Figure 2.** Excerpt from variant calling pipeline

