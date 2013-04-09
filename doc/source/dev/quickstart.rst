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

In addition to providing 


Of targets and make
-------------------



Tasks and targets
-----------------

Central to task processing is the `target` concept. 

