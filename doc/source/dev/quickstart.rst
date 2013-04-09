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


Wrapper library
---------------

As mentioned in the :ref:`installation`, the tests dependencies mean
there are wrappers for the following software:

1. `bwa <http://bio-bwa.sourceforge.net/>`_
2. `samtools <http://samtools.sourceforge.net/>`_
3. `GATK <http://www.broadinstitute.org/gatk/>`_ - set an environment
   variable `GATK_HOME` to point to your installation path
4. `picard <http://picard.sourceforge.net/>`_ - set an environment
   variable `PICARD_HOME` to point to your installation path
5. `fastqc <http://www.bioinformatics.babraham.ac.uk/projects/fastqc/>`_   

There are also wrappers for the following software:

1. `cutadapt <http://code.google.com/p/cutadapt/>`_ - install with `pip
   install cutadapt`
2. `htslib <https://github.com/samtools/htslib>`_ - make a link from
   `vcf` to `htscmd` to use the shortcut commands
3. `annovar <http://www.openbioinformatics.org/annovar/>`_
4. `tabix <http://sourceforge.net/projects/samtools/files/tabix/>`_
5. `vcftools <http://vcftools.sourceforge.net/perl_module.html>`_
6. `snpeff <http://snpeff.sourceforge.net/>`_

More wrappers are continuously being added. 

