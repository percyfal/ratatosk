Quickstart
==========

*ratatosk* was devel

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

