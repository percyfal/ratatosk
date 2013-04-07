Installation
============

Pre-requisites
--------------

It is recommended that you first create a virtual environment in which
to install the packages. Install
[virtualenvwrapper](http://virtualenvwrapper.readthedocs.org/en/latest/)
and use
[mkvirtualenv](http://virtualenvwrapper.readthedocs.org/en/latest/command_ref.html)
to create a virtual environment.

Installation
------------

To install the development version of `ratatosk`, do

.. code-block:: bash
	
	git clone https://github.com/percyfal/ratatosk
	python setup.py develop

Dependencies
------------

To begin with, you may need to install
`Tornado <http://www.tornadoweb.org/>`_ and
[Pygraphviz](http://networkx.lanl.gov/pygraphviz/) (see
[Luigi](https://github.com/spotify/luigi/blob/master/README.md) for
further information).

The tests depend on the following software to run:

1. [bwa](http://bio-bwa.sourceforge.net/)
2. [samtools](http://samtools.sourceforge.net/)
3. [GATK](http://www.broadinstitute.org/gatk/) - set an environment
   variable `GATK_HOME` to point to your installation path
4. [picard](http://picard.sourceforge.net/) - set an environment
   variable `PICARD_HOME` to point to your installation path
5. [fastqc](http://www.bioinformatics.babraham.ac.uk/projects/fastqc/)   

There are also wrappers for the following software:

1. [cutadapt](http://code.google.com/p/cutadapt/) - install with `pip
   install cutadapt`
2. [htslib](https://github.com/samtools/htslib) - make a link from
   `vcf` to `htscmd` to use the shortcut commands
3. [annovar](http://www.openbioinformatics.org/annovar/)
4. [tabix](http://sourceforge.net/projects/samtools/files/tabix/)
5. [vcftools](http://vcftools.sourceforge.net/perl_module.html)
6. [snpeff](http://snpeff.sourceforge.net/)

More wrappers are continuously being added. 

Running the tests
-----------------

Cd to the luigi test directory (`tests`) and run

.. code-block:: bash

	nosetests -v -s test_commands.py
	
To run a given task (e.g. TestCommand.test_bwaaln), do

.. code-block:: bash

	nosetests -v -s test_commands.py:TestCommand.test_bwaaln
