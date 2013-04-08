Background
==========


Implementation
--------------

Basic job task
^^^^^^^^^^^^^^
`ratatosk.job` defines, among other things, a *default shell job
runner*, which is a wrapper for running tasks in shell, and a *base
job task* that subclasses `luigi.Task`. The base job task implements a
couple of functions that are essential for general behaviour:

* `_update_config` that reads the configuration file and overrides
  default settings. It is run from `__init__`, meaning that it is read
  for *every task* (see issues)
  
Program modules
^^^^^^^^^^^^^^^

`ratatosk` submodules are named after the application/program to be
run (e.g. `ratatosk.lib.align.bwa` for `bwa`). For consistency, the modules
shoud contain

1. a **job runner** that subclasses
   `ratatosk.job.DefaultShellJobRunner`. The runner specifies how the
   program is run
   
2. **input** file task(s) that subclass `ratatosk.job.JobTask` and
   that depend on external tasks in `ratatosk.lib.files.external`. The idea is
   that all acceptable file formats be defined as external inputs, and
   that parent tasks therefore must use one/any of these inputs
   
3. a **main job task** that subclasses `ratatosk.job.JobTask` and has
   as default parent task one of the inputs (previous point). The
   `_config_section` should be set to the module name (e.g. `bwa` for
   `ratatosk.lib.align.bwa`). It should also return the *job runner*
   defined in 1.
   
4. **tasks** that subclass the *main job task*. The
   `_config_subsection` should represent the task name in some way
   (e.g. `aln` for `bwa aln`command)
   
5. possibly **wrapper tasks** that group common tasks in a module

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

