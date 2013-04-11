Adding task wrappers
====================

In essence, ratatosk is a library of program wrappers. There are
already a couple of wrappers available, but many more could easily be
added. Here is a short HOWTO on how to add a wrapper module
``myprogram``.

1. Create the file
------------------

Create the file ``myprogram.py``, with at least the following imports:

.. code-block:: python

   from ratatosk.job import JobTask
   from ratatosk.jobrunner import DefaultShellJobRunner
   from ratatosk.utils import rreplace


2. Add job runners
------------------

At the very least, there should exist the following:

.. code-block:: python

   class MyProgramJobRunner(DefaultShellJobRunner):
        pass

This is in part for consistency, in part in case the ``myprogram``
program group needs special handling of command construction (see e.g.
:ref:`ratatosk.lib.tools.gatk`).

3. Add default inputs
----------------------------

There should be at least one input class that has as ``parent_task`` one of the
:ref:`ratatosk.lib.files.external` classes. Mainly here for naming consistency.

.. code-block:: python

   class InputFastqFile(JobTask):
       _config_section = "myprogram"
       _config_subsection = "InputFastqFile"
       target = luigi.Parameter(default=None)
       parent_task = luigi.Parameter(default="ratatosk.lib.files.external.FastqFile")


4. Add wrapper tasks
--------------------

Once steps 1-3 are done, tasks can be added. If the program has
subprograms (e.g. ``bwa aln``), it is advisable to create a generic
'top' job task; see for instance how :ref:`ratatosk.lib.align.bwa` is
set up). In any case, a task should at least consist of the following:

.. code-block:: python

   class MyProgram(JobTask):
	   # Corresponding section and subsection in config file
	   _config_section = "myprogram"
	   _config_subsection = "myprogram_subsection"
	   # Name of executable. This is a parameter so the user can specify
	   # the version
	   executable = luigi.Parameter(default="myprogram")
	   # Name of sub_executable. 
	   sub_executable = luigi.Parameter(default="my_subprogram")
	   # program options which should be a list
	   options = luigi.Parameter(default=(), is_list=True)
	   # parent_task, which governs the task(s) on which MyProgram depends
	   parent_task = luigi.Parameter(default=("myprogram.InputFastqFile", ), is_list=True)
	   # Target suffix 
	   suffix = luigi.Parameter(default=".sai")
	   # Add label if this task should add label to file name (e.g.
	   # file.txt -> file.label.txt)
	   label = luigi.Parameter(default="label")

	   # The following two options are hacky. add_label adds a
	   # label to a parent task, diff_label removes it. The latter
	   # is needed in cases where several labels differ between
	   # MyProgram and the parent task (e.g. if MyProgram target =
	   # file.label1.label2.label3.txt, parent task target =
	   # file.label1.txt, then diff_label = .label2.label3)
	   diff_label = luigi.Parameter(default=())
	   # Adds a label to parent_task target
	   add_label = luigi.Parameter(default=())

	   # Must be present
	   def job_runner(self):
	       return MyProgramJobRunner()

	   # Here gather the *required* arguments to 'myprogram'. Often input
	   # redirected to output suffices
	   def args(self):
	       return [self.input(), ">", self.output()]

	   # The following functions are inherited from JobTask and changing
	   # their behaviour is often not necessary

	   # For single requirements, the BaseJobTask function often
	   # suffices. For more complex requirements, a reimplementation is
	   # needed. Idea is to generate the target name of the parent class
	   # as source to the current task
	   # def requires(self):
	   #     cls = self.parent()[0]
	   #     return cls(target=self.source()[0])

	   # def exe(self):
	   #     """Executable of this task"""
	   #     return self.executable

	   # Subprogram name, e.g. 'aln' in 'bwa aln'	
	   # def main(self):
	   #     return self.sub_executable

	   # Returns the options string. This may need a lot of tampering
	   # with, see e.g. 'ratatosk.gatk.VariantEval' (but see also comment
	   # in issues)
	   # def opts(self):
	   #     return list(self.options)

	   # Output = target
	   # def output(self):
	   #     return luigi.LocalTarget(self.target)

Note that in many cases you only have to reimplement ``job_runner``
and ``args``, and in some cases the ``requires`` function.

To actually run the task, you need to import the module in your
script, and ``luigi`` will automagically add the task ``MyProgram``
and its options.
