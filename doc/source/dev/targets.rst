.. _handler_and_targets:

Of handlers and targets
========================

.. warning:: The target collecting implementation is still under heavy
   development. The API is likely to change every now and then.

All tasks subclass :class:`.BaseJobTask` and therefore inherit an
option :attr:`target <ratatosk.job.BaseJobTask.target>`. By passing a
target option, a task can be run like a make target:

.. code-block:: text

   ratatosk_run.py Task --target target.txt

Should ``Task`` have any further dependencies, these will be
calculated on-the-fly and run if incomplete.

Target generator functions
--------------------------

For more complex workflows, such as pipelines, the situation is more
complicated. Generally, we want to run hundreds of targets, and
providing them as arguments to options is cumbersome, to say the
least. Therefore, :mod:`ratatosk` implements the concept of *target
generator functions* whose purpose is to generate a list of
experimental units that contain information of names at different
levels.

The experimental units are python objects that must inherit from the
abstract base class :class:`ratatosk.experiment.ISample`. This ensures
that the target generator function returns objects with defined
properties and entries, such as sample and project identifier. The
goal is to provide an interface that makes it easy to write any
function of choice that is tailored for a given file organization and
naming convention. See
:class:`ratatosk.ext.scilife.sample.generic_target_generator` and
:class:`ratatosk.ext.scilife.sample.target_generator` for examples.

Registering target generator functions
--------------------------------------

Target generator functions can be registered in two ways:

1. to :data:`backend.__handlers__ <ratatosk.backend>`, which serves as
   a global container, via the :func:`.handler.register` function. See
   :func:`.handler.setup_global_handlers` for an example of how this
   is done.
2. to the task attribute :attr:`.BaseJobTask._handlers` via the
   :func:`.handler.register_task_handler` function, as implemented in
   :func:`MergeSamFiles.requires
   <ratatosk.lib.tools.picard.MergeSamFiles.requires>` and
   :func:`CombineVariants.requires
   <ratatosk.lib.tools.gatk.CombineVariants.requires>`.
   
   .. note:: these target generator functions are used for collecting
      targets to *merge*. They currently do not return :class:`.ISample`
      objects but lists of file names. This inconsistency will be
      resolved, either by changing the handler name, or by generating the
      target names from :class:`.ISample` objects within the
      :func:`requires` function.

The target generator functions are provided to :mod:`ratatosk` as
option paramaters, and can therefore be defined in configuration
files. In the above cases, one could use

.. code-block:: text

   settings:
     target_generator_handler: my.module.tgf

   ratatosk.lib.tools.gatk.MergeSamFiles:
     target_generator_handler: my.module.collect_bam_files

Adding custom handlers
----------------------------------------

In general, handler functions and classes are registered by one of the
:func:`register` functions in :mod:`ratatosk.handler`. Each of these
functions takes as input a *handler object* of class
:class:`ratatosk.handler.IHandler`. Custom classes and functions can
therefore be added by instantiating a subclass of an :class:`IHandler
<ratatosk.handler.IHandler>` object (e.g. :class:`RatatoskHandler
<ratatosk.handler.RatatoskHandler>`) and passing the string
representation of the class/function along with a label descriptor as
init arguments:

.. code-block:: python

   h = RatatoskHandler(label="HandlerLabel", mod="my.handler.function")
   # Will register handler to backend.__handlers__["HandlerLabel"]
   register(h)

Keeping track of information in pipelines
-----------------------------------------

Finally, some words of how targets are collected and handled in the
pipeline modules. First, targets are loaded via the global target
generator function (registered in
:data:`backend.__handlers__["target_generator_handler"]
<ratatosk.backend>`). Then, in order to make the targets accessible to
all task-specific target generator handlers, targets are stored in
:data:`backend.__global_vars__["targets"] <ratatosk.backend>`. For
instance, collecting a list of bam files to merge could then be
generated as follows:

.. code-block:: python

   sample_runs = backend.__global_vars__.get("targets")
   bam_list = list(set([x.prefix("sample_run") + task.suffix) for x in sample_runs])
   return bam_list

This is (almost) how
:func:`ratatosk.ext.scilife.sample.collect_sample_runs` works.
