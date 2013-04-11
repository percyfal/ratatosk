Tasks
=====

The main :mod:`ratatosk` task class is a subclass of `luigi.Task
<https://github.com/spotify/luigi/blob/master/luigi/task.py>`_ and is
a core component of :mod:`ratatosk` functionality. If you haven't already
done so, be sure to read up on the `luigi documentation
<https://github.com/spotify/luigi/blob/master/README.md>`_ , in
particular the section `conceptual overview
<https://github.com/spotify/luigi/blob/master/README.md#conceptual-overview>`_.

Tasks in :py:mod:`ratatosk.job`
--------------------------------

All :py:mod:`ratatosk` tasks subclass classes that are based on a base
job task called :py:class:`ratatosk.job.BaseJobTask`. The main task
classes used (and which should be used) by the wrapper library tasks
are defined in :py:mod:`ratatosk.job`, including

 * :py:class:`.JobTask`: main job task which adds a default *job runner* to the
   base job task
 * :py:class:`.InputJobTask`: a task that depends on an external file
 * :py:class:`.JobWrapperTask`: task that wraps several tasks into one unit
 * :py:class:`.NullJobTask`: a task that always completes
 * :py:class:`.PipedTask`: a task that chains tasks in a pipe
 * :py:class:`.PipelineTask`: a wrapper task for predefined pipelines

Therefore, in order to understand how :mod:`ratatosk` tasks work, you just
need to get a basic understanding of :py:class:`ratatosk.job.BaseJobTask`. 

:py:class:`ratatosk.job.BaseJobTask`
-----------------------------------------------------------

Configurable attributes
^^^^^^^^^^^^^^^^^^^^^^^

To begin with, there are a couple of attributes essential to the
behaviour of all subclasses, some of which are configurable at
run-time (they are :py:class:`luigi.Parameter` objects). The most
important ones are

:py:attr:`.config_file`
  Main configuration file

:py:attr:`.custom_config`
  Custom configuration file that is used for tuning options in predefined pipelines

:py:attr:`options <ratatosk.job.BaseJobTask.options>`
  Program options for wrapped executable, represented by a list

:py:attr:`parent_task <ratatosk.job.BaseJobTask.parent_task>`
  Defines the task on which this task depends, encoded as a string
  that represents a python module (e.g.
  'ratatosk.lib.tools.gatk.UnifiedGenotyper'. Several parent tasks can
  be defined.

:py:attr:`target <ratatosk.job.BaseJobTask.target>`
  The output target name of this task.

:py:attr:`suffix <ratatosk.job.BaseJobTask.suffix>`
  The output suffix of this task. Can be a list in case several outputs are produced.

:py:attr:`label <ratatosk.job.BaseJobTask.label>` 
  The label that is attached to the resulting output (e.g. file.txt -> file.label.txt)

:py:attr:`exe_path <ratatosk.job.BaseJobTask.exe_path>`
  Path to executable

:py:attr:`executable <ratatosk.job.BaseJobTask.executable>`
  Name of executable

:py:attr:`sub_executable <ratatosk.job.BaseJobTask.sub_executable>`
  Name of executable, if applicable

Non-configurable attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition, there are a couple of important non-configurable attributes:

:attr:`_config_section <ratatosk.job.BaseJobTask._config_section>`
  The configuration section to which this task belongs

:attr:`_config_subsection <ratatosk.job.BaseJobTask._config_subsection>`
  The configuration subsection to which this task belongs

:attr:`_handlers <ratatosk.job.BaseJobTask._handlers>`
  Handlers registered to this task

:attr:`_parent_cls <ratatosk.job.BaseJobTask._parent_cls>`
  Placeholder for registered parent classes


Functions
^^^^^^^^^^

The most important functions include

:meth:`_register_parent_task() <ratatosk.job.BaseJobTask._register_parent_task>`
  Registers classes to _parent_cls. In practice parses string
  representation of a python module and tries to load the module,
  falling back on a default class on failure

:meth:`_update_config() <ratatosk.job.BaseJobTask._update_config>`
  Reads configuration files and sets the attributes of the task

:meth:`output() <ratatosk.job.BaseJobTask.output>`
  Defines the output target as a luigi.LocalTarget class

:meth:`requires() <ratatosk.job.BaseJobTask.requires>`
  Defines the dependencies. 

:meth:`args() <ratatosk.job.BaseJobTask.args>`
  Defines what the final program command string looks like. This
  function should often be overridden in subclasses

:meth:`target_iterator() <ratatosk.job.BaseJobTask.target_iterator>`
  Helper function that iterates over targets defined by a user
  supplied function :meth:`target_generator_handler`. This is the function
  that enables tasks to compute target file names, and should generate
  a 3-tuple consisting of (name, merge-prefix, read-prefix)

:meth:`_make_source_file_name() <ratatosk.job.BaseJobTask._make_source_file_name>`
  Calculates *source* file names from *target* by adding/subtracting
  indices and labels

Initialization
--------------

When a task is instantiated, it basically needs to do the following
things:

1. read configuration files and update configuration
2. register parent tasks

That's all there is to it.

Job runners
===========

Job runners govern how a task is run. In practice, they do the
following

1. create argument list from the args function
2. fix path names for outputs, generating temporary file names so
   that all operations are atomic
3. submit the command string via *subprocess*

