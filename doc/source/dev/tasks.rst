Tasks
=====

The main *ratatosk* task class is a subclass of `luigi.Task
<https://github.com/spotify/luigi/blob/master/luigi/task.py>`_ and is
a core component of *ratatosk* functionality. If you haven't already
done so, be sure to read up on the `luigi documentation
<https://github.com/spotify/luigi/blob/master/README.md>`_ , in
particular the section `conceptual overview
<https://github.com/spotify/luigi/blob/master/README.md#conceptual-overview>`_.

Tasks in :ref:`ratatosk.job`
----------------------------

All *ratatosk* tasks subclass classes that are based on a base job
task called :ref:`ratatosk.job.BaseJobTask
<ratatosk.job.BaseJobTask>`. The main task classes used (and which
should be used) by the wrapper library tasks are defined in
:ref:`ratatosk.job`, including

 * JobTask: main job task which adds a default *job runner* to the
   base job task
 * InputJobTask: a task that depends on an external file
 * JobWrapperTask: task that wraps several tasks into one unit
 * NullJobTask: a task that always completes
 * PipedTask: a task that chains tasks in a pipe
 * PipelineTask: a wrapper task for predefined pipelines

Therefore, in order to understand how *ratatosk* tasks work, you just
need to get a basic understanding of :ref:`ratatosk.job.BaseJobTask`. 

:ref:`ratatosk.job.BaseJobTask`
-----------------------------------------------------------

Configurable attributes
^^^^^^^^^^^^^^^^^^^^^^^

To begin with, there are a couple of attributes essential to the
behaviour of all subclasses, some of which are configurable at
run-time (they are *luigi.Parameter* objects). The most important ones
are

config_file
  Main configuration file

custom_config
  Custom configuration file that is used for tuning options in predefined pipelines

options
  Program options for wrapped executable, represented by a list

parent_task
  Defines the task on which this task depends, encoded as a string
  that represents a python module (e.g.
  'ratatosk.lib.tools.gatk.UnifiedGenotyper'. Several parent tasks can
  be defined.

target
  The output target name of this task.

suffix
  The output suffix of this task. Can be a list in case several outputs are produced.

label 
  The label that is attached to the resulting output (e.g. file.txt -> file.label.txt)

exe_path
  Path to executable

executable
  Name of executable

sub_executable
  Name of executable, if applicable

Non-configurable attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition, there are a couple of important non-configurable attributes:

_config_section
  The configuration section to which this task belongs

_config_subsection
  The configuration subsection to which this task belongs

_handlers
  Handlers registered to this task

_parent_cls
  Placeholder for registered parent classes


Functions
^^^^^^^^^^

The most important functions include

_register_parent_task
  Registers classes to _parent_cls. In practice parses string
  representation of a python module and tries to load the module,
  falling back on a default class on failure

_update_config
  Reads configuration files and sets the attributes of the task

output
  Defines the output target as a luigi.LocalTarget class

requires
  Defines the dependencies. 

args
  Defines what the final program command string looks like. This
  function should often be overridden in subclasses

target_iterator
  Helper function that iterates over targets defined by a user
  supplied function *target_generator_handler*. This is the function
  that enables tasks to compute target file names, and should generate
  a 3-tuple consisting of (name, merge-prefix, read-prefix)

_make_source_file_name
  Calculates *source* file names from *target* by adding/subtracting
  indices and labels

Initialisation
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
2. fixes path names for outputs, generating temporary file names so
   that all operations are atomic
3. submits the command string via *subprocess*

