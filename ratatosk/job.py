# Copyright (c) 2013 Per Unneberg
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
import random
import sys
import os
import yaml
from datetime import datetime
import subprocess
import logging
import warnings
import luigi
from subprocess import Popen, PIPE
from itertools import izip
from luigi.task import flatten
import ratatosk.shell as shell
import ratatosk
from ratatosk.jobrunner import DefaultShellJobRunner, PipedJobRunner
from ratatosk import backend
from ratatosk.handler import RatatoskHandler, register_attr
from ratatosk.config import get_config, get_custom_config
from ratatosk.utils import rreplace, update, config_to_dict
from ratatosk.log import get_logger
from ratatosk.experiment import ISample

logger = get_logger()

##############################
# Job tasks
##############################
class BaseJobTask(luigi.Task):
    """Main job task from which all ratatosk tasks should inherit.
    
    """
    config_file = luigi.Parameter(is_global=True, default=os.path.join(os.path.join(ratatosk.__path__[0], os.pardir, "config", "ratatosk.yaml")), description="Main configuration file.")
    """Main configuration file. For pipeline tasks, a predefined
    configuration file is loaded, disabling this option. Use
    :attr:`.custom_config` to configure pipeline tasks."""

    custom_config = luigi.Parameter(is_global=True, default=None, description="Custom configuration file for tuning options in predefined pipelines in which workflow may not be altered.")
    """Custom configuration file for tuning options in predefined pipelines."""

    dry_run = luigi.Parameter(default=False, is_global=True, is_boolean=True, description="Generate pipeline graph/flow without running any commands")
    """Dry run: don't actually run anything."""

    restart = luigi.Parameter(default=False, is_global=True, is_boolean=True, description="Restart pipeline from scratch.")
    """NOT YET IMPLEMENTED: Restart from scratch."""

    restart_from = luigi.Parameter(default=None, is_global=True, description="NOT YET IMPLEMENTED: Restart pipeline from a given task.")
    """NOT YET IMPLEMENTED: restart from a given task."""

    options = luigi.Parameter(default=(), description="Program options", is_list=True)
    """Program options to pass to task executable"""

    parent_task = luigi.Parameter(default=(), description="Main parent task(s) from which the current task receives of its input", is_list=True)
    """Parent task(s) on which the task depends."""

    num_threads = luigi.Parameter(default=1, description="Number of threads to run. Set to 1 if task.can_multi_thread is false")
    """Number of threads to run. Reset to 1 if :attr:`.can_multi_thread` False."""

    pipe  = luigi.BooleanParameter(default=False, description="Piped input/output. In practice refrains from including input/output file names in command list.")

    target = luigi.Parameter(default=None, description="Output target name")
    """Output target name for this task. Note: output should, as far as possible, be a singleton"""

    suffix = luigi.Parameter(default=(), description="File suffix for target", is_list=True)
    """File suffix name for target"""

    use_long_names = luigi.Parameter(default=False, description="Use long names (including all options) in graph vizualization", is_boolean=True, is_global=True)
    """Use all names, including options, in graph visualization. Useful for debugging purposes."""

    use_target_names = luigi.Parameter(default=False, description="Use target names in graph visualization", is_boolean=True, is_global=True)
    """Use target names in graph visualization."""

    label = luigi.Parameter(default=None)
    """Output label for this task. Used to generate target name. For
    instance, if source=file.txt, label=.label, and suffix=.txt, then
    target=file.label.txt"""

    diff_label = luigi.Parameter(default=None, is_list=True)
    """NB: TARGETED FOR REMOVAL. Label that is the difference in
    labels between tasks that are "far apart" in the dependency tree."""

    add_label = luigi.Parameter(default=None, is_list=True)
    """NB: TARGETED FOR REMOVAL. Label to be added to source."""

    exe_path = luigi.Parameter(default=None)
    """Path to main program; used by job runner"""

    executable = None
    """Name of executable to run a program"""

    sub_executable = None
    """Name of 'sub_executable' (e.g. for GATK, bwa)."""

    n_reduce_tasks = 8

    can_multi_thread = False
    """Flag to indicate whether this task can run in multi-threaded mode."""

    max_memory_gb = 3
    """Max memory this process may use. In general, max_memory_gb X
    workers should be less than the memory of the computing
    resource."""

    _handlers = {}
    """Handlers attached to a task"""

    _config_section = None
    """Configuration, main section. This is needed to group a task in
    a given namespace in the configuration file."""

    _parent_cls = []
    """Parent task classes."""

    _target_iter = 0
    """Counter."""

    def __init__(self, *args, **kwargs):
        """Initializes job task. A job task can be customized via
        configuration files. There are currently two configuration
        files:

        1. config_file, passed via option ``--config-file``
        2. custom_config, passed via option ``--custom-config``

        The reason there being two files is that updating
        ``parent_task`` is disabled in the custom configuration,
        thereby ensuring that predefined workflows in the regular
        configuration cannot be tampered with. However, other options
        can be modified.

        Options are updated in the by the following order of
        precedence:

        1. reads the configuration file updating the kwargs
        2. reads the custom configuration if present, updating relevant kwargs
        3. checks if any command line options have been passed, and if so, update kwargs
        4. use the default value

        Once the configuration has been set, the parent tasks are
        registered via
        :func:`ratatosk.job.BaseJobTask._register_parent_task`.

        """
        self._parent_cls = []
        self._handlers = {}
        params = self.get_params()
        param_values = self.get_param_values(params, args, kwargs)
        param_values_dict = {x[0]:x[1] for x in self.get_param_values(params, args, kwargs)}
        # 1. Main configuration file
        for key, value in param_values:
            if key == "config_file":
                config_file = value
                config = get_config()
                config.add_config_path(config_file)
                kwargs = self._update_config(config, param_values_dict, *args, **kwargs)
        # 2. Custom configuration file
        for key, value in param_values:
            if key == "custom_config":
                if not value:
                    continue
                custom_config_file = value
                # This must be a separate instance
                custom_config = get_custom_config()
                custom_config.add_config_path(custom_config_file)
                kwargs = self._update_config(custom_config, param_values_dict, disable_parent_task_update=True, *args, **kwargs)

        # 3. Finally, check if options were passed via the command line 
        for key, value in self.get_params():
            new_value = None
            # Got a command line option => override config file. Currently overriding parent_task *is* possible here (FIX ME?)
            if value.default != param_values_dict.get(key, None):
                new_value = param_values_dict.get(key, None)
                logger.debug("option '{0}'; got value '{1}' from command line, overriding configuration file setting and default '{2}' for task class '{3}'".format(key, new_value, value.default, self.__class__))
                kwargs[key] = new_value
        super(BaseJobTask, self).__init__(*args, **kwargs)
        # TODO: now that all parameters have been collected, global sections should be updated here
        # Update global configuration here for printing everything in PrintConfig task
        # backend.__global_config__ = update(backend.__global_config__, vars(config)["_sections"])

        # Register parent tasks
        parents = [v for k, v in self.get_param_values(params, args, kwargs) if k == "parent_task"].pop()
        # In case parent_task is defined as a string, not a list
        if not isinstance(parents, tuple):
            parents = [parents]
        self._register_parent_task(parents)
        if self.dry_run:
            print "DRY RUN: " + str(self)

    def _register_parent_task(self, parents):
        """Register parent task class(es) to task. Uses
        RatatoskHandler to register class to _parent_cls placeholder.
        If the task cannot be found, falls back on the default class.

        It is also possible to supply more tasks than there are
        default parent tasks. In case the extra parent task cannot be
        found, the default fallback task in this case is a NullJobTask
        that always passes.

        :param parents: list of python classes represented as strings in option parent_task

        """
        default_parents = self.get_param_default("parent_task")
        if not isinstance(default_parents, tuple):
            default_parents = [default_parents]
        if len(parents) != len(default_parents):
            logger.warn("length of parent list ({}) differs from length of default_parents list ({}); this may result in unpredicted behaviour".format(len(parents), len(default_parents)))
        if len(parents) > len(default_parents):
            len_diff = len(parents) - len(default_parents)
            default_parents = list(default_parents) + ["ratatosk.job.NullJobTask" for i in range(0, len_diff)]
        for p,d in izip(parents, default_parents):
            h = RatatoskHandler(label="_parent_cls", mod=p, load_type="class")
            register_attr(self, h, default_handler=d)
        
    def _update_config(self, config, param_values_dict, disable_parent_task_update=False, *args, **kwargs):
        """Update configuration for this task. All task options should
        have a default. Order of preference:

        
        1. if command line option encountered, override all config file settings
        2. if custom config file setting, override config and default
        3. if config file, override default
        4. default value

        :param config: configuration instance
        :param param_values_dict: task parameter dict
        :param disable_parent_task_update: disable parent task update for custom configurations (best practice pipeline execution order should stay fixed)

        :returns: an updated parameter list for the task.
        """
        # Set section to module name and subsection to class name
        # unless _config_section and _config_subsection set. The
        # latter are needed for classes that live outside their
        # namespace, e.g. subclasses in pipelines
        _section = self.__module__
        try:
            _subsection =  self.__class__.__name__ 
        except:
            _subsection = None
        if self._config_section:
            _section = self._config_section
        if not config:
            return kwargs
        if not config.has_section(_section):
            return kwargs
        if not _subsection:
            d = {_section:param_values_dict}
        else:
            d = {_section:{_subsection:param_values_dict}}
        backend.__global_config__ = update(backend.__global_config__, d)
        for key, value in self.get_params():
            new_value = None
            if config.has_key(_section, key):
                new_value = config.get(_section, key)
            if config.has_section(_section, _subsection):
                if config.has_key(_section, key, _subsection):
                    new_value = config.get(_section, key, _subsection)
                    logger.debug("Reading config file, setting '{0}' to '{1}' for task class '{2}'".format(key, new_value, self.__class__))
            if new_value:
                if key == "parent_task" and disable_parent_task_update:
                    logger.debug("disable_parent_task_update set; not updating '{0}' for task class '{1}'".format(key, self.__class__))
                else:
                    kwargs[key] = new_value
                    logger.debug("Updating config, setting '{0}' to '{1}' for task class '{2}'".format(key, new_value, self.__class__))
            else:
                logger.debug("Using default value '{0}' for '{1}' for task class '{2}'".format(value.default, key, self.__class__))
                pass
        return kwargs

    def path(self):
        """Main path of this executable. 

        :returns: :attr:`.exe_path`
        """
        return self.exe_path

    def exe(self):
        """Executable of this task."""
        return self.executable

    def main(self):
        """For commands that have subcommands"""
        return self.sub_executable

    def opts(self):
        """Generic options placeholder.

        :returns: the options list
        :rtype: list

        """
        return list(self.options)

    def args(self):
        """Generic argument list. Used to generate list of required
        inputs and outputs. Needs implementation in task subclasses.

        :returns: the argument list
        :rtype: list

        """
        pass

    def threads(self):
        """Get number of threads."""
        if self.can_multi_thread:
            return self.num_threads
        else:
            return 1

    def sfx(self, index=0):
        """Get suffix"""
        if isinstance(self.suffix, tuple) or isinstance(self.suffix, list):
            if len(self.suffix) > 0:
                return self.suffix[index]
            else:
                return ""
        else:
            return self.suffix

    # TODO: make into properties/fix naming
    def adl(self, index=0):
        """Get add label"""
        if isinstance(self.add_label, tuple) or isinstance(self.add_label, list):
            return self.add_label[index]
        else:
            return self.add_label

    def dil(self, index=0):
        """Get diff label"""
        if isinstance(self.diff_label, tuple) or isinstance(self.diff_label, list):
            return self.diff_label[index]
        else:
            return self.diff_label

    def lab(self):
        """Get the label"""
        return str(self.label)

    def max_memory(self):
        """Get the maximum memory (in Gb) that the task may use"""
        return self.max_memory_gb

    def add_suffix(self):
        """Some programs (e.g. samtools sort) have the bad habit of
        adding a suffix to the output file name. This needs to be
        accounted for in the temporary output file name."""
        return ""
        
    def init_local(self):
        """Setup local settings for run"""
        pass

    def run(self):
        """Init job runner.
        """
        self.init_local()
        self.job_runner().run_job(self)

    def parent(self):
        """Parent task class(es). List of tuples consisting of
        uninstantiated python objects paired with their used for dynamic generation of
        task dependencies.
        """
        return self._parent_cls

    def output(self):
        """Task output. In many cases this defaults to the target and
        doesn't need reimplementation in the subclasses. """
        return luigi.LocalTarget(self.target)

    def requires(self):
        """Task requirements. In many cases this is a single source
        whose name can be generated following the code below, and
        therefore doesn't need reimplementation in the subclasses."""
        return [cls(target=source) for cls, source in izip(self.parent(), self.source())]

    def complete(self):
        """
        If the task has any outputs, return true if all outputs exists.
        Otherwise, return whether or not the task has run or not
        """
        outputs = flatten(self.output())
        inputs = flatten(self.input())
        if self.dry_run:
            return False
        if self.restart:
            return False
        if len(outputs) == 0:
            # TODO: unclear if tasks without outputs should always run or never run
            warnings.warn("Task %r without outputs has no custom complete() method" % self)
            return False
        for output in outputs:
            if not output.exists():
                return False
            # Local addition: if any dependency is newer, then run
            # 20120329: causes troubles for annovar download, commenting out for now
            # if any([os.stat(x.fn).st_mtime > os.stat(output.fn).st_mtime for x in inputs if x.exists()]):
            #     return False
        else:
            return True

    def get_param_default(self, k):
        """Get the default value for a param."""
        params = self.get_params()
        for key, value in params:
            if k == key:
                return value.default
        return None

    def target_iterator(self):
        """Iterator for targets. 

        FIX ME: make standalone function? No real reason for it to be attached to a task.
        """
        tgt_fun = backend.__handlers__.get("target_generator_handler")
        kwargs = vars(self)
        if tgt_fun:
            target_list = tgt_fun(**kwargs)
        else:
            return
        if not target_list:
            return
        for tgt in target_list:
            assert isinstance(tgt, ISample), "Target iterater must return an object of type 'ISample'"
            yield tgt

    def source(self):
        """Make source file names from parent tasks in self.parent()"""
        self._target_iter = 0
        if self.diff_label:
            assert len(self.diff_label) == len(self.parent()), "if diff_label is defined, it must have as many elements as parent_task"
            return [self._make_source_file_name(p, diff_label=dl) for p, dl in izip(self.parent(), self.diff_label)]
        elif self.add_label:
            assert len(self.add_label) == len(self.parent()), "if add_label is defined, it must have as many elements as parent_task"
            return [self._make_source_file_name(p, add_label=al) for p, al in izip(self.parent(), self.add_label)]
        elif self.diff_label and self.add_label:
            assert len(self.diff_label) == len(self.parent()), "if diff_label is defined, it must have as many elements as parent_task"
            assert len(self.add_label) == len(self.parent()), "if add_label is defined, it must have as many elements as parent_task"
            return [self._make_source_file_name(p, diff_label=dl, add_label=al) for p, dl, al in izip(self.parent(), self.diff_label, self.add_label)]
        else:
            return [self._make_source_file_name(p) for p in self.parent()]

    def _make_source_file_name(self, parent_cls, diff_label=None, add_label=None):
        """Make source file name for parent tasks. Uses parent_cls to
        get parent class suffix (i.e. source suffix as viewed
        from self). The optional argument diff_label is needed for
        cases where the parent class is several steps up in the
        workflow, meaning that several labels have been added along
        the way. This is an irritating and as of yet unresolved issue.

        :param parent_cls: parent class
        :param diff_label: the "difference" in labels between self and parent.  E.g. if self.target=file.merge.sort.recal.bam depends on task with output file.merge.bam, and self.label=.recal, we would need to set the difference (.sort) here.
        :param add_label: label that should be added to parent source, e.g. read suffix 


        :return: parent task target name (source)
        """
        src_label = parent_cls().label
        tgt_suffix = self.sfx()
        src_suffix = parent_cls().sfx()
        target = self.target
        if isinstance(self.target, tuple) or isinstance(self.target, list):
            target = self.target[self._target_iter]
            self._target_iter += 1
        if isinstance(tgt_suffix, tuple) or isinstance(tgt_suffix, list):
            if len(tgt_suffix) > 0:
                tgt_suffix = tgt_suffix[0]
        if isinstance(src_suffix, tuple) or isinstance(src_suffix, list):
            if len(src_suffix) > 0:
                src_suffix = src_suffix[0]
        # Start by setting source, stripping tgt_suffix if present
        source = target
        if tgt_suffix:
            source = rreplace(target, tgt_suffix, "", 1)
        # Then remove the target label and optional diff_label
        if self.label:
            source = rreplace(source, self.label, "", 1)
        if diff_label:
            source = rreplace(source, str(diff_label), "", 1)
        if add_label:
            source = source + add_label
        if src_label:
            # Trick: remove src_label first if present since
            # the source label addition here corresponds to a
            # "diff" compared to target name
            source = rreplace(source, str(src_label), "", 1) + str(src_label) + str(src_suffix)
        else:
            source = source + str(src_suffix)
        if src_label:
            if source.count(str(src_label)) > 1:
                print "label '{}' found multiple times in target '{}'; this could be intentional".format(src_label, source)
            elif source.count(src_label) == 0:
                print "label '{}' not found in target '{}'; are you sure your target is correctly formatted?".format(src_label, source)
        return source
            
class JobTask(BaseJobTask):
    def job_runner(self):
        return DefaultShellJobRunner()

    def args(self):
        return []

class InputJobTask(JobTask):
    """Input job task. Should have as a parent task one of the tasks
    in ratatosk.lib.files.external"""
    def requires(self):
        cls = self.parent()[0]
        return cls(target=self.target)
    
    def run(self):
        """No run should be defined"""
        pass

class JobWrapperTask(JobTask):
    """Wrapper task that adds target by default"""
    def complete(self):
        return all(r.complete() for r in flatten(self.requires()))

    def run(self):
        pass

class NullJobTask(JobTask):
    """Task that always completes"""
    def run(self):
        pass

class GenericWrapperTask(JobWrapperTask):
    """Generic task wrapper.

    .. note:: Still under development

    The idea is to create a dependency to any task, so that the
    calling script effectively works as a make file.
    """
    parent_task = luigi.Parameter(default=("NullJobTask",), is_list=True)
    generic_wrapper_target = luigi.Parameter(default=(), is_list=True)
    task = luigi.Parameter(default=None)

    def requires(self):
        from luigi.task import Register
        if not self.task in Register.get_reg().keys():
            logger.warn("No such task {} in registry; skipping".format(self.task))
            return []
        else:
            cls = Register.get_reg()[self.task]
            return [cls(target=x) for x in self.generic_wrapper_target]

class InputPath(InputJobTask):
    """Helper task for PipedTask"""
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.Path")

class PipedTask(JobTask):
    """A piped task takes as input a set of tasks and uses the
    standard python module :py:mod:`subprocess.Popen` to communicate
    output between the tasks.

    """
    tasks = luigi.Parameter(default=[], is_list=True)
    """Task list to run."""

    # TODO: Is this needed? Better is probably to do a "regular" depends
    # def requires(self):
    #     return InputPath(target=os.curdir)
    
    def output(self):
        return luigi.LocalTarget(self.target)

    def job_runner(self):
        return PipedJobRunner()

    def args(self):
        return self.tasks

class PipelineTask(JobWrapperTask):
    """Wrapper task for predefined pipelines. Adds option
    target_generator_handler which must be defined in order to
    collect targets.
    """
    target_generator_handler = luigi.Parameter(default=None)
    """Target generator handler is a function that returns a list of
    :class:`ISample <ratatosk.experiment.ISample>` objects."""
    
class PrintConfig(JobTask):
    """Print global configuration for all tasks, including all
    parameters (customizations as well as defaults) and absolute path
    names to program executables (thus implicitly in many cases giving
    the program version, although there should be another function for
    getting program version information and inserting that info here).
    This task (or function) should probably be called after the last
    task so that we know the exact parameter settings and programs
    used to run an analysis. Aim for reproducible research :)
    """
    header = """# Created by {program} on {date}
#
# Command: TODO: insert command here 
#
# The ratatosk configuration file collects all configuration settings
# for a run. In principle you could use this file as input to rerun
# the analysis preserving exactly the same settings.
#
""".format(program="ratatosk", date=datetime.today().strftime("at %H:%M on %A %d, %B %Y"))

    def requires(self):
        return []

    def output(self):
        filename = "ratatosk_config_{}.yaml".format(datetime.today().strftime("%Y_%m_%d_%H_%M"))
        with open(filename, "w") as fh:
            fh.write(self.header)
            fh.write(yaml.safe_dump(config_to_dict(backend.__global_config__), default_flow_style=False, allow_unicode=True, width=1000))
        return luigi.LocalTarget(filename)

    def run(self):
        return NotImplemented


def name_prefix():
    """NOT IMPLEMENTED YET

    Generate a name prefix based on available labels for task
    graph. Traverse dependency tree, recording all possible joins of
    labels from end leaf to "top" leaf. Note that this is similar, but
    not identical, to the longest path problem (not all nodes have a
    label and should therefore not contribute to string). See
    http://en.wikipedia.org/wiki/Longest_path_problem.

    EDIT: note that traversing the tree is crucial also for the
    desired option

    --restart-from TASK (start from a given task)

    Touching the first file is *not* enough since the requires
    only looks at the directly preceding tasks.
    """ 

    pass

# Generic target generator functions and classes TODO:
# JobTask.target_iterator takes a task as input, but this function
# does not take arguments. How pass on to generic_target_generator?
# Something along the lines of the backend or **kwargs
def generic_target_generator(target_generator_infile=None, **kwargs):
    """NOT IMPLEMENTED YET.

    Generic target generator. Should take as input a file name, read
    and return contents.
    """ 
    pass
