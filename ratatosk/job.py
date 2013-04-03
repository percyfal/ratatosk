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
import importlib
from itertools import izip
from luigi.task import flatten
import ratatosk.shell as shell
import ratatosk
from ratatosk import backend
from ratatosk.config import get_config, get_custom_config
from ratatosk.utils import rreplace, update, config_to_dict

# Use luigi-interface for now
logger = logging.getLogger('luigi-interface')

class JobRunner(object):
    run_job = NotImplemented

class DefaultShellJobRunner(JobRunner):
    """Default job runner to use for shell jobs."""
    def __init__(self):
        pass

    @staticmethod
    def _fix_paths(job):
        """Modelled after hadoop_jar.HadoopJarJobRunner._fix_paths.
        """
        tmp_files = []
        args = []
        for x in job.args():
            if isinstance(x, luigi.LocalTarget): # input/output
                if x.exists(): # input
                    args.append(x.path)
                else: # output
                    ypath = x.path + '-luigi-tmp-%09d' % random.randrange(0, 1e10)
                    y = luigi.LocalTarget(ypath)
                    logger.info("Using temp path: {0} for path {1}".format(y.path, x.path))
                    args.append(y.path)
                    if job.add_suffix():
                        x = luigi.LocalTarget(x.path + job.add_suffix())
                        y = luigi.LocalTarget(y.path + job.add_suffix())
                    tmp_files.append((y, x))
            else:
                args.append(str(x))
        return (tmp_files, args)

    @staticmethod
    def _get_main(job):
        """Add main program to command. Override this for programs
        where main is given as a parameter, e.g. for GATK"""
        return job.main()

    def _make_arglist(self, job):
        """Make and return the arglist"""
        if job.path():
            exe = os.path.join(job.path(), job.exe())
        else:
            exe = job.exe()
        if not exe:# or not os.path.exists(exe):
            logger.error("Can't find executable: {0}, full path {1}".format(exe,
                                                                            os.path.abspath(exe)))
            raise Exception("executable does not exist")
        arglist = [exe]
        if job.main():
            arglist.append(self._get_main(job))
        if job.opts():
            arglist += job.opts()
        # Need to call self.__class__ since fix_paths overridden in
        # DefaultGzShellJobRunner
        (tmp_files, job_args) = self.__class__._fix_paths(job)
        
        arglist += job_args
        return arglist
        
    def run_job(self, job):
        arglist = self._make_arglist(job)
        cmd = ' '.join(arglist)
        logger.info("\nJob runner '{0}';\n\trunning command '{1}'\n".format(self.__class__, cmd))
        (stdout, stderr, returncode) = shell.exec_cmd(cmd, shell=True)
        if returncode == 0:
            logger.info("Shell job completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                # This is weird; using the example in luigi
                # (a.move(b)) doesn't work, and using just b.path
                # fails unless it contains a directory (e.g. './file'
                # works, 'file' doesn't)
                a.move(os.path.join(os.curdir, b.path))
        else:
            raise Exception("Job '{}' failed: \n{}".format(' '.join(arglist), " ".join([stderr])))
                

# Aaarrgh - it doesn't get uglier than this. Some programs
# "seamlessly" read and write gzipped files. In the job runner we work
# with temp files that lack the .gz suffix, so the output is not
# compressed... Took me a while to figure out. Solution for now
#  is to add suffix 'gz' to the temp file. Obviously this won't work
#  for uncompressed input (SIGH), but as I discuss in issues, maybe
#  this is a good thing.
class DefaultGzShellJobRunner(DefaultShellJobRunner):
    """Temporary fix for programs that determine if output is zipped
    based on the suffix of the file name. When working with tmp files
    this will not work so an extra .gz suffix needs to be added. This
    should be taken care of in a cleaner way."""
    def __init__(self):
        pass

    @staticmethod
    def _fix_paths(job):
        """Modelled after hadoop_jar.HadoopJarJobRunner._fix_paths.
        """
        tmp_files = []
        args = []
        for x in job.args():
            if isinstance(x, luigi.LocalTarget): # input/output
                if x.exists(): # input
                    args.append(x.path)
                else: # output
                    ypath = x.path + '-luigi-tmp-%09d' % random.randrange(0, 1e10) + '.gz'
                    y = luigi.LocalTarget(ypath)
                    logger.info("Using temp path: {0} for path {1}".format(y.path, x.path))
                    args.append(y.path)
                    if job.add_suffix():
                        x = luigi.LocalTarget(x.path + job.add_suffix())
                        y = luigi.LocalTarget(y.path + job.add_suffix())
                    tmp_files.append((y, x))
            else:
                args.append(str(x))
        return (tmp_files, args)


class BaseJobTask(luigi.Task):
    config_file = luigi.Parameter(is_global=True, default=os.path.join(os.path.join(ratatosk.__path__[0], os.pardir, "config", "ratatosk.yaml")), description="Main configuration file.")
    custom_config = luigi.Parameter(is_global=True, default=None, description="Custom configuration file for tuning options in predefined pipelines in which workflow may not be altered.")
    dry_run = luigi.Parameter(default=False, is_global=True, is_boolean=True, description="Generate pipeline graph/flow without running any commands")
    restart = luigi.Parameter(default=False, is_global=True, is_boolean=True, description="Restart pipeline from scratch.")
    restart_from = luigi.Parameter(default=None, is_global=True, description="NOT YET IMPLEMENTED: Restart pipeline from a given task.")
    options = luigi.Parameter(default=(), description="Program options", is_list=True)
    parent_task = luigi.Parameter(default=None, description="Main parent task from which the current task receives (parts) of its input")
    num_threads = luigi.Parameter(default=1)
    # Note: output should generate one file only; in special cases we
    # need to do hacks
    target = luigi.Parameter(default=None, description="Output target name")
    target_suffix = luigi.Parameter(default=None, description="File suffix for target")
    source_suffix = luigi.Parameter(default=None, description="File suffix for source")
    source = None
    # Use for changing labels in graph visualization
    use_long_names = luigi.Parameter(default=False, description="Use long names (including all options) in graph vizualization", is_boolean=True, is_global=True)

    # Path to main program; used by job runner
    exe_path = luigi.Parameter(default=None)
    # Name of executable to run a program
    executable = None
    # Name of 'sub_executable' (e.g. for GATK, bwa). 
    sub_executable = None
    
    _config_section = None
    _config_subsection = None
    n_reduce_tasks = 8
    can_multi_thread = False
    max_memory_gb = 3
    # Not all tasks are allowed to "label" their output
    label = luigi.Parameter(default=None)

    # Handlers attached to a task
    __handlers__ = {}

    def __init__(self, *args, **kwargs):
        params = self.get_params()
        param_values = self.get_param_values(params, args, kwargs)
        # Main configuration file
        for key, value in param_values:
            if key == "config_file":
                config_file = value
                config = get_config()
                config.add_config_path(config_file)
                kwargs = self._update_config(config, *args, **kwargs)
        for key, value in param_values:
            if key == "custom_config":
                if not value:
                    continue
                custom_config_file = value
                # This must be a separate instance
                custom_config = get_custom_config()
                custom_config.add_config_path(custom_config_file)
                kwargs = self._update_config(custom_config, disable_parent_task_update=True, *args, **kwargs)
        super(BaseJobTask, self).__init__(*args, **kwargs)
        if self.dry_run:
            print "DRY RUN: " + str(self)

    def _update_config(self, config, disable_parent_task_update=False, *args, **kwargs):
        """Update configuration for this task. All task options should
        have a default. Order of preference:

        1. if command line option encountered, override config file settings
        2. if configuration file has a setting, override default value for task.

        :param config: configuration instance
        :param disable_parent_task_update: disable parent task update for custom configurations (best practice pipeline execution order should stay fixed)

        :returns: an updated parameter list for the task.
        """
        # Update global configuration here for printing everything in PrintConfig task
        backend.__global_config__ = update(backend.__global_config__, vars(config)["_sections"])
        if not config:
            return kwargs
        if not config.has_section(self._config_section):
            return kwargs
        params = self.get_params()
        param_values = {x[0]:x[1] for x in self.get_param_values(params, args, kwargs)}
        if not self._config_subsection:
            d = {self._config_section:param_values}
        else:
            d = {self._config_section:{self._config_subsection:param_values}}
        backend.__global_config__ = update(backend.__global_config__, d)
        for key, value in self.get_params():
            new_value = None
            # Got a command line option => override config file
            if value.default != param_values.get(key, None):
                new_value = param_values.get(key, None)
                logger.debug("option '{0}'; got value '{1}' from command line, overriding configuration file setting default '{2}' for task class '{3}'".format(key, new_value, value.default, self.__class__))
            else:
                if config.has_key(self._config_section, key):
                    new_value = config.get(self._config_section, key)
                if config.has_section(self._config_section, self._config_subsection):
                    if config.has_key(self._config_section, key, self._config_subsection):
                        new_value = config.get(self._config_section, key, self._config_subsection)
                        logger.debug("Reading config file, setting '{0}' to '{1}' for task class '{2}'".format(key, new_value, self.__class__))

            if new_value:
                if key == "parent_task" and disable_parent_task_update:
                    logger.debug("disable_parent_task_update set; not updating '{0}' for task class '{1}'".format(key, self.__class__))
                else:
                    kwargs[key] = new_value
                    logger.debug("Updating config, setting '{0}' to '{1}' for task class '{2}'".format(key, new_value, self.__class__))
            else:
                pass
            logger.debug("Using default value '{0}' for '{1}' for task class '{2}'".format(value.default, key, self.__class__))
        return kwargs

    def path(self):
        """Main path of this executable"""
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

    def max_memory(self):
        """Get the maximum memory (in Gb) that the task may use"""
        return self.max_memory_gb

    def add_suffix(self):
        """Some programs (e.g. samtools sort) have the bad habit of
        adding a suffix to the output file name. This needs to be
        accounted for in the temporary output file name."""
        return ""
        
    def init_local(self):
        """Setup local settings"""
        pass

    def run(self):
        """Init job runner.
        """
        self.init_local()
        #if not self.dry_run:
        self.job_runner().run_job(self)

    def output(self):
        """Task output. In many cases this defaults to the target and
        doesn't need reimplementation in the subclasses. """
        return luigi.LocalTarget(self.target)

    def requires(self):
        """Task requirements. In many cases this is a single source
        whose name can be generated following the code below, and
        therefore doesn't need reimplementation in the subclasses."""
        cls = self.set_parent_task()
        source = self._make_source_file_name()
        return cls(target=source)

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
            if len(tgt) != 3:
                raise ValueError, "target generator handler must return 3-tuple"
            sample, sample_merge, sample_run = tgt
            yield sample, sample_merge, sample_run

    def set_parent_task(self):
        """Try to import a module task class represented as string in
        parent_task and use it as such.

        TODO: handle parent task list where tasks have several requirements?
        """
        opt_mod = ".".join(self.parent_task.split(".")[0:-1])
        opt_cls = self.parent_task.split(".")[-1]
        default_task = self.get_param_default("parent_task")
        default_mod = ".".join(default_task.split(".")[0:-1])
        default_cls = default_task.split(".")[-1]
        try:
            mod = __import__(opt_mod, fromlist=[opt_cls])
            cls = getattr(mod, opt_cls)
            ret_cls = cls
        except:
            logger.warn("No class {} found: using default class {} for task '{}'".format(".".join([opt_mod, opt_cls]), 
                                                                                         ".".join([default_mod, default_cls]),
                                                                                         self.__class__))
            ret_mod = __import__(default_mod, fromlist=[default_cls])
            ret_cls = getattr(ret_mod, default_cls)
        return ret_cls

    def set_parent_task_list(self):
        """Try to import a module task class represented as string in
        parent_task and use it as such
        """
        ret_cls = []
        opt_mod = []
        default_cls = []
        default_task = self.get_param_default("parent_task")
        for task in default_task:
            mod = ".".join(default_task.split(".")[0:-1])
            cls = default_task.split(".")[-1]
            imp_mod = __import__(mod, fromlist=[cls])
            default_cls.append(getattr(imp_mod, cls))

        for task in self.parent_task.split(","):
            opt_mod.append((".".join(task.split(".")[0:-1]),
                            task.split(".")[-1]))

        for o_mod, o_cls in opt_mod:
            try:
                mod = __import__(o_mod, fromlist=[o_cls])
                cls = getattr(mod, o_cls)
                ret_cls.append(cls)
            except:
                logger.warn("No such class '{0}': using default class '{1}'".format(".".join([o_mod, o_cls]),
                                                                                    ",".join(default_cls)))
                ret_cls = default_cls

        return ret_cls

    # Helper functions to calculate target and source file names. Move
    # to generic function outside class?
    def _make_source_file_name(self):
        """Construct source file name from a target.

        Change target_suffix to source_suffix. Remove label from a
        target file name. A target given to a task must have its file
        name modified for the requirement. This function should
        therefore be called in the requires function. Make sure only
        to replace the last label.

        :return: string
        """
        source = self.target
        if isinstance(self.target_suffix, tuple):
            if self.target_suffix[0] and not self.source_suffix is None:
                source = rreplace(source, self.target_suffix[0], self.source_suffix, 1)
        else:
            if self.target_suffix and not self.source_suffix is None:
                source = rreplace(source, self.target_suffix, self.source_suffix, 1)
        if not self.label:
            return source
        if source.count(self.label) > 1:
            logger.warn("label '{}' found multiple times in target '{}'; this could be intentional".format(self.label, source))
        elif source.count(self.label) == 0:
            logger.warn("label '{}' not found in target '{}'; are you sure your target is correctly formatted?".format(self.label, source))
        return rreplace(source, self.label, "", 1)
            
class JobTask(BaseJobTask):
    def job_runner(self):
        return DefaultShellJobRunner()

    def args(self):
        return []

class InputJobTask(JobTask):
    """Input job task. Should have as a parent task one of the tasks
    in ratatosk.lib.files.external"""
    def requires(self):
        cls = self.set_parent_task()
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

class GenericWrapper(JobWrapperTask):
    """Generic task wrapper"""
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

class PipelineTask(JobWrapperTask):
    """Wrapper task for predefined pipelines. Adds option
    target_generator_function which must be defined in order to
    collect targets.
    """
    target_generator_function = luigi.Parameter(default=None)

    def set_target_generator_function(self):
        """Try to import a module task class represented as string in
        target_generator_function and use it as such.

        """
        opt_mod = ".".join(self.target_generator_function.split(".")[0:-1])
        opt_fn = self.target_generator_function.split(".")[-1]
        if not self.target_generator_function:
            return None
        try:
            m = __import__(opt_mod, fromlist=[opt_fn])
            fn = getattr(sys.modules[m.__name__], opt_fn)
            return fn
        except:
            logger.warn("No function '{}' found: need to define a generator function for task '{}'".format(".".join([opt_mod, opt_fn]), 
                                                                                                           self.__class__))
            return None
    
    
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
    """Generate a name prefix based on available labels for task
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


