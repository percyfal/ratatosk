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
import datetime
import subprocess
import logging
import warnings
import luigi
from itertools import izip
from luigi.task import flatten
from cement.utils import shell
import ratatosk
from ratatosk import interface
from ratatosk.utils import rreplace

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
                    y = luigi.LocalTarget(x.path + \
                                              '-luigi-tmp-%09d' % random.randrange(0, 1e10))
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

    def run_job(self, job):
        if not job.exe():# or not os.path.exists(job.exe()):
            logger.error("Can't find executable: {0}, full path {1}".format(job.exe(),
                                                                            os.path.abspath(job.exe())))
            raise Exception("executable does not exist")
        arglist = [job.exe()]
        if job.main():
            arglist.append(self._get_main(job))
        if job.opts():
            arglist.append(job.opts())
        (tmp_files, job_args) = DefaultShellJobRunner._fix_paths(job)
        
        arglist += job_args
        cmd = ' '.join(arglist)
        logger.info(cmd)
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
                
class BaseJobTask(luigi.Task):
    config_file = luigi.Parameter(is_global=True, default=os.path.join(os.path.join(ratatosk.__path__[0], os.pardir, "config", "ratatosk.yaml")), description="Main configuration file.")
    custom_config = luigi.Parameter(is_global=True, default=None, description="Custom configuration file for tuning options in predefined pipelines in which workflow may not be altered.")
    dry_run = luigi.Parameter(default=False, is_global=True, is_boolean=True)
    restart = luigi.Parameter(default=False, is_global=True, is_boolean=True, description="Restart pipeline from scratch.")
    options = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default=None)
    num_threads = luigi.Parameter(default=1)
    # Note: output should generate one file only; in special cases we
    # need to do hacks
    target = luigi.Parameter(default=None)
    target_suffix = luigi.Parameter(default=None)
    source_suffix = luigi.Parameter(default=None)
    source = None
    # Needed for merging samples; list of tuples (sample, sample_run)
    # sample_runs = luigi.Parameter(default=[], is_global=True)
    sample_runs = []
    _config_section = None
    _config_subsection = None
    task_id = None
    n_reduce_tasks = 8
    can_multi_thread = False
    max_memory_gb = 3
    # Not all tasks are allowed to "label" their output
    label = luigi.Parameter(default=None)
    suffix = luigi.Parameter(default=None)

    def __init__(self, *args, **kwargs):
        params = self.get_params()
        param_values = self.get_param_values(params, args, kwargs)
        # Main configuration file
        for key, value in param_values:
            if key == "config_file":
                config_file = value
                kwargs = self._update_config(config_file, *args, **kwargs)
        # If a pipeline config has been loaded, but the user
        # nevertheless wants to change program options, the
        # --custom-config flag can be used. Note that updating
        # 'parent_task' then is disabled so that program execution
        # order cannot be changed.
        for key, value in param_values:
            if key == "custom_config":
                custom_config = value
                kwargs = self._update_config(custom_config, disable_parent_task_update=True, *args, **kwargs)
        super(BaseJobTask, self).__init__(*args, **kwargs)
        if self.dry_run:
            print "DRY RUN: " + str(self)

    def __repr__(self):
        return self.task_id

    def _update_config(self, config_file, disable_parent_task_update=False, *args, **kwargs):
        """Update configuration for this task"""
        config = interface.get_config(config_file)
        if not config:
            return kwargs
        if not config.has_section(self._config_section):
            return kwargs
        params = self.get_params()
        param_values = {x[0]:x[1] for x in self.get_param_values(params, args, kwargs)}
        for key, value in self.get_params():
            new_value = None
            # Got a command line option => override config file
            if value.default != param_values.get(key, None):
                new_value = param_values.get(key, None)
                logger.debug("option '{0}'; got value '{1}' from command line, overriding configuration file setting for task class '{2}'".format(key, new_value, self.__class__))
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
            logger.debug("Using default value for '{0}' for task class '{1}'".format(key, self.__class__))
        return kwargs

    def exe(self):
        """Executable"""
        return None

    def main(self):
        """For commands that have subcommands"""
        return None

    def opts(self):
        """Generic options placeholder"""
        return self.options

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
        """Setup local settings (e.g. read from config file?)

        """
        pass

    def run(self):
        self.init_local()
        if not self.dry_run:
            self.job_runner().run_job(self)

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
            if any([os.stat(x.fn).st_mtime > os.stat(output.fn).st_mtime for x in inputs if x.exists()]):
                return False
        else:
            return True

    def get_param_default(self, k):
        """Get the default value for a param."""
        params = self.get_params()
        for key, value in params:
            if k == key:
                return value.default
        return None

    def set_parent_task(self):
        """Try to import a module task class represented as string in
        parent_task and use it as such
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
            logger.warn("No class {} found: using default class {}".format(".".join([opt_mod, opt_cls]), 
                                                                           ".".join([default_mod, default_cls])))
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

    # Helper functions to calculate target and source file names
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
            if self.target_suffix[0] and self.source_suffix:
                source = rreplace(source, self.target_suffix[0], self.source_suffix, 1)
        else:
            if self.target_suffix and self.source_suffix:
                source = rreplace(source, self.target_suffix, self.source_suffix, 1)
        if not self.label:
            return source
        if source.count(self.label) > 1:
            logger.warn("label '{}' found multiple times in target '{}'; this could be intentional".format(self.label, source))
        elif source.count(self.label) == 0:
            logger.warn("label '{}' not found in target '{}'; are you sure your target is correctly formatted?".format(self.label, source))
        return rreplace(source, self.label, "", 1)


    # these functions are needed to add label information to the
    # self.input(), which is generated by the self.requires()
    # function. In many cases, this should be identical to 
    # self.targets, but here return luigi.LocalTarget's
    def add_label_to_input(self):
        """Add label to input which has been generated from the
        requires function. 
        """
        if isinstance(self.input(), list):
            if not self.label:
                return [luigi.LocalTarget(x.fn) for x in self.input()]
            else:
                return [luigi.LocalTarget(os.path.splitext(x.fn)[0] + self.label + os.path.splitext(x.fn)[1]) for x in self.input()]
        else:
            if not self.label:
                return luigi.LocalTarget(self.input().fn)
            else:
                return luigi.LocalTarget(os.path.splitext(self.input().fn)[0] + self.label + os.path.splitext(self.input().fn))[1]
            
    # Make a general utility function
    def replace_suffix(self, new_suffix=None):
        """Replace suffix of input with new suffix. Old suffix is
        calculated by os.path.splitext, so here we depend on file.txt
        type file names.

        """
        if isinstance(self.input(), list):
            if not new_suffix:
                return [luigi.LocalTarget(x.fn) for x in self.input()]
            else:
                return [luigi.LocalTarget(os.path.splitext(x.fn)[0] + new_suffix) for x in self.input()]
        else:
            if not new_suffix:
                return luigi.LocalTarget(self.input().fn)
            else:
                return luigi.LocalTarget(os.path.splitext(x.fn) + new_suffix)

                        
class JobTask(BaseJobTask):
    def job_runner(self):
        return DefaultShellJobRunner()

    def args(self):
        return []

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

