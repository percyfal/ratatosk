import os
import luigi
import logging
import random
from luigi.task import flatten
from cement.utils import shell

logger = logging.getLogger('pm-luigi')
logger.setLevel(logging.DEBUG)
streamHandler = logging.StreamHandler()
streamHandler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(levelname)s: %(message)s')
streamHandler.setFormatter(formatter)

logger.addHandler(streamHandler)



class Task(luigi.Task):

    def complete(self):
        """
        If the task has any outputs, return true if all outputs exists.
        Otherwise, return whether or not the task has run or not
        """
        outputs = flatten(self.output())
        inputs = flatten(self.input())
        if len(outputs) == 0:
            # TODO: unclear if tasks without outputs should always run or never run
            warnings.warn("Task %r without outputs has no custom complete() method" % self)
            return False

        for output in outputs:
            if not output.exists():
                return False
            if any([os.stat(x.fn).st_mtime > os.stat(output.fn).st_mtime for x in inputs if x.exists()]):
                return False
        else:
            return True

    def run_job(self, args):
        """
        Run job in shell.

        :param args: array of args to pass to the job
        """
        # TODO: look at hadoop_jar.py
        arglist = [self.exe(), self.main()]
        (tmp_files, job_args) = Task._fix_paths(args)
        arglist += job_args

        logger.info(' '.join(arglist))
        (stdout, stderr, returncode) = shell.exec_cmd(' '.join(arglist), shell=True)

        if returncode == 0:
            logger.info("Process completed")
            for a, b in tmp_files:
                logger.info("renaming {0} to {1}".format(a.path, b.path))
                a.move(b.path)

    @staticmethod
    def _fix_paths(args):
        """Modelled after hadoop_jar.HadoopJarJobRunner._fix_paths.
        
        TODO: write a JobTask-like class like hadoop_jar.HadoopJarJobTask
        """
        tmp_files = []
        out_args = []
        for x in args:
            if isinstance(x, luigi.LocalTarget): # input/output
                if x.exists(): # input
                    out_args.append(x.path)
                else: # output
                    y = luigi.LocalTarget(x.path + \
                                              '-luigi-tmp-%09d' % random.randrange(0, 1e10))
                    tmp_files.append((y, x))
                    logger.info("Using temp path: {0} for path {1}".format(y.path, x.path))
                    out_args.append(y.path)
            else:
                out_args.append(str(x))
        return (tmp_files, out_args)
