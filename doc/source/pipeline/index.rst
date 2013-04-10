Predefined workflows
====================

The user can modify execution order of tasks by customising the
`parent_task` attribute. However, some workflows should be immutable,
thereby representing "standard" or "best-practice" pipelines. This is
currently achieved by treating some tasks differently. For instance,
when the task `HaloPlex` is called, the following code is executed in
`ratatosk_run.py`:

.. code-block:: python

        if task in config_dict.keys():
            opt_dict['--config-file'] = config_dict[task]['config']
            task_cls = config_dict[task]['cls']

where `config_dict[task]['config']` points to predefined config files
located in the `ratatosk/config` folder. Best practice pipeline
classes are currently located in :ref:`ratatosk.pipeline`. Thereafter,
it calls the main luigi run function

.. code-block:: python	
 
   if task_cls:
        task_args = dict_to_opt(opt_dict)
        luigi.run(task_args, main_task_cls=task_cls)

For a pipeline to run, the final targets have to be calculated. This
is currently done by providing a handler function in the configuration
that the pipeline will registor in `backend.__handlers__`. For
instance, the corresponding configuration section in the example
configuration file `J.Doe_00_01.yaml` is

.. code-block:: text

   settings:
     target_generator_handler: test.site_functions.target_generator


In contrast to `parent_task`, there is no default function to fall
back on, so not providing this function will result in an error.

Incidentally, this demonstrates the boilerplate code needed to add a
new predefined pipeline. In `ratatosk.pipeline.__init__.py`, add

.. code-block:: python

   config_dict{
       'bestpractice' : {'config' : os.path.join(ratatosk.__path__[0], os.pardir, "config", "bestpractice.yaml"),
                         'cls' : pipeline.BestPractice},
	   ...
	   }

	
and in `ratatosk.pipeline.bestpractice`

.. code-block:: python

   class BestPractice(PipelineTask):
	   ...

	   def requires(self):
		   self.targets = [tgt for tgt in self.target_iterator()]
		   targets = ["...".format(x[2], self.final_target_suffix) for x in target_list]
		   return [FinalTarget(target=tgt) for tgt in target_list, ...]
			
This feature is likely to change soon. Among other things, it would be
nice to dynamically generate target names based on task labels.

If a pipeline config has been loaded, but the user nevertheless wants
to change program options, the `--custom-config` flag can be used.
Note then that updating `parent_task` is disabled so that program
execution order cannot be changed - after all, it is a fixed pipeline.
This allows for project-specific configuration files that contain
metadata information about the project itself, as well as allowing for
configurations of analysis options.

Pipelines
=========

.. toctree::
   :maxdepth: 20

   align
   seqcap
   haloplex
   
