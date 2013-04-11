Helper scripts
===============

.. _ratatosk_run:

ratatosk_run.py
---------------

:program:`ratatosk_run.py` is a helper script that is shipped with the ratatosk
distribution. It collects the library wrapper tasks, thus serving as
an interface to small tasks, as well as pipeline tasks.

Running ``ratatosk_run.py -h`` shows the main options, as well as
the available tasks (abbreviated output for clarity):

.. code-block:: text

   usage: ratatosk_run.py [-h] [--config-file CONFIG_FILE] [--dry-run] [--lock]
			  [--workers WORKERS] [--lock-pid-dir LOCK_PID_DIR]
			  [--scheduler-host SCHEDULER_HOST]
			  [--restart-from RESTART_FROM]
			  [--custom-config CUSTOM_CONFIG]
			  [--target-generator-file TARGET_GENERATOR_FILE]
			  [--use-long-names] [--local-scheduler] [--restart]

                       {HsMetricsNonDup,Bampe,Bgzip,PicardMetricsNonDup,CombineFilteredVariants,WrapperTask...}

   optional arguments:
     -h, --help            show this help message and exit
     --config-file CONFIG_FILE
			   config_file Main configuration file. [default: /Users/
			   peru/opt/ratatosk/ratatosk/../config/ratatosk.yaml]
     --dry-run             dry_run Generate pipeline graph/flow without running
			   any commands [default: False]
     --lock                lock Do not run if the task is already running
			   [default: False]
     --workers WORKERS     workers Maximum number of parallel tasks to run
			   [default: 1]
     --lock-pid-dir LOCK_PID_DIR
			   lock_pid_dir Directory to store the pid file [default:
			   /var/tmp/luigi]
     --scheduler-host SCHEDULER_HOST
			   scheduler_host Hostname of machine running remote
			   scheduler [default: localhost]
     --restart-from RESTART_FROM
			   restart_from NOT YET IMPLEMENTED: Restart pipeline
			   from a given task. [default: None]
     --custom-config CUSTOM_CONFIG
			   custom_config Custom configuration file for tuning
			   options in predefined pipelines in which workflow may
			   not be altered. [default: None]
     --target-generator-file TARGET_GENERATOR_FILE
			   target_generator_file Target generator file name
			   [default: None]
     --use-long-names      use_long_names Use long names (including all options)
			   in graph vizualization [default: False]
     --local-scheduler     local_scheduler Use local scheduling [default: False]
     --restart             restart Restart pipeline from scratch. [default:
			   False]


To run a specific task, you use one of the positional arguments. In
this way, it works much like a Makefile. A make command resolves
dependencies based on the desired *target* file name, so you would do
``make target`` to generate ``target``. With :mod:`ratatosk`, the
target is passed via the ``--target`` option. For instance, to run Bampe
you would do:

.. code-block:: text

   usage: ratatosk_run.py Bampe [-h] [--options OPTIONS]
				[--num-threads NUM_THREADS] [--pipe]
				[--target TARGET] [--label LABEL]
				[--diff-label DIFF_LABEL] [--exe-path EXE_PATH]
				[--tasks TASKS] [--add-label ADD_LABEL]
				[--parent-task PARENT_TASK] [--suffix SUFFIX]
				[--read-group READ_GROUP] [--platform PLATFORM]
				[--config-file CONFIG_FILE] [--dry-run] [--lock]
				[--workers WORKERS] [--lock-pid-dir LOCK_PID_DIR]
				[--scheduler-host SCHEDULER_HOST]
				[--restart-from RESTART_FROM]
				[--custom-config CUSTOM_CONFIG]
				[--target-generator-file TARGET_GENERATOR_FILE]
				[--use-long-names] [--local-scheduler]
				[--restart]

   optional arguments:
     -h, --help            show this help message and exit
     --options OPTIONS     Bampe.options Program options [default: ()]
     --num-threads NUM_THREADS
			   Bampe.num_threads Number of threads to run. Set to 1
			   if task.can_multi_thread is false [default: 1]
     --pipe                Bampe.pipe Piped input/output. In practice refrains
			   from including input/output file names in command
			   list. [default: False]
     --target TARGET       Bampe.target Output target name [default: None]
     --label LABEL         Bampe.label [default: None]
     --diff-label DIFF_LABEL
			   Bampe.diff_label [default: None]
     --exe-path EXE_PATH   Bampe.exe_path [default: None]
     --tasks TASKS         Bampe.tasks [default: []]
     --add-label ADD_LABEL
			   Bampe.add_label [default: ('_R1_001', '_R2_001')]
     --parent-task PARENT_TASK
			   Bampe.parent_task [default:
			   ('ratatosk.lib.align.bwa.Aln',
			   'ratatosk.lib.align.bwa.Aln')]
     --suffix SUFFIX       Bampe.suffix [default: .bam]
     --read-group READ_GROUP
			   Bampe.read_group [default: None]
     --platform PLATFORM   Bampe.platform [default: Illumina]
     --config-file CONFIG_FILE
			   config_file Main configuration file. [default: /Users/
			   peru/opt/ratatosk/ratatosk/../config/ratatosk.yaml]
     --dry-run             dry_run Generate pipeline graph/flow without running
			   any commands [default: False]
     --lock                lock Do not run if the task is already running
			   [default: False]
     --workers WORKERS     workers Maximum number of parallel tasks to run
			   [default: 1]
     --lock-pid-dir LOCK_PID_DIR
			   lock_pid_dir Directory to store the pid file [default:
			   /var/tmp/luigi]
     --scheduler-host SCHEDULER_HOST
			   scheduler_host Hostname of machine running remote
			   scheduler [default: localhost]
     --restart-from RESTART_FROM
			   restart_from NOT YET IMPLEMENTED: Restart pipeline
			   from a given task. [default: None]
     --custom-config CUSTOM_CONFIG
			   custom_config Custom configuration file for tuning
			   options in predefined pipelines in which workflow may
			   not be altered. [default: None]
     --target-generator-file TARGET_GENERATOR_FILE
			   target_generator_file Target generator file name
			   [default: None]
     --use-long-names      use_long_names Use long names (including all options)
			   in graph vizualization [default: False]
     --local-scheduler     local_scheduler Use local scheduling [default: False]
     --restart             restart Restart pipeline from scratch. [default:
			   False]

Options specific to :py:class:`.Bampe` are prefixed with 'Bampe'. To
actually run the task, provide the target name *target.bam* and run

.. code-block:: text

	ratatosk_run.py Bampe \
	  --target target.bam
	  --config-file config/ratatosk.yaml
	  
Here I've used a 'global' config file (`ratatosk.yaml
<https://github.com/percyfal/ratatosk/blob/master/config/ratatosk.yaml>`_).
You actually don't need to pass it as in the example above as it's
loaded by default. The source file names will be generated internally
and if the source files exist, the task will run.

