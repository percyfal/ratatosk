## ratatosk ##

`ratatosk` is a library of [luigi](https://github.com/spotify/luigi)
tasks, currently focused on, but not limited to, common
bioinformatical tasks. 

## Installing  ##

### Pre-requisites ###

It is recommended that you first create a virtual environment in which
to install the packages. Install
[virtualenvwrapper](http://virtualenvwrapper.readthedocs.org/en/latest/)
and use
[mkvirtualenv](http://virtualenvwrapper.readthedocs.org/en/latest/command_ref.html)
to create a virtual environment.

### Installation ###

To install the development version of `ratatosk`, do
	
	git clone https://github.com/percyfal/ratatosk
	python setup.py develop

### Dependencies ###

To begin with, you may need to install
[Tornado](http://www.tornadoweb.org/) and
[Pygraphviz](http://networkx.lanl.gov/pygraphviz/) (see
[Luigi](https://github.com/spotify/luigi/blob/master/README.md) for
further information).

The tests depend on the following software to run:

1. [bwa](http://bio-bwa.sourceforge.net/)
2. [samtools](http://samtools.sourceforge.net/)
3. [GATK](http://www.broadinstitute.org/gatk/) - set an environment
   variable `GATK_HOME` to point to your installation path
4. [picard](http://picard.sourceforge.net/) - set an environment
   variable `PICARD_HOME` to point to your installation path
5. [cutadapt](http://code.google.com/p/cutadapt/) - install with `pip
   install cutadapt`
6. [fastqc](http://www.bioinformatics.babraham.ac.uk/projects/fastqc/)

You also need to install the test data set:

	git clone https://github.com/percyfal/ngs.test.data
	python setup.py develop
	
Note that you **must** use *setup.py develop*.

## Running the tests  ##

Cd to the luigi test directory (`tests`) and run

	nosetests -v -s test_wrapper.py
	
To run a given task (e.g. TestLuigiWrappers.test_fastqln), do

	nosetests -v -s test_wrapper.py:TestLuigiWrappers.test_fastqln

### Task visualization and tabulation ###

By default, the tests use a local scheduler, implemented in luigi. For
production purposes, there is also a
[central planner](https://github.com/spotify/luigi/blob/master/README.md#using-the-central-planner).
Among other things, it allows for visualization of the task flow by
using [Tornado](http://www.tornadoweb.org/) and
[Pygraphviz](http://networkx.lanl.gov/pygraphviz/). Results are
displayed in *http://localhost:8081*, results "collected" at
*http://localhost:8082/api/graph*. 

In addition, I have extended the luigi daemon and server code to
generate a table representation of the tasks (in
*http://localhost:8083*). The aim here would be to define a grouping
function that groups task lists according to a given feature (e.g.
sample, project).

In order to view tasks, run

	bin/ratatoskd &
	
in the background, set the PYTHONPATH to the current directory and run
the tests:

	PYTHONPATH=. nosetests -v -s test_wrapper.py
	
## Examples in tests ##

NOTE: these are still not real unit tests in that they in some cases
are inter-dependent. See issues.

These examples are currently based on the tests in
[ratatosk.tests.test_wrapper](https://github.com/percyfal/ratatosk/blob/master/test/test_wrapper.py).

### Creating file links ###

The task
[ratatosk.lib.files.fastq.FastqFileLink](https://github.com/percyfal/ratatosk/blob/master/ratatosk/lib/files/fastq.py)
creates a link from source to a target. The source in this case
depends on an *external* task
([ratatosk.lib.files.external.FastqFile](https://github.com/percyfal/ratatosk/blob/master/ratatosk/lib/files/external.py)
meaning this file was created by some outside process (e.g. sequencing
machine).

	nosetests -v -s test_wrapper.py:TestMiscWrappers.test_fastqln

![FastqLn](https://raw.github.com/percyfal/ratatosk/master/doc/test_fastqln.png)

A couple of comments are warranted. First, the boxes shows tasks,
where the `FastqFile` is an external task. The file it points to must
exist for the task `FastqFileLink` executes. The color of the box
indicates status; here, green means the task has completed
successfully. Second, every task has its own set of options that can
be passed via the command line or in the code. In the `FastqFileLink`
task box we can see the options that were passed to the task. For
instance, the option `use_long_names=True` prints complete task names,
as shown above. 
	
### Alignment with bwa sampe ###

Here's a more useful example; paired-end alignment using `bwa`.

	nosetests -v -s test_wrapper.py:TestBwaWrappers.test_bwasampe

![BwaSampe](https://raw.github.com/percyfal/ratatosk/master/doc/test_bwasampe.png)
	
### Wrapping up metrics tasks ###

The class
[ratatosk.lib.tools.picard.PicardMetrics](https://github.com/percyfal/ratatosk/blob/master/ratatosk/lib/tools/picard.py)
subclasses
[ratatosk.job.JobWrapperTask](https://github.com/percyfal/ratatosk/blob/master/ratatosk/job.py)
that can be used to require that several tasks have completed. Here
I've used it to group picard metrics tasks:

	nosetests -v -s test_wrapper.py:TestPicardWrappers.test_picard_metrics

![PicardMetrics](https://raw.github.com/percyfal/ratatosk/master/doc/test_picard_metrics.png)


Here, I've set the option `--use-long-names` to `False`, which changes
the output to show only the class names for each task. This example
utilizes a configuration file that links tasks together. More about
that in the next example.

### Working with parent tasks and configuration files ###

All tasks have a default requirement, which I call `parent_task`. In
the current implementation, all tasks subclass `ratatosk.job.JobTask`,
which provides a `parent_task` class variable. This variable can be
changed, either at the command line (option `--parent-task`) or in a
configuration file. The `parent_task` variable is a string
representing a class in a python module, and could therefore be any
python code of choice. In addition to the `parent_task` variable,
`JobTask` provides variables `_config_section` and
`_config_subsection` that point to sections and subsections in the
config file, which should be in yaml format (see
[google app](https://developers.google.com/appengine/docs/python/config/appconfig)
for nicely structured config files). By default, all `metrics`
functions have as parent class
`ratatosk.lib.tools.picard.InputBamFile`. This can easily be modified
in the config file to:

    picard:
      InputBamFile:
        parent_task: ratatosk.lib.tools.samtools.SamToBam
      HsMetrics:
        parent_task: ratatosk.lib.tools.picard.SortSam
        targets: targets.interval_list
        baits: targets.interval_list
      DuplicationMetrics:
        parent_task: ratatosk.lib.tools.picard.SortSam
      AlignmentMetrics:
        parent_task: ratatosk.lib.tools.picard.SortSam
      InsertMetrics:
        parent_task: ratatosk.lib.tools.picard.SortSam
    
    samtools:
      SamToBam:
        parent_task: ratatosk.lib.align.BwaSampe


Note also that `InputBamFile` has been changed to depend on
`ratatosk.lib.tools.samtools.SamToBam` (default value is
`ratatosk.lib.files.external.BamFile`). 

## Examples with *run_ratatosk.py* ##

The installation procedure will install an executable script,
`run_ratatosk.py`, in your search path. The script collects all tasks
currently available in the ratatosk modules:

    usage: run_ratatosk.py [-h] [--config-file CONFIG_FILE] [--dry-run] [--lock]
                           [--workers WORKERS] [--lock-pid-dir LOCK_PID_DIR]
                           [--scheduler-host SCHEDULER_HOST]
                           [--restart-from RESTART_FROM]
                           [--custom-config CUSTOM_CONFIG] [--print-config]
                           [--use-long-names] [--local-scheduler] [--restart]
                           
                           {RawRealignerTargetCreator,IndexBam,BwaAlnWrapperTask,VariantEval,SamtoolsJobTask,PrintReads,ClipReads,DuplicationMetrics,RawIndelRealigner,HaloBwaSampe,BaseRecalibrator,RealignerTargetCreator,InputSamFile,WrapperTask,PicardJobTask,InputVcfFile,GATKJobTask,InsertMetrics,PicardMetrics,InputFastqFile,EnvironmentParamsContainer,RawUnifiedGenotyper,SortSam,SortBam,AlignmentMetrics,Task,HaloPlex,UnifiedGenotyper,BwaJobTask,VariantFiltration,BwaAln,HsMetrics,SamToBam,JobTask,BwaSampe,MergeSamFiles,PrintConfig,InputBamFile,BaseJobTask,IndelRealigner,SampeToSamtools}
                           ...


To run a specific task, you use one of the positional arguments. In
this way, it works much like a Makefile. A make command resolves
dependencies based on the desired *target* file name, so you would do
`make target` to generate `target`. With `ratatosk`, the target is
passed via the `--target` option. For instance, to run BwaSampe you
would do:

	run_ratatosk.py BwaSampe \
	  --target target.bam
	  --config-file config/ratatosk.yaml
	  
Here I've used a 'global' config file
[ratatosk.yaml](https://github.com/percyfal/ratatosk/blob/master/config/ratatosk.yaml).
You actually don't need to pass it as in the example above as it's
loaded by default.

### Dry run ###

The `--dry-run` option will resolve dependencies but not actually run
anything. In addition, it will print the tasks that will be called.
By passing a target
	
	run_ratatosk.py RawIndelRealigner 
	  --target P001_101_index3/P001_101_index3_TGACCA_L001.trimmed.sync.sort.merge.realign.bam
	  --custom-config ~/opt/ratatosk/examples/J.Doe_00_01.yaml --dry-run

we get the dependencies as specified in the config file:

![DryRun](https://raw.github.com/percyfal/ratatosk/master/doc/ratatosk_dry_run.png)

The task `RawIndelRealigner` is defined in
`ratatosk.pipeline.haloplex` and is a modified version of
`ratatosk.lib.tools.gatk.IndelRealigner`. It is used for analysis of
HaloPlex data (next section).

### HaloPlex calling pipeline  ###

Here's an example of a variant calling pipeline defined for analysis of HaloPlex data:

	run_ratatosk.py HaloPlex --project J.Doe_00_01 
	  --projectdir ~/opt/ngs_test_data/data/projects/ 
	  --workers 4 --custom-config ~/opt/ratatosk/config/J.Doe_00_01.yaml
	
resulting in 

![HaloPlex](https://raw.github.com/percyfal/ratatosk/develop/doc/ratatosk_pipeline_haloplex.png)

Blue boxes mean active processes (the command was run with `--workers
4`, which uses 4 parallel processes). Note that we need to know what
labels are applied to the file name (see issues). In this iteration,
for the predefined pipelines the file names have been hardcoded. 

## Best practice pipelines ##

The user can modify execution order of tasks by customising the
`parent_task` attribute. However, some workflows should be immutable,
thereby representing "standard" or "best-practice" pipelines. This is
currently achieved by treating some tasks differently. For instance,
when the task `HaloPlex` is called, the following code is executed in
`run_ratatosk.py`:

```python
if task == "HaloPlex":
    args = sys.argv[2:] + ['--config-file', config_dict['haloplex']]
    luigi.run(args, main_task_cls=ratatosk.pipeline.haloplex.HaloPlex)
```

where `config_dict['haloplex']` points to predefined config files
located in the `ratatosk/config` folder. Best practice pipeline
classes are currently located in `ratatosk.pipeline`. For a pipeline
to run, the final targets have to be calculated. This is currently
done by providing a function in the configuration that the pipeline
will load in the `set_target_generator_function`. For instance, the
corresponding configuration section in the example configuration file
`J.Doe_00_01.yaml` is

```
pipeline:
  HaloPlex:
    target_generator_function: test.site_functions.target_generator
```

In contrast to `parent_task`, there is no default function to fall
back on, so not providing this function will result in an error.

Incidentally, this demonstrates the boilerplate code needed to add a
new predefined pipeline. In `ratatosk.pipeline.__init__.py`, add

```python
config_dict{
	'bestpractice' : os.path.join(ratatosk.__path__[0], os.pardir, "config", "bestpractice.yaml"),
	...
	}
```
	
and in `ratatosk.pipeline.bestpractice`

```python
class BestPractice(PipelineTask):
	...
		
	def requires(self):
		tgt_fun = self.set_target_generator_function()
		# Need to pass the class to tgt_fun
		target_list = tgt_fun(self)
		target_list = ["...".format(x, self.final_target_suffix) for x in target_list]
		return [FinalTarget(target=tgt) for tgt in target_list, ...]
			
```

This feature is likely to change soon. Among other things, it would be
nice to dynamically generate target names based on task labels.

If a pipeline config has been loaded, but the user nevertheless wants
to change program options, the `--custom-config` flag can be used.
Note then that updating `parent_task` is disabled so that program
execution order cannot be changed - after all, it is a fixed pipeline.
This allows for project-specific configuration files that contain
metadata information about the project itself, as well as allowing for
configurations of analysis options.

## Implementation ##

The implementation is still under heavy development and testing so
expect many changes in near future. 

### Basic job task ###

`ratatosk.job` defines, among other things, a *default shell job
runner*, which is a wrapper for running tasks in shell, and a *base
job task* that subclasses `luigi.Task`. The base job task implements a
couple of functions that are essential for general behaviour:

* `_update_config` that reads the configuration file and overrides
  default settings. It is run from `__init__`, meaning that it is read
  for *every task* (see issues)
  
* `set_parent_task` that sets the parent task for a task. The function
  parses a string (`module.class`) and tries to load `class` from
  `module`, falling back to the default parent task on failure. Here
  it would be nice to implement validation of the parent task in some
  way (via interface classes?)
  
* `set_parent_task_list` that sets a parent task list. This is
  currently not used, and I'm not sure if this is the right way to go;
  the motivation stems from the fact that if a task is to be run on
  several targets (e.g. UnifiedGenotyper on sample.bam,
  sample.clip.bam) the task would only depend on the first file. EDIT:
  or would it? This is probably more related to giving the task the
  correct file name, so this configuration option should probably
  provide a list of 2-tuples of from,to string substitutions.
  
* `set_target_generator_function` tries to set a function that is used
  to generate target names for a task. It is up to the end user to
  define an appropriate function. 
  
* `_make_source_file_name` generates source file name from a target,
  based on `target_suffix`, `source_suffix`, and `label`.

### Program modules ###

`ratatosk` submodules are named after the application/program to be
run (e.g. `ratatosk.lib.align.bwa` for `bwa`). For consistency, the modules
shoud contain

1. a **job runner** that subclasses
   `ratatosk.job.DefaultShellJobRunner`. The runner specifies how the
   program is run
   
2. **input** file task(s) that subclass `ratatosk.job.JobTask` and
   that depend on external tasks in `ratatosk.lib.files.external`. The idea is
   that all acceptable file formats be defined as external inputs, and
   that parent tasks therefore must use one/any of these inputs
   
3. a **main job task** that subclasses `ratatosk.job.JobTask` and has
   as default parent task one of the inputs (previous point). The
   `_config_section` should be set to the module name (e.g. `bwa` for
   `ratatosk.lib.align.bwa`). It should also return the *job runner*
   defined in 1.
   
4. **tasks** that subclass the *main job task*. The
   `_config_subsection` should represent the task name in some way
   (e.g. `aln` for `bwa aln`command)
   
5. possibly **wrapper tasks** that group common tasks in a module

### Configuration parser ###

Python's standard configuration parser works on `.ini` files allowing
section levels followed by customizations. It would be nice with at
least sections/subsections (python's `ConfigObj` does this), but since
I prefer yaml files, I have implemented a config parser that enforces
section and subsections, treating everything below that level as
lists/dicts/variables.

## HOWTO: Adding task wrappers ##

In essence, `ratatosk` is a library of program wrappers. There are
already a couple of wrappers available, but many more could easily be
added. Here is a short HOWTO on how to add a wrapper module
`myprogram`.

### 1. Create the file ###

Create the file `myprogram.py` (doh!), with at least the following imports:

```python
import os
import luigi
from ratatosk.job import JobTask, DefaultShellJobRunner
from ratatosk.utils import rreplace
```

### 2. Add job runners ###

At the very least, there should exist the following:

```python
class MyProgramJobRunner(DefaultShellJobRunner):
    pass
```

This is in part for consistency, in part in case the `myprogram`
program group needs special handling of command construction (see e.g.
`ratatosk.lib.tools.gatk`).

### 3. Add default inputs ###

There should be at least one input class that subclasses one of the
`ratatosk.lib.files.external` classes. Mainly here for naming consistency.

```python
class InputFastqFile(JobTask):
    _config_section = "myprogram"
    _config_subsection = "InputFastqFile"
    target = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="ratatosk.lib.files.external.FastqFile")
    
    def requires(self):
        cls = self.set_parent_task()
        return cls(target=self.target)
    def output(self):
        return luigi.LocalTarget(self.target)
    def run(self):
        pass
```

### 4. Add wrapper tasks ###

Once steps 1-3 are done, tasks can be added. If the program has
subprograms (e.g. `bwa aln`), it is advisable to create a generic
'top' job task. In any case, a task should at least consist of the
following:

```python
class MyProgram(JobTask):
	# Corresponding section and subsection in config file
	_config_section = "myprogram"
    _config_subsection = "myprogram_subsection"
	# Name of executable. This is a parameter so the user can specify
	# the version
	executable = luigi.Parameter(default="myprogram")
	# program options
    options = luigi.Parameter(default=None)
    parent_task = luigi.Parameter(default="myprogram.InputFastqFile")
	# Target and source suffixes are necessary for generating target
	# names
    target_suffix = luigi.Parameter(default=".sai")
    source_suffix = luigi.Parameter(default=".fastq.gz")
	# Add label if this task should add label to file name (e.g.
	# file.txt -> file.label.txt)
	label = luigi.Parameter(default="label")

	# Must be present
	def job_runner(self):
        return MyProgramJobRunner()

	# Here gather the *required* arguments to 'myprogram'. Often input
	# redirected to output suffices
    def args(self):
        return [self.input(), ">", self.output()]

    # For single requirements, the BaseJobTask often suffices. For
	# more complex requirements, a reimplementation is needed. Idea is
	# to generate the source name of the parent class that was used to
	# generate the target
    #def requires(self):
    #    cls = self.set_parent_task()
    #    source = self._make_source_file_name()
    #    return cls(target=source)
    
    # The following functions are inherited from JobTask and changing
	# their behaviour is often not necessary

    #def exe(self):
    #    """Executable of this task"""
    #    return self.executable

	# Subprogram name, e.g. 'aln' in 'bwa aln'	
    #def main(self):
    #    return "subprogram"

	# Returns the options string. This may need a lot of tampering
	# with, see e.g. 'ratatosk.gatk.VariantEval' (but see also comment
	# in issues)
    #def opts(self):
    #return self.options

	# Output = target
    #def output(self):
    #    return luigi.LocalTarget(self.target)

```

Note that in many cases you only have to reimplement `job_runner` and
`args`.

To actually run the task, you need to import the module in your
script, and `luigi` will automagically add the task `MyProgram` and
its options.

## DEPRECATED: Example scripts  ##

NOTE: most of these are currently broken due to recent changes in
parameter naming, but I've kept them here for now until I've
implemented corresponding examples with *run_ratatosk.py*.

There are a couple of example scripts in
[ratatosk.examples](https://github.com/percyfal/ratatosk/tree/master/examples)
The following examples show how to modify behaviour with configuration
files and custom classes. Note that currently you'll need to modify
the reference paths in the config files manually to point to the
*ngs_test_data* installation. The test data consists of two samples,
one of which (P001_101) has data from two flowcell runs.

The basic configuration setting is 

    bwa:
      bwaref: ../../ngs_test_data/data/genomes/Hsapiens/hg19/bwa/chr11.fa
    
    gatk:
      unifiedgenotyper:
        ref: ../../ngs_test_data/data/genomes/Hsapiens/hg19/seq/chr11.fa
    
    picard:
      input_bam_file:
        parent_task: ratatosk.lib.tools.samtools.SamToBam
      hs_metrics:
        parent_task: ratatosk.lib.tools.picard.SortSam
        targets: ../test/targets.interval_list
        baits: ../test/targets.interval_list
      duplication_metrics:
        parent_task: ratatosk.lib.tools.picard.SortSam
      alignment_metrics:
        parent_task: ratatosk.lib.tools.picard.SortSam
      insert_metrics:
        parent_task: ratatosk.lib.tools.picard.SortSam
    
    samtools:
      samtobam:
        parent_task: ratatosk.lib.tools.samtools.SampeToSamtools


### Basic align seqcap pipeline ###

In examples directory, running

	python pipeline.py  --project J.Doe_00_01 --indir path/to/ngs_test_data/data/projects --config-file align_seqcap.yaml
	
will execute a basic analysis pipeline:

![AlignSeqcap](https://raw.github.com/percyfal/ratatosk/master/doc/example_align_seqcap.png)

### Adding adapter trimming  ###

Changing the following configuration section (see `align_adapter_trim_seqcap.yaml`):

	bwa:
	  aln:
        parent_task: ratatosk.lib.utils.cutadapt.CutadaptJobTask

and running 

	python pipeline.py  --project J.Doe_00_01 --indir /path/to/ngs_test_data/data/projects --config-file align_adapter_trim_seqcap.yaml
	
runs the same pipeline as before, but on adapter-trimmed data.

### Merging samples over several runs ###

Sample *P001_101_index3* has data from two separate runs that should
be merged. The class `ratatosk.lib.tools.picard.MergeSamFiles` merges
sample_run files and places the result in the sample directory. The
implementation currently depends on the directory structure
'sample/fc1', sample/fc2' etc.

	python pipeline_merge.py  --project J.Doe_00_01 --indir /path/to/ngs_test_data/data/projects --config-file align_seqcap_merge.yaml  --sample P001_101_index3

results in 

![AlignSeqcapMerge](https://raw.github.com/percyfal/ratatosk/master/doc/example_align_seqcap_merge.png)

See `align_seqcap_merge.yaml` for relevant changes. Note that in this
implementation the merged files end up directly in the sample
directory (i.e. *P001_101_index3*).

### Extending workflows with subclassed tasks ###

It's dead simple to add tasks of a given type. Say you want to
calculate hybrid selection on bam files that have and haven't been
mark duplicated. By subclassing an existing task and giving the new
class it's own configuration file location, you can configure the new
task to depend on whatever you want. In `pipeline_custom.py` I have
added the following class:

```python
class HsMetricsNonDup(HsMetrics):
	"""Run on non-deduplicated data"""
	_config_subsection = "hs_metrics_non_dup"
	parent_task = luigi.Parameter(default="ratatosk.lib.tools.picard.DuplicationMetrics")
```

The `picard` configuration section in the configuration file
`align_seqcap_custom.yaml` now has a new subsection:

```yaml
hs_metrics_non_dup:
	parent_task:
	targets: ../test/targets.interval_list
	baits: ../test/targets.interval_list
```

Running 

	python pipeline_custom.py  --project J.Doe_00_01 --indir /path/to/ngs_test_data/data/projects --config-file align_seqcap.yaml --sample P001_102_index6
	
will add hybrid selection calculation on non-deduplicated bam file for sample *P001_102_index6*:

![CustomDedup](https://raw.github.com/percyfal/ratatosk/master/doc/example_align_seqcap_custom_dup.png)


## TODO/future ideas/issues ##

See
[issue list](https://github.com/percyfal/ratatosk/issues?state=open)
for a complete list. Some of the most pressing issues to fix include

* Calculation of target names by getting the path between two nodes

* Validation of parent tasks, possibly via Target output types.

* Use pipes whereever possible.

* Integrate with hadoop

* Controlling the number of threads / worker

* SLURM/drmaa integration 
