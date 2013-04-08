## ratatosk ##

`ratatosk` is a library of [luigi](https://github.com/spotify/luigi)
tasks, currently focused on, but not limited to, common
bioinformatical tasks. 


Examples with *ratatosk_run_scilife.py*
---------------------------------------

NB: these examples don't actually do anything except plot the
dependencies. To actually run the pipelines, see the examples in the
extension module
[ratatosk.ext.scilife](https://github.com/percyfal/ratatosk.ext.scilife)

### Dry run ###

The `--dry-run` option will resolve dependencies but not actually run
anything. In addition, it will print the tasks that will be called.
By passing a target

	ratatosk_run.py RawIndelRealigner --target sample.merge.realign.bam 
		--custom-config /path/to/ratatosk/examples/J.Doe_00_01.yaml --dry-run

we get the dependencies as specified in the config file:

![DryRun](https://raw.github.com/percyfal/ratatosk/master/doc/ratatosk_dry_run.png)

The task `RawIndelRealigner` is defined in
`ratatosk.pipeline.haloplex` and is a modified version of
`ratatosk.lib.tools.gatk.IndelRealigner`. It is used for analysis of
HaloPlex data.

### Merging samples over several runs ###

Samples that have data from two separate runs should be merged. The
class `ratatosk.lib.tools.picard.MergeSamFiles` merges sample_run
files and places the result in the sample directory. The
`MergeSamFiles` task needs information on how to find files to merge.
This is currently done by registering a handler via the configuration
option `target_generator_handler`. In the custom configuration file
`J.Doe_00_01.yaml`, we have

    picard:
      MergeSamFiles:
        parent_task: ratatosk.lib.tools.picard.SortSam
        target_generator_handler: test.site_functions.collect_sample_runs

where the function `test.site_functions.collect_sample_runs` is
defined as

```python
def collect_sample_runs(task):
    return ["sample/fc1/sample.sort.bam",
            "sample/fc2/sample.sort.bam"]
```

This can be any python function, with the only requirement that it
return a list of source file names. This task could be run as follows

	ratatosk_run.py MergeSamFiles  --target sample.sort.merge.bam
	  --config-file /path/to/ratatosk/examples/J.Doe_00_01.yaml

resulting in (dry run version shown here)

![AlignSeqcapMerge](https://raw.github.com/percyfal/ratatosk/master/doc/example_align_seqcap_merge.png)

### Adding adapter trimming  ###

Changing the following configuration section (see `J.Doe_00_01_trim.yaml`):

	misc:
	  ResyncMates:
		parent_task: ratatosk.lib.utils.cutadapt.CutadaptJobTask

	bwa:
	  aln:
		parent_task: ratatosk.lib.utils.misc.ResyncMatesJobTask

and running 

	ratatosk_run.py MergeSamFiles  
		--target P001_101_index3/P001_101_index3.trimmed.sync.sort.merge.bam 
		--config-file ~/opt/ratatosk/examples/J.Doe_00_01_trim.yaml

	
runs the same pipeline as before, but on adapter-trimmed data.

![AlignSeqcapMergeTrim](https://raw.github.com/percyfal/ratatosk/master/doc/example_align_seqcap_merge_trim.png)

### Extending workflows with subclassed tasks ###

It's dead simple to add tasks of a given type. Say you want to
calculate hybrid selection on bam files that have and haven't been
mark duplicated. By subclassing an existing task and giving the new
class it's own configuration file location, you can configure the new
task to depend on whatever you want. In `ratatosk.lib.tools.picard` I
have added the following class:

```python
class HsMetricsNonDup(HsMetrics):
	"""Run on non-deduplicated data"""
	_config_subsection = "hs_metrics_non_dup"
	parent_task = luigi.Parameter(default="ratatosk.lib.tools.picard.MergeSamFiles")
```
and a picard metrics wrapper task

```python
class PicardMetricsNonDup(JobWrapperTask):
    """Runs hs metrics on both duplicated and de-duplicated data"""
    def requires(self):
        return [InsertMetrics(target=self.target + str(InsertMetrics.target_suffix.default[0])),
                HsMetrics(target=self.target + str(HsMetrics.target_suffix.default)),
                HsMetricsNonDup(target=rreplace(self.target, str(DuplicationMetrics.label.default), "", 1) + str(HsMetrics.target_suffix.default)),
                AlignmentMetrics(target=self.target + str(AlignmentMetrics.target_suffix.default))]
```

The `picard` configuration section in the configuration file
`J.Doe_00_01_nondup.yaml` now has a new subsection:

```yaml
picard:
  PicardMetricsNonDup:
    parent_task: ratatosk.lib.tools.picard.DuplicationMetrics
```

Running 

	ratatosk_run.py PicardMetricsNonDup  --target P001_101_index3/P001_101_index3.sort.merge.dup
	  --config-file ~/opt/ratatosk/examples/J.Doe_00_01_nondup.yaml
	
will add hybrid selection calculation on non-deduplicated bam file for sample *P001_101_index3*:

![CustomDedup](https://raw.github.com/percyfal/ratatosk/master/doc/example_align_seqcap_custom_dup.png)


More information
================

 * Documentation: http://ratatosk.readthedocs.org/en/latest/
 * Code: https://github.com/percyfal/ratatosk
