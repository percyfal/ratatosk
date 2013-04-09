Writing a pipeline
==================

One of the main uses of `ratatosk` is to write an analysis pipeline.
The steps involved are best explained by following an example.


Example: a variant calling pipeline
-----------------------------------

Assume we want to define a pipeline that performs the following tasks:

1. align reads to a reference
2. merge reads from several runs
3. 

Limitations of the current implementation
-----------------------------------------

It wouldn't be fair to not comment on some of the limitations and
problems with the current implementation.

1. There is no validation of task dependencies, and there probably
   never will due to lack of production time. This means you can
   connect any tasks, regardless of whether the output from the parent
   task can actually be used by a task. Sorting these dependencies out
   is left to the end user.
2. It can be nefariously difficult to derive the required labels that
   will be used to generate source names. See issues.


