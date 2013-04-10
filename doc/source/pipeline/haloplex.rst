HaloPlex pipeline
=================

Variant calling
---------------

.. figure:: ../../grf/ratatosk_pipeline_haloplex_1.png
   :alt: HaloPlex pipeline
   :scale: 80%
   :align: center
   
   **Figure 1.** HaloPlex pipeline, part 1. The input consists of two
   read pairs from one sample, thus illustrating the merge operation
   at sample level. The figure has been partitioned for clarity.

.. figure:: ../../grf/ratatosk_pipeline_haloplex_2.png
   :alt: HaloPlex pipeline 
   :scale: 80%
   :align: center    
   
   **Figure 2.** HaloPlex pipeline, continued.

Blue boxes mean active processes (the command was run with `--workers
4`). Note that we need to know what labels are applied to the file
name (see issues). In this iteration, for the predefined pipelines the
file names have been hardcoded.

Variant summary
---------------

.. figure:: ../../grf/ratatosk_pipeline_haloplex_summary.png
   :alt: HaloPlexSummary pipeline 
   :scale: 80%
   :align: center    
   
   **Figure 3.** HaloPlexSummary pipeline. Often there are so many
   samples that this step needs to be performed separately from the
   HaloPlex task.
