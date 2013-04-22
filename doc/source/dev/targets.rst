Targets in pipelines - collecting information 
===============================================

.. warning:: The target collecting implementation is still under heavy
   development. The API is likely to change every now and then.

All tasks subclass :class:`.BaseJobTask` and therefore inherit an
option :attr:`target <ratatosk.job.BaseJobTask.target>`. By passing a
target option, a task can be run like a make target:

.. code-block:: text

   ratatosk_run.py Task --target target.txt

Should ``Task`` have any further dependencies, these will be
calculated on-the-fly and run if incomplete.

For more complex workflows, such as pipelines, the situation is more
complicated. Generally, we want to run hundreds of targets, and
providing them as arguments to options is cumbersome, to say the
least. Therefore, :mod:`ratatosk` implements the concept of 


Target generator functions
--------------------------


Keeping track of information in pipelines
-----------------------------------------
