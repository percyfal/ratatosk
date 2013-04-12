.. ratatosk documentation master file, created by
   sphinx-quickstart on Fri Apr  5 11:46:36 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ratatosk - a pipeline management framework
==========================================

.. warning:: ratatosk is currently in pre-release status and is under
   very active development. Use it at your own risk.

:mod:`ratatosk` is a library of `luigi <https://github.com/spotify/luigi>`_
tasks, currently focused on, but not limited to, common
bioinformatical tasks.

Core features include

 * Simple program wrapper interface
 * Simple configuration files for defining workflows and modifying
   program options
 * Task wrapper library makes for efficient code reuse. Once a wrapper
   is available, creating a workflow is just a matter of stitching
   tasks together via the configuration file
 * Piped tasks
 * Workflow visualization
 * Parallelization via the *luigi* scheduler
 * Easy to include external library code 

Documentation
-------------

.. toctree:: 
   :maxdepth: 1

   changes
   contributors
   api/index
   ext/index

.. toctree::
   :maxdepth: 2

   dev/index
   howtos/index
   pipeline/index

