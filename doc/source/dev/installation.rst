Installation
============

Pre-requisites
--------------

It is recommended that you first create a virtual environment in which
to install the packages. Install
`virtualenvwrapper <http://virtualenvwrapper.readthedocs.org/en/latest/>`_
and use
`mkvirtualenv <http://virtualenvwrapper.readthedocs.org/en/latest/command_ref.html>`_
to create a virtual environment.

.. _installation:

Installation
------------

You can download and install :mod:`ratatosk` from the `Python Package
Index <https://pypi.python.org/pypi/ratatosk>`_ with the command

.. code-block:: text

   pip install ratatosk

Alternatively, to install the development version of :mod:`ratatosk`,
do

.. code-block:: text
	
	git clone https://github.com/percyfal/ratatosk
	python setup.py develop

Known installation issues
-------------------------

luigi
^^^^^

The current :mod:`luigi` `Python Package Index
<https://pypi.python.org/pypi/luigi>`_ is out of date. You may
therefore need to manually install the development version from
`github <https://github.com/spotify/luigi>`_.

pygraphviz
^^^^^^^^^^

Installing `Pygraphviz <http://networkx.lanl.gov/pygraphviz/>`_ with
``pip install pygraphviz`` often fails because the installer cannot
find the :program:`graphviz` library. One solution lies in modifying the
``setup.py`` that comes with the :mod:`pygrahviz` package. After a failed pip
install in virtual environment ``virtualenv`` (or whatever you called
it), you will typically find the failed build in
``~/.virtualenvs/virtualenv/build/pygraphviz``. In that directory,
modify the following section in  ``setup.py``:

.. code-block:: text

   # If the setup script couldn't find your graphviz installation you can
   # specify it here by uncommenting these lines or providing your own:
   # You must set both 'library_path' and 'include_path'

   # Linux, generic UNIX
   library_path='/usr/lib64/graphviz'
   include_path='/usr/include/graphviz'



Dependencies
------------

To begin with, you may need to install
`Tornado <http://www.tornadoweb.org/>`_ and
`Pygraphviz <http://networkx.lanl.gov/pygraphviz/>`_ (see
`Luigi <https://github.com/spotify/luigi/blob/master/README.md>`_ for
further information).

The tests depend on the following software to run:

1. `bwa <http://bio-bwa.sourceforge.net/>`_
2. `samtools <http://samtools.sourceforge.net/>`_
3. `GATK <http://www.broadinstitute.org/gatk/>`_ - set an environment
   variable `GATK_HOME` to point to your installation path
4. `picard <http://picard.sourceforge.net/>`_ - set an environment
   variable `PICARD_HOME` to point to your installation path
5. `fastqc <http://www.bioinformatics.babraham.ac.uk/projects/fastqc/>`_   


Running the tests
-----------------

Cd to the test directory (``test``) and run

.. code-block:: text

	nosetests -v -s test_commands.py
	
To run a given task (e.g.
``TestCommand.test_bwaaln``), do

.. code-block:: text

	nosetests -v -s test_commands.py:TestCommand.test_bwaaln

Task visualization and tabulation
-------------------------------------

By default, the tests use a local scheduler, implemented in luigi. For
production purposes, there is also a `central planner
<https://github.com/spotify/luigi/blob/master/README.md#using-the-central-planner>`_.
Among other things, it allows for visualization of the task flow by
using `Tornado <http://www.tornadoweb.org/>`_ and
`Pygraphviz <http://networkx.lanl.gov/pygraphviz/>`_. Results are
displayed in *http://localhost:8081*, results "collected" at
*http://localhost:8082/api/graph*.

In addition, I have extended the luigi daemon and server code to
generate a table representation of the tasks (in
*http://localhost:8083*). The aim here would be to define a grouping
function that groups task lists according to a given feature (e.g.
sample, project).

In order to view tasks, run

.. code-block:: text

	ratatoskd &
	
in the background and run the tests:

.. code-block:: text

	nosetests -v -s test_commands.py
	
