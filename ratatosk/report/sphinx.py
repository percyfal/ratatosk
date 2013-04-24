# Copyright (c) 2013 Per Unneberg
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import os
import luigi
import re
import texttable as tt
import cPickle as pickle
from mako.template import Template
from ratatosk.job import JobTask
from ratatosk.log import get_logger

TEMPLATEPATH = os.path.join(os.path.dirname(__file__), os.pardir, "data", "templates", "doc")

templates = {
    'make' : (Template(filename=os.path.join(TEMPLATEPATH, "Makefile.mako")),''),
    'sample' : (Template(filename=os.path.join(TEMPLATEPATH, "source", "samples", "sample.mako")), '.rst'),
    'index' : (Template(filename=os.path.join(TEMPLATEPATH, "source", "index.mako")), '.rst'),
    'conf' : (Template(filename=os.path.join(TEMPLATEPATH, "source", "conf.mako")), '.py')
    }

docdirs = ["build", "source", os.path.join("source", "_static"), os.path.join("source", "_templates"),
           os.path.join("source", "samples")]

logger = get_logger()

def _setup_directories(root):
    """create directory structure"""
    if not os.path.exists(root):
        logger.info("Making documentation output directory '{}'".format(root))
        for d in docdirs:
            if not os.path.exists(os.path.join(root, d)):
                logger.info("Making directory " + str(os.path.join(root, d)))
                os.makedirs(os.path.join(root, d))

kw = {
    'docroot':None,
    'sample_id':None,
    'project_id':None,
    'project_name':None,
    'application':None,
    'date':None,
    'samples':[['SAMPLE']],
    'pickled_samples':None,
    }

def make_rst_table(data):
    """Make rst table with :py:mod:`Texttable`"""
    if data is None:
        return ""
    else:
        tab_tt = tt.Texttable()
        tab_tt.set_precision(2)
        tab_tt.add_rows(data)
        return tab_tt.draw()

class SphinxReport(JobTask):
    """Make sphinx report"""
    indir = luigi.Parameter(description="Analysis input directory", default=None)
    outdir = luigi.Parameter(description="Documentation directory", default="doc")
    sample = luigi.Parameter(default=[], description="Samples to process.", is_list=True)
    flowcell = luigi.Parameter(default=[], description="Flowcells to process.", is_list=True)
    lane = luigi.Parameter(default=[], description="Lanes to process.", is_list=True)
    # Add metrics types and parameters to be parsed here here
    samples = []
    
    def _setup(self):
        kw['docroot'] = self.outdir
        # List requirements for completion, consisting of classes above
        if self.indir is None:
            logger.error("Need input directory to run")
            self.targets = []
        self.samples = [tgt for tgt in self.target_iterator()]
        kw['samples'].extend([["`{} <samples/{}.rst>`_".format(s.sample_id(), s.sample_id())] for s in self.samples])
        kw['samples'] = make_rst_table(kw['samples'])
        _setup_directories(self.outdir)
        pickled_samples = []
        for s in self.samples:
            for k, v in s._prefix.items():
                if not k in ["project"]:
                    s._prefix[k] = os.path.relpath(s.prefix(k), self.outdir)
            pickled_samples.append(s)
        kw['pickled_samples'] = os.path.join(self.outdir, "samples.pickle" )
        with open(kw['pickled_samples'], "w") as fh:
            pickle.dump(pickled_samples, fh)
        for k,v in templates.items():
            outfile = os.path.splitext(os.path.join(self.outdir, os.path.relpath(v[0].filename, TEMPLATEPATH)))[0] + v[1]
            with open(outfile, "w") as fh:
                fh.write(v[0].render(**kw))

    def output(self):
        return luigi.LocalTarget(self.outdir)

    def complete(self):
        return False

    def run(self):
        self._setup()

    def requires(self):
        return []
