#!/usr/bin/env python
from setuptools import setup, find_packages
import sys
import os
import glob

setup(name = "pm",
      version = "0.1.0",
      author = "Per Unneberg",
      author_email = "per.unneberg@scilifelab.se",
      description = "Project/pipeline manager",
      license = "MIT",
      scripts = glob.glob('scripts/*.py') + ['scripts/pm2'],
      install_requires = [
        "drmaa >= 0.5",
        "sphinx >= 1.1.3",
        "couchdb >= 0.8",
        "reportlab >= 2.5",
        "cement >= 2.0.2",
        "mock",
        "PIL",
        "pyPdf",
        "logbook >= 0.4",
        "pandas >= 0.9",
        "biopython",
        "luigi",
        "nose"
        ],
      test_suite = 'nose.collector',
      packages=find_packages(exclude=['tests']),
      package_data = {'pm':[
            'data/grf/*',
            'data/templates/*',
            ]}
      )

os.system("git rev-parse --short --verify HEAD > ~/.pm_version")
