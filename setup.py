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

from setuptools import setup, find_packages
import sys
import os
import glob

setup(name = "ratatosk",
      version = "0.1.0",
      author = "Per Unneberg",
      author_email = "per.unneberg@scilifelab.se",
      description = "Project/pipeline manager",
      license = "Apache",
      scripts = glob.glob('scripts/*.py') + ['bin/ratatoskd'],
      install_requires = [
        "drmaa >= 0.5",
        "mock",
        # Currently works with luigi version 1.0, commit hash tag
        # da20852fa10a60a388 - would want to put this here in master
        # in case something breaks in the future
        "simplejson",
        "pyyaml",
        "tornado",
        "luigi",
        "nose",
        "nose-timer",
        ],
      test_suite = 'nose.collector',
      packages=find_packages(exclude=['ez_setup', 'test*']),
      namespace_packages = [
        'ratatosk',
        'ratatosk.ext',
        ],
      # packages=[
      #       'ratatosk'
      #       ],
      package_data = {
        'ratatosk':[
            'data/grf/*',
            'data/templates/*',
            'config/*'
            ]}
      )

os.system("git rev-parse --short --verify HEAD > ~/.ratatosk_version")
