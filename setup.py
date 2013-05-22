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
import os
import glob

setup(name = "ratatosk",
      version = "0.1.2",
      author = "Per Unneberg",
      author_email = "per.unneberg@scilifelab.se",
      description = "Project/pipeline manager",
      license = "Apache",
      scripts = glob.glob('scripts/*.py') + ['bin/ratatoskd'],
      install_requires = [
        "mako",
        "simplejson",
        "pyyaml",
        "tornado",
        "luigi>=1.0",
        "matplotlib>=1.2.1",
        "pysam",
        "pygraphviz",
        ## Required for testing
        "nose",
        ## Required to build documentation
        # "PIL",
        ],
      test_suite = 'nose.collector',
      packages=find_packages(exclude=['ez_setup', 'test*']),
      namespace_packages = [
        'ratatosk',
        'ratatosk.ext',
        ],
      package_data = {
        'ratatosk':[
            'data/grf/*',
            'data/templates/*',
            'config/*'
            ]},
      # See
      # http://stackoverflow.com/questions/3472430/how-can-i-make-setuptools-install-a-package-thats-not-on-pypi
      # for requiring github version
      # dependency_links = ['https://github.com/spotify/luigi/tarball/master#egg=luigi-1.0'],
      )

os.system("git rev-parse --short --verify HEAD > ~/.ratatosk_version")
