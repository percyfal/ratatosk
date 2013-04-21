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
"""
The experiment module contains abstract class definitions and
implementations of experimental units.

Classes
-------
"""
import os
import abc

# GOAL: ('P001_101_index3', 'seqcap/P001_101_index3/P001_101_index3', 'seqcap/P001_101_index3/120924_AC003CCCXX/P001_101_index3_TGACCA_L001')
class ISample(object):
    """A sample interface class"""
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def project_id(self):
        """Project identifier"""
        return
    @abc.abstractproperty
    def sample_id(self):
        """Sample/unit identifier"""
        return
    @abc.abstractmethod
    def prefix(self, group):
        """Return full prefix at specified group level (e.g. project, sample, sample run)"""
        return
    @abc.abstractmethod
    def path(self, group):
        """Return path specified group level (e.g. project, sample, sample run)"""
        return
    @abc.abstractproperty
    def levels(self):
        """Registered factor levels"""
        return
    @abc.abstractmethod
    def add_level(self, level):
        """Register a factor level"""
        return
    @abc.abstractmethod
    def get_level(self, level):
        """Get the value of a given level"""
        return

class Sample(ISample):
    """A class describing a sample."""
    _levels = {}
    _sample_id = None
    _project_id = None
    _project_prefix = None
    _sample_prefix = None
    _sample_run_prefix = None

    def __init__(self, project_id=None, sample_id=None, project_prefix=None, sample_prefix=None, sample_run_prefix=None, **kwargs):
        self._project_id = project_id
        self._sample_id = sample_id
        self._prefix = {'project' : project_prefix,
                        'sample' : sample_prefix,
                        'sample_run' : sample_run_prefix
                        }
    def project_id(self):
        return self._project_id
    def sample_id(self):
        return self._sample_id
    def prefix(self, group="sample"):
        if not group in ["project", "sample", "sample_run"]:
            raise ValueError, "No such prefix level '{}'".format(group)
        return self._prefix[group]
    def path(self, group="sample"):
        if not group in ["project", "sample", "sample_run"]:
            raise ValueError, "No such prefix level '{}'".format(group)
        d = os.path.dirname(self._prefix[group])
        if not d:
            d = os.curdir
        return d
    def levels(self):
        return
    def add_level(self, level):
        return
    def get_level(self, level):
        return
