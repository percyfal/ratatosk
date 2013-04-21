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

# NOTE: this test suite only verifies that the commands are formatted
# correctly. No actual spawning of subprocesses is done.

import os
import re
import unittest
from ratatosk.experiment import Sample


class TestABCSample(unittest.TestCase):
    def setUp(self):
        self.sample = Sample(project_id="project", sample_id="sample", sample_prefix="project/sample/sample", project_prefix="project", sample_run_prefix="project/sample/samplerun/samplerun")

    def test_sample(self):
        self.assertEqual(self.sample.prefix(), "project/sample/sample")
        self.assertEqual(self.sample.path(), "project/sample")
        self.assertEqual(self.sample.prefix("project"), "project")
        self.assertEqual(self.sample.path("project"), ".")
        self.assertEqual(self.sample.prefix("sample_run"), "project/sample/samplerun/samplerun")
        self.assertEqual(self.sample.path("sample_run"), "project/sample/samplerun")
        self.assertEqual(self.sample.project_id(), "project")
