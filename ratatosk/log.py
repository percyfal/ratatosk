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
import logging

def setup_logging():
    # use a variable in the function object to determine if it has run before
    if getattr(setup_interface_logging, "has_run", False):
        return

    logger = logging.getLogger('ratatosk-interface')
    logger.setLevel(logging.DEBUG)

    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(logging.DEBUG)

    file_format = "%(asctime)s (%(levelname)s) %(namespace)s : " + \
        "%(message)s"

    formatter = logging.Formatter(file_format)
    streamHandler.setFormatter(formatter)

    logger.addHandler(streamHandler)
    setup_interface_logging.has_run = True

def get_logger():
    logger = logging.getLogger('ratatosk-interface')
    if not logger.handlers:
        logger = logging.getLogger()
    return logger
