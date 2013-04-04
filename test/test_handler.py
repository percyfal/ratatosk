import os
import glob
import shutil
import sys
import unittest
import luigi
import yaml
import logging
from ratatosk import interface, backend
from ratatosk.config import get_config
from ratatosk.lib.tools.picard import MergeSamFiles
from ratatosk.utils import fullclassname, rreplace
from types import GeneratorType
from ratatosk.handler import register, register_task_handler, RatatoskHandler

logging.basicConfig(level=logging.DEBUG)

def local_target_generator(task):
    return None

class TestHandler(unittest.TestCase):
    def test_register_handler(self):
        backend.__handlers__ = {}
        target_handler = RatatoskHandler(label="target_generator_handler", mod="test.test_handler.local_target_generator")
        self.assertEqual(backend.__handlers__, {})
        register(target_handler)
        self.assertEqual(fullclassname(backend.__handlers__['target_generator_handler']), "test.test_handler.local_target_generator")

    def test_register_task_handler(self):
        obj = MergeSamFiles()
        obj.__handlers__ = {}
        target_handler = RatatoskHandler(label="target_generator_handler", mod="test.test_handler.local_target_generator")
        self.assertEqual(obj.__handlers__, {})
        register_task_handler(obj, target_handler)
        self.assertEqual(fullclassname(obj.__handlers__['target_generator_handler']), "test.test_handler.local_target_generator")

    def test_iterate_targets(self):
        obj = MergeSamFiles()
        target_handler = RatatoskHandler(label="target_generator_handler", mod="test.test_handler.local_target_generator")
        register(target_handler)

    def test_ratatosk_handler(self):
        h = RatatoskHandler(label="labeldef", mod="Mod")
        self.assertEqual(h.mod(), "Mod")
        self.assertEqual(h.label(), "labeldef")
        self.assertIsInstance(h, RatatoskHandler)
