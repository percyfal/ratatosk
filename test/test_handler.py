import os
import glob
import shutil
import sys
import unittest
import luigi
import yaml
import logging
import ngstestdata as ntd
from ratatosk import interface, backend
from ratatosk.lib.tools.picard import MergeSamFiles
from ratatosk.utils import fullclassname, rreplace
from types import GeneratorType
from ratatosk.handler import register, RatatoskHandler, target_generator_validator as tgv

logging.basicConfig(level=logging.DEBUG)

def target_generator_validator(func):
    def wrap():
        print "Wrapping func"
        func()
        print dir(func())

    return wrap

def main_target_generator(func):
    tmp = func()
    if not isinstance(tmp, GeneratorType):
        print "function is not a generator function; skipping"
        return
    try:
        for sample, run, flowcell in tmp:
            print sample
    except:
        print "Too few values to unpack"
        pass

def target_generator():
    l = [(1,2,3), (4,5,6), (7,8,9)]
    while l:
        yield l.pop()

def target_generator_2():
    yield (1,2)
def target_generator_3():
    return (1,2,3)

class TestHandler(unittest.TestCase):
    def test_register_handler(self):
        target_handler = RatatoskHandler(label="target_generator_handler", mod="test.site_functions.collect_sample_runs")
        self.assertEqual(backend.__handlers__, {})
        register(target_handler)
        self.assertEqual(fullclassname(backend.__handlers__['target_generator_handler']), "test.site_functions.collect_sample_runs")

    def test_iterate_targets(self):
        obj = MergeSamFiles()
        target_handler = RatatoskHandler(label="target_generator_handler", mod="test.site_functions.target_generator")
        register(target_handler)
        obj.target="/Users/peru/opt/ngs_test_data/data/projects/J.Doe_00_01/P001_101_index3/P001_101_index3.sort.merge.bam"
        for t in obj.target_iterator():
            print t
        sources = [x[2] + os.path.basename(rreplace(obj.target.replace(x[0], ""), "{}{}".format(obj.label, obj.target_suffix), obj.source_suffix, 1)) for x in obj.target_iterator()]
        print sources
        print obj.target
        print os.path.basename(rreplace(obj.target.replace(x[0], ""), "{}{}".format(obj.label, obj.target_suffix), obj.source_suffix, 1))
        print dir(obj._make_source_files)

    def test_ratatosk_handler(self):
        h = RatatoskHandler(label="labeldef", mod="Mod")
        self.assertEqual(h.mod(), "Mod")
        self.assertEqual(h.label(), "labeldef")
        self.assertIsInstance(h, RatatoskHandler)

class TestTargetInterface(unittest.TestCase):
    def test_validator(self):
        gv = target_generator_validator(target_generator)
        print gv
        gv()

    def test_generator(self):
        main_target_generator(target_generator)
        main_target_generator(target_generator_2)

    def test_gen(self):
        print "First"
        for tmp in target_generator():
            print tmp
        print "second"
        for tmp in target_generator():
            print tmp
        print "setting function"
        tg = target_generator()
        for tmp in tg:
            print tmp
        print "second"
        for tmp in tg:
            print tmp


        tmp = target_generator()
        print dir(tmp)
        print tmp.next()
