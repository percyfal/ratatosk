import os
import glob
import shutil
import sys
import unittest
import luigi
import yaml
import logging
import ngstestdata as ntd
from types import GeneratorType

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
