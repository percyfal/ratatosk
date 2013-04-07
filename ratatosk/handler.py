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
import sys
import os
import logging
import luigi
from types import GeneratorType
from ratatosk import backend
from ratatosk.utils import fullclassname

# Use luigi-interface for now
logger = logging.getLogger('luigi-interface')

class IHandler(object):
    """A handler interface class"""
    def label(self):
        raise NotImplementedError
    def mod(self):
        raise NotImplementedError
    def load_type(self):
        raise NotImplementedError


class RatatoskHandler(IHandler):
    """Ratatosk handler class. Ensures we have at least label and mod
    functions that register uses to place mod in
    backend.__handlers__[label].

    FIX ME: add interface attribute?
    """
    _label = None
    _mod = None
    _load_type = None
    
    def __init__(self, label, mod, load_type="function"):
        """Initialize handler with label (key in backend.__handlers__)
        and mod (string representation of class/function). Load_type
        deterimines what load function to use.
        """
        setattr(self, "_label", label)
        setattr(self, "_mod", mod)
        setattr(self, "_load_type", load_type)

    def label(self):
        return self._label

    def mod(self):
        return self._mod

    def load_type(self):
        return self._load_type


def _load_module_function(handler_obj):
    """Load a module function represented as a string.

    :param handler_obj: handler object
    
    :return: function on success, None otherwise
    """
    opt_mod = ".".join(handler_obj.mod().split(".")[0:-1])
    opt_fn = handler_obj.mod().split(".")[-1]
    try:
        m = __import__(opt_mod, fromlist=[opt_fn])
        fn = getattr(sys.modules[m.__name__], opt_fn)
        return fn
    except:
        logger.warn("No function '{}' found: failed to register handler '{}'".format(".".join([opt_mod, opt_fn]), 
                                                                                     handler_obj.label()))
        return None

def _load_module_class(handler_obj):
    """Load a module class represented as a string.

    :param handler_obj: handler containing class
    
    :return: class on success, None otherwise
    """
    opt_mod = ".".join(handler_obj.mod().split(".")[0:-1])
    opt_cls = handler_obj.mod().split(".")[-1]
    try:
        mod = __import__(opt_mod, fromlist=[opt_cls])
        cls = getattr(mod, opt_cls)
        ret_cls = cls
        return ret_cls
    except:
        logger.warn("No class '{}' found: failed to register handler '{}'".format(".".join([opt_mod, opt_fn]), 
                                                                                  handler_obj.label()))
        return None

        # logger.warn("No class {} found: using default class {} for task '{}'".format(".".join([opt_mod, opt_cls]), 
        #                                                                              ".".join([default_mod, default_cls]),
        #                                                                              self.__class__))

def _load(handler_obj):
    if handler_obj.load_type() == "function":
        return _load_module_function(handler_obj)
    elif handler_obj.load_type() == "class":
        return _load_module_class(handler_obj)
    else:
        raise Exception, "Unknown load_type {}".format(handler_obj.load_type())
        
def register(handler_obj, default_handler=None):
    """Register a module function represented as string in
    to a backend handler.
    
    :param handler_obj: the handler object to register
    :param default_handler: the default handler to fall back on

    :return: None
    """
    if not handler_obj:
        logging.warn("No handler object provided; skipping")
        return
    if not isinstance(handler_obj, IHandler):
        raise ValueError, "handler object must implement the IHandler interface"
    hdl = _load(handler_obj)
    if not hdl:
        return
    if handler_obj.label() in backend.__handlers__:
        old_hdl = backend.__handlers__[handler_obj.label()]
        if fullclassname(old_hdl) == fullclassname(hdl):
            logging.info("Handler object '{}' already registered; skipping".format(fullclassname(hdl)))
            return
        else:
            logging.warn("Trying to reset already registered '{}' which is not supported".format(handler_obj.label()))
            return
    else:
        backend.__handlers__[handler_obj.label()] = hdl

def register_task_handler(obj, handler_obj, default_handler=None):
    """Register a module function represented as string in
    to a task handler.
    
    :param obj: the object to which the handler is registered
    :param handler_obj: the handler object to register
    :param default_handler: the default handler to fall back on

    :return: None
    """
    if not handler_obj:
        logging.warn("No handler object provided; skipping")
        return
    if not isinstance(handler_obj, IHandler):
        raise ValueError, "handler object must implement the IHandler interface"
    hdl = _load(handler_obj)
    if not hdl:
        return
    if handler_obj.label() in obj._handlers:
        old_hdl = obj._handlers[handler_obj.label()]
        if fullclassname(old_hdl) == fullclassname(hdl):
            logging.info("Handler object '{}' already registered in {}; skipping".format(fullclassname(hdl), obj))
            return
        else:
            logging.warn("Trying to reset already registered '{}' which is not supported".format(handler_obj.label()))
            return
    else:
        obj._handlers[handler_obj.label()] = hdl

def register_attr(obj, handler_obj, default_handler=None):
    """Register a class represented as string to a task attribute.
    
    :param obj: the object to which the handler class is registered
    :param handler_obj: the handler object to register, with attribute defined by handler_obj.label()
    :param default_handler: the default handler to fall back on

    :return: None
    """
    attr_list = getattr(obj, handler_obj.label(), [])
    if not handler_obj:
        logging.warn("No handler object provided; skipping")
        return
    if not isinstance(handler_obj, IHandler):
        raise ValueError, "handler object must implement the IHandler interface"
    hdl = _load(handler_obj)
    if not hdl:
        return
    attr_list.append(hdl)
    setattr(obj, handler_obj.label(), attr_list)

def target_generator_validator(fn):
    """Validate target generator function. Must return 3-tuple
    (sample, sample target prefix (merge target), sample run prefix
    (read pair prefix)

    :param fn: function to validate

    :return: True if ok, false otherwise
    """
    if data and not len(data[-1]) == 3:
        raise ValueError, "target generator function must return a 3-tuple consisting of sample, sample target prefix, sample run prefix"
    return True

def setup_global_handlers(hlist=["target_generator_handler"]):
    """Helper function to setup global handlers defined in 'settings'
    section. 
    
    """
    if not "settings" in backend.__global_config__:
        return
    for key in hlist:
        if key in backend.__global_config__["settings"]:
            h = RatatoskHandler(label=key, mod=backend.__global_config__["settings"][key])
            register(h)
