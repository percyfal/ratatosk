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
import os
import collections
from datetime import datetime
import time
import luigi
import glob
import itertools
import logging

logger = logging.getLogger('luigi-interface')

# http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
# FIX ME: make override work
def update(d, u, override=True, expandvars=True):
    """Update values of a nested dictionary of varying depth"""
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        else:
            if expandvars and isinstance(v, str):
                u[k] = os.path.expandvars(v)
            d[k] = u[k]
    return d

# FIXME: implement replacement for cement.ConfigHandler check
# Would allow passing d["_sections"] or d
def config_to_dict(d):
    """Convert config handler or OrderedDict entries to dict for yaml
    output.

    :param d: config handler or ordered dict
    """
    if d is None:
        return {}
    if isinstance(d, dict):
        pass
    else:
        raise TypeError("unsupported type <{}>".format(type(d)))
    u = {}
    for k, v in d.iteritems():
        u[k] = {}
        if isinstance(v, collections.Mapping):
            for x, y in v.iteritems():
                if isinstance(y, collections.Mapping):
                    u[k][x] = dict(y)
                else:
                    u[k][x] = y
        else:
            u[k] = v
    return u

# http://stackoverflow.com/questions/2556108/how-to-replace-the-last-occurence-of-an-expression-in-a-string
def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)

# http://stackoverflow.com/questions/2020014/get-fully-qualified-class-name-of-an-object-in-python
def fullclassname(o):
    return o.__module__ + "." + o.__name__

def utc_time():
    """Make an utc_time with appended 'Z'"""
    return str(datetime.utcnow()) + 'Z'

def make_fastq_links(targets, indir, outdir, fastq_suffix="001.fastq.gz", ssheet="SampleSheet.csv"):
    """Given a set of targets and an output directory, create links
    from targets (source raw data) to an output directory.

    :param targets: list of tuples consisting of (sample, sample target prefix, sample run prefix)
    :param outdir: (top) output directory
    :param fastq_suffix: fastq suffix
    :param ssheet: sample sheet name

    :returns: new targets list with updated output directory
    """
    newtargets = []
    for tgt in targets:
        fastq = glob.glob("{}*{}".format(tgt[2], fastq_suffix))
        if len(fastq) == 0:
            logger.warn("No fastq files for prefix {} in {}".format(tgt[2], "make_fastq_links"))
        for f in fastq:
            newpath = os.path.join(outdir, os.path.relpath(f, indir))
            if not os.path.exists(os.path.dirname(newpath)):
                logger.info("Making directories to {}".format(os.path.dirname(newpath)))
                os.makedirs(os.path.dirname(newpath))
                if not os.path.exists(os.path.join(os.path.dirname(newpath), ssheet)):
                    try:
                        os.symlink(os.path.abspath(os.path.join(os.path.dirname(f), ssheet)), 
                                   os.path.join(os.path.dirname(newpath), ssheet))
                    except:
                        logger.warn("No sample sheet found for {}".format())
                        
            if not os.path.exists(newpath):
                logger.info("Linking {} -> {}".format(newpath, os.path.abspath(f)))
                os.symlink(os.path.abspath(f), newpath)
            if not os.path.lexists(os.path.join(os.path.dirname(newpath), ssheet)) and os.path.exists(os.path.abspath(os.path.join(os.path.dirname(f), ssheet))):
                os.symlink(os.path.abspath(os.path.join(os.path.dirname(f), ssheet)), os.path.join(os.path.dirname(newpath), ssheet))
        newtargets.append((tgt[0], 
                           os.path.join(outdir, os.path.relpath(tgt[1], indir)),
                           os.path.join(outdir, os.path.relpath(tgt[2], indir))))
    return newtargets
    
# Shamelessly stolen from http://twistedmatrix.com/trac/browser/tags/releases/twisted-8.2.0/twisted/python/procutils.py
# See http://stackoverflow.com/questions/5226958/which-equivalent-function-in-python    
def which(name, flags=os.X_OK):
    """Search PATH for executable files with the given name.
    
    On newer versions of MS-Windows, the PATHEXT environment variable will be
    set to the list of file extensions for files considered executable. This
    will normally include things like ".EXE". This fuction will also find files
    with the given name ending with any of these extensions.
	
    On MS-Windows the only flag that has any meaning is os.F_OK. Any other
    flags will be ignored.
    
    @type name: C{str}
    @param name: The name for which to search.
    
    @type flags: C{int}
    @param flags: Arguments to L{os.access}.
    
    @rtype: C{list}
    @param: A list of the full paths to files found, in the
    order in which they were found.
    """
    result = []
    exts = filter(None, os.environ.get('PATHEXT', '').split(os.pathsep))
    path = os.environ.get('PATH', None)
    # if path is None:
    #     return []
    # for p in os.environ.get('PATH', '').split(os.pathsep):
    #     p = os.path.join(p, name)
    #     if os.access(p, flags):
    #         result.append(p)
    #         for e in exts:
	#             pext = p + e
    #             if os.access(pext, flags):
	#                 result.append(pext)
    return result

def opt_to_dict(opts):
    """Transform option list to a dictionary.

    :param opts: option list
    
    :returns: option dictionary
    """
    if isinstance(opts, dict):
        return
    if isinstance(opts, str):
        opts = opts.split(" ")
    args = list(itertools.chain.from_iterable([x.split("=") for x in opts]))
    opt_d = {k: True if v.startswith('-') else v
             for k,v in zip(args, args[1:]+["--"]) if k.startswith('-')}
    return opt_d


def dict_to_opt(opt_dict):
    """Transform option dict to an option list.

    :param opt_dict: option dict
    
    :returns: option list
    """
    args = list(itertools.chain.from_iterable([(k,v) for k,v in opt_dict.iteritems()]))
    ret_args = [x for x in args if not isinstance(x, bool)]
    return ret_args
