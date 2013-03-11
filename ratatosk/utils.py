import collections
from cement.core import config

# http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
# FIX ME: make override work
def update(d, u, override=True):
    """Update values of a nested dictionary of varying depth"""
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

def config_to_dict(d):
    """Convert config handler or OrderedDict entries to dict for yaml
    output.

    :param d: config handler or ordered dict
    """
    if d is None:
        return {}
    if isinstance(d, config.CementConfigHandler):
        d = d._sections
    elif isinstance(d, dict):
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
        
