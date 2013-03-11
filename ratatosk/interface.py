import logging
from pm.ext import ext_yamlconfigparser

def setup_interface_logging():
    # From luigi.interface - setup pm-specific logging interface?
    # use a variable in the function object to determine if it has run before
    if getattr(setup_interface_logging, "has_run", False):
        return

    logger = logging.getLogger('luigi-interface')
    logger.setLevel(logging.DEBUG)

    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(levelname)s: %(message)s')
    streamHandler.setFormatter(formatter)

    logger.addHandler(streamHandler)
    setup_interface_logging.has_run = True

def setup_config_handler():
    return ext_yamlconfigparser.YAMLParserConfigHandler

def get_config(config_file=None):
    """Copied from luigi.interface. This part needs rethinking. In
    particular, rewrite ConfigParser to subclass RawConfigParser?"""
    config_parser = ext_yamlconfigparser.YAMLParserConfigHandler()
    config_parser.parse_file(config_file)
    return config_parser
