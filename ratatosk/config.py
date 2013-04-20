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
import yaml
import logging
import collections
from ConfigParser import NoSectionError, NoOptionError, DuplicateSectionError
from ratatosk import backend
from ratatosk.utils import update, config_to_dict
from ratatosk.log import get_logger

#logger = logging.getLogger('luigi-interface')
logger = get_logger()

try:
    from collections import OrderedDict as _default_dict
except ImportError:
    # fallback for setup.py which hasn't yet built _collections
    _default_dict = dict

class RatatoskConfigParser(object):
    """Ratatosk configuration parser. Works on yaml files.
    """
    NO_DEFAULT = object()
    _instance = None
    _config_paths = []
    _cls_dict = collections.OrderedDict

    @classmethod
    def add_config_path(cls, path):
        if path and not any(os.path.samefile(path, x) for x in cls._instance._config_paths):
            logger.debug("adding config path {}".format(path))
            cls._instance._config_paths.append(path)
        else:
            return
        cls._instance.reload()

    @classmethod
    def del_config_path(cls, path):
        if path and any(os.path.samefile(path, x) for x in cls._instance._config_paths):
            logger.debug("removing config path {}".format(path))
            try:
                i = [os.path.samefile(path, x) for x in cls._instance._config_paths].index(True)
                del cls._instance._config_paths[i]
            except ValueError:
                logger.warn("No such path {} in _config_paths".format(path))
        else:
            return
        # Need to clear sections before reloading
        cls._instance._sections = cls._cls_dict()
        cls._instance.reload()

    @classmethod
    def instance(cls, *args, **kwargs):
        """Singleton getter"""
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
            loaded = cls._instance.reload()
            logger.info("Loaded %r" % loaded)
        return cls._instance

    @classmethod
    def clear(cls):
        cls._instance._config_paths = []
        cls._instance._sections = cls._cls_dict()

    def reload(self):
        return self._instance.read(self._instance._config_paths)

    def __init__(self, defaults=None, dict_type=_default_dict, *args, **kw):
        self._dict = dict_type
        self._sections = self._dict()
        self._defaults = self._dict()
        _cls_dict = self._dict
    
    def read(self, file_paths):
        """
        Read config files.

        :param file_path: The file system path to the configuration file.

        :returns: boolean
        """
        for path in file_paths:
            try:
                with open(path) as fp:
                    _sections = yaml.load(fp)
                    if _sections is None:
                        _sections = {}
                self._sections = update(self._sections, _sections)
            except IOError:
                logging.warn("No such file {}".format(path))
                return False
        return True
        
    def parse_file(self, file_path):
        """
        Parse config file settings from file_path, overwriting existing 
        config settings.  If the file does not exist, returns False.
        
        :param file_path: The file system path to the configuration file.
        :returns: boolean
        
        """
        if file_path is None:
            return None
        file_path = os.path.abspath(os.path.expanduser(file_path))
        if os.path.exists(file_path):
            self.read(file_path)
            return True
        else:
            logger.debug("config file '{}' does not exist, skipping...".format(file_path))
            return False
     
    def keys(self, section, subsection=None):
        """
        Return a list of keys within 'section'.
        
        :param section: The config section.
        :param subsection: The config subsection.
        :returns: List of keys in the `section` or `subsection`.
        :rtype: list
        
        """
        return self.options(section, subsection)
    
    def options(self, section, subsection=None):
        try:
            opts = self._sections[section].copy()
        except KeyError:
            raise NoSectionError(section)
        if subsection:
            try:
                opts = opts[subsection].copy()
            except KeyError:
                raise NoSectionError(subsection)
        opts = update(opts, self._defaults)
        if '__name__' in opts:
            del opts['__name__']
        return opts.keys()

    def has_key(self, section, key, subsection=None):
        """
        Return whether or not a 'section' has the given 'key'.
        
        :param section: The section of the configuration. I.e. [block_section].
        :param key: The key within 'section'.
        :returns: True if the config `section` has `key`.
        :rtype: boolean
        
        """
        if key in self.options(section, subsection):
            return True
        else:
            return False
        
    def sections(self):
        """Return a list of section names"""
        return self._sections.keys()

    def get(self, section, option, subsection=None):
        """Get an option"""
        if not section in self.sections():
            raise NoSectionError(section)
        if subsection:
            if not subsection in self._sections[section]:
                raise NoSectionError(subsection)
            if not option in self._sections[section][subsection]:
                raise NoOptionError(option, subsection)
            return self._sections[section][subsection][option]
        if not option in self._sections[section]:
            raise NoOptionError(option, section)
        return self._sections[section][option]
     
    def set(self, section, option, value=None, subsection=None):
        """Set an option"""
        if not section:
            sectdict = self._defaults
        else:
            try:
                sectdict = self._sections[section]
            except KeyError:
                raise NoSectionError(section)
        if subsection:
            try:
                sectdict = sectdict[subsection]
            except KeyError:
                raise NoSectionError(subsection)
        sectdict[self.optionxform(option)] = value

    def optionxform(self, optionstr):
        return optionstr.lower()

    def has_section(self, section, subsection=None):
        """Indicate whether the named section is present in the configuration"""
        if subsection:
            return subsection in self._sections.get(section, {})
        return section in self._sections

    def get_sections(self):
        """
        Return a list of configuration sections or [blocks].
        
        :returns: List of sections.
        :rtype: list
        
        """
        return self.sections()
    
    def get_section_dict(self, section, subsection=None):
        """
        Return a dict representation of a section.
        
        :param section: The section of the configuration.
        :param subsection: The subsection of the configuration.
        :returns: Dictionary reprisentation of the config section.
        :rtype: dict
                
        """
        dict_obj = dict()
        for key in self.keys(section, subsection):
            dict_obj[key] = self.get(section, key, subsection=subsection)
        return dict_obj

    def add_section(self, section, subsection=None):
        """
        Adds a block section to the config.
        
        :param section: The section to add.
        
        """
        if subsection:
            if not self.has_section(section):
                raise NoSectionError(section)
            if subsection in self._sections[section]:
                raise DuplicateSectionError(section)
            self._sections[section][subsection] = self._dict()
        else:
            if section in self._sections:
                raise DuplicateSectionError(section)
            self._sections[section] = self._dict()

    def del_section(self, section, subsection=None):
        """
        Deletes a block section to the config.
        
        :param section: The section to delete.
        :param subsection: The section to delete.
        
        """
        if subsection:
            if not self.has_section(section):
                raise NoSectionError(section)
            if not subsection in self._sections[section]:
                raise NoSectionError(subsection)
            del self._sections[section][subsection]
        else:
            if not self.has_section(section):
                raise NoSectionError(section)
            del self._sections[section]
        
    def save(self, config, filename):
        """Save configuration to file"""
        config_d = config_to_dict(config)
        with open(filename, "w") as fh:
            fh.write(yaml.safe_dump(config_d, default_flow_style=False, allow_unicode=True, width=1000))


# This is hackish; we can't use one instance to hold both config file
# and custom-config file since parent tasks settings in the latter
# will override those in the former, violating rules about pipeline
# immutability
class RatatoskCustomConfigParser(RatatoskConfigParser):
    """Ratatosk configuration parser. Works on yaml files.
    """
    _instance = None
    _custom_config_paths = []

    @classmethod
    def add_config_path(cls, path):
        if path and not any(os.path.samefile(path, x) for x in cls._instance._custom_config_paths):
            logger.debug("adding config path {}".format(path))
            cls._instance._custom_config_paths.append(path)
        else:
            return
        cls._instance.reload()

    @classmethod
    def del_config_path(cls, path):
        if path and path in cls._instance._custom_config_paths:
            logger.debug("removing config path {}".format(path))
            try:
                i = cls._instance._custom_config_paths.index(path)
                del cls._instance._custom_config_paths[i]
            except ValueError:
                logger.warn("No such path {} in _custom_config_paths".format(path))
                
        else:
            return
        # Need to clear sections before reloading
        cls._instance._sections = cls._cls_dict()
        cls._instance.reload()

    @classmethod
    def clear(cls):
        cls._instance._custom_config_paths = []
        cls._instance._sections = cls._cls_dict()

    def reload(self):
        return self._instance.read(self._instance._custom_config_paths)

def get_config():
    return RatatoskConfigParser.instance()

def get_custom_config():
    """Get separate parser for custom config; else custom config
    parent_task setting will override config file settings"""
    return RatatoskCustomConfigParser.instance()

def setup_config(config_file=None, custom_config_file=None, **kwargs):
    """Helper function to setup config at startup"""
    if config_file:
        config = get_config()
        config.add_config_path(config_file)
        backend.__global_config__ = update(backend.__global_config__, vars(config)["_sections"])
    if custom_config_file:
        custom_config = get_custom_config()
        custom_config.add_config_path(custom_config_file)
        backend.__global_config__ = update(backend.__global_config__, vars(custom_config)["_sections"])



