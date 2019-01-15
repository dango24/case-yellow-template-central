"""
.. module:: acme.plugin
    :synopsis: Module containing classes used by the ACME plugin system
    :platform: RHEL, OSX, Ubuntu
    
.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>
    
"""

import datetime
import hashlib
import imp
import inspect
import logging
import json
import os

import acme


class PluginController(acme.SerializedObject):
    """
    Class to manage loading plugin files.
    
    .. warning:
        The trust system with plugins is currently based on filesystem 
        discretionary access controls, which can be abused in a variety of ways.
        We should be looking to implement a manifest system/cryptographic
        verification of module files prior to loading them. 
        
    """

    path = None             #: The path of our module file (or package directory)
    plugin_type = None      #: The type of plugin used.
    classes = None          #: List of classes which we will seek to import. 
    
    plugins = []            #: Loaded plugins
    
    
    def __init__(self, path=None, domain=None, classes=None, *args, **kwargs):
        """
        
        :param path: The path to search for plugins
        :type path: string
        :param domain: The plugin domain ('i.e.' ServiceAgent, SessionAgent, ComplianceModule)
        :type domain: string
        :param target_classes: A list of Class references to use to filter descendents
        :type target_classes: List of class references
        
        
        """
        
        if path is not None:
            self.path = path
            
        if domain is not None:
            self.domain = domain
            
        if classes is not None:
            self.classes = classes
    
    def load(self):
        """
        Method to search and load our plugin files.
        """
        
        self.plugins = self.get_plugins(path=self.path, classes=self.classes)
    
    def get_plugins(self, path=None, classes=None, version=None):
        """
        Function which will return a list of plugins from the provided path.
        """
        
        logger = logging.getLogger(self.logger_name)

        if path is None:
            path = self.path
            
        if classes is None:
            classes = self.classes
        
        plugins = []
        
        fs_items = []
        
            
        ## If this is a Richter package, re-call ourselves with modified
        ## path
        if self.is_richter_package(path):
            richter_path = os.path.join(path, "module")
            richter_version = self.richter_package_version(path)
            plugins = self.get_plugins(path=richter_path,
                                    classes=classes,
                                    version=richter_version)
            return plugins
        
        if os.path.isdir(path):
            fs_items = map(lambda x: os.path.join(path,x),os.listdir(path))
        elif os.path.isfile(path):
            fs_items.append(path)
            
        for fs_item in fs_items:
            p = None
            
            ## if we're a Richter package, adjust our path and recurse
            if self.is_richter_package(fs_item):
                richter_path = os.path.join(fs_item, "module")
                richter_version = self.richter_package_version(fs_item)
                plugins.extend(self.get_plugins(path=richter_path,
                                                classes=classes,
                                                version=richter_version))
            
            elif os.path.isfile(fs_item):
                if (fs_item.lower().endswith(".py") 
                                    and not fs_item.lower().startswith("__init__")):
                    p = Plugin(path=fs_item, target_classes=classes, 
                                                            version=version)
                elif (fs_item.lower().endswith(".pyc")
                                    and not fs_item.lower().startswith("__init__")
                                    and not os.path.isfile(fs_item[:-1])):
                    p = Plugin(path=fs_item,target_classes=classes, 
                                                            version=version)
            elif (os.path.isdir(fs_item) 
                    and os.path.isfile(os.path.join(fs_item,"__init__.py"))):
                p = Plugin(path=fs_item,target_classes=classes, 
                                                            version=version)
            
            if p:
                start_time = datetime.datetime.utcnow()
                try:
                    p.load()
                except Exception as exp:
                    logger.error("Failed to load plugin from path:'{}' Error:{}".format(fs_item,exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                
                end_time = datetime.datetime.utcnow()
                p.load_time = end_time - start_time
                plugins.append(p)
        
        return plugins
    
    def richter_package_version(self, path):
        """
        Method to return the version string for the given path
        """
        
        logger = logging.getLogger(__name__)
        
        version = None
        try:
            with open(os.path.join(path,"version.json")) as fh:
                version = json.load(fh)["version"]
        except Exception as exp:
            logger.warning("Failed to determine version from Richter package:'{}'. {}".format(
                                                                path,
                                                                exp.message))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        return version
    
    def is_richter_package(self, path): 
        """
        Method which will attempt to determine if the provided path
        is a Richter package.
        """
        
        is_richter_package = True
        
        if not os.path.isdir(path):
            is_richter_package = False
        
        if not os.path.isfile(os.path.join(path,"bom.json")):
            is_richter_package = False
        
        if not os.path.isfile(os.path.join(path, "version.json")):
            is_richter_package = False
            
        if not os.path.isdir(os.path.join(path, "module")):
            is_richter_package = False
        
        return is_richter_package
        
    def get_targets(self):
        """
        Function which will return a list of plugins from the provided path.
        """
        
        targets = {}
        
        for plugin in self.plugins:
            targets.update(plugin.targets)
            
        return targets

class Plugin(acme.SerializedObject):
    """
    Class which represents an ACME plugin, which can be either a Python
    module (.py file), or a Python package (directory with __init__.py file).
    
    """
        
    path = None             #: The path of our module file (or package directory)
    module = None           #: The python module reference
    
    target_classes = []     #: List of classes which we will seek to import. 
    targets = {}            #: Targets imported, keyed by identifier
    
    load_time = None        #: :py:class:`datetime.timedelta` object representing our load_time
    load_failures = []      #: Load failures
    
    
    def __init__(self,path=None, target_classes=None, version=None, *args,**kwargs):
        """
        
        :param path: The path to the module file or package directory
        :type path: string
        :param target_classes: A list of classes who's decendents are represented by this plugin
        :type target_classes: List of Class References
        
        """
        
        if path:
            self.path = path
            
        if target_classes:
            self.target_classes = target_classes
            
        self._version = version
        
        key_map = { "path" : None,
                    "version" : "<getter=version>",
                    "hash" : "<getter=sha_hash>",
                    "size" : "<getter=size>",
                    "load_time" : "<type=timedelta>",
                }
            
        super(Plugin,self).__init__(key_map=key_map,*args,**kwargs)
        
    def load(self, path=None, target_classes=None):
        """
        Method to load our file from the provided path.
        
        :param path: The path to the module file or package directory
        :type path: string
        :param target_classes: A list of classes who's decendents are represented by this plugin
        :type target_classes: List of Class References
        
        """
        
        module_name = None
        module_import_path = None
        
        ## Reset state vars
        self.module = None
        self.targets = {}
        self.load_time = None
        self.load_failures = []
        is_richter_package = None
        
        try:
            if path is None:
                path = self.path
            else:
                self.path = path
            
            if target_classes is None:
                target_classes = self.target_classes
            else:
                self.target_classes = target_classes
                        
            if os.path.isfile(path):
                if (path.lower().endswith(".py") 
                                    and not path.lower().startswith("__init__")):
                    module_name = os.path.basename(path)[:-3]
                    module_import_path = os.path.dirname(path)
                elif (path.lower().endswith(".pyc")
                                    and not path.lower().startswith("__init__")):
                    module_name = os.path.basename(path)[:-4]
                    module_import_path = os.path.dirname(path)
                else:
                    module_name = os.path.basename(path)
                    module_import_path = os.path.dirname(path)
            elif os.path.isdir(path):
                module_name = os.path.basename(path)
                module_import_path = os.path.dirname(path)
            else:
                raise PluginLoadFailure("Failed to load! no plugin exists at path:'{}'".format(path))
            
            
            module_data = imp.find_module(module_name,[module_import_path])
            module = imp.load_module(module_name,*module_data)
            
            self.module = module
            if self._version:
                module.version = self._version
            
            self.targets = self.get_targets(module=module,
                                                    classes=target_classes)
        except Exception as exp:
            self.load_failures.append("{}".format(exp))
            raise
        
    def get_targets(self,module=None,classes=None):
        """
        Method which will return a list of targets from the provided module.
        
        :param module: The path to the module file or package directory
        :type module: Python module
        
        :param classes: List of Class references to use as descendent references
        
        """
        
        if module is None:
            module = self.module
            
        if classes is None:
            classes = self.target_classes
        
        targets = {}
        
        for name,obj in inspect.getmembers(module):
            if inspect.isclass(obj):
                for target_class in classes:
                    if issubclass(obj,target_class):
                        target = obj()
                        target.version = self.version()
                        identifier = target.__class__.__name__
                        
                        try:
                            identifier = target.identifier
                        except:
                            pass
                        
                        targets[identifier] = target
                        break     
        
        return targets
        
    def sha_hash(self,path=None):
        """
        Method which returns the SHA1 hash of the provided path. If no path
        is provided we will default to instance var `path`
        
        :param path: The path to the module file or package directory
        :type path: string
        
        """
        
        hash = None
        
        if path is None:
            path = self.path
        
        if os.path.isfile(path):
            sha = hashlib.sha256()
            
            with open(path,"rb") as fh:
                sha.update(fh.read())
                hash = sha.hexdigest()
        elif os.path.isdir(path):
            package_data = ""
            
            for item in os.listdir(path):
                hash = self.sha_hash(os.path.join(path,item))
                if hash is None:
                    hash = "NOHASH:'{}'\n".format(os.path.join(path,item))
                
                package_data += "{}\n".join(hash)
                
            sha = hashlib.sha256()
            sha.update(package_data)
            hash = sha.hexdigest()
            
        return hash
        
    def size(self,path=None):
        """
        The size of the plugin
        """
        
        size = None
        
        if path is None:
            path = self.path
        
        if os.path.isfile(path):
            size = os.path.getsize(path)
        elif os.path.isdir(path):
            dir_size = 0
            for item in os.listdir(path):
                item_size = self.size(os.path.join(path,item))
                if item_size:
                    dir_size += item_size
            size = dir_size
        
        return size
    
    def version(self):
        """
        Method to return our plugin version.
        """
        
        version = self._version
        
        if not version:
            try:
                version = self.module.version
            except:
                pass
        
        if not version:
            try:
                version = self.module.__version__
            except:
                pass
        
        return version
    


class PluginLoadFailure(Exception):
    """
    Exception thrown in the event of a failure to load a plugin.
    """
    pass    
            
    

