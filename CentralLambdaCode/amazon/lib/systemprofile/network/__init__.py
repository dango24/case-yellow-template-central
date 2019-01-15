"""**systemprofile.network** - Package which is responsible for interogating various
    system network components to return commonly needed data points.

:platform: RHEL5, OSX, Ubuntu
:synopsis: This is the module that is used to establish a common 
    interrogation interface for network settings
    across various platforms and data systems.

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import codecs
import json
import logging
import os
import socket
import subprocess
import threading

import systemprofile

#MARK: Defaults
DEFAULT_DIG_DOMAINTEST_RETRIES=1

#MARK: -
#MARK: Classes
class NetworkProfileBase(object):
    """
    Class which provides system interogation routines for common query 
    elements, such as hostname, IP information, connectivity data, etc...
    
    This class will typically be monkey-patched by an OS-specific 
    implementation.
    """
    
    logger_name = "NetworkProfile"
    
    sysfile_lock = None
    
    def __init__(self,file_dir=None):
        """
        Constructor
        """
        pass
        
#MARK: Module vars
profiler = NetworkProfileBase()

def configure_macos():
    """
    Method to configure this module for use with OS X
    """
    
    global profiler
    
    import networkprofile_macos
    
    profiler = networkprofile_macos.NetworkProfileMacOS()
    
def configure_ubuntu():
    """
    Method to configure this model for use with Linux
    """
    
    global profiler
    
    import networkprofile_ubuntu
    
    profiler = networkprofile_ubuntu.NetworkProfileUbuntu()

platform = systemprofile.current_platform()
if platform == "OS X" or platform == "macOS":
    configure_macos()
elif platform == "Ubuntu":
    configure_ubuntu()

import proxy
profiler.proxy = proxy.profiler

