"""**proxy_ubuntu** - Package which provides functionality
    related to the configuration of system proxy settings on Ubuntu
    
:platform: Ubuntu
:synopsis: This is the root module that is used to establish a common 
    interrogation interface for configuring system proxy settings across
    multiple client platforms

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import logging
import os
import socket
import subprocess
import sys

import systemprofile

from . import ProxyProfileBase
from .. import profiler as networkprofiler

from . import ProxyConfigError
from . import ProxyConfigInterfaceError
from . import ProxyConfigServiceError

#MARK: -
#MARK: Classes
class ProxyProfileUbuntu(ProxyProfileBase):
    """
    Class which provides system interogation routines for common query 
    elements, such as hostname, IP information, connectivity data, etc...

    This class will typically be monkey-patched by an OS-specific 
    implementation.
    """
    
    logger_name = "ProxyProfileUbuntu"
    
    
        
