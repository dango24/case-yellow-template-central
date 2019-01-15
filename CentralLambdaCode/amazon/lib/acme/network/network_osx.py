
"""
**network_osx** - Shim which is responsible for handling network state 
    tracking and network change events for OS X.

:platform: OSX
:synopsis: Package which provides various facilities for querying network status.

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""


#MARK: Imports
import logging
from . import NetworkState
import acme


class NetworkStateOSX(NetworkState):
    """
    Class used to track current network state
    """
    
    logger_name = "network.osx-shim"


        


