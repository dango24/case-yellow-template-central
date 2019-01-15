
"""
**network_ubuntu** - Shim which is responsible for handling network state 
    tracking and network change events for Ubuntu.

:platform: Ubuntu
:synopsis: Package which provides various facilities for querying network status.

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""


#MARK: Imports
import logging
from . import NetworkState
import acme


class NetworkStateUbuntu(NetworkState):
    """
    Class used to track current network state
    """
    
    logger_name = "network.ubuntu-shim"

            


