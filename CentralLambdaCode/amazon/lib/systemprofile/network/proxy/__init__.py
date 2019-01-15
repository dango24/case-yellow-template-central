"""**systemprofile.network.proxy** - Package which provides functionality
    related to the configuration of system proxy settings.

:platform: macOS, Ubuntu
:synopsis: This is the root module that is used to establish a common 
    interrogation interface for configuring system proxy settings across
    multiple client platforms

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import codecs
import json
import logging
import platform as platform_module
import os
import socket
import subprocess
import systemprofile
import threading

#MARK: Defaults
DEFAULT_DIG_DOMAINTEST_RETRIES=1

#MARK: -
#MARK: Classes
class ProxyProfileBase(object):
    """
    Class which provides system interogation routines for common query 
    elements, such as hostname, IP information, connectivity data, etc...
    
    This class is essentially an abstract class and will be monkey-patched 
    by an OS-specific implementation to provide function.
    """
    
    def set_proxy(self, service, address, port, interface=None, username=None, 
                                                password=None):
        """
        Method that configures our proxy server for the provided interface. To disable proxy
        configurations, pass url of 'None'
        
        :param string service: The service to configure (i.e. 'http', 'https')
        :type service: string
        :param string address: The proxy server IP or URL (i.e. 'proxy.amazon.com')
        :param int port: The proxy server TCP port number
        :param string interface: The interface to configure (i.e. 'en0'). This is not supported on all platforms.
        :param string username: The username to use for authentication (omit if unauthenticated)
        :param string password: The password to use for authentication (requires valid username)
    
        """
        
        raise systemprofile.PlatformUnsupportedError("set_proxy() is not supported on platform: {}".format(systemprofile.platform))
    
    def set_proxy_bypassaddresses(self, addresses, interface=None):
        """
        Method to set proxy bypass addresses for a given service
        
        :param addresses: A list of addresses to bypass
        :type addresses: list<string>
        :param str interface: Our network interface name (not supported on all platforms)
        """
        
        raise systemprofile.PlatformUnsupportedError("set_proxy_bypassaddresses() is not supported on platform: {}".format(systemprofile.platform))
        
    def set_proxy_service_state(self, service, state, interface=None):
        """
        Method to enable or disable our proxy service for the given interface
        
        :param str service: Our proxy service to configure
        :param bool state: Our state (enabled=True, disabled=False)
        :param str interface: Our network interface name (not supported on all platforms)
        
        """
        
        raise systemprofile.PlatformUnsupportedError("set_proxy_service_state() is not supported on platform: {}".format(systemprofile.platform))
        
    def set_proxy_autodiscovery_state(self, state, interface=None):
        """
        Enable proxy autodiscovery for the given interface.
        
        :param bool state: Our state (enabled=True, disabled=False)
        :param str interface: Our network interface name (not supported on all platforms)
        
        """
        
        raise systemprofile.PlatformUnsupportedError("set_proxy_autodiscovery_state() is not supported on platform: {}".format(systemprofile.platform))
        
    def set_proxyautoconfig_url(self, url, interface=None):
        """
        Enable proxy auto configuration via a PAC file at the specified URL
        
        :param str url: Our URL to use for PAC
        :param str interface: Our network interface name (not supported on all platforms)
        
        """
        
        raise systemprofile.PlatformUnsupportedError("set_proxyautoconfig_url() is not supported on platform: {}".format(systemprofile.platform))
    
    def set_proxyautoconfig_state(self, state, interface=None):
        """
        Enable or disable proxy auto configuration via a PAC file.
        
        :param bool state: Our state (enabled=True, disabled=False)
        :param str interface: Our network interface name (not supported on all platforms)

        """
        
        raise systemprofile.PlatformUnsupportedError("set_proxyautoconfig_url() is not supported on platform: {}".format(systemprofile.platform))
        
    
#MARK: Module vars
def configure_macos():
    """
    Method to configure this module for use with macOS
    """
    
    global profiler
    
    import proxyprofile_macos
    
    profiler = proxyprofile_macos.ProxyProfileMacOS()

def configure_ubuntu():
    """
    Method to configure this model for use with Linux
    """
    
    global profiler
    
    import proxyprofile_ubuntu
    
    profiler = proxyprofile_ubuntu.ProxyProfileUbuntu()

#MARK: Exceptions -
class ProxyConfigError(Exception):
    """
    Exception which is thrown when we fail to configure our proxy service.
    """
    pass

class ProxyConfigInterfaceError(ProxyConfigError):
    """
    Exception which is thrown when we fail to configure our proxy service
    due to problems with the interface.
    """
    pass

class ProxyConfigServiceError(ProxyConfigError):
    """
    Exception which is thrown when we fail to configure our proxy service
    due to problems configuring a service.
    """
    pass


#MARK: Module Code -
profiler = ProxyProfileBase()
platform = systemprofile.current_platform()
if platform == "OS X" or platform == "macOS":
    configure_macos()
elif platform == "Ubuntu":
    configure_ubuntu()
