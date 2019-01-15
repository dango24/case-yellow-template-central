"""**proxy_macos** - Package which provides functionality
    related to the configuration of system proxy settings on macOS
    
:platform: macOS
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
class ProxyProfileMacOS(ProxyProfileBase):
    """
    Class which provides system interogation routines for common query 
    elements, such as hostname, IP information, connectivity data, etc...

    This class will typically be monkey-patched by an OS-specific 
    implementation.
    """
    
    logger_name = "ProxyProfileMacOS"
        
    def __init__(self):
        """
        Constructor
        """
        
        self.networksetup = "/usr/sbin/networksetup"
        
        ## Map our proxy service to macOS 'networksetup' actions
        self.networksetup_service_map = { "http" :  "webproxy",
                    "https" : "securewebproxy",
                    "ftp" : "ftpproxy",
                    "rtsp" : "streamingproxy",
                    "gopher" : "gopherproxy",
                    "socks" : "socksfirewallproxy",
                }
        
        ProxyProfileBase.__init__(self)
    
    def _proxyname_for_service(self, service):
        """
        Method to return a networksetup proxy name for the provided service for
        reference by the networksetup CLI program.
        
        i.e. "https" returns "securewebproxy"
        """
        
        service = service.lower()
                    
        if service in self.networksetup_service_map.keys():
            return self.networksetup_service_map[service]
    
    def _statestring_for_value(self,value):
        """
        Method to return a state string for the provided value for use
        with the networksetup CLI program.
        
        i.e. "off", 0, "0", "disabled" all return "off"
        
        """
        
        bool_state = None
        
        try:
            if value.lower() == "off" or value == "0":
                bool_state = False
            elif value.lower() == "disabled":
                bool_state = False
            elif value.lower() == "disable":
                bool_state = False
        except AttributeError:
            pass
        
        if bool_state is None:
            bool_state = bool(value)
        
        if bool_state:
            str_state = "on"
        else:
            str_state = "off"
        
        return str_state
    
    #MARK: macOS Overrides
    def set_proxy(self, service, address, port, interface=None, username=None, 
                                                password=None):
        """
        Method that configures our proxy server for the provided interface. To disable proxy
        configurations, pass url of 'None'
        
        :param string service: The service to configure (i.e. 'http', 'https')
        :type service: string
        :param string address: The proxy server IP or URL (i.e. 'proxy.amazon.com')
        :param int port: The proxy server TCP port number
        :param string interface: The interface to configure (i.e. 'en0')
        :param string username: The username to use for authentication (omit if unauthenticated)
        :param string password: The password to use for authentication (requires valid username)
    
        """
        
        logger = logging.getLogger(self.logger_name)
        
        name = self._proxyname_for_service(service)
        
        networkservice = networkprofiler.ns_servicename_for_interface(interface)
        
        if not name:
            raise ProxyConfigServiceError("Service: {} could not be found.".format(
                                                                    service))
        
        if not networkservice:
            raise ProxyConfigInterfaceError("Interface: {} could not be resolved.".format(
                                                                    interface))
        
        cmd = [self.networksetup, "-set{}".format(name), networkservice, 
                                                address, "{}".format(port)]
        
        logger_cmd = []
        logger_cmd.extend(cmd)
        
        if username and password:
            cmd.append("on")
            cmd.extend([username, password])
            
            logger_cmd.append("on")
            logger_cmd.extend([username, "******"])
        else:
            cmd.append("off")
            logger_cmd.append("off")
        
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(logger_cmd)))
            output = subprocess.check_output(cmd)
            return_code = 0
        except subprocess.CalledProcessError as exp:
            raise ProxyConfigError("Failed to configure proxy for service {}: {}.".format(
                                                service,
                                                exp)), None, sys.exc_info()[2]
    
    def set_proxy_bypassaddresses(self, addresses, interface=None):
        """
        Method to set proxy bypass addresses for a given service
        
        :param addresses: A list of addresses to bypass
        :type addresses: list<string>
        :param str interface: Our network interface name (not supported on all platforms)
        """
        
        logger = logging.getLogger(self.logger_name)
        
        networkservice = networkprofiler.ns_servicename_for_interface(interface)
        
        if not networkservice:
            raise ProxyConfigInterfaceError("Interface: {} could not be resolved.".format(
                                                                    interface))
        
        cmd = [self.networksetup, "-setproxybypassdomains", networkservice]
        
        if addresses:
            cmd.extend(addresses)
        else:
            cmd.append("")
        
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
            return_code = 0
        except subprocess.CalledProcessError as exp:
            raise ProxyConfigError("Failed to configure proxy addresses for interface:{} Error:{}.".format(interface, exp)), None, sys.exc_info()[2]
    
    def set_proxy_service_state(self, service, state, interface=None):
        """
        Method to enable or disable our proxy service for the given interface
        
        :param str service: Our proxy service to configure
        :param bool state: Our state (enabled=True, disabled=False)
        :param str interface: Our network interface name (not supported on all platforms)
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        str_state = self._statestring_for_value(state)
        
        name = self._proxyname_for_service(service)
        
        networkservice = networkprofiler.ns_servicename_for_interface(interface)
        
        if not name:
            raise ProxyConfigServiceError("Service: {} could not be found.".format(
                                                                    service))
        if not networkservice:
            raise ProxyConfigInterfaceError("Interface: {} could not be resolved.".format(
                                                                    interface))
        
        cmd = [self.networksetup, "-set{}state".format(name), networkservice, 
                                                                    str_state]
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as exp:
            raise ProxyConfigError("Failed to configure proxy state: {}.".format(exp)), None, sys.exc_info()[2]
    
    def set_proxy_autodiscovery_state(self, state, interface=None):
        """
        Enable proxy autodiscovery for the given interface.
        
        :param bool state: Our state (enabled=True, disabled=False)
        :param str interface: Our network interface name (not supported on all platforms)
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        str_state = self._statestring_for_value(state)
        
        networkservice = networkprofiler.ns_servicename_for_interface(interface)
        
        cmd = [self.networksetup, "-setproxyautodiscovery", networkservice, str_state]
        
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as exp:
            raise ProxyConfigError("Failed to configure proxy autoddiscovery state: {}.".format(exp)), None, sys.exc_info()[2]
            
    def set_proxyautoconfig_url(self, url, interface=None):
        """
        Enable proxy auto configuration via a PAC file at the specified URL
        
        :param str url: Our URL to use for PAC
        :param str interface: Our network interface name (not supported on all platforms)
        
        """
        
        logger = logging.getLogger(self.logger_name)
                
        networkservice = networkprofiler.ns_servicename_for_interface(interface)
        
        cmd = [self.networksetup, "-setautoproxyurl", networkservice, url]
        
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as exp:
            raise ProxyConfigError("Failed to configure proxy autoddiscovery state: {}.".format(exp)), None, sys.exc_info()[2]
    
    def set_proxyautoconfig_state(self, state, interface=None):
        """
        Enable or disable proxy auto configuration via a PAC file.
        
        :param bool state: Our state (enabled=True, disabled=False)
        :param str interface: Our network interface name (not supported on all platforms)

        """
        
        logger = logging.getLogger(self.logger_name)
        
        str_state = self._statestring_for_value(state)
        
        networkservice = networkprofiler.ns_servicename_for_interface(interface)
        
        cmd = [self.networksetup, "-setautoproxystate", networkservice, str_state]
        
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as exp:
            raise ProxyConfigError("Failed to configure proxy autoddiscovery state: {}.".format(exp)), None, sys.exc_info()[2]
        
