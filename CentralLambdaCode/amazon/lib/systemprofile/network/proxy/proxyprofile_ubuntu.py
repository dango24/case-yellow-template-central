"""**proxyprofile_ubuntu** - Package which provides functionality
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

PROXY_MODE_NONE = "none"
PROXY_MODE_MANUAL = "manual"

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
    
    def __init__(self):
        """
        Constructor
        """
        
        self.gsettings = "/usr/bin/gsettings"
        
        ## Map our proxy service to gsettings paths
        self.gsettings_service_map = { "http" :  "org.gnome.system.proxy.http",
                            "https" : "org.gnome.system.proxy.https",
                            "ftp" : "org.gnome.system.proxy.ftp",
                            "socks" : "org.gnome.system.proxy.socks",
            }
    
        ProxyProfileBase.__init__(self)

    def _proxyname_for_service(self, service):
        """
        Method to return a gsettings configuration path for the provided service 
        for reference by the gsettings CLI program.
        
        i.e. "https" returns "org.gnome.system.proxy.https"
        """
        
        service = service.lower()
                    
        if service in self.gsettings_service_map.keys():
            return self.gsettings_service_map[service]
    
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
            str_state = "true"
        else:
            str_state = "false"
        
        return str_state
    
    def set_proxy_mode(self, value):
        """
        Method which sets the global proxy mode via gsettings.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        cmd = [self.gsettings, "set", "org.gnome.system.proxy", 
                                                            "mode", value]
                                                            
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as exp:
            raise ProxyConfigError("Failed to configure proxy for service {}: {}.".format(
                                                service,
                                                exp)), None, sys.exc_info()[2]
        
        
    
    #MARK: Ubuntu Overrides
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
                
        if not name:
            raise ProxyConfigServiceError("Service: {} could not be found.".format(
                                                                    service))
        if interface:
            logger.warning("set_proxy() - interface is not supported, ignoring...")
            
        cmds = []
        
        if address:
            cmd = [self.gsettings, "set", "org.gnome.system.proxy", 
                                                    "use-same-proxy", "false"]        
            cmds.append(cmd)
            
        cmd = [self.gsettings, "set", name, "host", address]        
        cmds.append(cmd)
        
        cmd = [self.gsettings, "set", name, "port", str(port)]        
        cmds.append(cmd)
        
        logger_cmds = cmds[:]
        
        if username and password:
            cmd = [self.gsettings, "set", name, "authentication-user", username]    
            cmds.append(cmd)
            logger_cmds.append(cmd)
            
            cmd = [self.gsettings, "set", name, "authentication-password", password]    
            cmds.append(cmd)
            logger_cmd = cmd[:-1]
            logger_cmd.append("********")
            logger_cmds.append(logger_cmd)
            
            cmd = [self.gsettings, "set", name, "use-authentication", "true"]    
            cmds.append(cmd)
            logger_cmds.append(cmd)
        
        else:
            cmd = [self.gsettings, "set", name, "authentication-user", ""]    
            cmds.append(cmd)
            logger_cmds.append(cmd)
            
            cmd = [self.gsettings, "set", name, "authentication-password", ""]    
            cmds.append(cmd)
            logger_cmds.append(cmd)
            
            cmd = [self.gsettings, "set", name, "use-authentication", "false"]    
            cmds.append(cmd)
            logger_cmds.append(cmd)
        
        
        cmd = [self.gsettings, "set", name, "enabled", "true"]        
        cmds.append(cmd)
        logger_cmds.append(cmd)
        
        for cmd, logger_cmd in zip(cmds, logger_cmds):
            try:
                logger.log(5,"Running command: ('{}')".format("' '".join(logger_cmd)))
                output = subprocess.check_output(cmd)
            except subprocess.CalledProcessError as exp:
                raise ProxyConfigError("Failed to configure proxy for service {}: {}.".format(
                                                    service,
                                                    exp)), None, sys.exc_info()[2]
        
        if address:
            self.set_proxy_mode(PROXY_MODE_MANUAL)
    
    def set_proxy_bypassaddresses(self, addresses, interface=None):
        """
        Method to set proxy bypass addresses for a given service
        
        :param addresses: A list of addresses to bypass
        :type addresses: list<string>
        :param str interface: Our network interface name (not supported on all platforms)
        """
        
        logger = logging.getLogger(self.logger_name)
          
        if interface:
            logger.warning("set_proxy_bypassaddresses() - interface is not supported, ignoring...")
                                                    
        cmds = []
        
        cmd = [self.gsettings, "set", "org.gnome.system.proxy", "ignore-hosts"]        
        
        if addresses:
            cmd.append("{}".format(addresses))
        else:
            cmd.append("")
        
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
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
        
        name = self._proxyname_for_service(service)
                
        if not name:
            raise ProxyConfigServiceError("Service: {} could not be found.".format(
                                                                    service))
        if interface:
            logger.warning("set_proxy() - interface is not supported, ignoring...")
            
        str_state = self._statestring_for_value(state)
        
        cmd = [self.gsettings, "set", name, "enabled", str_state]        
        
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as exp:
            raise ProxyConfigError("Failed to configure proxy state for service {}: {}.".format(
                                                service,
                                                exp)), None, sys.exc_info()[2]
        
    def set_proxyautoconfig_url(self, url, interface=None):
        """
        Enable proxy auto configuration via a PAC file at the specified URL
        
        :param str url: Our URL to use for PAC
        :param str interface: Our network interface name (not supported on all platforms)
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if interface:
            logger.warning("set_proxyautoconfig_url() - interface is not supported, ignoring...")
        
        cmd = [self.gsettings, "set", "org.gnome.system.proxy", 
                                                        "autoconfig-url", url]        
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as exp:
            raise ProxyConfigError("Failed to configure PAC URL: {}.".format(exp)), None, sys.exc_info()[2]
            
        self.set_proxy_mode(PROXY_MODE_MANUAL)
        
    def set_proxyautoconfig_state(self, state, interface=None):
        """
        Enable or disable proxy auto configuration via a PAC file.
        
        :param bool state: Our state (enabled=True, disabled=False)
        :param str interface: Our network interface name (not supported on all platforms)

        """
        
        logger = logging.getLogger(self.logger_name)
        
        if interface:
            logger.warning("set_proxyautoconfig_url() - interface is not supported, ignoring...")
        
        if not state:
            cmd = [self.gsettings, "set", "org.gnome.system.proxy", 
                                                        "autoconfig-url", ""]        
            try:
                logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
                output = subprocess.check_output(cmd)
            except subprocess.CalledProcessError as exp:
                raise ProxyConfigError("Failed to configure PAC URL: {}.".format(exp)), None, sys.exc_info()[2]
            
        if state:
            self.set_proxy_mode(PROXY_MODE_MANUAL)
        else:
            self.set_proxy_mode(PROXY_MODE_NONE)
        