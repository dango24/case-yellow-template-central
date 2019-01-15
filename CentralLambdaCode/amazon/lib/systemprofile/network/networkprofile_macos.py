"""
**networkprofile_macos** - Package which is responsible for interogating various
    system network components for the macOS platform.

:platform: macOS
:synopsis: This is the module that is used to provide a common 
    interrogation interface for network functions on macOS platform

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import datetime
import logging
import os
import re
import subprocess
import sys

from . import NetworkProfileBase

#MARK: -
#MARK: Classes
class NetworkProfileMacOS(NetworkProfileBase):
    """
    Class which provides system interogation routines for common query 
    elements, such as hostname, IP information, connectivity data, etc...
    
    This class will typically be monkey-patched by an OS-specific 
    implementation.
    """
    
    logger_name = "NetworkProfileMacOS"
    
    sysfile_lock = None
    
    def __init__(self,file_dir=None):
        """
        Constructor
        """
        self.networksetup = "/usr/sbin/networksetup"
        
        self.service_map_cache = {}
        self.service_map_cache_ttl = datetime.timedelta(seconds=30)
    
    def network_interfaces(self):
        """
        Method which returns all network interfaces
        """
        
        interfaces = {}
        
        ## Compile our regex
        interface_re = re.compile("^([^\s?].*?): flags=(\d+)<.*> mtu (\d+)")
        ether_re = re.compile("ether (.*)$")
        status_re = re.compile("status: (.*)$")
        ipv4_re = re.compile("inet ([\.|\d]*) netmask ([^\s]*?)\s(broadcast (.*))?")
        
        cmd = ["/sbin/ifconfig"]
        
        try:
            output = subprocess.check_output(cmd)
            return_code = 0
        except subprocess.CalledProcessError as exp:
            raise Exception("Failed to run command ('{}'): {}.".format(
                                                        "' '".join(cmd),exp)), None, sys.exc_info()[2]
        
        interface = {}
        for line in output.splitlines():
            r = interface_re.findall(line)
            if r:
                dev, flags, mtu = r[0]
                interface = { "dev" : dev,
                                "mtu": mtu,
                                "flags" : flags}
                interfaces[dev] = interface
                continue
            
            r = ether_re.findall(line)
            if r:
                interface["ether"] = r[0]
                continue
            
            r = status_re.findall(line)
            if r:                
                interface["status"] = r[0]
                continue
            
            r = ipv4_re.findall(line)
            if r:
                # example r[0] = [('10.0.2.16', '0xffffff00', 'broadcast 10.0.2.255', '10.0.2.255')]
                ipv4, netmask, rejunk, broadcast = r[0]
                ip_data = {}
                ip_data["ipv4"] = ipv4
                ip_data["netmask"] = netmask
                ip_data["broadcast"] = broadcast
                
                if "ip_addresses" in interface:
                    interface["ip_addresses"].append(ip_data)
                else:
                    interface["ip_addresses"] = [ip_data]
                
                continue
        
        return interfaces
    
    def active_interfaces(self):
        """
        Method which returns all active interfaces
        """
        
        return {dev: data for dev, data in self.network_interfaces().iteritems() 
                    if "status" in data and data["status"] == "active" and
                        "ip_addresses" in data and data["ip_addresses"]}            
                
        
    #MARK: networksetup methods
    def ns_list_network_services(self):
        """
        Method to return a list of Network Services
        """
        
        logger = logging.getLogger(self.logger_name)
        
        services = []
        
        cmd = [self.networksetup, "-listallnetworkservices"]
                                                                
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
            return_code = 0
        except subprocess.CalledProcessError as exp:
            raise Exception("Failed to get mac address for interface {}: {}.".format(
                                                interface,
                                                exp)), None, sys.exc_info()[2]
        
        for line in output.splitlines():
            ## Skip first line of output ("An asterisk (*) denotes that a network service is disabled.")
            if line.startswith("An asterisk"):
                continue
            
            ## Filter out disable interface marker
            if line.startswith("*"):
                services.append(line[1:])
            else:
                services.append(line)
        
        return services
        
    def ns_get_mac_address(self, interface):
        """
        Method to resolve a MAC address for a given interface using
        the networksetup cli binary.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        mac_address = None
        
        cmd = [self.networksetup, "-getmacaddress", interface]
                                                                
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
            return_code = 0
        except subprocess.CalledProcessError as exp:
            raise Exception("Failed to get mac address for interface {}: {}.".format(
                                                interface,
                                                exp)), None, sys.exc_info()[2]
        
        search = re.match("^.*: (.*) \(.*", output)
        if search:
            mac_address = search.groups()[0]
        
        return mac_address
        
    def ns_servicename_for_interface(self, interface):
        """
        Method to return a networksetup networkservice name for the provided
        interface. This method keeps a short-lived cache to prevent 
        flooding CLI calls.
        
        :param string interface: The interface to lookup (i.e. "en0")
        
        :returns: string - The network service name (i.e. "Wi-Fi")
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        now = datetime.datetime.utcnow()
        
        if interface in self.service_map_cache.keys():
            if now < self.service_map_cache[interface]["date"] + self.service_map_cache_ttl:
                service_name = self.service_map_cache[interface]["service"]
                logger.log(5,"Found interface:'{}' in service_map_cache, returning networkservice:'{}'".format(
                                                interface, service_name))
                return service_name
        
        try:
            my_mac = self.ns_get_mac_address(interface)
        except Exception:
            logger.debug("Failed to lookup mac address for interface:{}".format(
                                                                interface))
        
        servicename = None
        for service in self.ns_list_network_services():
            if interface.lower() == service.lower():
                servicename = service
                break
            
            try:
                service_mac = self.ns_get_mac_address(service)
                if my_mac == service_mac:
                    servicename = service
            except Exception:
                pass
        
        if servicename:
            self.service_map_cache[interface] = { "date" : now,
                                                    "service" : servicename }
        return servicename
    
    
        
