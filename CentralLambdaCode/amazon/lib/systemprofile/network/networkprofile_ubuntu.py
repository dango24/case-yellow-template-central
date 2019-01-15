"""
**networkprofile_ubuntu** - Package which is responsible for interogating various
    system network components for the Ubuntu platform.

:platform: Ubuntu
:synopsis: This is the module that is used to provide a common 
    interrogation interface for network functions on Ubuntu platform

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import datetime
import logging
import os
import re
import subprocess
import sys
import platform

from . import NetworkProfileBase
from .. import get_english_env

#MARK: -
#MARK: Classes
class NetworkProfileUbuntu(NetworkProfileBase):
    """
    Class which provides system interogation routines for common query 
    elements, such as hostname, IP information, connectivity data, etc...
    
    This class will typically be monkey-patched by an OS-specific 
    implementation.
    """
    
    logger_name = "NetworkProfileUbuntu"
    
    def network_interfaces(self):
        """
        Method which returns all network interfaces
        
        :raises Exception: If an unknown error occurs. 
        
        """
        
        distro, version, id = platform.dist()
        
        if version.startswith("14") or version.startswith("16"):
            return self._network_interfaces14()
        else:
            return self._network_interfaces18()
    
    
    def _network_interfaces14(self):
        """
        Method which returns all network interfaces on Ubuntu14/16
        
        :raises Exception: If an unknown error occurs. 
        
        """
        
        interfaces = {}
        
        ## Compile our regex
        interface_re = re.compile("^([^\s?].*?)\s{4,10}Link encap:(.*?)\s{2}(HWaddr\s(.*)\s{2})?")
        
        status_re = re.compile("status: (.*)$")
        ipv4_re = re.compile("inet addr:([\.|\d]*)\s{2}(Bcast:([^\s]*?)\s{2})?(Mask:(.*))?")
        ipv6_re = re.compile("inet6 addr:\s?(.*)\s{1,2}(Scope:(.*))?")        
        other_re = re.compile("\s*(.*)\s{2}MTU:(.*)\s{2}Metric:(.*)")
        
        
        cmd = ["/sbin/ifconfig"]
        try:
            output = subprocess.check_output(cmd, env=get_english_env())
            return_code = 0
        except subprocess.CalledProcessError as exp:
            raise Exception("Failed to run command ('{}'): {}.".format(
                                                        "' '".join(cmd),exp)), None, sys.exc_info()[2]
        interface = {}
        for line in output.splitlines():
            r = interface_re.findall(line)
            if r:
                dev, encap, rejunk, ether = r[0]
                interface = { "dev" : dev,
                                "encap": encap,
                                "ether": ether}
                interfaces[dev] = interface
                continue
            
            r = other_re.findall(line)
            if r:
                raw_flags, mtu, metric = r[0]
                interface["str_flags"] = raw_flags.split(" ")
                interface["mtu"] = mtu
                interface["metric"] = metric
                if "RUNNING" in interface["str_flags"]:
                    interface["status"] = "active"
                else:
                    interface["status"] = "inactive"
                continue
            
            
            
            r = ipv4_re.findall(line)
            if r:
                # example r[0] = [('10.7.246.215', 'Bcast:10.7.247.255  ', '10.7.247.255', 'Mask:255.255.252.0', '255.255.252.0')]
                ipv4, rejunk, broadcast, rejunk1, netmask = r[0]
                ip_data = {}
                ip_data["ipv4"] = ipv4
                ip_data["netmask"] = netmask
                ip_data["broadcast"] = broadcast
                
                if "ip_addresses" in interface:
                    interface["ip_addresses"].append(ip_data)
                else:
                    interface["ip_addresses"] = [ip_data]
                
                continue
            
            r = ipv6_re.findall(line)
            if r:
                # example r[0] = [('fe80::4637:e6ff:fe61:40b6/64', 'Scope:Link', 'Link')]
                ipv6, rejunk, scope = r[0]
                ip_data = {}
                ip_data["ipv6"] = ipv6
                ip_data["scope"] = scope
                
                if "ip_addresses" in interface:
                    interface["ip_addresses"].append(ip_data)
                else:
                    interface["ip_addresses"] = [ip_data]
                
                continue
            
        return interfaces
    
    def _network_interfaces18(self):
        """
        Method which returns all network interfaces on Ubuntu18
        
        :raises Exception: If an unknown error occurs. 
        
        """
        
        interfaces = {}
        
        ## Compile our regex
        interface_re = re.compile("^([^\s?].*?): flags=\d+(<.*>)  mtu (\d+)")
        ipv4_re = re.compile("inet ([\.|\d]*)  netmask ([^\s]*?) \s(broadcast (.*))?")
        ipv6_re = re.compile("inet6 \s?(.*)  prefixlen (\d+)  (scopeid (.*))?")        
        ether_re = re.compile("ether (.*)  txqueuelen (\d+)  (.*)")
        
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
                dev, str_flags, mtu = r[0]
                interface = { "dev" : dev,
                                "mtu": mtu,
                                "str_flags" : str_flags}
                interfaces[dev] = interface
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
            
            r = ipv6_re.findall(line)
            if r:
                # example r[0] = [('fe80::4637:e6ff:fe61:40b6', '64', 'scopeid Link', 'Link')]
                ipv6, prefixlen, rejunk, scope = r[0]
                ip_data = {}
                ip_data["ipv6"] = ipv6
                ip_data["prefixlen"] = prefixlen
                ip_data["scope"] = scope
                
                if "ip_addresses" in interface:
                    interface["ip_addresses"].append(ip_data)
                else:
                    interface["ip_addresses"] = [ip_data]
                
                continue
            
            r = ether_re.findall(line)
            if r:
                ether, txqueuelen, link = r[0]
                interface["ether"] = ether
                continue
        
        return interfaces
    
    def active_interfaces(self):
        """
        Method which returns all active interfaces
        """
        
        return {dev: data for dev, data in self.network_interfaces().iteritems() 
                    if ("str_flags" in data.keys() and data["str_flags"] 
                            and "UP" in data["str_flags"] 
                            and "RUNNING" in data["str_flags"]
                            and "LOOPBACK" not in data["str_flags"] 
                            and "ip_addresses" in data and data["ip_addresses"])} 
        
