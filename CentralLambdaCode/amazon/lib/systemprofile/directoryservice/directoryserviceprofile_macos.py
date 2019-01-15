"""
**ndirectoryserviceprofile_macos** - Package which is responsible for 
            Active Directory calls specific to macOS
:platform: macOS
:synopsis: This is the module that is used to provide a common 
    interrogation interface for directoryservice functions on the macOS 
    platform

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import datetime
import logging
import plistlib
import os
import re
import subprocess
import sys

from . import DirectoryServiceProfile

import systemprofile
if systemprofile.platform == "OS X" or systemprofile.platform == "macOS":
    import SystemConfiguration
    from Foundation import CFRelease

#MARK: -
#MARK: Classes
class DirectoryServiceProfileMacOS(DirectoryServiceProfile):
    
    logger_name = "DirectoryServiceProfileMacOS"
    
    def bound_ad_domain(self):
        """
        Method to return the AD domain that we are currently bound to.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        domain = None
        
        cmd = ["/usr/sbin/dsconfigad","-show","-xml"]
        try:
            output = subprocess.check_output(cmd)
            if output:
                data = plistlib.readPlistFromString(output)
                domain = data["General Info"]["Active Directory Domain"]        
        except Exception as exp:
            logger.warning("Could not check for AD domain; failed to run dsconfigad. Error:{}".format(exp ))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        return domain            
    
    def last_known_domain_controller(self):
        """
        Method which returns our last known domain controller.
        
        This function is only
        available from a root context.
        
        :returns string: FQDN of the last known domain controller
        :raises :py:class:`systemprofile.PermissionsDeniedError`:
        
        """
        
        if os.geteuid() != 0:
            raise systemprofile.PermissionsDeniedError("Network site lookups require elevated privileges!")
        
        dc = None
        
        try:
            if not self.domain_info:
                self.load_domain_info()
            
            info = self.domain_info
            
            if info["type"] == "ActiveDirectory":
                sn = info["shortname"]
                
                plist_path = os.path.join("/Library/Preferences/OpenDirectory/DynamicData/Active Directory","{}.plist".format(sn))
                
                if os.path.exists(plist_path):
                    cmd = ["/usr/bin/plutil","-convert","xml1","-o","-",plist_path]
                    plist_data = subprocess.check_output(cmd)
                    
                    plist = plistlib.readPlistFromString(plist_data)
                    dc = plist["last used servers"]["/Active Directory/{}".format(sn)]["host"]
        except Exception as exp:
            logger.error("Failed to load network site: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        return dc
    
    def load_domain_info(self):
        """
        Method to load our basic domain information from SystemConfiguration
        """
        sc_store = None
        
        default_domain_info = {"domain" : None,
                                        "forest" : None,
                                        "shortname" : None,
                                        "type" : None,
                                        "realm" : None,
                                        }
        if not self.domain_info:
            self.domain_info = default_domain_info
        try:
            sc_store = SystemConfiguration.SCDynamicStoreCreate(None,"acmed",None,None)
            
            r = SystemConfiguration.SCDynamicStoreCopyValue(sc_store,"com.apple.opendirectoryd.ActiveDirectory")
            
            if r:
                self.domain_info["domain"] = r["DomainNameDns"]
                self.domain_info["forest"] = r["DomainForestName"]
                self.domain_info["shortname"] = r["DomainNameFlat"]
                self.domain_info["type"] = "ActiveDirectory"
            else:
                self.domain_info = default_domain_info
            
            r = SystemConfiguration.SCDynamicStoreCopyValue(sc_store,"Kerberos-Default-Realms")
            if r:
                self.domain_info["realm"] = r[0]
            else:
                self.domain_info["realm"] = None
        except Exception as exp:
            logger.error("An error occurred while loading domain info: {}".format(
                                                                        exp))
        finally:
            ## Release our sc_store reference (note: this crashes on Sierra 
            ## with a segfault)
            version = systemprofile.profiler.system_version()
            if sc_store and version[0] == "10" and version[1] <= "11":
                CFRelease(sc_store)
    
    def network_site(self):
        """
        Method to return our current network site. This function is only
        available from a root context.
        
        :returns string: The site name.
        :raises :py:class:`systemprofile.PermissionsDeniedError`: 
        """
    
        if os.geteuid() != 0:
            raise systemprofile.PermissionsDeniedError("Network site lookups require elevated privileges!")
        
        if not self.on_domain():
            return None
        
        network_site = None
        
        try:
            if not self.domain_info:
                self.load_domain_info()
                
            info = self.domain_info
            
            if info["type"] == "ActiveDirectory":
                sn = info["shortname"]
                
                plist_path = os.path.join("/Library/Preferences/OpenDirectory/DynamicData/Active Directory","{}.plist".format(sn))
                
                if os.path.exists(plist_path):
                    cmd = ["/usr/bin/plutil","-convert","xml1","-o","-",plist_path]
                    plist_data = subprocess.check_output(cmd)
                    
                    plist = plistlib.readPlistFromString(plist_data)
                    network_site = plist["ActiveDirectory"]["sitename"]
        except Exception as exp:
            logger.error("Failed to load network site: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        return network_site
    
        
    
