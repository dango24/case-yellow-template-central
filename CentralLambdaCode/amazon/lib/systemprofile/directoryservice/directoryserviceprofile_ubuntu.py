"""
**ndirectoryserviceprofile_ubuntu** - Package which is responsible for 
            Active Directory calls specific to Ubuntu
:platform: Ubunt
:synopsis: This is the module that is used to provide a common 
    interrogation interface for directoryservice functions on the Ubuntu
    platform

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import datetime
import logging
import os
import re
import subprocess
import sys
import json

from . import DirectoryServiceProfile

#MARK: -
#MARK: Classes
class DirectoryServiceProfileUbuntu(DirectoryServiceProfile):
    
    logger_name = "DirectoryServiceProfileUbuntu"
    sssd_cache_db_path = "/var/cache/ad_facts/db"
    
    def is_sssd_installed(self):
        """
        Method which returns a boolean if SSSD is installed
        """
        
        sssd_installed = False
        if os.path.exists(self.sssd_cache_db_path):
            sssd_installed = True
        return sssd_installed
    
    def bound_ad_domain(self):
        """
        Method which returns the bound AD domain, if applicable.
        """
        
        if self.is_sssd_installed():
            return self._bound_ad_domain_sssd()
        else:
            return self._bound_ad_domain_pbis()

    def _bound_ad_domain_pbis(self):
        """
        Method which returns the bound AD domain using PBIS, if applicable.
        This method should not be invoked directly.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        domain = None
                
        regex = re.compile("^\t{1,}DNS Domain:\s*(.*?)$")
        
        try:
            cmd = ["/opt/pbis/bin/get-status"]
            output = subprocess.check_output(cmd)
            for line in output.splitlines():
                r = regex.search(line)
                if r and r.groups() and r.groups()[0]:
                    domain = r.groups()[0].lower()
                    break
        
        except Exception as exp:
            logger.warning("Failed to lookup bound AD domain using cmd:'{}', Error:{}".format(
                                                    " ".join(cmd),exp))
        
        return domain

    def _bound_ad_domain_sssd(self):
        """
        Method which returns the bound AD domain using SSSD, if applicable.
        This method should not be invoked directly.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        domain = None
                
        try:
            with open(self.sssd_cache_db_path, 'r') as file:
                ad_facts = json.load(file)
            domain = str(ad_facts['domain_name'])
        
        except Exception as exp:
            logger.warning("Failed to lookup bound AD domain from SSSD cache. Error:{}".format(exp))
        
        return domain
    
    def last_known_domain_controller(self):
        """
        Method which returns our last known domain controller.
                
        :returns string: FQDN of the last known domain controller
        :raises :py:class:`systemprofile.PermissionsDeniedError`:
        
        """
        
        if self.is_sssd_installed():
            return self._last_known_domain_controller_sssd()
        else:
            return self._last_known_domain_controller_pbis()

    def _last_known_domain_controller_pbis(self):
        """
        Method which returns our last known domain controller using PBIS.
        This method should not be invoked directly.
        
        This function is only
        available from a root context.
        
        :returns string: FQDN of the last known domain controller
        :raises :py:class:`systemprofile.PermissionsDeniedError`:
        
        """
        logger = logging.getLogger(self.logger_name)
        
        dc = None
        
        regex = re.compile("^\t{1,}DC Name:\s*(.*?)$")
        
        try:
            cmd = ["/opt/pbis/bin/get-status"]
            output = subprocess.check_output(cmd)
            for line in output.splitlines():
                r = regex.search(line)
                if r and r.groups():
                    dc = r.groups()[0].lower()
                    break
        except Exception as exp:
            logger.warning("Failed to lookup network site using cmd:'{}', Error:{}".format(
                                                    " ".join(cmd),exp))
    
        return dc

    def _last_known_domain_controller_sssd(self):
        """
        Method which returns our last known domain controller using SSSD.
        This method should not be invoked directly.
                
        :returns string: FQDN of the last known domain controller
        :raises :py:class:`systemprofile.PermissionsDeniedError`:
        
        """
        logger = logging.getLogger(self.logger_name)
        
        dc = None
        
        try:
            with open(self.sssd_cache_db_path, 'r') as file:
                ad_facts = json.load(file)
            dc = str(ad_facts['domain_controller'])
        
        except Exception as exp:
            logger.warning("Failed to lookup last known domain controller from SSSD cache. Error:{}".format(exp))
    
        return dc

    def network_site(self):
        """
        Method to extract our network site
        """
        
        if self.is_sssd_installed():
            return self._network_site_ssdd()
        else:
            return self._network_site_pbis()
    
    def _network_site_pbis(self):
        """
        Method to extract our network site using PBIS
        This method should not be invoked directly.
        """
        logger = logging.getLogger(self.logger_name)
        
        network_site = None
        
        regex = re.compile("^\tSite:\s*(.*?)$")
        
        try:
            cmd = ["/opt/pbis/bin/get-status"]
            output = subprocess.check_output(cmd)
            for line in output.splitlines():
                r = regex.search(line)
                if r and r.groups() and r.groups()[0]:
                    network_site = r.groups()[0]
                    break
        except Exception as exp:
            logger.warning("Failed to lookup network site using cmd:'{}', Error:{}".format(
                                                    " ".join(cmd),exp))
    
        return network_site

    def _network_site_ssdd(self):
        """
        Method to extract our network site using SSSD
        This method should not be invoked directly.
        """
        logger = logging.getLogger(self.logger_name)
        
        network_site = None
        
        try:
            with open(self.sssd_cache_db_path, 'r') as file:
                ad_facts = json.load(file)
            network_site = str(ad_facts['computer_site'])
        
        except Exception as exp:
            logger.warning("Failed to lookup network site from SSSD cache. Error:{}".format(exp))
    
        return network_site
                
    def load_groups_for_user(self, username): 
        """
        Method which returns a list of groups for user.
        """
        
        groups = DirectoryServiceProfile.load_groups_for_user(self, 
                                                            username=username)
                                                            
        groups = [x.replace("^"," ") for x in groups]
        
        return groups
        
    def groups_contain_networkgroups(self, groups):
        """
        Method to determine whether the passed result set includes
        Network groups (i.e. Active Directory).
        
        For Ubuntu, likewise/pbis caches groups even while off domain,
        so we always return True. 
        """
        
        return True
    
        
    
