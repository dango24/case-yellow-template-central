"""**systemprofile.directoryservice** - Package which is responsible for
    interogating directoryservice components (Active Directory integration)

:platform: macOS, Ubuntu
:synopsis: This is the module that is used to establish a common 
    interrogation interface for directoryservice integration needs
    across various platforms and data systems.

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
from collections import MutableSequence
import datetime
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading

import systemprofile

#MARK: Defaults
DEFAULT_DIG_DOMAINTEST_RETRIES=1

GROUP_CACHE_TTL = datetime.timedelta(days=1)   #: Our group cache time-to-live: this property defines how often we will re-lookup group data for any given user.  

#MARK: -
#MARK: Classes
class DirectoryServiceProfile(object):
    """
    Class which provides system interogation routines for common query 
    elements, such as group membership, group caching, etc...
    
    This class will typically be monkey-patched by an OS-specific 
    implementation.
    """
    
    logger_name = "DirectoryService"
    
    id_path = "/usr/bin/id"
    
    group_cache = []                #: A list of cached user groups
    
    enable_group_lookup = True      #: If this is set to false, we will use only cached data for group lookups.
    
    def __init__(self):
        """
        Constructor.
        """
        
        self.group_cache = CacheRepository()
        self.domain_info = {}
        
     
    def load_domain_info(self):
        """
        Method to load our basic domain information from Internal store.
        You should override this method to provide platform-specific function.
        """
        default_domain_info = {"domain" : None,
                                        "forest" : None,
                                        "shortname" : None,
                                        "type" : None
                                        }
        if not self.domain_info:
            self.domain_info = default_domain_info
        
    def bound_ad_domain(self):
        """
        Method to return the AD domain that we are currently bound to.
        """
        
        return None
        
    def on_domain(self,domain=None):
        """
        Method which determines whether or not we have Domain connectivity.
        This method is currently just testing DNS, we should migrate to a
        more secure, certificates based check in the future.
        
        .. warning:
            In the event of network problems this call may block for up to
            2 seconds.
        
        :param str domain: The domain to check against. This should be a 
            qualified DNS search path (i.e. 'domain.amazon.com')
        
        """
        on_domain = False
        
        logger = logging.getLogger(self.logger_name)
        
        logger.log(5,"Blocking call `on_domain()` started... (Thread:{})".format(
                                            threading.current_thread().name))
        
        if not domain:
            domain = self.bound_ad_domain()
        
        if domain:
            try:
                cmd = ["/usr/bin/dig","+short","SRV","+time=1","+tries={}".format(
                                            DEFAULT_DIG_DOMAINTEST_RETRIES),
                                        "_ldap._tcp.{}".format(domain)]
                output = subprocess.check_output(cmd)
                if output:
                    on_domain = True
                    
            except subprocess.CalledProcessError as exp:
                logger.error("Failed to run domain checks: assuming off-domain status... (exit code:{})".format(
                                                            exp.returncode))
                logger.log(5,"Failed dig command:{}".format(cmd))
            except Exception as exp:
                logger.error("Failed to run domain checks: assuming off-domain status...")
                logger.debug("Failed to load domain info using '{}'. Error:{}".format(
                                                            " ".join(cmd),exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        logger.log(5,"Blocking call `on_domain()` ended... (Thread:{})".format(
                                            threading.current_thread().name))
        
        return on_domain
    
    def network_site(self):
        """
        Method to return our current network site. If we are not on the
        corporate LAN, this method should return None
        """
        return None
    
    def user_in_group(self, group=None, groups=None, username=None):
        """
        Method which checks if the provided user is in the provided group.
        
        :param group: The groupname to lookup.
        :type group: string
        :param groups: A list of groups to lookup
        :type groups: list<string>
        :param username: The username to lookup. If ommitted, we will default to system owner
        :type username: string
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if username is None:
            username = systemprofile.profiler.owner()
        
        if groups is None:
            groups = []
        
        if group:
            groups.append(group)
        
        logger.log(2,"Checking membership for user:'{}' (groups:'{}')".format(
                                                        username,
                                                        "', '".join(groups)))
        
        if not username or not groups:
            return False
        
        result = False
        
        my_groups = self.groups_for_user(username=username)
        
        domain_regex = re.compile('.*\\\\(.*)')
        
        for group in groups:
            for my_group in my_groups:
                if my_group.lower() == group.lower():
                    result = True
                    break
                else:
                    ## Attempt to strip out domain prefix (i.e. 'clienteng' should 
                    ## match 'ANT\clienteng'
                    try:
                        domain_group = domain_regex.findall(group)[0]
                        
                        if domain_group.lower() == group.lower():
                            result = True
                            break
                    except IndexError:
                        pass
            
        if result:
            logger.log(2,"Verified user:'{}' is member (groups:'{}')".format(
                                                        username,
                                                        "', '".join(groups)))
        else:
            logger.log(2,"Verified user:'{}' is not member (groups:'{}')".format(
                                                        username,
                                                        "', '".join(groups)))
        
        return result
    
    def groups_for_user(self, username=None):
        """
        Method that will return a list of groups for the provided user.
        """
        
        logger = logging.getLogger(__name__)
        
        if username is None:
            username = systemprofile.profiler.owner()
            
        if not username:
            return []
            
        cache = self.groupcache_for_user(username)
        
        if not cache and self.enable_group_lookup:
            logger.log(5, "No group cache exists for user:'{}', looking up groups...".format(
                                                                username))
            cache = self.cache_groups_for_user(username)
        elif cache and cache.is_expired() and self.enable_group_lookup:
            logger.log(5, "Group cache has expired for user:'{}', looking up groups...".format(
                                                                username))
            cache = self.cache_groups_for_user(username)
            
        if not cache:
            return []
        else:
            return cache.groups
    
    def groupcache_for_user(self, username):
        """
        Method to return our group cache for the provide user.
        
        :param str username: The username who's cache we want to retrieve
        
        :returns: :py:class:`UserGroupCache`
        :returns: None if no cache is found
        
        """
        
        result = None
        
        for cache in self.group_cache:
            if cache.username.lower() == username.lower():
                result = cache
                break
        
        return result
    
    def expire_group_cache(self):
        """
        Method to expire our group cache.
        """
        
        for group in self.group_cache:
            group.cache_date = None
    
    def cache_groups_for_user(self, username):
        """
        Method to lookup and cache groups for the provide user.
        
        :param str username: The username who's groups we want to lookup and cache
        
        """
        
        groups = self.load_groups_for_user(username)
        
        cache = self.groupcache_for_user(username)
        
        if cache is None:
            cache = UserGroupCache(username=username)
            self.group_cache.append(cache)
        
        cache.update_groups(groups)
        
        return cache
    
    def load_groups_for_user(self, username): 
        """
        Method which returns a list of groups for user.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        cmd = [self.id_path, username]
        
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as exp:
            raise Exception("Failed to run command ('{}'): {}.".format(
                                                        "' '".join(cmd),exp)), None, sys.exc_info()[2]
                                
        ''' 
        Example output:
        uid=537038463(beauhunt) gid=1896053708(ANT\Domain Users) groups=1896053708(ANT\Domain Users),310278088(ANT\aws-blue-L4up),354029302(ANT\jehunter-org-all),494450976(ANT\clienteng),497527405(ANT\Client-Engineering),521425848(ANT\cktsdb-access),1339216914(ANT\mdm-ios-network-access),1341580617(ANT\CorpInfra),1372044702(ANT\ADMIN-ROLE-ACCOUNT),1402129519(ANT\confluence-global),401(com.apple.sharepoint.group.1),1979463215(ANT\FS-DesignTech-Installers),1992057917(ANT\App-Licensing),2098584867(ANT\App-Pager-Admin),42397405(ANT\App-SMSData),139691726(ANT\sha2-sharefile-access),236111466(ANT\fs-lux-EU_trans_dev_dw),489248385(ANT\Opr-WksAccts),33(_appstore)        
        '''
        
        ## Parse groups out of output (group names are wrapped in parenthesis
        ## Extract first entry (which is the username)
        regex = re.compile("\((.*?)\)")
        groups = set(regex.findall(output)[1:])
        
        return list(groups)

class CacheRepository(MutableSequence):
    """
    Class which holds our cache. This is basically a fancy list that better
    supports UserGroupCache object serialization.
    """
    
    def __init__(self, data=None, cache_class=None, *args, **kwargs):
        
        if data is not None:
            self._list = list(data)
        else:
            self._list = list()
        
        if cache_class is None:
            self.cache_class = UserGroupCache
        else:
            self.cache_class = cache_class
            
        MutableSequence.__init__(self)
    
    #Mark: List compatability methods
    def __len__(self):
        return len(self._list)
    
    def __getitem__(self, ii):
        return self._list[ii]
        
    def __delitem__(self, ii):
        del self._list[ii]
        
    def __setitem__(self, ii, val):
        self._list[ii] = val
        return self._list[ii]
        
    def __str__(self):
        return self.__repr__()
    
    def __repr__(self):
        return """<CacheRepository data:{}>""".format(self._list)
    
    def insert(self, ii, val):
        self._list.insert(ii, val)
        
    def append(self, val):
        list_idx = len(self._list)
        self.insert(list_idx, val)
    
    #Mark: Serialization methods
    def to_dict(self):
        """
        Method which outputs our object in dictionary form supported by
        json.
        """
        
        cached_group_data = []
        data = {"cached_objects" : cached_group_data}
        
        for c_group in self._list:
            cached_group_data.append(c_group.to_dict())
        
        return data
        
    def load_dict(self, data):
        """
        Method which loads our object from the provide data
        """
        
        objects = []
        
        for entry in data["cached_objects"]:
            obj = self.cache_class(data=entry)
            objects.append(obj)
            
        self._list = objects
            
    def save_to_file(self, filepath):
        """
        Method to serialize our cache to file.
        """
        
        data = self.to_dict()
        
        with open(filepath, "w") as fh:
            fh.write(json.dumps(data,indent=4))
            
    def load_from_file(self, filepath):
        """
        Method to load from the provided file
        """
            
        with open(filepath, "r") as fh:
            string_data = fh.read()
            
        dict_data = json.loads(string_data)
        
        self.load_dict(dict_data)

class UserGroupCache(object):
    """
    Basic model class representing a user-group cache.
    
    :param str username: The username represented by this cache
    :param list<string> groups: A list of groups (by name) that this user belongs to
    :param datetime cache_date: The date of this cache (default utcnow())
    
    """
    
    ttl = GROUP_CACHE_TTL
    date_format = "%Y-%m-%dT%H:%M:%S"
    
    def __init__(self, username=None, groups=None, cache_date=None, 
                                                            data=None,
                                                            *args,
                                                            **kwargs):
        
        self.username = None                #: The username represented by this cache
        self.groups = []                    #: Groups containing the user.
        self.cache_date = None      #: The date of the lookup
        
        if username is not None:
            self.username = username
        
        if groups is not None:
            self.groups = groups
        
        if cache_date is not None:
            self.cache_date = cache_date
        
        if data is not None:
            self.load_dict(data)
        
    def is_expired(self):
        """
        Method that returns whether or not our cache has expired.
        """
        
        result = False
        
        now = datetime.datetime.utcnow()
        
        if not self.cache_date:
            result = True
        elif self.cache_date + self.ttl <= now:
            result = True
        
        return result
        
    def update_groups(self, groups, force=None):
        """
        Method which will update our cached groups. This will update
        our cache_date only if network groups are found.
        
        :param groups: List of new groups for the user
        :type groups: List<string> of group names 
        :param force: If true, we will override cached network groups
                    regardless of whether network groups exist in the result set
                    By default, this is False to prevent local-only updates
                    from wiping out previously cached directory groups 
                    (MacOS and Ubuntu 'lose' network groups when they can't 
                    talk to AD).
        """
        
        if force or self.groups_contain_networkgroups(groups):
            self.groups = groups
            self.cache_date = datetime.datetime.utcnow()
        elif not force:
            self.update_local_groups(groups)
        
    def update_local_groups(self, groups):
        """
        Method which will update our cached local (i.e. POSIX) groups.
        This will not update our cache_date
        """
        
        logger = logging.getLogger(__name__)
        
        groups_to_remove = []
        groups_to_add = []
        
        ## Determine new groups
        for new_group in groups:
            ## Ignore any network groups
            if new_group.find("\\") >= 0:
                continue
                
            if not new_group.lower() in [g.lower() for g in self.groups]:
                groups_to_add.append(new_group)
                
        ## Determine lost groups
        for p_group in self.groups:
            
            ## Ignore any network groups
            if p_group.find("\\") >= 0:
                continue
        
            if not p_group.lower() in [new_group.lower() for new_group in groups]:
                groups_to_remove.append(p_group)
                
        logger.log(1, "Updating cache groups, removing: {}".format(
                                                            groups_to_remove))
        logger.log(1, "Updating cache groups, adding: {}".format(
                                                            groups_to_add))
        
        for p_group in groups_to_remove:
            self.groups.remove(p_group)
            
        for new_group in groups_to_add:
            self.groups.append(new_group)
        
    def groups_contain_networkgroups(self, groups):
        """
        Method to determine whether the passed result set includes
        Network groups (i.e. Active Directory).
        """
        
        result = False
        
        domain_regex = re.compile('.*\\\\(.*)')
        
        for group in groups:
            if domain_regex.findall(group):
                result = True
                break
                
        return result
        
    def load_dict(self, data):
        """
        Method to load our cache with the provided data.
        """
        logger = logging.getLogger(__name__)
        
        try:
            self.username = data["username"]
        except KeyError:
            pass
            
        try:
            self.groups = data["groups"]
        except KeyError:
            pass
            
        try:
            self.cache_date = datetime.datetime.strptime(data["cache_date"],
                                                        self.date_format)
        except KeyError:
            pass
        except ValueError as exp:
            logger.warning("Failed to load UserGroupCache: {}".format(exp))
        
    def to_dict(self):
        """
        Method to output a dictionary representing our object in a JSON
        serializable format.
        """
        data = {}
        data["username"] = self.username
        data["groups"] = self.groups
        if self.cache_date:
            data["cache_date"] = self.cache_date.strftime(self.date_format)
            
        return data
        
    def __repr__(self):
        return "<UserGroupCache:{}>".format(self.username)
    

#MARK: Module vars
profiler = DirectoryServiceProfile()

def configure_macos():
    """
    Method to configure this module for use with OS X
    """
    
    global profiler
    
    import directoryserviceprofile_macos
    
    profiler = directoryserviceprofile_macos.DirectoryServiceProfileMacOS()
    
def configure_ubuntu():
    """
    Method to configure this model for use with Linux
    """
    
    global profiler
    
    import directoryserviceprofile_ubuntu
    
    profiler = directoryserviceprofile_ubuntu.DirectoryServiceProfileUbuntu()

platform = systemprofile.current_platform()
if platform == "OS X" or platform == "macOS":
    configure_macos()
elif platform == "Ubuntu":
    platform = "Ubuntu"
    configure_ubuntu()


