"""**systemprofile** - Package which is responsible for interogating various
    system components to return commonly needed data points.

:platform: RHEL5, macOS, Ubuntu
:synopsis: This is the root module that is used to establish a common 
    interrogation interface across various platforms and data systems.

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
import threading

__version__ = "1.4.24"

#MARK: Defaults
DEFAULT_DIG_DOMAINTEST_RETRIES=1

#MARK: -
#MARK: Classes
class SystemProfileBase(object):
    """
    Class which provides system interogation routines for common query 
    elements, such as hostname, IP information, connectivity data, etc...
    
    This class will typically be monkey-patched by an OS-specific 
    implementation.
    """
    
    file_dir = "/usr/local/amazon/var/acme" #: Directory used for file storage.
    logger_name = "SystemProfile"
    amzn_managed_file_path = None           #:  File used as flag to check if system is amazon managed.
    sysfile_lock = None
    
    def __init__(self,file_dir=None):
        """
        Constructor
        """
        
        if file_dir:
            self.file_dir = file_dir
    
        self.sysfile_lock = threading.RLock()
        
        self.hardware_info = None           #: Var for caching HW info
        
            
    def system_identifier(self):
        """
        Method to fetch our system identifier from disk
        """
        
        system_file = os.path.join(self.file_dir,"system.data")
        
        data = None
        json_data = None
        
        if os.path.exists(system_file):
            with self.sysfile_lock:
                with codecs.open(system_file) as fh:
                    data = fh.read()
        if data:
            json_data = json.loads(data)
            if "SystemID" in json_data:
                return json_data["SystemID"]
        
        return None
            
    def set_system_identifier(self,identifier):
        """
        Method to update our system identifier on disk
        """
        
        system_file = os.path.join(self.file_dir,"system.data")
        
        data = None
        json_data = None
        
        with self.sysfile_lock:
            if os.path.exists(system_file):
                with codecs.open(system_file,"r") as fh:
                    data = fh.read()
                json_data = json.loads(data)
            else:
                json_data = {}
                
            json_data["SystemID"] = identifier
            
            data = json.dumps(json_data,indent=4)
            
            with codecs.open(system_file,"w") as fh:
                fh.write("{}\n".format(data))
        
    def system_version(self):
        """
        Method to return our system version.
        """
        
        distro, version, id = platform_module.dist()
        
        return version
    
    def primary_ip_address(self):
        """
        Method to return our default routes IP address
        """
        interface = self.primary_interface()
        ip = None
        
        if interface:
            ip = self.ip_address_for_interface(interface)
        
        return ip
        
    def primary_interface(self):
        """
        Method to return our primary interface (default route)
        """
        
        return None
        
    def ip_address_for_interface(self,interface):
        """
        Method which accepts an interface and returns it's IP address.
        """
        return None
        
    def firewall_status(self):
        """
        Method to return our firewall status.
        """
        return None

    def network_site(self):
        """
        Method to return our current network site. If we are not on the
        corporate LAN, this method should return None
        
        .. warning: 
            This call has been depricated, use :py:func:`directoryservice.DirectoryServiceProfile.network_site` - beauhunt@ 2017-07-26
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        try:
            logger.warning("SystemProfileBase.network_site() has been deprecated! use `directoryservice.DirectoryServiceProfile.network_site`")
            raise RuntimeError
        except RuntimeError:
            logger.debug("network_site() call trace:", exc_info=1)
    
        try:
            return self.directoryservice.network_site()
        except AttributeError:
            raise RuntimeError("directoryservice not loaded!"), None, sys.exc_info()[2]
        
    def online(self):
        """
        Method to determine whether or not our system has network connectivity.
        """
        return None
       
    def last_known_domain_controller(self):
        """
        Method to return the last known domain controller used by the system.
        
        .. warning: 
            This call has been depricated, use :py:func:`directoryservice.DirectoryServiceProfile.last_known_domain_controller` - beauhunt@ 2017-07-26
        
        """
        
        logger = logging.get_logger(self.logger_name)
        
        try:
            logger.warning("SystemProfileBase.last_known_domain_controller() has been deprecated! use `directoryservice.DirectoryServiceProfile.last_known_domain_controller`")
            raise RuntimeError
        except RuntimeError:
            logger.debug("last_known_domain_controller() call trace:", exc_info=1)
    
        try:
            return self.directoryservice.last_known_domain_controller()
        except AttributeError:
            raise RuntimeError("directoryservice not loaded!"), None, sys.exc_info()[2]
                
    def on_domain(self,domain=None):
        """
        Method which determines whether or not we have Domain connectivity.
        This is simply a convenience method for :py:func:`directoryService.DirectoryServiceProfile.on_domain`
        
        :returns: bool - True if we are connected to our domain
        
        """
        
        try:
            return self.directoryservice.on_domain()
        except AttributeError:
            raise RuntimeError("directoryservice not loaded!"), None, sys.exc_info()[2]
                
    def on_vpn(self):
        """
        Method to return whether we have VPN connectivity.
        """
        
        return None
        
    def hardware_make(self):
        """
        Method to return the hardware model for this system
        """
        
        return None
        
    def hardware_model(self):
        """
        Method to return the hardware model for this system
        """
        
        return None
        
    def serial_number(self):
        """
        Method to return our device serial number
        """
        
        return None
        
    def asset_tag(self):
        """
        Method to return our device asset tag
        """
        
        return None
        
    def owner(self):
        """
        Method to return our device owner, if set.
        """
        
        return None
    
    def hardware_identifier(self):
        """
        Method which returns our hardware identifier.
        """
        
        return None
    
    def system_type(self):
        """
        Method to return our device type ('Desktop','Laptop','Server', etc...)
        """
        
        return None
    
    def platform(self):
        """
        Our system's platform, this is equivalent to :py:func:`current_platform`
        """
        
        ## Defer to our module function
        return current_platform()
        
        
    def architecture(self):
        """
        Method to return our device architecture ('x86_64','i32','arm', etc...)
        """
        
        return platform_module.machine()
    
    def has_builtin_ethernet(self):
        """
        Method to determine if this system has on-board ethernet
        """
        
        return True
    
    def has_builtin_wifi(self):
        """
        Method to determine if this system has on-board wifi
        """
        
        return False
    
    def hostname(self):
        """
        Returns the systems hostname.
        """
        
        fqdn = socket.gethostname()
        
        hostname = fqdn.split(".")[0].lower()
        
        return hostname
    
    def fqdn(self):
        """
        Returns the systems fully qualified domain name.
        """
        
        fqdn = socket.gethostname()
        
        return fqdn
                
    def mac_address(self):
        """
        Returns the systems mac address. 
        """
        
        return None
        
    def bound_ad_domain(self):
        """
        Method to return the AD domain that we are currently bound to.
        
        .. warning: 
            This call has been depricated, use :py:func:`directoryservice.DirectoryServiceProfile.bound_ad_domain` - beauhunt@ 2017-07-26
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        try:
            logger.warning("SystemProfileBase.bound_ad_domain() has been deprecated! use `directoryservice.DirectoryServiceProfile.bound_ad_domain`")
            raise RuntimeError
        except RuntimeError:
            logger.debug("bound_ad_domain() call trace:", exc_info=1)
        
        try:
            return self.directoryservice.bound_ad_domain()
        except AttributeError:
            raise RuntimeError("directoryservice not loaded!"), None, sys.exc_info()[2]
    
    def system_start_date(self):
        """
        Method to output our system start date.
        
        :returns: :py:class:`datetime.datetime` object (UTC)
        """

        return None
        
    def last_login_for_user(self,user,tty=None):
        """
        Method to return the last login for the provided user.
        """
        
        return None
    
    def current_user(self):
        """
        Method to return our current GUI user.
        """
        
        return None
    
    def is_amazon_managed(self):
        """
        Method to determine whether the system is Amazon managed or not.
        """
        amzn_managed = False
        try:
            amzn_managed = os.path.isfile(self.amzn_managed_file_path)
        except:
            pass
        return amzn_managed
    
#MARK: Module vars
profiler = SystemProfileBase()

def current_platform():
    """
    Method which returns the current platform
    """
    platform = None
    
    if os.uname()[0] == "Darwin":
        platform = "OS X"
    elif os.uname()[0] == "Linux":
        if os.path.exists("/proc/version"):
            with open("/proc/version","r") as fh:
                data = fh.read()
            if "ubuntu" in data.lower():
                platform = "Ubuntu"
            elif "red hat" in data.lower():
                platform = "RedHat"
            else:
                platform = "Linux"
    
    return platform

def configure_osx():
    """
    Method to configure this module for use with macOS
    """
    
    global profiler
    
    import systemprofile_osx
    
    profiler = systemprofile_osx.SystemProfileOSX()

def configure_ubuntu():
    """
    Method to configure this model for use with Linux
    """
    
    global profiler
    
    import systemprofile_ubuntu
    
    profiler = systemprofile_ubuntu.SystemProfileUbuntu()
    
def get_english_env():
    """
    Method to return current environmental variables with 
    overrides to specify english output (used by supporting subsystems).
    """
    
    env = os.environ.copy()
    env["LANGUAGE"] = "en_US"
    env["LANG"] = "en_US.UTF-8"
    env["LC_ALL"] = "en_US.UTF-8"
    env["LC_MESSAGES"] = "en_US.UTF-8"
    env["LC_COLLATE"] = "en_US.UTF-8"
    env["LC_CTYPE"] = "en_US.UTF-8"
    
    return env

platform = current_platform()
if platform == "OS X" or platform == "macOS":
    configure_osx()
elif platform == "Ubuntu":
    platform = "Ubuntu"
    configure_ubuntu()

import network
profiler.network = network.profiler

import directoryservice
profiler.directoryservice = directoryservice.profiler

class PermissionsDeniedError(Exception):
    """
    Exception raised in the event that we are attempting a privileged operation
    as an unpriviledged user.
    """
    pass
    
class PlatformUnsupportedError(Exception):
    """
    Exception raised in the event that an operation is not supported on the
    current platform.
    """
    pass
