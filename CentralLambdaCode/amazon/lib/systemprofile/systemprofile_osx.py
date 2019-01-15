"""**systemprofile_osx** - Shim which is responsible for interogating various
    macOS system facilities to return commonly needed data points.

:platform: macOS
:synopsis: This is the macOS module that is used to provide a common 
    interrogation interface for various macOS data systems, such as IOKit,
    System Profiler, OpenDirectory, and the SystemConfiguration DynamicStore

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import codecs
import datetime
import logging
import os
import platform
import plistlib
import re
import socket
import subprocess
import threading
import math

import systemprofile
from util_helper import get_bytes

if systemprofile.platform == "OS X" or systemprofile.platform == "macOS":
    import SystemConfiguration
    from Foundation import CFRelease


#MARK: -
#MARK: Classes
class SystemProfileOSX(systemprofile.SystemProfileBase):
    """
    Class which provides macOS system interogation routines for common query 
    elements, such as hostname, IP information, connectivity data, etc...
    
    """
    
    file_dir = "/usr/local/amazon/var/acme" #: Directory used for files.
    system_info_plist_path = "/Library/Preferences/com.amazon.deviceinfo.plist"
    
    logger_name = "SystemProfileOSX"
    
    external_address = "4.2.2.2"            #: External IP space to use for connectivity tests by scutil
    
    sysfile_lock = None
    
    amzn_managed_file_path = "/Library/.AmznManaged"
     
    def ip_address_for_interface(self,interface):
        """
        Method to return our primary IP address (default route)
        """
        
        if not interface:
            return None
            
        ip = None
        logger = logging.getLogger(self.logger_name)
        
        try:
            cmd = ["/sbin/ifconfig",interface]
            output = subprocess.check_output(cmd)
            r = re.search("\sinet (.*?) .*",output)
            if r:
                g = r.groups()
                if g:
                    ip = g[0]
        except Exception as exp:
            logger.error("Failed to run determine IP using '{}'. Error:{}".format(
                                                        " ".join(cmd),exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        return ip
    
    def system_version(self):
        """
        Method to return our system version.
        """
        
        version, info, arch = platform.mac_ver()
        
        return version
    
    def primary_interface(self):
        """
        Method to return our primary interface (default route)
        """
        
        logger = logging.getLogger(self.logger_name)
        
        interface = "en0"
        
        try:
            cmd = ["/sbin/route","get","0.0.0.0"]
            output = subprocess.check_output(cmd)
            if output:
                r = re.search("\s*interface: (.*)",output)
                if r:
                    interface = r.groups()[0]
                
        except Exception as exp:
            logger.error("Failed to determine primary interface using '{}'. Error:{}".format(
                                                        " ".join(cmd),exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        return interface
    
    def online(self,*args,**kwargs):
        """
        Method which returns whether we have Internet connectivity.
        """
        online = False
        
        logger = logging.getLogger(self.logger_name)
        
        try:
            cmd = ["/usr/sbin/scutil","-r",self.external_address]
            output = subprocess.check_output(cmd)
            output = output.replace("flags =","")
            if output and output.strip().startswith("Reachable"):
                online = True
        except Exception as exp:
            logger.error("Failed to run connectivity tests using '{}'. Error:{}".format(
                                                        " ".join(cmd),exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        return online
    
    def on_vpn(self):
        """
        Method to determine if we are on VPN or not.
        """
        on_vpn = False
        
        interface = self.primary_interface()
        
        if interface and interface.startswith("utun"):
            on_vpn = True
        
        return on_vpn

    def load_disk_info(self):
        """Method to load our disk type and disk size info"""
        logger = logging.getLogger(self.logger_name)
        cmd = ["/usr/sbin/system_profiler", "SPStorageDataType", "-xml"]
        disk_dict = ""
        try:
            output = subprocess.check_output(cmd)
            if output:
                data = plistlib.readPlistFromString(output)
                disk_dict = data[0]['_items']
        except Exception as exp:
            logger.error("Failed to hardware type info using '{}'. Error:{}".format(" ".join(cmd), exp))
            logger.log(5, "Failure stack trace (handled cleanly):", exc_info=1)
        return disk_dict

    def load_hardware_info(self):
        """
        Method to load our hardware information.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        cmd = ["/usr/sbin/system_profiler","SPHardwareDataType","-xml"]
        try:
            output = subprocess.check_output(cmd)
            if output:
                data = plistlib.readPlistFromString(output)
                hardware_dict = data[0]['_items'][0]
                self.hardware_info = hardware_dict
        
        except Exception as exp:
            logger.error("Failed to load Hardware info using '{}'. Error:{}".format(
                                                        " ".join(cmd),exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
    
    def current_user(self):
        """
        Method to load our basic domain information from SystemConfiguration
        """
        
        user = None
        sc_store = None
        try:
            sc_store = SystemConfiguration.SCDynamicStoreCreate(None,"system_profiler",None,None)
            r = SystemConfiguration.SCDynamicStoreCopyValue(sc_store,"State:/Users/ConsoleUser")
            if r:
                user = r["Name"]
        finally:
            if sc_store:
                pass
                ##CFRelease(sc_store)  # Note: this may have been causing a crash...
        return user
    
    def hardware_make(self):
        """
        Method to return our hardware make (vendor)
        """
        #Todo: may be good to check for virt here.
        return "Apple"
    
    def hardware_model(self):
        """
        Method to return the hardware model for this system
        """
        model = None
        
        if not self.hardware_info:
            self.load_hardware_info()
        
        try:
            model = self.hardware_info["machine_model"]
        except:
            pass
            
        return model
        
        """
        # Run sysctl:
        cmd = ["/usr/sbin/sysctl","-n","hw.model"]
        [output,return_code] = az_subprocess(cmd)
        if return_code == 0 and output:
            model = output
        
        return model
        """
    
    def hardware_identifier(self):
        """
        Method which returns our hardware identifier.
        """
        logger = logging.getLogger(self.logger_name)
        
        identifier = None
        
        if not self.hardware_info:
            self.load_hardware_info()
            
        try:
            identifier = self.hardware_info["platform_UUID"]
        except:
            pass
            
        return identifier
        
        """
        my_dict = {}
        
        cmd = ['/usr/sbin/system_profiler','SPHardwareDataType','-xml']
        
        try:
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as exp:
            logger.warning("Colud not check for built-in wifi; failed to run system_profiler. Error:{}".format(exp ))
            return_code = exp.returncode
        
        if return_code == 0 and output:
            data = plistlib.readPlistFromString(output)
            if data:
                try:
                    identifier = data[0]['_items'][0]["platform_UUID"]
                except Exception as exp:
                    logger.error("Failed to load hardware identifier data. Error:{}".format(exp))
        
        return identifier
        """
    
    def serial_number(self):
        """
        Method to return our device serial number
        """
        sn = None
        
        if self.hardware_info is None:
            self.load_hardware_info()
            
        try:
            sn = self.hardware_info["serial_number"]
        except Exception:
            pass
            
        return sn

    def physical_memory(self):
        """
        Method to return memory (RAM)
        """
        ram = None
        if self.hardware_info is None:
            self.load_hardware_info()
        try:
            ram_str = self.hardware_info["physical_memory"]
            size, suffix = ram_str.split(" ")
            ram = get_bytes(size,suffix)
        except Exception:
            pass
        return ram

    def cpu_cores(self):
        """
        Method to return number of CPU cores
        """
        cpu_cores = None

        if self.hardware_info is None:
            self.load_hardware_info()
        try:
            cpu_cores = self.hardware_info["number_processors"]
        except Exception:
            pass
        return int(cpu_cores)

    def cpu_type(self):
        """
        Method to return CPU Type
        """
        cpu_type = None
        if self.hardware_info is None:
            self.load_hardware_info()
        try:
            cpu_type = self.hardware_info["cpu_type"]
        except Exception:
            pass
        return cpu_type

    def system_type(self):
        """
        Method to return our device type ('Desktop','Laptop','Server', etc...)
        """
        
        type = None
        model = self.hardware_model()
        
        if model:
            if ("mini" in model.lower() or "macpro" in model.lower() 
                                                    or "imac" in model.lower()):
                if os.path.exists("/Library/Server"):
                    type = "Server"
                else:
                    type = "Desktop"
            elif "book" in model.lower():
                type = "Laptop"
        else:
            type = "Unknown"
        
        return type
      
    def owner(self):
        """
        Method to return our device owner, as set in /Library/Preferences/com.amazon.deviceinfo.plist
        """
        logger = logging.getLogger(self.logger_name)
        
        owner = None
        
        plist_path = self.system_info_plist_path
        
        if os.path.exists(plist_path):
            try:
                cmd = ["/usr/bin/plutil","-convert","xml1","-o","-",plist_path]
                plist_data = subprocess.check_output(cmd)
                
                plist = plistlib.readPlistFromString(plist_data)
                owner = plist["deviceOwner"]
            except Exception as exp:
                logger.error("Failed to lookup device owner:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                
        return owner    
        
    def asset_tag(self):
        """
        Method to return our device asset tag, as set in /Library/Preferences/com.amazon.deviceinfo.plist
        
        """
        logger = logging.getLogger(self.logger_name)
        
        asset_tag = None
        
        plist_path = self.system_info_plist_path
        
        if os.path.exists(plist_path):
            try:
                cmd = ["/usr/bin/plutil","-convert","xml1","-o","-",plist_path]
                plist_data = subprocess.check_output(cmd)
                
                plist = plistlib.readPlistFromString(plist_data)
                asset_tag = plist["deviceAssetID"]
            except Exception as exp:
                logger.error("Failed to lookup device asset tag:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                
        return asset_tag    
    
    def has_builtin_ethernet(self):
        """
        Method to determine if this system has on-board ethernet
        """
        logger = logging.getLogger(self.logger_name)
        
        builtin = False
        
        network_dict = {}
        
        # Run system profiler and look for built-in Ethernet:
        # If present, it's always en0, and type is Ethernet.
        # On systems lacking built-in Ethernet, en0 is of type AirPort (Wi-Fi).
        # Preferring system_profiler as networksetup lacks XML output.
        cmd = ['/usr/sbin/system_profiler','SPNetworkDataType','-xml']
        
        try:
            output = subprocess.check_output(cmd)
            return_code = 0
        
        except subprocess.CalledProcessError as exp:
            logger.warning("Colud not check for built-in ethernet; failed to run system_profiler. Error:{}".format(exp ))
            return_code = exp.returncode
        
        if return_code == 0 and output:
            data = plistlib.readPlistFromString(output)
            if data:
                network_dict = data[0]['_items']
                if network_dict:
                    for i in range(0,len(network_dict)):
                        try:
                            the_interface = network_dict[i]['interface']
                        except KeyError:
                            the_interface = 'invalid'
                        try:
                            the_interface_hardware = network_dict[i]['hardware']
                        except KeyError:
                            the_interface_hardware = 'invalid'
                        if (the_interface_hardware == 'Ethernet' 
                                                and the_interface == 'en0'):
                            builtin = True
        return builtin
    
    def has_builtin_wifi(self):
        """
        Method to determine if this system has on-board wifi
        """
        logger = logging.getLogger(self.logger_name)
        
        builtin = False
        
        network_dict = {}
        
        # Run system profiler and look for built-in AirPort:
        # If present, it's always en0, and type is AirPort.
        # On systems lacking built-in Ethernet, en0 is of type AirPort (Wi-Fi).
        # Preferring system_profiler as networksetup lacks XML output.
        cmd = ['/usr/sbin/system_profiler','SPNetworkDataType','-xml']
        
        try:
            output = subprocess.check_output(cmd)
            return_code = 0
        except subprocess.CalledProcessError as exp:
            logger.warning("Colud not check for built-in wifi; failed to run system_profiler. Error:{}".format(exp ))
            return_code = exp.returncode
        
        if return_code == 0 and output:
            data = plistlib.readPlistFromString(output)
            if data:
                network_dict = data[0]['_items']
                if network_dict:
                    for i in range(0,len(network_dict)):
                        try:
                            the_interface = network_dict[i]['interface']
                        except KeyError:
                            the_interface = 'invalid'
                        try:
                            the_interface_hardware = network_dict[i]['hardware']
                        except KeyError:
                            the_interface_hardware = 'invalid'
                        if (the_interface_hardware == 'AirPort' 
                                                and the_interface == 'en0'):
                            builtin = True
        return builtin
    
    def hostname(self):
        """
        Returns our hostname
        """
        logger = logging.getLogger(self.logger_name)
        
        cmd = ["/usr/sbin/scutil","--get","LocalHostName"]
        try:
            output = subprocess.check_output(cmd)
            return_code = 0
        except subprocess.CalledProcessError as exp:
            logger.warning("Colud not check for hostname; failed to run scutil. Error:{}".format(exp ))
            return_code = exp.returncode
            
        if output:
            return output.rstrip()
    
    def mac_address(self, interface=None):
        """
        Returns our MAC address for our built-in primary network adapter (en0).
        Output will be in colon-delimited format (i.e. '0a:0b:0c:0d:0e:0f')
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if not interface:
            interface = "en0"
        
        cmd = ["/sbin/ifconfig", interface]
        
        try:
            output = subprocess.check_output(cmd)
            return_code = 0
        except subprocess.CalledProcessError as exp:
            logger.warning("Colud not check for MAC address; failed to run ifconfig. Error:{}".format(exp ))
            return_code = exp.returncode
        
        index = output.find("ether ")
        if index:
            return output[index+6:index+23]
    
    def get_generated_computername(self):
        """
        Method to return our computer name, derived from the primary MAC 
        address for this system
        """
        mac_address = None
        
        has_ethernet = self.computer_has_builtin_ethernet()
        model = self.hardware_model()
        
        if has_ethernet:
            if "macpro" in model.lower():
                mac_address = self.get_hardware_address("Ethernet 1")
            else:
                mac_address = self.get_hardware_address("Ethernet")
        else:
            mac_address = self.get_hardware_address("Wi-Fi")
            
        return mac_address.strip().replace(":","")
    
    def last_login_for_user(self,user,tty="console"):
        """
        Method to return the last login for the provided user.
        
        :returns: py:class:`datetime.datetime` object representing last login date (UTC)
        :returns: py:class:`None` If no date is found
        """
        
        logger = logging.getLogger(self.logger_name)
        
        date = None
        
        now = datetime.datetime.utcnow()
        
        try:
            last_cmd = ["/usr/bin/last","{}".format(user)]
            last_ps = subprocess.Popen(last_cmd,stdout=subprocess.PIPE)
            
            awk_cmd = ["/usr/bin/awk","/{}/ {{print $3\" \"$4\" \"$5\" \"$6\" \"{}}}".format(tty,now.year)]
            awk_ps = subprocess.Popen(awk_cmd,stdout=subprocess.PIPE,
                                                    stdin=last_ps.stdout) 
            head_cmd = ["/usr/bin/head","-1"]
            output = subprocess.check_output(head_cmd,stdin=awk_ps.stdout)
            if output:
                date = datetime.datetime.strptime(output.strip(),"%a %b %d %H:%M %Y")
                ## Convert to utc
                tz_hours = round((datetime.datetime.now() - datetime.datetime.utcnow()).total_seconds() / 3600)
                tz_timedelta = datetime.timedelta(hours=tz_hours)
                date = date - tz_timedelta
        except Exception as exp:
            logger.error("Failed to determine login date for user:'{} ({})' using '{} | {}'. Error:{}".format(
                                                    user,
                                                    tty,
                                                    " ".join(last_cmd),
                                                    " ".join(awk_cmd),
                                                    exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
        return date
    
    def system_start_date(self):
        """
        Method to output our system start date.
        
        :returns: :py:class:`datetime.datetime` object (UTC)
        """
        logger = logging.getLogger(self.logger_name)
        
        date = None
        
        try:
            stat = os.stat("/var/run/com.apple.loginwindow.didRunThisBoot")
            date = datetime.datetime.fromtimestamp(stat.st_ctime)
            
            ## Convert to utc
            tz_hours = round((datetime.datetime.now() - datetime.datetime.utcnow()).total_seconds() / 3600)
            tz_timedelta = datetime.timedelta(hours=tz_hours)
            
            date = date - tz_timedelta
        except Exception as exp:
            logger.error("Failed to determine system start date. Error:{}".format(
                                                    exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        return date

    def firewall_status(self):

        cmd = ['/sbin/pfctl', '-s', 'info']
        try:
            output = subprocess.check_output(cmd)
            returnCode = 0
        except subprocess.CalledProcessError, errorObject:
            output = ''
            returnCode = errorObject.returncode
        return output, returnCode

    def get_boot_disk_details(self):
        ##get filesystem properties to determine encryption type
        logger = logging.getLogger(self.logger_name)
        file_system_type = 'invalid'
        #get filesystem, volumeUUID for apfs
        cmd = ['/usr/sbin/diskutil','info','-plist','/']
        try:
            output = subprocess.check_output(cmd)
            return_code = 0
        except subprocess.CalledProcessError as exp:
            logger.error("Failed to get boot disk details. Error:{}".format(exp))
            return_code = exp.returncode
        if output:
            output_array = plistlib.readPlistFromString(output)
            if output_array:
                try:
                    file_system_type = output_array['FilesystemType']
                except KeyError:
                    logger.error('get_boot_disk_details: Cannot get Filesystem format details for startup volume.')
        return file_system_type


    def is_disk_encryption_required(self):
        ## Returns true iff FileVault is required.
        boot_volume_encryption_required = False
        system_type = self.system_type()
        if system_type == "Laptop":
            boot_volume_encryption_required = True
        return boot_volume_encryption_required

    def boot_volume_file_vault_status(self):
        ## Get CoreStorage details for the startup disk.
        # Defaults:
        logger = logging.getLogger(self.logger_name)
        boot_volume_family_uuid = 'invalid'
        boot_volume_conversion_status = 'invalid'
        boot_volume_family_enc_type = 'invalid'
        # Assume encryption is off:
        boot_volume_encryption_enabled = False
        file_system_type = self.get_boot_disk_details()
        if file_system_type == 'hfs':
            # Run diskutil to get volume info:
            cmd = ['/usr/sbin/diskutil','cs','info','-plist','/']
            try:
                output = subprocess.check_output(cmd)
                return_code = 0
                if output:
                    output_array = plistlib.readPlistFromString(output)
                    if output_array:
                        try:
                            # Try to get details for logical volume:
                            boot_volume_family_uuid = output_array['MemberOfCoreStorageLogicalVolumeFamily']
                            boot_volume_conversion_status = output_array['CoreStorageLogicalVolumeConversionState']
                        except KeyError:
                            logger.error('boot_volume_file_vault_status: Cannot get CoreStorage details for startup volume.')
            except subprocess.CalledProcessError as exp:
                logger.error("Failed to get volume info. Error:{}".format(exp))
                return_code = exp.returncode
            
            # Should be CoreStorage:
            if boot_volume_family_uuid != 'invalid':
                # Run diskutil to get family info:
                cmd = ['/usr/sbin/diskutil','cs','info','-plist',boot_volume_family_uuid]
                try:
                    output = subprocess.check_output(cmd)
                    return_code = 0
                    if output:
                        output_array = plistlib.readPlistFromString(output)
                        if output_array:
                            try:
                                # Try to get details for family:
                                boot_volume_family_enc_type = output_array['CoreStorageLogicalVolumeFamilyEncryptionType']
                                if boot_volume_family_enc_type == 'AES-XTS':
                                    boot_volume_encryption_enabled = True
                            except KeyError:
                                logger.error('boot_volume_file_vault_status: Cannot get encryption type for volume family.')
                except subprocess.CalledProcessError as exp:
                    logger.error("Failed to get family info. Error:{}".format(exp))
                    return_code = exp.returncode

        elif file_system_type == 'apfs':
            output = None
            cmd = ['/usr/sbin/diskutil','apfs','list','-plist']
            try:
                output = subprocess.check_output(cmd)
                return_code = 0
                if output:
                    output_array = plistlib.readPlistFromString(output)
                    if output_array:
                        apfs_boot_volume_data = output_array['Containers'][0]['Volumes'][0]
                        try:
                            apfs_encryption_state = apfs_boot_volume_data['Encryption']
                        except KeyError:
                            logger.error('bootVolumeFileVaultStatus: exception detecting encryption state')
                            try:
                                apfs_encryption_state = apfs_boot_volume_data['Encryption Progress']
                            except KeyError:
                                logger.error('bootVolumeFileVaultStatus: exception detecting encryption progress')
                        if apfs_encryption_state:
                            boot_volume_encryption_enabled = True
                            boot_volume_conversion_status = 'Complete'
                        elif not apfs_encryption_state:
                            boot_volume_encryption_enabled = False
                            boot_volume_conversion_status = 'Unknown'
            except subprocess.CalledProcessError as exp:
                logger.error("Failed to get apfs details. Error:{}".format(exp))
                return_code = exp.returncode

        # Return:
        return boot_volume_encryption_enabled, boot_volume_conversion_status

    def get_file_vault_status(self):
        # Determine if FileVault is necessary:
        boot_volume_encryption_required = self.is_disk_encryption_required()
        # Determine if FileVault is enabled:
        boot_volume_encryption_enabled, boot_volume_conversion_status = self.boot_volume_file_vault_status()
        # Determine requirement status:
        if not boot_volume_encryption_required:
            status_required = False
        else:
            status_required = True
        # Determine encryption status:
        if not boot_volume_encryption_enabled:
            status_encryption = False
        else:
            status_encryption = True
        # Return:
        return status_required, status_encryption, boot_volume_conversion_status

    def check_secure_token(self, user):
        """
        Method to check if a user is SecureToken enabled
        
        :returns True: If checking SecureToken is successful

        """
        logger = logging.getLogger(self.logger_name)
        result = False
        output = None
        
        cmd = ["/usr/sbin/sysadminctl", "-secureTokenStatus", user]
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT) ## Command writes to stderr, not stdout
            if output:
                if output.find("Secure token is ENABLED for user") != -1:
                    result = True
        except subprocess.CalledProcessError as exp:
            logger.error("Error checking secure token status for account {}. Error:{}".format(user, exp))
            return_code = exp.returncode
 
        return result
        
#MARK: -
#MARK: Module setup
systemprofile.SystemProfile = SystemProfileOSX
