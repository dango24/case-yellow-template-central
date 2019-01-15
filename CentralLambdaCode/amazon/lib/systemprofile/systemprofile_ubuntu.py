"""
**systemprofile_ubuntu** - Shim which is responsible for interogating various
     Ubuntu system facilities to return commonly needed data points.

:platform: Ubuntu
:synopsis: This is the Ubuntu module that is used to provide a common 
    interrogation interface for various data systems, such as netstat,
    ifconfig, pbis, etc...
    
.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

import codecs
import datetime
import json
import logging
import os
import platform
import re
import socket
import subprocess
import threading
from util_helper import get_bytes

import systemprofile

from pkg_resources import parse_version  #importing library for comparing dpkg package versions

class PyExecCmd(object):
    """
        Helper class to run a complex command through Python subprocess
    """
    def __init__(self):
        return
    
    def exec_cmd(self, allcmds, *args, **kwargs):
        """ Execute the command passed as the list of lists, with top level list representing a component of the piped workflow and the 
         low-level list representing the individual components of that command.
         It uses Popen invocation without shell=True. The command may contain
         multiple piped commands. Returns the <stdout> and <stderr> of
         executing the command.
         Args:
             @param allcmds:      type array
         Returns:
             tuple
        """
        
        env = systemprofile.get_english_env()
        
        numcmds = len(allcmds)
        popen_objs = []
        for i in range(numcmds):
            scmd = allcmds[i]
            stdin = None if i == 0 else popen_objs[i-1].stdout
            stderr = subprocess.STDOUT if i < (numcmds - 1) else subprocess.PIPE
            thiscmd_p = subprocess.Popen(scmd, stdin=stdin,stdout=subprocess.PIPE,stderr=stderr,env=env,
                                                            *args, **kwargs)
            if i != 0: popen_objs[i-1].stdout.close()
            popen_objs.append(thiscmd_p)
        # Collect output from the final command
        (cmdout, cmderr) = popen_objs[-1].communicate()

        # Set return codes
        for i in range(len(popen_objs) - 1):
            popen_objs[i].wait()

        # Now check if any command failed
        for i in range(numcmds):
            if popen_objs[i].returncode:
                raise subprocess.CalledProcessError(popen_objs[i].returncode,allcmds[i])

        # All commands succeeded
        return (cmdout, cmderr)

class SystemProfileUbuntu(systemprofile.SystemProfileBase):
    """
    Class which provides Ubuntu system interogation routines for common query 
    elements, such as hostname, IP information, connectivity data, etc...
    
    """
    
    file_dir = "/usr/local/amazon/var/acme" #: Directory used for files.
    logger_name = "SystemProfile-ubuntu"
    installed_packages_status_file = "/var/lib/dpkg/status" # File containing status(Package name, status, architecture, version etc.) of installed packages.
    laptop_info_file = "/etc/.laptop" # If this file is present in system it means current system is laptop.
    amzn_managed_file_path = "/etc/amazon/00defaults.json"
    sysfile_lock = None
    
    hardware_info = None
    
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
            output = subprocess.check_output(cmd, 
                                        env=systemprofile.get_english_env())
                                        
            distro, version, id = platform.dist()
        
            if version.startswith("14") or version.startswith("16"):
                r = re.search("\sinet addr:(.*?) .*",output)
            else:
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
    
    def primary_interface(self):
        """
        Method to return our primary interface (default route)
        """
        
        logger = logging.getLogger(self.logger_name)
        
        interface = "eth0"
        
        try:
            netstat_cmd = ["/bin/netstat","-r"]
            netstat_ps = subprocess.Popen(netstat_cmd,stdout=subprocess.PIPE,
                                        env=systemprofile.get_english_env())
            
            awk_cmd = ["/usr/bin/awk","/^default/ {print $8}"]
            awk_ps = subprocess.Popen(awk_cmd,stdout=subprocess.PIPE,
                                        stdin=netstat_ps.stdout,
                                        env=systemprofile.get_english_env()) 
            head_cmd = ["/usr/bin/head","-1"]
            output = subprocess.check_output(head_cmd, stdin=awk_ps.stdout,
                                        env=systemprofile.get_english_env())
            if output:
                interface = output.strip()
        except Exception as exp:
            logger.error("Failed to determine primary interface using '{} | {}'. Error:{}".format(
                                                        " ".join(netstat_cmd),
                                                        " ".join(awk_cmd),
                                                        exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        return interface
    
    def online(self,*args, **kwargs):
        """
        Method which returns whether we have Internet connectivity. On Ubuntu,
        this check is limited to verifying there is an active interface with
        an established gateway. 
        
        :returns bool: True if we are online.
        """
        
        result = False
        
        interfaces = self.network.active_interfaces()
        
        for interface in interfaces.values():
            if "ip_addresses" in interface:
                found_qualified_interface = False
                for ip_entry in interface["ip_addresses"]:
                    try:
                        if (ip_entry["ipv4"] and ip_entry["netmask"] 
                                            and ip_entry["broadcast"]):
                            found_qualified_interface = True
                            break
                    except KeyError:
                        pass
                    
                    try:
                        if ip_entry["ipv6"] and ip_entry["scope"] == "Link":
                            found_qualified_interface = True
                            break
                    except KeyError:
                        pass
            
                if found_qualified_interface:
                    result = True
                    break
        
        return result
    
    def on_vpn(self):
        """
        Method to determine if we are on VPN or not.
        """
        on_vpn = False
        
        interface = self.primary_interface()
        
        if interface and interface.startswith("cscotun"):
            on_vpn = True
        
        return on_vpn
    
    def load_hardware_info(self):
        """
        Method to load our hardware information.
        """
        
        bit_bucket = open(os.devnull, 'w')
        
        with open(os.devnull,"w") as bit_bucket:
            logger = logging.getLogger(self.logger_name)
            cmd = ["/usr/bin/lshw","-class","system","-json"]
            try:
                output = subprocess.check_output(cmd, stderr=bit_bucket,
                                        env=systemprofile.get_english_env())
                json_data = json.loads(output)
                self.hardware_info = json_data
                
            except Exception as exp:
                logger.error("Failed to load Hardware info using '{}'. Error:{}".format(
                                                            " ".join(cmd),exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

    def load_disk_info(self):
        """
        Method to load our disk information.
        """
        disk_info = []
        p = PyExecCmd()
        logger = logging.getLogger(self.logger_name)
        try:
            with open(os.devnull, "w") as bit_bucket:
                cmd = [['/bin/lsblk', '-d', '-o', 'name,rota,size', '-P']]
                output, _ = p.exec_cmd(cmd)
                if output:
                    data = output.split("\n")
                if data:
                    for i in data:
                        if i:
                            dict_data = {}
                            matches = re.findall(r'\"(.+?)\"', i)
                            dict_data["name"] = matches[0]
                            dict_data["size"] = matches[2] + "B"
                            dict_data["rota"] = matches[1]
                            disk_info.append(dict_data)
        except Exception as exp:
            logger.error("Failed to load disk info using '{}'. Error:{}".format(" ".join(cmd), exp))
            logger.log(5, "Failure stack trace (handled cleanly):", exc_info=1)
        return disk_info

    def hardware_make(self):
        """
        Method to return the hardware make for this system
        """
        
        make = None
        
        if self.hardware_info is None:
            self.load_hardware_info()
            
        try:
            make = self.hardware_info["vendor"]
        except Exception:
            pass
            
        return make
    
    def hardware_model(self):
        """
        Method to return the hardware model for this system
        """
        
        model = None
        
        if self.hardware_info is None:
            self.load_hardware_info()
            
        try:
            model = self.hardware_info["version"]
        except Exception:
            pass
            
        return model
    
    def hardware_identifier(self):
        """
        Method which returns our hardware identifier.
        """        
        id = None
        
        if self.hardware_info is None:
            self.load_hardware_info()
        
        try:
            id = self.hardware_info["configuration"]["uuid"]
        except Exception:
            pass
        
        return id
    
    def serial_number(self):    
        """
        Method to return our device serial number
        """
        sn = None
        
        if self.hardware_info is None:
            self.load_hardware_info()
            
        try:
            sn = self.hardware_info["serial"]
        except Exception:
            pass
            
        return sn

    def cpu_cores(self):
        """
        Method to return number of cpu cores
        """
        cpu_cores = None
        p = PyExecCmd()
        cmd = [["/bin/cat", "/proc/cpuinfo"], ["/bin/grep", "^cpu cores"], ["/usr/bin/sort"], ["/usr/bin/uniq"]]
        logger = logging.getLogger(self.logger_name)
        try:
            with open(os.devnull, "w") as bit_bucket:
                try:
                    output, _ = p.exec_cmd(cmd)
                    if output:
                        data = output.split(":")[1]
                        cpu_cores = data.strip()
                except Exception as exp:
                    logger.error("Failed to load /proc/cpuinfo info using '{}'. Error:{}".format(" ".join(cmd), exp))
                    logger.log(5, "Failure stack trace (handled cleanly):", exc_info=1)
        except Exception:
            pass
        return int(cpu_cores)

    def cpu_type(self):
        """
        Method to return our device cpu type
        """
        cpu_type = None
        p = PyExecCmd()
        cmd = [["/bin/cat", "/proc/cpuinfo"], ["/bin/grep", "name"], ["/usr/bin/uniq"]]
        logger = logging.getLogger(self.logger_name)
        try:
            with open(os.devnull, "w") as bit_bucket:
                output, _ = p.exec_cmd(cmd)
                if output:
                    data = output.split(":")[1]
                    cpu_type_data = data.strip().split("@")[0]
                    cpu_type = cpu_type_data[:-4].strip()
        except Exception as exp:
            logger.error("Failed to load /proc/cpuinfo info using '{}'. Error:{}".format(" ".join(cmd), exp))
            logger.log(5, "Failure stack trace (handled cleanly):", exc_info=1)
        return cpu_type

    def physical_memory(self):
        """
        Method to return our ram.
        """
        ram_in_kb = None
        ram = None
        cmd = [["/bin/cat", "/proc/meminfo"], ["/bin/grep", "^MemTotal"]]
        p = PyExecCmd()
        logger = logging.getLogger(self.logger_name)
        try:
            with open(os.devnull, "w") as bit_bucket:
                output, _ = p.exec_cmd(cmd)
                if output:
                    data = output.split(":")[1]
                    ram_in_kb = data.strip()
        except Exception as exp:
            logger.error("Failed to load mem info using '{}'. Error:{}".format(" ".join(cmd), exp))
            logger.log(5, "Failure stack trace (handled cleanly):", exc_info=1)
        if ram_in_kb:
            size,suffix = ram_in_kb.split(" ")
            ram = get_bytes(size,suffix)
        return ram

    def system_type(self):
        """
        Method to return our device type ('Desktop','Laptop','Server', etc...)
        """
        chassis = None
        
        if self.hardware_info is None:
            self.load_hardware_info()
        
        try:
            chassis = self.hardware_info["configuration"]["chassis"]
        except KeyError:
            try:
                ## VMWare does not provide a chassis type
                chassis = self.hardware_info["description"]
            except Exception:
                pass
        except Exception:
            pass
        
        if not chassis:
            chassis = "Unknown"
        elif chassis.lower() == "computer":
            chassis = "Computer"
        elif chassis.lower() == "desktop":
            chassis = "Desktop"
        elif chassis.lower() == "laptop":
            chassis = "Laptop"
        elif chassis.lower() == "notebook":
            chassis = "Notebook"
        elif chassis.lower() == "portable computer":
            chassis = "Portable Computer"
        elif chassis.lower() == "tablet":
            chassis = "Tablet"
        elif chassis.lower() == "handheld":
            chassis = "Handheld"
        elif chassis.lower() == "server":
            chassis = "Server"
        elif chassis.lower() == "workspace":
            chassis = "Workspace"
        elif chassis.lower() == "vm":
            chassis = "VirtualMachine"
        
        return chassis
    
    def owner(self):
        """
        Method to return our system owner.
        """
        
        owner = None
        
        if os.path.isfile("/etc/amazon/50primary_user.json"):
            ownership_file = "/etc/amazon/50primary_user.json"
            try:
                with open(ownership_file, "r") as fh:
                    file_content = json.load(fh)
                    owner = str(file_content['primary_user'])
            except Exception as exp:
                logger = logging.getLogger(self.logger_name)
                logger.error("Failed to read system owner: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                           exc_info=1)   
        elif os.path.isfile("/etc/.primaryuser"):
            ownership_file = "/etc/.primaryuser"
            try:
                with open(ownership_file, "r") as fh:
                    owner = fh.readline().strip()
            except Exception as exp:
                logger = logging.getLogger(self.logger_name)
                logger.error("Failed to read system owner: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
            
        return owner
    
    def mac_address(self, interface=None):
        """
        Returns our MAC address for our built-in primary network adapter (en0)
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if not interface:
            interface = self._get_builtin_mac_interface()
        
        if not interface:
            logger.warning("No built-in networking interface could be recognised: cannot determine system MAC Address.")
            return None
        
        cmd = ["/sbin/ifconfig", interface]
        
        try:
            output = subprocess.check_output(cmd, 
                                        env=systemprofile.get_english_env())
            return_code = 0
        except subprocess.CalledProcessError as exp:
            logger.warning("Could not check for MAC address (interface:{}); failed to run ifconfig. Error:{}".format(interface,exp))
            return None
        
        index = output.find("HWaddr ")
        if index:
            return output[index+7:index+24]
    
    def _get_builtin_mac_interface(self):
        """
        This method is used to determine the best interface to use
        for MAC address association. This will be the lowest interface
        on the system, regardless of active state.
        
        This should be compatable with: https://www.freedesktop.org/wiki/Software/systemd/PredictableNetworkInterfaceNames/
        
        :returns: The name of the interface if found, None if none were found.
        """
        
        interface = None
        
        interface_patterns = ["eth.*","p\d{1,2}p\d{1,2}","en.*","em.*","wlan.*"]
        
        logger = logging.getLogger(self.logger_name)
        
        cmd = ["/sbin/ifconfig","-a","-s"]
        
        try:
            output = subprocess.check_output(cmd, 
                                        env=systemprofile.get_english_env())
            return_code = 0
        except subprocess.CalledProcessError as exp:
            logger.warning("Could not check for MAC address; failed to run ifconfig. Error:{}".format(exp ))
            return None 
        
        for pattern in interface_patterns:
            if interface is not None:
                break
            for line in output.splitlines():
                try:
                    current_interface = re.match("^(.*?)\s+.*",line).groups()[0]
                    
                    if re.match(pattern,current_interface):
                        if interface is None:
                            interface = current_interface
                        elif interface > current_interface:
                            interface = current_interface
                except Exception:
                    logger.log(5,"Could not determine interface from line:'{}'".format(line))
                    continue
        
        return interface
    
    
    def last_login_for_user(self, user, tty=None):
        """
        Method to return the last login for the provided user.
        
        :returns: py:class:`datetime.datetime` object representing last login date (UTC)
        :returns: py:class:`None` If no date is found
        
        .. warning:
            In Ubuntu, this method will only return a date if the user is 
            currently logged in. This is due to our reliance on /usr/bin/who,
            as Ubuntu does not seem to record GUI sessions in `last`. If this
            becomes a problem, we could modify this method to consult 
            /var/log/auth.log
        
        """
        logger = logging.getLogger(self.logger_name)
        
        distro, version, id = platform.dist()
        
        if version.startswith("11") or version.startswith("12"):
            logger.debug("_last_login_for_user_ubuntu12()")
            return self._last_login_for_user_ubuntu12(user=user, tty=tty)
        elif version.startswith("14"):
            logger.debug("_last_login_for_user_ubuntu14()")
            return self._last_login_for_user_ubuntu14(user=user, tty=tty)
        else:
            logger.debug("_last_login_for_user_ubuntu14() (other)")
            return self._last_login_for_user_ubuntu14(user=user, tty=tty)            
    
    def _last_login_for_user_ubuntu12(self, user, tty=None):
        """
        Method to return the last login for the provided user for Ubuntu 12.04.
        
        :returns: py:class:`datetime.datetime` object representing last login date (UTC)
        :returns: py:class:`None` If no date is found
        
        .. warning:
            In Ubuntu 12, this method will only return a date if the user is 
            currently logged in. This is due to our reliance on /usr/bin/who,
            as Ubuntu does not seem to record GUI sessions in `last`. If this
            becomes a problem, we could modify this method to consult 
            /var/log/auth.log
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if tty is None:
            tty = "tty7"
        
        date = None
        
        now = datetime.datetime.utcnow()
        
        try:
            last_cmd = ["/usr/bin/who"]
            last_ps = subprocess.Popen(last_cmd,stdout=subprocess.PIPE,
                                        env=systemprofile.get_english_env())
            
            awk_cmd = ["/usr/bin/awk","/{}/ && /{}/ {{print $3\"T\"$4}}".format(user,tty)]
            awk_ps = subprocess.Popen(awk_cmd,stdout=subprocess.PIPE,
                                        stdin=last_ps.stdout,
                                        env=systemprofile.get_english_env()) 
            head_cmd = ["/usr/bin/head","-1"]
            
            logger.log(5,"Looking up login date for user:'{} ({})' using '{}' | '{}' | '{}'...".format(
                                                    user,
                                                    tty,
                                                    "'  '".join(last_cmd),
                                                    "' '".join(awk_cmd),
                                                    "' '".join(head_cmd),
                                                    ))
            
            output = subprocess.check_output(head_cmd, stdin=awk_ps.stdout,
                                        env=systemprofile.get_english_env())
            if output:
                date = datetime.datetime.strptime(output.strip(),"%Y-%m-%dT%H:%M")
                ## Convert to utc
                tz_hours = round((datetime.datetime.now() - datetime.datetime.utcnow()).total_seconds() / 3600)
                tz_timedelta = datetime.timedelta(hours=tz_hours)
                date = date - tz_timedelta
        except Exception as exp:
            logger.error("Failed to determine login date for user:'{} ({})' using '{} | {} | {}'. Error:{}".format(
                                                    user,
                                                    tty,
                                                    " ".join(last_cmd),
                                                    " ".join(awk_cmd),
                                                    " ".join(head_cmd),
                                                    exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
        return date

    def _last_login_for_user_ubuntu14(self, user, tty=None):
        """
        Method to return the last login for the provided user for Ubuntu 14.04.
        
        :returns: py:class:`datetime.datetime` object representing last login date (UTC)
        :returns: py:class:`None` If no date is found
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if tty is None:
            tty = ":0"
        
        date = None
        
        now = datetime.datetime.utcnow()
        
        try:
            last_cmd = ["/usr/bin/last", "-F", user]
            last_ps = subprocess.Popen(last_cmd,stdout=subprocess.PIPE,
                                        env=systemprofile.get_english_env())
                        
            awk_cmd = ["/usr/bin/awk","/{}/ {{print $4\" \"$5\" \"$6\" \"$7\" \"$8}}".format(tty)]
            awk_ps = subprocess.Popen(awk_cmd,stdout=subprocess.PIPE,
                                            stdin=last_ps.stdout,
                                            env=systemprofile.get_english_env()) 
            head_cmd = ["/usr/bin/head","-1"]
            
            logger.log(5,"Looking up login date for user:'{} ({})' using command('{}' | '{}' | '{}')...".format(
                                                    user,
                                                    tty,
                                                    "'  '".join(last_cmd),
                                                    "' '".join(awk_cmd),
                                                    "' '".join(head_cmd),
                                                    ))
            
            output = subprocess.check_output(head_cmd, stdin=awk_ps.stdout,
                                    env=systemprofile.get_english_env())
            if output:
                date = datetime.datetime.strptime(output.strip(),"%a %b %d %H:%M:%S %Y")
                
                ## Convert to utc
                tz_hours = round((datetime.datetime.now() - datetime.datetime.utcnow()).total_seconds() / 3600)
                tz_timedelta = datetime.timedelta(hours=tz_hours)
                date = date - tz_timedelta
        except Exception as exp:
            logger.error("Failed to determine login date for user:'{} ({})' using '{} | {} | {}'. Error:{}".format(
                                                    user,
                                                    tty,
                                                    " ".join(last_cmd),
                                                    " ".join(awk_cmd),
                                                    " ".join(head_cmd),
                                                    exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
        return date
    
    def system_start_date(self):
        """
        Method to output our system start date.
        
        :returns: :py:class:`datetime.datetime` object (UTC)
        """
            
        date = None
        
        logger = logging.getLogger(self.logger_name)
                
        try:
            cmd = ["cat","/proc/uptime"]

            output = subprocess.check_output(cmd,
                                        env=systemprofile.get_english_env())
            date_string = output.split()[0].strip()

            ## date_string will return elapsed seconds from the time of reboot 

            date = datetime.datetime.utcnow() - datetime.timedelta(seconds = float(date_string))
        except Exception as exp:
            logger.error("Failed to determine system start date. Error:{}".format(
                                                    exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
        return date
    
    def current_user(self):
        """
        Method to return our current GUI user.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        username = None
        try:
            w_cmd = ["/usr/bin/who"]
            w_ps = subprocess.Popen(w_cmd, stdout=subprocess.PIPE,
                                        env=systemprofile.get_english_env())
            
            awk_cmd = ["/usr/bin/awk","/{}/ {{print $1; exit}}".format("\\(:[0-9]\\)")]
            
            output = subprocess.check_output(awk_cmd, stdin=w_ps.stdout,
                                        env=systemprofile.get_english_env())
            
            if output:
                username = output.strip()
                
        except Exception as exp:
            logger.error("Failed to determine current user using '{} | {}'. Error:{}".format(
                                                    " ".join(w_cmd),
                                                    " ".join(awk_cmd),
                                                    exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
        return username

    def _get_os_version(self):
        """
        Method to return version of ubuntu ex: 14.04, 16.04 etc.

        :returns str, int(error_encountered_status)

        :ex
           ("14.04",0)
        """

        logger = logging.getLogger(self.logger_name)

        # this will keep track of whether error encountered while evaluation
        error_encountered = 0

        os_version = ""
        try:
            os_version_command = ["/usr/bin/lsb_release","-r","--short"]
            output = subprocess.check_output(os_version_command,
                                    env=systemprofile.get_english_env())
            if output:
                os_version = output.strip()

        except subprocess.CalledProcessError as exp:
            error_encountered = 1
            logger.error("Failed to get operating system version: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        return os_version, error_encountered

    def _get_installed_packages(self):
        """
        Method to return all installed packages with their version number and status.

        :returns Dictionary of packages, int(error_encountered_status)

        :ex
           ({"libexempi3": {"version":"2.2.1-1ubuntu1","status":"install ok installed"},..}, 0)
        """

        logger = logging.getLogger(self.logger_name)

        # this will keep track of whether error encountered while execution
        error_encountered = 0

        installed_packages = {}
        if os.path.isfile(self.installed_packages_status_file):
            logger.info("Reading installed packages status file:{}".format(self.installed_packages_status_file))
            try:
                current_package = ""
                current_architecture = ""
                current_status = ""
                current_version = ""
                with open(self.installed_packages_status_file, "r") as fh:
                    for line in fh:
                        # clearing any leading or trailing spaces
                        line = line.strip()
                        line_stripped = line.split(":")
                        identifier = line_stripped[0].strip().lower()
                        
                        # taking value by check which will avoid cases like
                        # (empty line case)
                        # Package:                    (no package name even though identifier is there)
                        # some info about stuff       (no package keyword is there)
                        value = ""
                        if len(line_stripped) > 1 and line_stripped[1]:
                            value = line_stripped[1].strip()

                        if identifier == "package" and value:
                            current_package = value
                        elif identifier == "architecture" and value:
                            current_architecture = value
                        elif identifier == "status" and value:
                            current_status = value
                        elif identifier == "version" and value:
                            current_version = value

                        # if all the information(package name, architecture, status and version) are available
                        # then adding it to installed_packages
                        if current_package and current_architecture and current_status and current_version:
                            installed_packages[current_package] = {}
                            installed_packages[current_package]["status"] = current_status
                            installed_packages[current_package]["version"] = current_version

            except Exception as exp:
                error_encountered = 1
                logger.error("Failed to read installed packages status file {}: {}".format(self.installed_packages_status_file, exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        else:
            error_encountered = 1
            logger.error("Installed packages status file not present: {}".format(self.installed_packages_status_file))

        return installed_packages, error_encountered

    def _is_installed(self, package_name):
        """
        Method to check if specific package is installed or not.

        returns Boolean, int(error_encountered_status)

        :ex
           (True, 0)
        """

        logger = logging.getLogger(self.logger_name)

        # this will keep track of whether error is encountered while execution
        error_encountered = 0

        try:
            logger.info("Checking if package {} is installed.".format(package_name))
            dpkg_query_cmd  = ["/usr/bin/dpkg-query", "-s", package_name]
            is_installed_ps = subprocess.Popen(dpkg_query_cmd, 
                                        stdout=subprocess.PIPE, 
                                        stderr=subprocess.PIPE,
                                        env=systemprofile.get_english_env())
            output, error   = is_installed_ps.communicate()

            # means error is something other than package not installed
            if error and 'is not installed' not in error:
                error_encountered = 1
            elif output and "install ok installed" in output:
                return True, error_encountered

        except subprocess.CalledProcessError as exp:
            error_encountered = 1
            logger.error("Failure to query dpkg-query: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        return False, error_encountered

    def _is_laptop(self):
        """
        Method to check if current system is laptop or not.

        :returns Boolean, int(error_encountered_status)

        :ex
           (True, 0)
        """

        logger = logging.getLogger(self.logger_name)

        # this will keep track of whether error is encountered while evaluation
        error_encountered = 0
        
        laptop_identifiers = ["Notebook", "Laptop", "Portable Computer", "Handheld", "Tablet"]
        system_type = self.system_type()
        if system_type in laptop_identifiers:
            return True, error_encountered
        elif system_type == "Unknown":
            error_encountered = 1
            logger.error("Failed to detect system type.")
            
        return False, error_encountered

    def packages_minimum_version_installed(self):
        """
        Method to evaluate whether apt software is current. If any apt 
        upgrades pending, we are flagging system as non-compliant.

        :returns Boolean, int(error_encountered_status)

        :ex
           (True, 0)
        """

        logger = logging.getLogger(self.logger_name)

        # this will keep track of whether any error is encountered while evaluation
        error_encountered = 0

        try:
            cmd = ["/usr/bin/apt-get","-s","upgrade"]
            output = subprocess.check_output(cmd,
                                         env=systemprofile.get_english_env())
            for line in output.strip().split('\n'):
                if line.startswith("Inst"):
                    return False, error_encountered
        except Exception as exp: 
            error_encountered = 1
            logger.error("Failed to read apt-get upgrades with error: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        return True, error_encountered

    def ufw_inactive(self):
        """
        Method to report whether Uncomplicated Firewall(ufw) is enabled or not.

        :returns Boolean, int(error_encountered_status)

        :ex
           (True, 0)
        """

        logger = logging.getLogger(self.logger_name)

        # this will keep track of whether error occured while execution
        error_encountered = 0

        try:
            logger.info("Checking ufw status.");
            ufw_status_cmd = ["/usr/bin/sudo", "/usr/sbin/ufw", "status"]
            output = subprocess.check_output(ufw_status_cmd,
                                        env=systemprofile.get_english_env())
            if output and output.split()[1] == "inactive":
                return True, error_encountered
        except subprocess.CalledProcessError as exp:
            error_encountered = 1
            logger.error("Failed to check ufw status: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        except IndexError as exp:
            error_encountered = 1
            logger.error("Index out of bound while checking ufw status: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        return False, error_encountered

    def amazon_firewall_config_installed(self):
        """
        Method to check if amazon-firewall-config package is installed.

        :returns Boolean, int(error_encountered_status)

        :ex
           (True, 0)
        """
        if self.is_legacy_image():
            return self._is_installed("amazon-firewall-config")
        else:
            return self._is_installed("amazon-core")

    def tanium_client_installed(self):
        """
        Method to report whether tanium client is installed or not.

        :returns Boolean, int(error_encountered_status)

        :ex
           (True, 0)
        """

        return self._is_installed("taniumclient")

    def amazon_desktop_management_installed(self):
        """
        Method to check whether Amazon Management package is installed.

        :returns Boolean, int(error_encountered_status)

        :ex
           (True, 0)
        """
        if self.is_legacy_image():
            return self._is_installed("amazon-desktop-management")
        else:
            return self._is_installed("amazon-core")

    def standard_python_version_installed(self):
        """
        Method to check if installed version of python is 2.7.x.

        :returns Boolean, int(error_encountered_status)

        :ex
           (True, 0)
        """

        logger = logging.getLogger(self.logger_name)

        # this will keep track of whether error is encountered while execution
        error_encountered = 0

        try:
            python_version = platform.python_version()
            if python_version.startswith("2.7"):
                return True, error_encountered

        except Exception as exp:
            error_encountered = 1
            logger.error("Failed checking current version of python: {}".format(exp));
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        return False, error_encountered

    def standard_ubuntu_version_installed(self):
        """
        Method to check if any of the standard version 12.04, 14.04, 16,04 of ubuntu is installed or not.

        :returns Boolean, int(error_encountered_status)

        :ex
            (True, 0)
        """

        logger = logging.getLogger(self.logger_name)

        # this will keep track of whether error is encountered while execution
        error_encountered = 0

        logger.info("Checking for current version of ubuntu installed.");
        standard_versions = ["14.04", "16.04", "18.04"]
        ubuntu_version, error_encountered = self._get_os_version()
        if not error_encountered and ubuntu_version in standard_versions:
            return True, error_encountered

        return False, error_encountered

    def non_laptop_image_on_laptop(self):
        """
        Method to check if non laptop image is installed on laptop.
        This check will only be checked on laptops.

        :returns Boolean, int(error_encountered_status)

        :ex
           (False, 0)
        """

        logger = logging.getLogger(self.logger_name)

        # this will keep track of whether error is encountered while execution
        error_encountered = 0
        amazon_laptop = False

        # checking if current system is laptop
        is_laptop, error_encountered = self._is_laptop()

        try:
            if self.is_legacy_image():
                amazon_laptop = os.path.isfile(self.laptop_info_file)
            else:
                cmd = ["/usr/bin/amazon-config-read", "ansible_roles"]
                output = subprocess.check_output(cmd,
                                        env=systemprofile.get_english_env())
                if output and "laptop" in output:
                     amazon_laptop = True
            if not error_encountered and is_laptop and not amazon_laptop:
                return True, error_encountered
        except Exception as exp:
            error_encountered = 1
            logger.error("Failed while checking Amazon laptop status: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        return False, error_encountered

    def root_disk_encrypted(self):
        """
        Method to check if root disk is encrypted.
        We do it only for laptops.

        :returns Boolean, int(error_encountered_status)

        :ex
           (True,0)
        """

        logger = logging.getLogger(self.logger_name)

        # this will track whether error was encountered
        error_encountered = 0
        
        if self.is_legacy_image():
            encryption_check_cmd = ["/opt/amazon/bin/cltag"]
        else:
            encryption_check_cmd = ["/usr/lib/amazon/uquarantine/compliance/laptop_is_encrypted"]

        # checking if current system is laptop
        is_laptop, error_encountered = self._is_laptop()

        # we are not checking for non-laptop cases
        if not error_encountered and not is_laptop:
            return True, error_encountered

        try:
            if not error_encountered:
                # Using Popen because check_output will raise exception on
                # getting no results from cltag
                cmd_ps = subprocess.Popen(encryption_check_cmd, 
                            stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE,
                            env=systemprofile.get_english_env())
                output, error = cmd_ps.communicate()

                if self.is_legacy_image():
                    # If there is an output from cltag means disk is not encrypted.
                    if not output:
                        return True, error_encountered
                else:
                    if "appears to be an Amazon compliant laptop" in output:
                        return True, error_encountered
        except Exception as exp:
            error_encountered = 1
            logger.error("Failed to check disk encryption: ".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        return False, error_encountered

    def is_legacy_image(self):
        """
        Method to determine if Ubuntu system is legacy or ReIntegration image
        :returns Boolean
        """
        is_legacy_image = True
        if self.directoryservice.is_sssd_installed():
            is_legacy_image = False

        return is_legacy_image

systemprofile.SystemProfile = SystemProfileUbuntu