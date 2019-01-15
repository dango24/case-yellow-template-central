"""
.. module:: systemprofile.directoryservice.kerberos
    :synopsis: Module containing classes used for interaction with local 
        Kerberos facilities.
            
    :platform: RHEL, OSX, Ubuntu
    
.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>
    

"""

#MARK: Imports
import logging
import os
import re
import sys
import subprocess

import systemprofile


#MARK: - Module Functions

def _get_system_princ(keytab=None):
    """
    Method to lookup our system principal
    """
    
    princ = "{}$".format(systemprofile.profiler.hostname())
    
    return princ

def _get_system_tgt(keytab=None):
    """
    Load a ticket based on our system name.
    
    :param string keytab: Path to our Keytab file (if ommitted we will use system default)
    
    :raises KerberosError: If we fail to obtain our TGT 
    """
    
    princ = get_system_princ(keytab=keytab)
    
    return get_tgt(princ, keytab)

def _destroy_system_tgt():
    """
    Destroy a ticket based on our system name.
    
    :param string keytab: Path to our Keytab file (if ommitted we will use system default)
    
    :raises KerberosError: If we fail to destroy our TGT 
    """
    
    princ = get_system_princ()
    
    return destroy_tgt(princ)

def _has_system_tgt(keytab=None):
    """
    Method to determine if we have a TGT for our system.
    """
    
    return has_tgt(get_system_princ(keytab=None))
    
def _validate_access(keytab=None):
    """
    Method to validate access to the provided keytab. If no keytab is 
    provided, we will check against the default system keytab.
    """
    
    if keytab is None:
        ## Sanity check permissions.
        if os.geteuid() != 0:
            raise KerberosPermissionsError("Cannot access system keytab, must be EUID 0 (root)")        
    else:
        ## Sanity check our keytab and validate permissions.
        if not os.path.exists(keytab):
            raise KerberosError("Keytab:'{}' does not exist!".format(keytab))
        elif not os.access(keytab, os.R_OK):
            raise KerberosPermissionsError("Cannot get Kerberos ticket, access denied to keytab:'{}'".format(keytab))
    
    return True

def _get_tgt(princ, keytab=None):
    """
    Retrieves a Kerberos ticket via material in the 
    specified keytab. If no keytab is specified, we will use 
    the system default.
    
    :param string princ: The Kerberos principal name to load.
    :param string keytab: Path to our Keytab file (if ommitted we will use system default)
    
    :raises KerberosError: If we fail to obtain our TGT 
    
    """
    
    logger = logging.getLogger(__name__)
    
    cmd = ["/usr/bin/kinit"]
    
    ## Sanity check permissions (this will throw exceptions on error)
    validate_access(keytab=keytab)
    
    if keytab is None:
        logger.log(9,"Getting Kerberos TGT for princ:'{}' from system keytab.".format(
                                                            princ))
        cmd.append("-k")
    else:
        logger.log(9,"Getting Kerberos TGT for princ:'{}' from keytab:'{}'".format(
                                                            princ,
                                                            keytab))
        cmd.extend(["-t", keytab])
     
    cmd.append(princ)
    
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exp:
        output = exp.output
        if output.find("unable to reach") != -1:
            raise KDCUnreachableError()
        elif output.find("Failed to find") != -1:
            raise PrincError(princ=princ)
        elif output.find("no suitable keys") != -1:
            raise PrincError(princ=princ)
            
            
    if not has_tgt(princ):
        message = "Failed to fetch TGT for princ:'{}'".format(princ)
        if keytab:
            message += " using keytab:'{}'".format(keytab)
        
        raise KerberosError(message)
    
def _list_tgts():
    """
    Method to list all current ticket granting tickets.
    """
        
    return []

def _has_tgt(princ):
    """
    Method which can be used to determine if we are in the possession of 
    a TGT with the provided principal.
    
    :param string princ: The Kerberos principal name to search for.
    
    :returns bool: True if our ticket exists.
    """
    
    return get_tgt_data(princ) is not None
    
def _get_tgt_data(princ):
    """
    Method which returns TGT metadat regarding the provided principal.
    
    :param string princ: The Kerberos principal name to search for.
    
    :returns bool: True if our ticket exists.
    """
    
    result = False
    
    my_tgt = None
    
    for ticket in list_tgts():
        try:
            if "@" not in princ:
                princ = "{}@".format(princ)
                
            if princ.endswith("@"):
                if ticket["princ"][0:len(princ)].lower() == princ.lower():
                    my_tgt = ticket
            else:
                if ticket["princ"].lower() == princ.lower():
                    my_tgt = ticket
        except Exception:
            pass
            
        if my_tgt:
            break
                
    return my_tgt

def old_has_tgt(princ):
    """
    Method which can be used to determine if we are in the possession of 
    a TGT with the provided principal.
    
    :param string princ: The Kerberos principal name to search for.
    
    :returns bool: True if our ticket exists.
    """

    result = False
    
    cmd = ["/usr/bin/klist","--list-all"]
    
    try:
        output = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as exp:
        output = exp.output
        result = False
        
    if output.find(princ + "@") != -1:
        result = True

    return result
    
def _destroy_tgt(princ=None):
    """
    Destroy either our current ticket, or if provided, a
    ticket matching the provided principal.
    
    :param string princ: The principal to destroy. If not provided, we will
                    destroy our active ticket.
                    
    :raises KerberosError: If we fail to destroy our TGT 
    """
    
    raise KerberosError("Failed to destroy ticket.")


#MARK: Dynamic functions
get_system_princ = _get_system_princ
get_system_tgt = _get_system_tgt
has_system_tgt = _has_system_tgt
destroy_system_tgt = _destroy_system_tgt
validate_access = _validate_access
get_tgt = _get_tgt
get_tgt_data = _get_tgt_data
list_tgts = _list_tgts
has_tgt = _has_tgt
destroy_tgt = _destroy_tgt

#MARK: Context manager
class SystemCredentialContextManager(object):
    """
    Context manager which can be utilized to access system Kerberos
    credentials over a period of time.
    """
    
    def __init__(self):
        self.had_tgt = None
        
    def __enter__(self):
        """
        Context manager entry method.
        """
        if not has_system_tgt():
            self.had_tgt = False
            get_system_tgt()
        else:
            self.had_tgt = True
            
    def __exit__(self, type, value, traceback):
        """
        Context manager exit method
        """
        
        logger = logging.getLogger("SystemCredentialContextManager")
        
        if not self.had_tgt:
            try:
                destroy_system_tgt()
            except PrincError as exp:
                logger.debug("Could not destroy system credentials: {}".format(exp))
            except Exception as exp:
                logger.error("Failed to destroy system credentials: {}".format(exp))
    

#MARK: - Exceptions
class KerberosError(Exception):
    """
    Exception thrown in the event of a Kerberos error.
    """
    def __str__(self):
        if self.message:
            return self.message
        else:
            return "Kerberos operation failed!"

class KerberosPermissionsError(KerberosError):
    """
    Exception thrown in the event of a Kerberos permissions error.
    """
    def __str__(self):
        if self.message:
            return self.message
        else:
            return "Kerberos operation failed!"

class PrincError(KerberosError):
    """
    Exception thrown when an operation fails due to the provided principal name. 
    """
    
    def __init__(self, message=None, princ=None):
        
        self.princ = princ
        
        super(PrincError, self).__init__(message)
    
    def __str__(self):
        if self.message:
            return self.message
        elif self.princ:
            return "Operation failed using princ:{}".format(self.princ)
        else:
            return "Operation failed"

class KDCUnreachableError(KerberosError):
    """
    Exception thrown when no KDCs can be reached. 
    """
    
    def __str__(self):
        if self.message:
            return self.message
        else:
            return "Unable to reach any KDCs"

#MARK: Module vars
def configure_macos():
    """
    Method to configure this module for use with OS X
    """
    
    global get_system_princ, list_tgts, destroy_tgt
    
    import kerberos_macos
    
    get_system_princ = kerberos_macos.get_system_princ
    list_tgts = kerberos_macos.list_tgts
    destroy_tgt = kerberos_macos.destroy_tgt
    
def configure_ubuntu():
    """
    Method to configure this model for use with Linux
    """
    
    global get_system_princ, list_tgts, destroy_tgt
    
    import kerberos_ubuntu
    
    get_system_princ = kerberos_ubuntu.get_system_princ    
    list_tgts = kerberos_ubuntu.list_tgts
    destroy_tgt = kerberos_ubuntu.destroy_tgt
    
platform = systemprofile.current_platform()
if platform == "OS X" or platform == "macOS":
    configure_macos()
elif platform == "Ubuntu":
    platform = "Ubuntu"
    configure_ubuntu()


    
