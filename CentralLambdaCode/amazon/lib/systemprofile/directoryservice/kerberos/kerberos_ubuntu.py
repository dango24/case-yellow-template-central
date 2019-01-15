"""
.. module:: systemprofile.directoryservice.kerberos_ubuntu
    :synopsis: Ubuntu system shim for our Kerberos library.
            
    :platform: Ubuntu
    
.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>
    

"""

#MARK: Imports
import logging
import os
import re
import sys
import subprocess

import acme
import systemprofile

from . import validate_access, get_tgt_data, KerberosError, PrincError, KerberosPermissionsError

#MARK: - Module Functions
def get_system_princ(keytab=None):
    """
    Method to lookup our system principal (on ubuntu this may
    differ from hostname).
    """
    
    logger = logging.getLogger(__name__)
    
    princ = None
    
    ## Sanity check permissions (this will throw exceptions on error)
    validate_access(keytab=keytab)
    
    cmd = ["/usr/bin/klist"]
    
    if keytab is None:
        logger.log(9,"Looking Kerberos system principal from system keytab.")
        cmd.append("-k")
    else:
        logger.log(9,"Looking Kerberos system principal from keytab:'{}'".format(
                                                            princ,
                                                            keytab))
        cmd.extend(["-t",keytab])
    
    try:
        output = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as exp:
        raise PrincError("Unable to determine system principal!")
      
    regex = re.compile(".* (.*[^ ]\$)@")
    for line in output.splitlines()[2:]:
        match = regex.match(line)
        if match:
            princ = match.groups()[0]
            break
                    
    if princ is None:
        raise PrincError("Unable to determine system principal!")
    
    return princ

def cachename_for_princ(princ):
    """
    Method which will return the cache name for the provided princ.
    """
    
    ticket = get_tgt_data(princ=princ)
    
    cache_name = None
    
    try:
        cache_name = ticket["cache"]
    except Exception:
        pass
    
    return cache_name
          
def list_tgts():
    """
    Method to list all current ticket granting tickets.
    """
    
    cmd = ["/usr/bin/klist","-l"]
    
    try:
        output = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as exp:
        output = exp.output
        result = False
    
    p = re.compile("\s+")
        
    tickets = []
    for line in output.splitlines()[2:]:
        line_data = p.split(line)
        ticket_data = {}
        ticket_data["princ"] = line_data[0]
        ticket_data["cache"] = line_data[1]
        tickets.append(ticket_data)
        
    return tickets
    
def destroy_tgt(princ=None):
    """
    Destroy either our current ticket, or if provided, a
    ticket matching the provided principal.
    
    :param string princ: The principal to destroy. If not provided, we will
                    destroy our active ticket.
                    
    :raises KerberosError: If we fail to destroy our TGT 
    """
    
    cmd = ["/usr/bin/kdestroy"]
    
    if princ:
        cache = cachename_for_princ(princ)
        if cache:
            cmd.extend(["-c",cache])
        else:
            raise PrincError(princ=princ)
    
    try:
        output = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as exp:
        output = exp.output
        if output.find("No credentials cache") != -1:
            pass
        elif output.find("Can't find cache for") != -1:
            raise PrincError(princ=princ)
        else:
            raise KerberosError("Failed to destroy ticket.")



    
