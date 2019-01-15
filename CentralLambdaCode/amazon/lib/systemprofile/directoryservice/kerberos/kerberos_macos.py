"""
.. module:: systemprofile.directoryservice.kerberos_ubuntu
    :synopsis: macOS system shim for our Kerberos library.
            
    :platform: macOS
    
.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>
    

"""

#MARK: Imports
import os
import re
import sys
import subprocess
import plistlib

from . import KerberosError, PrincError, KerberosPermissionsError

#MARK: - Module Functions

def get_system_princ(keytab=None):
    """
    Method to lookup our system principal
    """
    
    princ = None
    
    cmd = ["/usr/sbin/dsconfigad","-xml","-show"]
    try:
        plist_data = subprocess.check_output(cmd)
        plist = plistlib.readPlistFromString(plist_data)
        princ = plist["General Info"]["Computer Account"]

    except subprocess.CalledProcessError as exp:
        raise PrincError("Unable to determine system principal!")
    
    if princ is None:
        raise PrincError("Unable to determine system principal!")
    
    return princ


def list_tgts():
    """
    Method to list all current ticket granting tickets.
    """
    
    cmd = ["/usr/bin/klist","--list-all"]
    
    try:
        output = subprocess.check_output(cmd)
    except subprocess.CalledProcessError as exp:
        output = exp.output
        result = False
    
    p = re.compile("\s+")
        
    tickets = []
    for line in output.splitlines()[1:]:
        line_data = p.split(line)
        ticket_data = {}
        if line_data[0] == "*":
            ticket_data["active"] = True
        ticket_data["princ"] = line_data[1]
        ticket_data["cache"] = line_data[2]
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
        cmd.extend(["-p",princ])
    
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



    
