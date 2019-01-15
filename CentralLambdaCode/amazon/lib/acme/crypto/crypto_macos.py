"""
**acme.crypto.crypto_macos** - Shim which is responsible for handling identity
    management functions for macOS.

:platform: macOS
:synopsis: Package which provides various facilities for handling ACME identities.

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""


#MARK: Imports
import logging
import os
import subprocess
import sys

from . import IdentityBase, IdentityError, IdentityNotFoundError, PassphraseError, generate_password

class IdentityMacOS(IdentityBase):
    """
    Class which provides identity loading and saving functionality for macOS
    
    :param common_name: The common name of our identity. This value is used
                    for retrieval and generation functions.
    
    """
    
    logger_name = "IdentityMacOS"
    
    ## Keychain specific bits
    keychain_path = "/Library/Keychains/System.keychain"
    keychain_item_template = "ACME.identity.{}"
    keychain_service_name = "identifier"
    keychain_comment_string = "ACME Crypto Key"
    
    @property
    def keychain_item_name(self):
        """
        Property which returns the name to use for our Keychain entry.
        """
        return self.keychain_item_template.format(self.common_name)
        
    def get_passphrase(self, keychain_item_name=None, keychain_service_name=None, keychain_path=None):
        """
        Method used to retrieve our secret used for loading/saving certificate
        data. On macOS systems, this passphrase resides in the system keychain.
        If no such Keychain item exists, we will call 'create_passpharse()' to
        create and store a new passphrase.
        
        :param keychain_item_name: The name of the item to retrieve from the Keychain
        :type keychain_item_name: string
        :param keychain_service_name: The name of the service to retrieve from 
        item in the Keychain
        :type keychain_service_name: string
        :param keychain_path: The path of the Keychain to search
        :type keychain_path: string
        
        :raises PassphraseError: If we are unable to load our passphrase.
        
        :returns: The fetched passphrase        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        passphrase = None
        if keychain_item_name is None:
            keychain_item_name = self.keychain_item_name
        if keychain_service_name is None:
            keychain_service_name = self.keychain_service_name
        if keychain_path is None:
            keychain_path = self.keychain_path
        
        cmd = ["/usr/bin/security", 
                "find-generic-password", 
                "-s", 
                keychain_item_name, 
                "-a", 
                keychain_service_name,
                "-g",
                keychain_path]
                
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(cmd)))
            output = subprocess.check_output(cmd,stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as exp:
            raise PassphraseError("Failed to retrieve passphrase from Keychain! No matching secret found!"), None, sys.exc_info()[2]
        
        for line in output.splitlines():
            if line.startswith("password:"):
                ## Strip out literal quotes from password
                passphrase = line[11:-1]
         
        if passphrase is None:
            raise PassphraseError("Failed to retrieve passphrase from Keychain!")
        else:
            return passphrase
        
    def create_passphrase(self, passphrase=None, keychain_item_name=None, keychain_service_name=None, keychain_comment_string=None, keychain_path=None):
        """
        Method that will create a new passphrase and store it in our
        Keychain.
        
        :param passphrase: The passphrase to store in the Keychain
        :type passphrase: string
        :param keychain_item_name: The name of the item to store in the Keychain
        :type keychain_item_name: string
        :param keychain_service_name: The name of the service to store in the 
        item in the Keychain
        :type keychain_service_name: string
        :param keychain_comment_string: The comment to store in the item in the 
        Keychain
        :type keychain_comment_string: string
        :param keychain_path: The path of the Keychain to search
        :type keychain_path: string
        
        :returns: The stored passphrase 
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if passphrase is None:
            passphrase = generate_password(length=64)
        if keychain_item_name is None:
            keychain_item_name = self.keychain_item_name
        if keychain_service_name is None:
            keychain_service_name = self.keychain_service_name
        if keychain_comment_string is None:
            keychain_comment_string = self.keychain_comment_string
        if keychain_path is None:
            keychain_path = self.keychain_path
        
        cmd = ["/usr/bin/security", 
                "add-generic-password", 
                "-s", 
                keychain_item_name, 
                "-a", 
                keychain_service_name,
                "-w",
                passphrase,
                "-j",
                keychain_comment_string,
                "-U",
                keychain_path]
                
        logger_cmd = cmd[:]
        logger_cmd[7] = "********"
                                                
        try:
            logger.log(5,"Running command: ('{}')".format("' '".join(logger_cmd)))
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as exp:
            raise PassphraseError("Failed to save passphrase to Keychain!"), None, sys.exc_info()[2]
        
        return passphrase
                                
        
