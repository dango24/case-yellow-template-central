"""
**acme.crypto.crypto_ubuntu** - Shim which is responsible for handling identity
    management functions for Ubuntu.

:platform: Ubuntu
:synopsis: Package which provides various facilities for handling ACME identities.

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""


#MARK: Imports
from . import IdentityBase

class IdentityUbuntu(IdentityBase):
    """
    Class which provides identity loading and saving functionality for Ubuntu
    
    :param common_name: The common name of our identity. This value is used
                    for retrieval and generation functions.
    
    """
    
    logger_name = "IdentityUbuntu"
     
