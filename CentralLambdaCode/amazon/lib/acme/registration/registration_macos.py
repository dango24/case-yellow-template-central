"""
.. module:: acme.registration_macos
    :synopsis: Platform shim of arme.registration for macOS platform
            
    :platform: macOS
    
.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>
    

"""

from . import ApplicantBase


class ApplicantMacOS(ApplicantBase):
    """
    Class which provides device registration functionality. 
    
    :param string uuid: The unique identifier to use for our device.
        
    """
    
    logger_name = "ApplicantMacOS"


    
