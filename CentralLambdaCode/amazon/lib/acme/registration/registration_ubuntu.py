"""
.. module:: acme.registration_ubuntu
    :synopsis: Platform shim of arme.registration for Ubuntu platform
            
    :platform: Ubuntu
    
.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>
    

"""

from . import ApplicantBase


class ApplicantUbuntu(ApplicantBase):
    """
    Class which provides device registration functionality. 
    
    :param string uuid: The unique identifier to use for our device.
        
    """
    
    logger_name = "ApplicantUbuntu"


    
