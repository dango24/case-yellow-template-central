'''
Created on Apr 17, 2018

@author: sbrg, thipperu
'''
import logging
import subprocess
from subprocess import CalledProcessError
from . import *
from . import UsherControllerModule
import systemprofile

class UsherControllerUbuntu(UsherControllerModule):
    '''
    This class has Ubuntu variables to override and method verify Watcher
    '''

    logger_name = "UsherControllerUbuntu"
    logger = logging.getLogger(logger_name)
    platform_version = systemprofile.profiler.system_version()
    
    try:
        platform_version_float = float(platform_version)
    except Exception as e:
        logger.info("Ubuntu platform version is not standard, platform version is:{}. Defaulting to ubuntu 14".format(platform_version))
        platform_version_float = 14.0
        
    if platform_version_float > 16.00:
            watcher_baseline_url = "https://s3-us-west-2.amazonaws.com/acmeinstallers-baseline/ubuntu16/ACMEGuardian.zip"
            acme_baseline_url = "https://s3-us-west-2.amazonaws.com/acmeinstallers-baseline/ubuntu16/ACME.zip"
    else:
            watcher_baseline_url = "https://s3-us-west-2.amazonaws.com/acmeinstallers-baseline/ubuntu14/ACMEGuarian.zip"
            acme_baseline_url = "https://s3-us-west-2.amazonaws.com/acmeinstallers-baseline/ubuntu14/ACME.zip"

    force_dependencies_cmd = ["apt-get", "install", "-fy"]
    install_cmd = "dpkg -i {}"
    restart_watcher_cmd = ["service", "acmeguardiand", "start"]
    disable_watcher_cmd = ["service", "acmeguardiand", "stop"]
    enable_watcher_cmd = ["service", "acmeguardiand", "start"]
    watcher_inst_name = "Watcher.zip"
    acme_inst_name = "ACME.zip"
    installer_ext = "deb"
    
    ##MARK: Methods solely used by usher configuration controller to verify if the installer is codesigned
    def verify_installer_codesign(self, package_path):
        """
        Method that verifies codesign
        """
        output = "Not applicable for ubuntu packages"
        status = True
        return status, output
