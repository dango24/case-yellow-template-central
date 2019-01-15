'''
Created on Apr 17, 2018

@author: sbrg, thipperu
'''
import logging
import subprocess
from subprocess import CalledProcessError
from acme.usher_utils import clean_directory
from . import *
from . import UsherControllerModule
import systemprofile

class UsherControllerOSX(UsherControllerModule):
    '''
    This class has OSX variables to override and methods to verify Watcher
    '''

    logger_name = "UsherControllerOSX"
    install_cmd = "/usr/sbin/installer -pkg {} -target /"
    verify_codesign_cmd = "/usr/sbin/pkgutil --check-signature {}"
    watcher_plist_path = "/Library/LaunchDaemons/com.amazon.acmeguardiand.plist"
    restart_watcher_cmd = ["launchctl", "load", "-w", watcher_plist_path]
    disable_watcher_cmd = ["launchctl", "unload", "-w", watcher_plist_path]
    enable_watcher_cmd = ["launchctl", "load", "-w", watcher_plist_path]
    watcher_baseline_url = "https://s3-us-west-2.amazonaws.com/acmeinstallers-baseline/macos/ACMEGuardian.zip"
    acme_baseline_url = "https://s3-us-west-2.amazonaws.com/acmeinstallers-baseline/macos/ACME.zip"
    watcher_inst_name = "Watcher.zip"
    acme_inst_name = "ACME.zip"
    force_dependencies_cmd = None
    installer_ext = "pkg"
    
    ##MARK: Methods solely used by usher configuration controller to verify if the installer is codesigned
    def verify_installer_codesign(self, package_path):
        """
        Method that verifies codesign
        """
        cmd = self.verify_codesign_cmd.format(package_path)
        cmd_list = cmd.split()
        output = subprocess.check_output(cmd_list, env=systemprofile.get_english_env())
        if "Status: signed by a certificate trusted by" in output:
            return True, output
        else:
            return False, output
        

