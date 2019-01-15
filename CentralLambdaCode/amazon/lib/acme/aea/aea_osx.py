from . import AEAModule
import os
import subprocess
import logging


class AEAMacOS(AEAModule):

    firefox_app_path = []
    firefox_bundle_id = "org.mozilla.firefox"
    firefox_resources_path = "Contents/Resources"
    firefox_auto_cfg_path = "mozilla.cfg"
    firefox_prefs_js_path = "defaults/pref/autoconfig.js"
    
    def find_firefox(self):
        """
        Function to find all Firefox instalations
        """
        logger = logging.getLogger(self.logger_name)
        self.firefox_app_path = []
        cmd = ["/usr/bin/mdfind","kMDItemCFBundleIdentifier={}".format(self.firefox_bundle_id)]
        try:
            output = subprocess.check_output(cmd)
            if output:
                for line in output.splitlines():
                    self.firefox_app_path.append(os.path.join(line, self.firefox_resources_path))
            logger.debug("Firefox resource locations are: {}".format(self.firefox_app_path))
        
        except Exception as exp:
            logger.error("Failed to find Firefox installations using '{}'. Error:{}".format(
                                                        " ".join(cmd),exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
