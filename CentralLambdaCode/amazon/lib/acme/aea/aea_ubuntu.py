from . import AEAModule
import os
import json
import logging
import urlparse

class AEAUbuntu(AEAModule):
    firefox_app_path = ["/usr/lib/firefox", 
                        "/usr/lib/firefox-esr"]
    firefox_auto_cfg_path = "mozilla.cfg"
    firefox_prefs_js_path = "defaults/pref/autoconfig.js"

    chrome_policy_path = "/etc/opt/chrome/policies/managed/AEA.json"
    chromium_policy_path = "/etc/chromium-browser/policies/managed/AEA.json"
    chrome_policy = """{
    "ExtensionSettings": {
        "chrome_ext_id": {
            "installation_mode": "force_installed",
            "update_url": "url/auto_update_chrome/updates.xml"
    }
    },
    "ExtensionInstallWhitelist": [""],
    "ExtensionInstallSources": ["url/*"]
}
"""

    chromium_policy = """{
    "ExtensionSettings": {
        "chromium_ext_id": {
            "installation_mode": "force_installed",
            "update_url": "url/auto_update_chromium/updates.xml"
    }
    },
    "ExtensionInstallWhitelist": [""],
    "ExtensionInstallSources": ["url/*"]
}
"""

    
    def set_chrome_policy(self, force_installed = True):
        '''
        Function to configure Chrome policy for silent installation of extension.
        :param bool force_installed: Parameter to enable/disable auto-install
        '''
        logger = logging.getLogger(self.logger_name)
        
        self.read_aea_config_file()
        json_data=json.loads(self.chrome_policy)
        
        for key in json_data["ExtensionSettings"].keys():
            new_key = key.replace("chrome_ext_id", self.chrome_ext_id)
            if new_key != key:
                json_data["ExtensionSettings"][new_key] = json_data["ExtensionSettings"][key]
                del json_data["ExtensionSettings"][key]
        
        json_data["ExtensionSettings"][self.chrome_ext_id]["update_url"] = self.chrome_ext_update_url
        json_data["ExtensionInstallWhitelist"] = [self.chrome_ext_id]
        parsed_url = urlparse.urlparse(self.landing_page_root)
        parsed_url = parsed_url._replace(path='*')
        extension_install_sources = parsed_url.geturl()
        json_data["ExtensionInstallSources"] = [extension_install_sources]
        
        if force_installed:
            json_data["ExtensionSettings"][self.chrome_ext_id]["installation_mode"] = "force_installed"
        else:
            json_data["ExtensionSettings"][self.chrome_ext_id]["installation_mode"] = "allowed"
        
        try:
            if not os.path.exists(os.path.dirname(self.chrome_policy_path)):
                os.mkdir(os.path.dirname(self.chrome_policy_path,0755))
        
            with open(self.chrome_policy_path, 'w') as f:
                f.write(json.dumps(json_data, indent=4))
                
            logger.info("Chrome policy successfully configured.")
        except Exception as exp:
            logger.error("Unable to configure Chrome policy due to error: {}".format(exp.message))
            raise exp
        

    def set_chromium_policy(self, force_installed = True):
        '''
        Function to configure Chromium policy for silent installation of extension.
        :param bool force_installed: Parameter to enable/disable auto-install
        '''
        logger = logging.getLogger(self.logger_name)
        
	try:
		self.read_aea_config_file()
		json_data=json.loads(self.chromium_policy)

		for key in json_data["ExtensionSettings"].keys():
		    new_key = key.replace("chromium_ext_id", self.chromium_ext_id)
		    if new_key != key:
		        json_data["ExtensionSettings"][new_key] = json_data["ExtensionSettings"][key]
		        del json_data["ExtensionSettings"][key]

		json_data["ExtensionSettings"][self.chromium_ext_id]["update_url"] = self.chrome_ext_update_url
		json_data["ExtensionInstallWhitelist"] = [self.chromium_ext_id]
		parsed_url = urlparse.urlparse(self.landing_page_root)
		parsed_url = parsed_url._replace(path='*')
		extension_install_sources = parsed_url.geturl()
		json_data["ExtensionInstallSources"] = [extension_install_sources]
		
		if force_installed:
		    json_data["ExtensionSettings"][self.chromium_ext_id]["installation_mode"] = "force_installed"
		else:
		    json_data["ExtensionSettings"][self.chromium_ext_id]["installation_mode"] = "allowed"
	except Exception as e:
		logger.error("Unable to set Chromium keys from policy file due to error:{}".format(e.message))
                raise e
        try:
            if not os.path.exists(os.path.dirname(self.chromium_policy_path)):
                os.mkdir(os.path.dirname(self.chromium_policy_path,0755))
        
            with open(self.chromium_policy_path, 'w') as f:
                f.write(json.dumps(json_data, indent=4))
                
            logger.info("Chromium policy successfully configured.")
        except Exception as exp:
            logger.error("Unable to configure Chromium policy due to error: {}".format(exp.message))
            raise exp

    def enable_chrome_autoinstall(self):
        '''
        Function to enable Chrome policy for silent installation of extension.
        '''
        logger = logging.getLogger(self.logger_name)
        logger.info("Enabling Chrome auto-install...")

        self.set_chrome_policy(force_installed = True)
        

    def disable_chrome_autoinstall(self):
        '''
        Function to disable Chrome policy for silent installation of extension.
        '''
        logger = logging.getLogger(self.logger_name)
        logger.info("Disabling Chrome auto-install...")

        self.set_chrome_policy(force_installed = False)

    def enable_chromium_autoinstall(self):
        '''
        Function to enable Chromium policy for silent installation of extension.
        '''
        logger = logging.getLogger(self.logger_name)
        logger.info("Enabling Chromium auto-install...")

        self.set_chromium_policy(force_installed = True)


    def disable_chromium_autoinstall(self):
        '''
        Function to disable Chromium policy for silent installation of extension.
        '''
        logger = logging.getLogger(self.logger_name)
        logger.info("Disabling Chromium auto-install...")

        self.set_chromium_policy(force_installed = False)

