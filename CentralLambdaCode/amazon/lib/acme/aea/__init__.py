"""
 .. module:: acme.aea
     :synopsis: Module containing classes used by acme for aea extension setup.
     :platform: OSX, Ubuntu

 .. moduleauthor:: Anuj Sharma <snuj@amazon.com>


 """

import acme
import logging
import os
import re
import requests
import json
from preference_action import FirefoxPreferenceAction


class AEAModule(acme.SerializedObject):
    """
    Class containing functions related to AEA extension silent installation.
    """
    logger_name = "AEAModule"
    
    firefox_app_path = None
    firefox_auto_cfg_path = None
    firefox_auto_cfg_array = []
    firefox_prefs_js_path = None

    def __init__(self):
        self.enabled_auto_disable_pref = "lockPref(\"extensions.autoDisableScopes\",14);"
        self.disabled_auto_disable_pref = "lockPref(\"extensions.autoDisableScopes\",15);"
        self.enabled_scope_pref = "lockPref(\"extensions.enabledScopes\",15);"
        self.obscure_value_pref = "pref(\"general.config.obscure_value\",0);"
        self.config_filename_pref = "pref(\"general.config.filename\",\"mozilla.cfg\");"
        
        self.aea_config_path = os.path.join(acme.core.BASE_DIR, "manifests/config/aea-config.json")
        self.chrome_ext_id = ""
	self.chromium_ext_id = ""        
	self.landing_page_root = ""
        self.chrome_ext_update_path = "auto_update_chrome/updates.xml"
        self.chrome_ext_update_url = ""
    
    
    def read_aea_config_file(self):
        """
        Method to read AEA Config file and set agent variables
        """
        
        logger = logging.getLogger(self.logger_name)
        
        try:
            with open(self.aea_config_path, 'r') as f:
                data = json.load(f)
            try:
                self.chrome_ext_id = data["chrome_extension_id"]
                self.chromium_ext_id = data["chromium_extension_id"]
		self.landing_page_root = data["extension_url"]
            
                self.chrome_ext_update_url = os.path.join(self.landing_page_root, 
                                                     self.chrome_ext_update_path)
            except KeyError as e:
                logger.error("Expected key: {} missing from AEA Config file: {}.".format(e, self.aea_config_path))
        except Exception as e:
            logger.error("Error occurred while reading AEA Config file: {}. Error: {}".format(self.aea_config_path, e))

#MARK: Chrome
    def enable_chrome_autoinstall(self):
        '''
        Function to enable auto-install policy for Chrome policy.
        '''
        pass
        
    def disable_chrome_autoinstall(self):
        '''
        Function to disable auto-install policy for Chrome policy.
        '''
        pass
       
#MARK: Firefox
    def set_firefox_preferences(self, config_path, preference_action):
        """
        Setting Firefox preferences in given config file.
        """
        logger = logging.getLogger(self.logger_name)
        try:
            logger.info("Modifying Firefox config: {0}".format(config_path))
            lines = []
            file_write_required = False
            pref1_required = True
            pref2_required = True
            # Setting preferences to check
            if preference_action == FirefoxPreferenceAction.REVERT_SCOPE_PREFERENCES:
                pref1 = self.disabled_auto_disable_pref
                pref2 = self.enabled_scope_pref
                pref1_check_text = "\"extensions.autoDisableScopes\","
                pref2_check_text = "\"extensions.enabledScopes\","
            elif preference_action == FirefoxPreferenceAction.ENABLE_SCOPE_PREFERENCES:
                pref1 = self.enabled_auto_disable_pref
                pref2 = self.enabled_scope_pref
                pref1_check_text = "\"extensions.autoDisableScopes\","
                pref2_check_text = "\"extensions.enabledScopes\","
            elif preference_action == FirefoxPreferenceAction.SETUP_CONFIG_FILE:
                pref1 = self.obscure_value_pref
                pref2 = self.config_filename_pref
                pref1_check_text = "\"general.config.obscure_value\","
                pref2_check_text = "\"general.config.filename\","
            else:
                raise Exception("Preference action value is not valid!")
            existing_pref1_line_no = -1
            existing_pref2_line_no = -1
            counter = 0
            if os.path.exists(config_path):
                with open(config_path, "r") as f:
                    lines = f.readlines()
            else:
                lines.append("// Dummy comment is must on top of new preference file.\n")
            for line in lines:
                line_to_process = re.sub(r'\s+', '', line)
                if line_to_process:
                    # Removing newline character.
                    line_to_process = line_to_process.rstrip()
                # Checking for our first preference
                if line_to_process == pref1:
                    pref1_required = False
                elif not line_to_process.startswith("//") and pref1_check_text in line_to_process:
                    existing_pref1_line_no = counter
                # Checking for our second preference
                if line_to_process == pref2:
                    pref2_required = False
                elif not line_to_process.startswith("//") and pref2_check_text in line_to_process:
                    existing_pref2_line_no = counter
                counter = counter + 1
            # Setting up preferences
            # Overwriting if already exists with different value.
            if pref1_required and existing_pref1_line_no > 0:
                lines[existing_pref1_line_no] = "{}\n".format(pref1)
                file_write_required = True
            elif pref1_required:
                # Appending if not exists
                lines.append("{}\n".format(pref1))
                file_write_required = True
            if pref2_required and existing_pref2_line_no > 0:
                lines[existing_pref2_line_no] = "{}\n".format(pref2)
                file_write_required = True
            elif pref2_required:
                lines.append("{}\n".format(pref2))
                file_write_required = True
            # writing to file if required
            if file_write_required:
                with open(config_path, "w") as f:
                    f.writelines(lines)
        except Exception as exp:
            logger.error("Failed to modify config: {}. ERROR:{}".format(config_path, exp))
            raise exp


    def find_all_firefox_config_files(self):
        """
        Find .cfg files in all Firefox installations.
        """
        logger = logging.getLogger(self.logger_name)

        for app in self.firefox_app_path:
            if os.path.exists(app):
                cfg_fname = self.get_firefox_cfg_file(os.path.join(app, self.firefox_prefs_js_path))
                if cfg_fname:
                    logger.debug("Found config file: {0} in Firefox preference file: {1}".format(cfg_fname, os.path.join(app, self.firefox_prefs_js_path)))
                    cfg_path = os.path.join(app, cfg_fname)
                if not cfg_fname:
                    # The prefs file is broken. Set up a new and proper prefs file with our values
                    logger.error("Firefox preference file with config file setting does not exist. Creating new Firefox preference file.")
                    self.set_firefox_preferences(os.path.join(app, self.firefox_prefs_js_path), FirefoxPreferenceAction.SETUP_CONFIG_FILE)
                    cfg_path = os.path.join(app, self.firefox_auto_cfg_path)
                    if cfg_path not in self.firefox_auto_cfg_array:
                        self.firefox_auto_cfg_array.append(cfg_path)
                else:
                    if cfg_path not in self.firefox_auto_cfg_array:
                        self.firefox_auto_cfg_array.append(cfg_path)
                logger.info("Added file:{0} to list of Firefox config files.".format(cfg_path))


    def get_firefox_cfg_file(self, firefox_prefs_js_path):
        """
        Get the existing .cfg file defined in given prefs.js path.
        """
        logger = logging.getLogger(self.logger_name)
        cfg_fname = ''
        prefs_file_dir = os.path.dirname(firefox_prefs_js_path)
        # list .js files and try to find the .cfg file definition in it.
        for file_name in os.listdir(prefs_file_dir):
            if file_name.endswith(".js"):
                # ToDo: Revisit this with a stronger regex.
                file_lines_list = []
                pref_file_name = ""
                with open(os.path.join(prefs_file_dir, file_name), "r") as fh:
                    file_lines_list = fh.readlines()
                    pref_file_name = fh.name
                # Iterate through each line to find our config.
                for file_line in file_lines_list:
                    if ("general.config.filename" in file_line):
                        match = re.findall(r'\((.*?)\)', file_line)[0]
                        param_list = match.split(",")
                        # param_list[0] would be general.config.filename
                        # param_list[1] would be the config file of our interest
                        cfg_fname = param_list[1]
                        # Clean up the string
                        cfg_fname = cfg_fname.strip()
                        cfg_fname = cfg_fname.replace("\'", "")
                        cfg_fname = cfg_fname.replace("\"", "")

        return cfg_fname


    def enable_firefox_extensions_scope(self):
        '''
        Function to enable scope preferences for silent installation of extension.
        '''
        try:
            self.firefox_auto_cfg_array = []
            self.find_firefox()
            logger = logging.getLogger(self.logger_name)
            logger.info("Enabling silent extension installation for Firefox.")
            self.find_all_firefox_config_files()
            for cfg_path in self.firefox_auto_cfg_array:
                self.set_firefox_preferences(cfg_path, FirefoxPreferenceAction.ENABLE_SCOPE_PREFERENCES)
                logger.info("Silent extension installation enabled successfully for config file: {}".format(cfg_path))
        except Exception as exp:
            logger.error("Unable to enable silent extension installation for Firefox due to error: {}".format(exp.message))
            raise exp


    def reset_firefox_extensions_scope(self):
        """
        Function to disable scope preferences for silent installation of extension.
        """
        try:
            self.firefox_auto_cfg_array = []
            self.find_firefox()
            logger = logging.getLogger(self.logger_name)
            logger.info("Disabling silent extension installation for Firefox.")
            self.find_all_firefox_config_files()
            for cfg_path in self.firefox_auto_cfg_array:
                self.set_firefox_preferences(cfg_path, FirefoxPreferenceAction.REVERT_SCOPE_PREFERENCES)
                logger.info("Silent extension installation disabled successfully for config file: {}".format(cfg_path))
        except Exception as exp:
            logger.error("Unable to disable silent extension installation for Firefox due to error: {}".format(exp.message))
            raise exp


    def find_firefox(self):
        """
        Function to find all Firefox instalations
        """
        pass

#MARK: - Module logic
AeaModule = AEAModule


def configure_osx():
    """
    Method to configure aea module for mac os.
    """

    import aea_osx
    global AeaModule
    AeaModule = aea_osx.AEAMacOS


def configure_ubuntu():
    """
    Method to configure aea module for ubuntu.
    """

    import aea_ubuntu
    global AeaModule
    AeaModule = aea_ubuntu.AEAUbuntu


if acme.platform == "OS X" or acme.platform == "macOS":
    configure_osx()
elif acme.platform == "Ubuntu":
    configure_ubuntu()
