import datetime
import logging
import os
import json
import ConfigParser
import subprocess
import plistlib
import acme
import acme.agent as agent
import acme.ipc as ipc
import pykarl.event
import getpass
import requests
import tempfile
import urlparse
from systemprofile import profiler

# bit mask for extension status
DEFAULT_STATUS = 0
INSTALLED = 0b01
ENABLED = 0b10
INSTALLED_DISABLED = INSTALLED ^ DEFAULT_STATUS
INSTALLED_ENABLED = INSTALLED ^ ENABLED ^ DEFAULT_STATUS

class AmazonEnterpriseAccessAgent(agent.BaseAgent):
    """
    PyACME Agent which will handle the client side of Amazon Enterprise Access tasks:
    1. Generate extension manifest file to enable browser extension local host messaging.
    2. Report extension status To KARL.
    3. Pop up notification window to remind user of installing the AEA extension.

    ATTENTION: for mac, the pop up app should be put in /Library/Application Support/Amazon/AmazonEnterpriseAccessPopUp.app
    for ubuntu, the pop up app should be put in /usr/local/amazon/bin/aea_ubuntu_popup.py

    source code of AmazonEnterpriseAccessPopUp.app is hosted as "AmazonEnterpriseAccess_PopUp_OSX" at code.amazon.com

    source code of aea_ubuntu_popup.py is located in PyACME/bin

    """
    def __init__(self, key_map=None, settings_keys=None, *args, **kwargs):
        self.identifier = "AmazonEnterpriseAccessAgent"
        self.name = "AmazonEnterpriseAccessAgent"
        self.run_frequency = datetime.timedelta(seconds=60)
        self.run_frequency_skew = datetime.timedelta(seconds=10)
        self.triggers = agent.AGENT_TRIGGER_SCHEDULED
        self.priority = agent.AGENT_PRIORITY_LOW
        self.key_map = key_map
        self.settings_keys = settings_keys
        
        # Define default value
        p = acme.platform.lower()
        if p == "os x" or p == "macos":
            self.platform = "macos"
        elif p == "ubuntu":
            self.platform = "ubuntu"
        else:
            raise Exception("Platform: {}  is not supported".format(p))

        self.aea_ui_enabled = False
        self.aea_generate_ext_manifest_enabled = True
        self.aea_report_ext_status_enabled = True
        self.aea_firefox_ext_silent_install_manifest_enabled = True
        self.aea_chrome_ext_silent_install_manifest_enabled = True
        self.aea_firefox_ext_silent_install_user_enabled = True
        self.aea_chrome_ext_silent_install_user_enabled = True
	self.aea_chromium_ext_silent_install_manifest_enabled = True
        self.aea_chromium_ext_silent_install_user_enabled = True
        self.aea_config_path = os.path.join(acme.core.BASE_DIR, "manifests/config/aea-config.json")

        self.chrome_ext_id = ""
        self.firefox_ext_id = ""
	self.chromium_ext_id = ""
        self.aea_ext_status_event_type = "AEAPluginStatus"
        self.aea_ext_status_subject_area = "ACME"
        self.landing_page_root = ""
        self.landing_page_index_path = "index.html" #Required for Bravehearts path
        self.landing_page_url = ""
        self.firefox_ext_download_path = "extensions/firefox.xpi"
        self.firefox_ext_download_url = ""
        
        self.username = getpass.getuser()
        self.current_user_home_dir = os.path.expanduser("~")

        self.chrome_ext_manifest_name = "amazon_enterprise_access.json"
        self.firefox_ext_manifest_name = "amazon_enterprise_access.json"
	self.chromium_ext_manifest_name = "amazon_enterprise_access.json"

        self.chrome_ext_exe_path = "/usr/local/amazon/bin/acme_amazon_enterprise_access"
        self.firefox_ext_exe_path = "/usr/local/amazon/bin/acme_amazon_enterprise_access"
	self.chromium_ext_exe_path = "/usr/local/amazon/bin/acme_amazon_enterprise_access"

        self.firefox_std_extensions_path = ""
                                                            
        if self.platform == "macos":
            self.chrome_state_file_path = os.path.join(self.current_user_home_dir,
                                                  "Library/Application Support/Google/Chrome/Local State")
            self.firefox_profiles_file_path = os.path.join(self.current_user_home_dir,
                                                      "Library/Application Support/Firefox/profiles.ini")

            self.aea_ext_status_popup_exe_path = "/Library/Application Support/Amazon/AmazonEnterpriseAccessPopUp.app"
            self.aea_browser_ext_state_file_path = os.path.join(self.current_user_home_dir,
                                                           "Library/Application Support/ACME/state",
                                                           "aea_browser_ext_state.json")
            self.aea_popup_pref_file = os.path.join(self.current_user_home_dir,
                                               "Library/Application Support/ACME/state/aea_popup_window.plist")

            self.chrome_ext_manifest_dir_path = os.path.join(self.current_user_home_dir,
                                                        "Library/Application Support/Google/Chrome/NativeMessagingHosts")

            self.firefox_ext_manifest_dir_path = os.path.join(self.current_user_home_dir,
                                                         "Library/Application Support/Mozilla/NativeMessagingHosts")
            
        elif self.platform == "ubuntu":
            self.aea_browser_ext_state_file_path = os.path.join(self.current_user_home_dir,
                                                           ".acme/state",
                                                           "aea_browser_ext_state.json")
            self.chrome_state_file_path = os.path.join(self.current_user_home_dir, ".config/google-chrome/Local State")
	    self.chromium_state_file_path = os.path.join(self.current_user_home_dir, ".config/chromium/Local State")
            self.firefox_profiles_file_path = os.path.join(self.current_user_home_dir, ".mozilla/firefox/profiles.ini")
            self.aea_popup_pref_file = os.path.join(self.current_user_home_dir,
                                               ".acme/state/aea_popup_pref_file.json")
            self.chrome_ext_manifest_dir_path = os.path.join(self.current_user_home_dir,
                                                        ".config/google-chrome/NativeMessagingHosts")
	    self.chromium_ext_manifest_dir_path = os.path.join(self.current_user_home_dir,
                                                        ".config/chromium/NativeMessagingHosts")
            self.firefox_ext_manifest_dir_path = os.path.join(self.current_user_home_dir,
                                                         ".mozilla/native-messaging-hosts")
            self.aea_ext_status_popup_exe_path = "/usr/local/amazon/bin/aea_ubuntu_popup.py"
           
            self.most_recent_profile_path_chromium = None
        else:
            pass

        self.most_recent_profile_path_chrome = None
        self.most_recent_profile_path_firefox = None

        # Read config file to override default value
        if self.key_map is None:
            self.setup_key_map()
        if self.settings_keys is None:
            self.setup_settings_keys()

        super(AmazonEnterpriseAccessAgent, self).__init__(name=self.name, key_map=self.key_map,
                                                          settings_keys=self.settings_keys, identifier=self.identifier,
                                                          *args, **kwargs)

    def setup_key_map(self):
        key_map = {}
        key_map.update(agent.BaseAgent.key_map)
        key_map["aea_ui_enabled"] = None
        key_map["aea_generate_ext_manifest_enabled"] = None
        key_map["aea_report_ext_status_enabled"] = None
        key_map["chrome_ext_id"] = None
	key_map["chromium_ext_id"] = None
        key_map["firefox_ext_id"] = None
        key_map["landing_page_url"] = None
        key_map["chrome_ext_manifest_name"] = None
	key_map["chromium_ext_manifest_name"] = None
        key_map["firefox_ext_manifest_name"] = None
        key_map["chrome_ext_exe_path"] = None
	key_map["chromium_ext_exe_path"] = None
        key_map["firefox_ext_exe_path"] = None
        key_map["aea_ext_status_popup_exe_path"] = None
        key_map["firefox_std_extensions_path"] = None
        key_map["aea_firefox_ext_silent_install_manifest_enabled"] = None
        key_map["aea_chrome_ext_silent_install_manifest_enabled"] = None
	key_map["aea_chromium_ext_silent_install_manifest_enabled"] = None
        
        self.key_map = key_map

    def setup_settings_keys(self):
        settings_keys = agent.BaseAgent.settings_keys[:]
        settings_keys.append("aea_ui_enabled")
        settings_keys.append("aea_generate_ext_manifest_enabled")
        settings_keys.append("aea_report_ext_status_enabled")
        settings_keys.append("chrome_ext_id")
	settings_keys.append("chromium_ext_id")
        settings_keys.append("firefox_ext_id")
        settings_keys.append("landing_page_url")
        settings_keys.append("chrome_ext_manifest_name")
	settings_keys.append("chromium_ext_manifest_name")
        settings_keys.append("firefox_ext_manifest_name")
        settings_keys.append("chrome_ext_exe_path")
	settings_keys.append("chromium_ext_exe_path")
        settings_keys.append("firefox_ext_exe_path")
        settings_keys.append("aea_ext_status_popup_exe_path")
        settings_keys.append("firefox_std_extensions_path")
        settings_keys.append("aea_firefox_ext_silent_install_manifest_enabled")
        settings_keys.append("aea_chrome_ext_silent_install_manifest_enabled")
	settings_keys.append("aea_chromium_ext_silent_install_manifest_enabled")
        self.settings_keys = settings_keys

    def execute(self, trigger=None, data=None):
        """
        Execute the agent tasks at a defined frequency.
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("{} Executing!".format(self.identifier))

        self.load_settings() # this needs to be called every time the agent run
        self.read_aea_config_file()
        self.get_popup_preference()
        
        try:
            self.enforce_prerequisites()
            if self.aea_generate_ext_manifest_enabled:
                self.generate_ext_manifest()
            if self.aea_firefox_ext_silent_install_manifest_enabled and self.aea_firefox_ext_silent_install_user_enabled:
                self.configure_firefox_silent_installation(enable = True)
            elif not self.aea_firefox_ext_silent_install_user_enabled:
                self.configure_firefox_silent_installation(enable = False)
            if self.aea_chrome_ext_silent_install_manifest_enabled and self.aea_chrome_ext_silent_install_user_enabled:
                self.configure_chrome_silent_installation(enable = True)
            elif not self.aea_chrome_ext_silent_install_user_enabled:
                self.configure_chrome_silent_installation(enable = False) 
	   
            #chromium is only on Ubuntu currently : 2018-08-07
            if self.platform == "ubuntu":
                if self.aea_chromium_ext_silent_install_manifest_enabled and self.aea_chromium_ext_silent_install_user_enabled:
                    self.configure_chromium_silent_installation(enable = True)
                elif not self.aea_chromium_ext_silent_install_user_enabled:
                    self.configure_chromium_silent_installation(enable = False)                 

            if self.aea_report_ext_status_enabled:
                self.report_ext_status_to_karl()
            if self.aea_ui_enabled and profiler.online():
                self.prepare_to_show_popup_window()
        except Exception as e:
            logger.error("Error happen when AmazonEnterpriseAccessAgent run. ERROR: {}".format(e))
            self.last_execution_status = agent.AGENT_EXECUTION_STATUS_ERROR
        else:
            self.last_execution_status = agent.AGENT_EXECUTION_STATUS_SUCCESS

        self.last_execution = datetime.datetime.utcnow()
        logger.info("{} Finished Executing!".format(self.identifier))


    """
    Enforce all prerequisite resources
    """

    def enforce_prerequisites(self):
        logger = logging.getLogger(self.logger_name)

        if self.platform == "macos":
            try:
                state_dir_path = os.path.join(self.current_user_home_dir, "Library/Application Support/ACME/state")
                if not os.path.isdir(state_dir_path):
                    os.makedirs(state_dir_path)

                if not os.path.isdir(self.chrome_ext_manifest_dir_path):
                    os.makedirs(self.chrome_ext_manifest_dir_path)

                if not os.path.isdir(self.firefox_ext_manifest_dir_path):
                    os.makedirs(self.firefox_ext_manifest_dir_path)


            except Exception as e:
                logger.error("Error happen when enforce prerequisites. ERROR:{}  ".format(e))

        elif self.platform == "ubuntu":
            try:
                state_dir_path = os.path.join(self.current_user_home_dir, ".acme/state")
                if not os.path.isdir(state_dir_path):
                    os.makedirs(state_dir_path)

                if not os.path.isdir(self.chrome_ext_manifest_dir_path):
                    os.makedirs(self.chrome_ext_manifest_dir_path)

                if not os.path.isdir(self.firefox_ext_manifest_dir_path):
                    os.makedirs(self.firefox_ext_manifest_dir_path)

		if not os.path.isdir(self.chromium_ext_manifest_dir_path):
                    os.makedirs(self.chromium_ext_manifest_dir_path)
            except Exception as e:
                logger.error("Error happen when enforce prerequisites. ERROR:{}  ".format(e))

        else:
            pass

    def read_aea_config_file(self):
        """
        Method to read AEA Config file and set agent variables
        """
        
        logger = logging.getLogger(self.logger_name)

        try:
            try:
                with open(self.aea_config_path, 'r') as f:
                    data = json.load(f)
            except ValueError as e:
                logger.error("Error found while parsing json, Error : {}".format(e))
                return 
            try:
                self.chrome_ext_id = data["chrome_extension_id"]
                self.firefox_ext_id = data["firefox_extension_id"]
                if self.platform == "ubuntu":
		    self.chromium_ext_id = data["chromium_extension_id"]

                self.landing_page_root = data["extension_url"]
            
                self.landing_page_url = os.path.join(self.landing_page_root, 
                                                     self.landing_page_index_path)
                self.firefox_ext_download_url = os.path.join(self.landing_page_root, 
                                                              self.firefox_ext_download_path)
                self.firefox_std_extensions_path = os.path.join("extensions", "{}.xpi".
                                                              format(self.firefox_ext_id))
            except KeyError as e:
                logger.error("Expected key: {} missing from AEA Config file: {}.".format(e, self.aea_config_path))
        except Exception as e:
            logger.error("Error occurred while reading AEA Config file: {}. Error: {}".format(self.aea_config_path, e))

    """
    Generate Extension Manifest File 
    """
    def generate_ext_manifest(self):
        """
        generate the manifest file for browser extension to do native host messaging
        :return:
        """
        self.create_chrome_extension_manifest(self.chrome_ext_manifest_name, self.chrome_ext_manifest_dir_path,
                                              self.chrome_ext_id, self.chrome_ext_exe_path)
        self.create_firefox_extension_manifest(self.firefox_ext_manifest_name, self.firefox_ext_manifest_dir_path,
                                              self.firefox_ext_id, self.firefox_ext_exe_path)

        #chromium is only supported on ubuntu : 2018-08-07
        if self.platform == "ubuntu":
    	    self.create_chromium_extension_manifest(self.chromium_ext_manifest_name, self.chromium_ext_manifest_dir_path,
                                              self.chromium_ext_id, self.chromium_ext_exe_path)

    def create_chrome_extension_manifest(self, manifest_name, manifest_dir_path, extension_id, exe_path):
        logger = logging.getLogger(self.logger_name)
        logger.info("Creating Chrome local host messaging manifest...")

        manifest = dict()
        manifest["name"] = os.path.splitext(manifest_name)[0]
        manifest["description"] = "amazon enterprise access extension communication"
        manifest["path"] = exe_path
        manifest["type"] = "stdio"
        manifest["allowed_origins"] = ["chrome-extension://{}/".format(extension_id)]

        try:
            json_string = json.dumps(manifest)
            if not os.path.exists(manifest_dir_path):
                logger.debug("dir path: {} doesn't exist. creating it...".format(manifest_dir_path))
                os.makedirs(manifest_dir_path)
            file_path = os.path.join(manifest_dir_path, manifest_name)
            with open(file_path, 'w') as f:
                logger.debug("writing manifest to {}".format(file_path))
                f.write(json_string)
        except Exception as e:
            logger.error("Error happen when generate Chrome manifest file. "
                              "path:{}. id:{}. ERROR: {}".format(manifest_dir_path, extension_id, e))

    def create_chromium_extension_manifest(self, manifest_name, manifest_dir_path, extension_id, exe_path):
        logger = logging.getLogger(self.logger_name)
        logger.info("Creating Chromium local host messaging manifest...")

        manifest = dict()
        manifest["name"] = os.path.splitext(manifest_name)[0]
        manifest["description"] = "amazon enterprise access extension communication"
        manifest["path"] = exe_path
        manifest["type"] = "stdio"
        manifest["allowed_origins"] = ["chrome-extension://{}/".format(extension_id)]

        try:
            json_string = json.dumps(manifest)
            if not os.path.exists(manifest_dir_path):
                logger.debug("dir path: {} doesn't exist. creating it...".format(manifest_dir_path))
                os.makedirs(manifest_dir_path)
            file_path = os.path.join(manifest_dir_path, manifest_name)
            with open(file_path, 'w') as f:
                logger.debug("writing manifest to {}".format(file_path))
                f.write(json_string)
        except Exception as e:
            logger.error("Error happen when generate Chromium manifest file. "
                              "path:{}. id:{}. ERROR: {}".format(manifest_dir_path, extension_id, e))

    def create_firefox_extension_manifest(self, manifest_name, manifest_dir_path, extension_id, exe_path):
        logger = logging.getLogger(self.logger_name)
        logger.info("Creating Firefox local host messaging manifest...")

        manifest = dict()
        manifest["name"] = os.path.splitext(manifest_name)[0]
        manifest["description"] = "amazon enterprise access extension communication"
        manifest["path"] = exe_path
        manifest["type"] = "stdio"
        manifest["allowed_extensions"] = [extension_id]

        try:
            json_string = json.dumps(manifest)
            if not os.path.exists(manifest_dir_path):
                logger.debug("dir path: {} doesn't exist. creating it...".format(manifest_dir_path))
                os.makedirs(manifest_dir_path)
            file_path = os.path.join(manifest_dir_path, manifest_name)
            with open(file_path, 'w') as f:
                logger.debug("writing manifest to {}".format(file_path))
                f.write(json_string)
        except Exception as e:
            logger.error("Error happen when generate Firefox manifest file. "
                              "path:{}. id:{}. ERROR: {}".format(manifest_dir_path, extension_id, e))

    """
    Report Extension Status To KARL 
    """
    def report_ext_status_to_karl(self):
        """
        Report the AEA browser extension to KARL only when status is changed.
        status include: username, browser, version number, installation status(not installed, installed & disabled, installed & enabled)
        :return:
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("Try to report AEA extension status to KARL...")

        self.update_most_recent_profile_path()
        self.check_aea_browser_ext_status_file()

        # Fetch previous browser extension status from file
        old_status = self.get_old_ext_status()

        # Fetch browser extension status from OS
        chrome_ext_status = self.get_chrome_ext_status()
        firefox_ext_status = self.get_firefox_ext_status()

        # Check if the state is changed, only send events when status change.
        changed_flag = False
        dispatcher = pykarl.event.dispatcher
        if old_status["chrome"] != chrome_ext_status:
            changed_flag = True
            logger.info("Chrome extension status changed, old: {}, new: {}".format(old_status["chrome"],
                                                                                    chrome_ext_status))
            if dispatcher.is_configured():
                chrome_evt = pykarl.event.Event(type=self.aea_ext_status_event_type,
                                                subject_area=self.aea_ext_status_subject_area,
                                                payload=chrome_ext_status)
                dispatcher.dispatch(chrome_evt)

        if old_status["firefox"] != firefox_ext_status:
            changed_flag = True
            logger.info("Firefox extension status changed, old: {}, new: {}".format(old_status["firefox"],
                                                                                    firefox_ext_status))
            if dispatcher.is_configured():
                firefox_evt = pykarl.event.Event(type=self.aea_ext_status_event_type,
                                                 subject_area=self.aea_ext_status_subject_area,
                                                 payload=firefox_ext_status)
                dispatcher.dispatch(firefox_evt)

	if self.platform == "ubuntu":
            chromium_ext_status = self.get_chromium_ext_status()
            if "chromium" in old_status:
                if old_status["chromium"] != chromium_ext_status:
                    changed_flag = True
                    logger.info("Chromium extension status changed, old: {}, new: {}".format(old_status["chromium"],
                                                                                        chromium_ext_status))
                    if dispatcher.is_configured():
                        chromium_evt = pykarl.event.Event(type=self.aea_ext_status_event_type,
                                                    subject_area=self.aea_ext_status_subject_area,
                                                    payload=chromium_ext_status)
                        dispatcher.dispatch(chromium_evt)

        # If anything change, persist new state
        if changed_flag:
            logger.info("AEA extension status changed, try to persist it at {}".format(self.aea_browser_ext_state_file_path))
            changed_state = dict()
            changed_state["chrome"] = chrome_ext_status
            if self.platform == "ubuntu":
                if chromium_ext_status:
                    changed_state["chromium"] = chromium_ext_status
            changed_state["firefox"] = firefox_ext_status
            self.persist_browser_ext_status(changed_state)

    def check_aea_browser_ext_status_file(self):
        """
        Check if the extension status file exist. If not, create a default one.
        :return:
        """
        logger = logging.getLogger(self.logger_name)

        path = self.aea_browser_ext_state_file_path

        try:
            if os.path.isfile(path):
                return
            else:
                default_status = dict()
                default_status["username"] = ""
                default_status["browser"] = ""
                default_status["extension_version"] = ""
                default_status["installation_status"] = DEFAULT_STATUS
                status = dict()
                status["chrome"] = default_status
                if self.platform == "ubuntu":
                    status["chromium"] = default_status
                status["firefox"] = default_status
                self.persist_browser_ext_status(status)
        except Exception as e:
            logger.error("Error happen when checking AEA extension status file. ERROR: {}".format(e))

    def persist_browser_ext_status(self, state):
        """
        Persist browser extension status to file for later comparison.
        :param state:
        :return:
        """
        logger = logging.getLogger(self.logger_name)

        path = self.aea_browser_ext_state_file_path
        try:
            with open(path, 'w') as f:
                f.write(json.dumps(state))
        except Exception as e:
            logger.error("Error happen when persist AEA extension status at {}. ERROR: {}".format(path, e))

    def get_old_ext_status(self):
        """
        Get previous extension status from file
        :return:
        """
        logger = logging.getLogger(self.logger_name)

        status = None
        path = self.aea_browser_ext_state_file_path
        try:
            with open(path, 'r') as f:
                status = json.load(f)
        except Exception as e:
            logger.error("Error happen when getting old extension status at {}. ERROR: {}".format(path, e))
            pass
        return status

    def get_chrome_ext_status(self):
        """
        Get current Chrome AEA extension status
        :return:
        """
        logger = logging.getLogger(self.logger_name)

        status = dict()
        status["username"] = self.username
        status["browser"] = "chrome"
        status["extension_version"] = ""
        status["installation_status"] = DEFAULT_STATUS

        data = self.get_chrome_ext_data()
        if data is not None:
            try:
                if self.chrome_ext_id in data:
                        status["extension_version"] = data[self.chrome_ext_id]["manifest"]["version"]
                        status["installation_status"] = self.transform_chrome_ext_status(data[self.chrome_ext_id]["state"])
            except Exception as e:
                logger.error("Error happen when get AEA Chrome extension status. ERROR: {}".format(e))

        return status

    def get_chromium_ext_status(self):
        """
        Get current Chromium AEA extension status
        :return:
        """
        logger = logging.getLogger(self.logger_name)

        status = dict()
        status["username"] = self.username
        status["browser"] = "chromium"
        status["extension_version"] = ""
        status["installation_status"] = DEFAULT_STATUS

        data = self.get_chromium_ext_data()
        if data is not None:
            try:
                if self.chromium_ext_id in data:
                        status["extension_version"] = data[self.chromium_ext_id]["manifest"]["version"]
                        status["installation_status"] = self.transform_chromium_ext_status(data[self.chromium_ext_id]["state"])
            except Exception as e:
                logger.error("Error happen when get AEA Chromium extension status. ERROR: {}".format(e))

        return status

    def get_firefox_ext_status(self):
        """
        Get current Firefox AEA extension status
        :return:
        """
        logger = logging.getLogger(self.logger_name)

        status = dict()
        status["username"] = self.username
        status["browser"] = "firefox"
        status["extension_version"] = ""
        status["installation_status"] = DEFAULT_STATUS

        data = self.get_firefox_ext_data()
        if data is not None:
            try:
                section = None
                for sec in data:
                    if sec["id"] == self.firefox_ext_id:
                        section = sec
                        break
                if section is not None:
                    status["extension_version"] = section["version"]
                    status["installation_status"] = self.transform_firefox_ext_status(section["active"])
            except Exception as e:
                logger.error("Error happen when get AEA Firefox extension status. ERROR: {}".format(e))

        return status

    def get_chrome_ext_data(self):
        """
        Get data on all extensions of Chrome from an internal file of Chrome
        :return:
        """
        logger = logging.getLogger(self.logger_name)

        data = None
        path = self.most_recent_profile_path_chrome

        if path is not None:
            possible_file_paths = [os.path.join(path, "Secure Preferences"),
                                   os.path.join(path, "Preferences")
                                   ]
            try:
                for path in possible_file_paths:
                    with open(path, 'r') as f:
                        d = json.load(f)
                        if "extensions" in d and "settings" in d["extensions"]:
                            data = d["extensions"]["settings"]
                    if data is not None:
                        break
            except Exception as e:
                logger.error("Error happen when get all Chrome extension data from {}. ERROR: {}".format(path, e))

        return data

    def get_chromium_ext_data(self):
        """
        Get data on all extensions of Chromium from an internal file of Chromium
        :return:
        """
        logger = logging.getLogger(self.logger_name)

        data = None
        path = self.most_recent_profile_path_chromium

        if path is not None:
            possible_file_paths = [os.path.join(path, "Secure Preferences"),
                                   os.path.join(path, "Preferences")
                                   ]
            try:
                for path in possible_file_paths:
                    with open(path, 'r') as f:
                        d = json.load(f)
                        if "extensions" in d and "settings" in d["extensions"]:
                            data = d["extensions"]["settings"]
                    if data is not None:
                        break
            except Exception as e:
                logger.error("Error happen when get all Chromium extension data from {}. ERROR: {}".format(path, e))

        return data

    def get_firefox_ext_data(self):
        """
        Get data on all extension of Firefox from an internal file of Firefox
        :return:
        """
        logger = logging.getLogger(self.logger_name)

        data = None
        path = self.most_recent_profile_path_firefox

        if path is not None:
            data_file_path = os.path.join(path, "extensions.json")
            try:
                with open(data_file_path, 'r') as f:
                    d = json.load(f)
                    data = d["addons"]
            except Exception as e:
                logger.error("Error happen when get all Firefox extension data from {}. ERROR: {}".format(data_file_path, e))

        return data

    """
    Pop Up Notification Window 
    """
    def prepare_to_show_popup_window(self):
        if self.if_show_again():
            self.show_window()

    def show_window(self):
        if self.platform == "macos":
            self.show_window_mac()
        elif self.platform == "ubuntu":
            self.show_window_ubuntu()

    def show_window_mac(self):
        logger = logging.getLogger(self.logger_name)
        app_path = self.aea_ext_status_popup_exe_path
        if_routine_flag = "true"
        chrome_ext_id = self.chrome_ext_id
        firefox_ext_id = self.firefox_ext_id
        args = ['open', '-a', app_path, '--args', if_routine_flag, chrome_ext_id, firefox_ext_id]
        try:
            subprocess.Popen(args)
        except Exception as e:
            logger.error("Open Mac popup failed. ERROR: {}".format(e))

    def show_window_ubuntu(self):
        logger = logging.getLogger(self.logger_name)
        try:
            subprocess.call([self.aea_ext_status_popup_exe_path])
        except Exception as e:
            logger.error("Open Ubuntu popup failed. ERROR: {}".format(e))

    def if_show_again(self):
        """
        To decide if show the popup or not depending on several conditions
        :return:
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("Try to decide if pop up again...")

        result = False
        chrome_decision = False
        chromium_decision = False
        firefox_decision = False

        self.update_most_recent_profile_path()

        chrome_key = None
        firefox_key = None
        chromium_key = None

        if self.most_recent_profile_path_chrome:
            chrome_key = self.most_recent_profile_path_chrome.lower()
        if self.most_recent_profile_path_firefox:
            firefox_key = self.most_recent_profile_path_firefox.lower()
        if self.platform == "ubuntu":
            if self.most_recent_profile_path_chromium:
                chromium_key = self.most_recent_profile_path_chromium.lower()

        logger.debug("Chrome key: {}. Firefox key: {}. Chromium key: {}.".format(chrome_key, firefox_key, chromium_key))

        preferences = self.get_popup_preference()

        if self.check_if_chrome_installed():
            if chrome_key is None:
                chrome_decision = False
            else:
                if chrome_key in preferences:
                    chrome_decision = preferences[chrome_key]
                else:
                    chrome_decision = True

        if self.check_if_firefox_installed():
            if firefox_key is None:
                firefox_decision = False
            else:
                if firefox_key in preferences:
                    firefox_decision = preferences[firefox_key]
                else:
                    firefox_decision = True

        if self.platform == "ubuntu":
	    if self.check_if_chromium_installed():
                if chromium_key is None:
                    chromium_decision = False
                else:
                    if chromium_key in preferences:
                        chromium_decision = preferences[chromium_key]
                    else:
                        chromium_decision = True

        logger.debug("Chrome decision: {}. Firefox decision: {}. Chromium decision: {}".format(chrome_decision, firefox_decision, chromium_decision))

        if chrome_decision or firefox_decision or chromium_decision:
            result = True

        return result

    def get_popup_preference(self):
        """
        Get the pop up preferences which record if a specific profile want this popup or not
        :return:
        """
        logger = logging.getLogger(self.logger_name)

        preference = dict()

        try:
            if self.platform == "macos":
                preference = plistlib.readPlist(self.aea_popup_pref_file)
            elif self.platform == "ubuntu":
                with open(self.aea_popup_pref_file, 'r') as f:
                    preference = json.load(f)
                self.aea_chromium_ext_silent_install_user_enabled = preference["chromium_auto_install"]

            self.aea_chrome_ext_silent_install_user_enabled = preference["chrome_auto_install"]
            self.aea_firefox_ext_silent_install_user_enabled = preference["firefox_auto_install"]
            
        except Exception as e:
            logger.error("Error happen when getting pop up preference, will return empty preference. ERROR: {}".format(e))

        return preference

    """
    Utils
    """
    def update_most_recent_profile_path(self):
        """
        Update most recent used profile of Chrome, Firefox and Chromium
        :return:
        """
        logger = logging.getLogger(self.logger_name)
        try:
            self.most_recent_profile_path_chrome = self.get_most_recent_profile_path_chrome(self.chrome_state_file_path)
            self.most_recent_profile_path_firefox = self.get_most_recent_profile_path_firefox(self.firefox_profiles_file_path)
            if self.platform == "ubuntu":
    	        self.most_recent_profile_path_chromium = self.get_most_recent_profile_path_chromium(self.chromium_state_file_path)
        except Exception as e:
            logger.error("Error happen when update most recent profile. ERROR: {}".format(e))

    def transform_chrome_ext_status(self, data):
        """
        Transform raw data to be acceptable message format
        :param data:
        :return:
        """
        if data == 1:
            result = INSTALLED_ENABLED
        elif data == 0:
            result = INSTALLED_DISABLED
        else:
            raise Exception("Chrome extension status data format: {} is wrong".format(data))

        return result

    def transform_chromium_ext_status(self, data):
        """
        Transform raw data to be acceptable message format
        :param data:
        :return:
        """
        if data == 1:
            result = INSTALLED_ENABLED
        elif data == 0:
            result = INSTALLED_DISABLED
        else:
            raise Exception("Chromium extension status data format: {} is wrong".format(data))

        return result

    def transform_firefox_ext_status(self, data):
        """
        Transform raw data to be acceptable message format
        :param data:
        :return:
        """

        if data is True:
            result = INSTALLED_ENABLED
        elif data is False:
            result = INSTALLED_DISABLED
        else:
            raise Exception("Firefox extension status data format: {} is wrong".format(data))

        return result

    def update_popup_preference(self, decision, most_recent_profile_path_chrome, most_recent_profile_path_firefox, most_recent_profile_path_chromium=None):
        """
        Update and persist the pop up preference
        :param decision:
        :param most_recent_profile_path_chrome:
        :param most_recent_profile_path_firefox:
	:param most_recent_profile_path_chromium
        :return:
        """
        logger = logging.getLogger(self.logger_name)

        preference = dict()
        pref_file = self.aea_popup_pref_file

        try:
            with open(pref_file, 'r') as f:
                preference = json.load(f)
        except Exception as e:
            logger.error("Error happen when load in old popup preference. ERROR: {}".format(e))

        if most_recent_profile_path_chrome is not None:
            chrome_key = most_recent_profile_path_chrome.lower()
            preference[chrome_key] = decision

        if self.platform == "ubuntu":
	    if most_recent_profile_path_chromium is not None:
                chromium_key = most_recent_profile_path_chromium.lower()
                preference[chromium_key] = decision
                preference["chromium_auto_install"] = self.aea_chromium_ext_silent_install_user_enabled

        if most_recent_profile_path_firefox is not None:
            firefox_key = most_recent_profile_path_firefox.lower()
            preference[firefox_key] = decision

        preference["firefox_auto_install"] = self.aea_firefox_ext_silent_install_user_enabled
        preference["chrome_auto_install"] = self.aea_chrome_ext_silent_install_user_enabled

        try:
            with open(pref_file, 'w') as f:
                f.write(json.dumps(preference))
        except Exception as e:
            logger.error("Error happen when persist popup preference. ERROR: {}".format(e))

    def check_if_chrome_installed(self):
        logger = logging.getLogger(self.logger_name)

        if_installed = False
        platform = self.platform
        if platform == "macos":
            try:
                chrome_bundle_id = "com.google.Chrome"
                cmd = ["/usr/bin/mdfind","kMDItemCFBundleIdentifier={}".format(chrome_bundle_id)]
                output = subprocess.check_output(cmd)
                if output:
                    if_installed = True
            except Exception as exp:
                logger.error("Failed to find Chrome installations using '{}'. Error:{}".format(
                                                            " ".join(cmd),exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        elif platform == "ubuntu":
            try:
                p = subprocess.Popen(["/usr/bin/which", "google-chrome"], stdout=subprocess.PIPE)
                output, _ = p.communicate()
                if output:
                    if_installed = True
            except:
                pass
        else:
            pass

        return if_installed

    def check_if_chromium_installed(self):
        logger = logging.getLogger(self.logger_name)

        if_installed = False
	platform = self.platform
        if platform == "ubuntu":
            try:
                p = subprocess.Popen(["/usr/bin/which", "chromium-browser"], stdout=subprocess.PIPE)
                output, _ = p.communicate()
                if output:
                    if_installed = True
            except:
                pass
        else:
            pass

        return if_installed

    def check_if_firefox_installed(self):
        logger = logging.getLogger(self.logger_name)

        if_installed = False
        platform = self.platform
        if platform == "macos":
            try:
                firefox_bundle_id = "org.mozilla.firefox"
                cmd = ["/usr/bin/mdfind","kMDItemCFBundleIdentifier={}".format(firefox_bundle_id)]
                output = subprocess.check_output(cmd)
                if output:
                    if_installed = True
            except Exception as exp:
                logger.error("Failed to find Firefox installations using '{}'. Error:{}".format(
                                                            " ".join(cmd),exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
    
        elif platform == "ubuntu":
            try:
                p = subprocess.Popen(["/usr/bin/which", "firefox"], stdout=subprocess.PIPE)
                output, _ = p.communicate()
                if output:
                    if_installed = True
            except:
                pass
        else:
            pass

        return if_installed

    def get_most_recent_profile_path_chrome(self, chrome_state_file_path):
        logger = logging.getLogger(self.logger_name)
        profile_path = None
        profile = None
        try:
            with open(chrome_state_file_path, 'r') as f:
                content = f.read()
            state = json.loads(content)
            profile_info = state['profile']
            profiles = profile_info['info_cache']
            profile_names = profiles.keys()
            if len(profile_names) == 1:  # only one profile, last_used key will not show up, just use this one
                profile = profile_names[0]
            else:  # multiple profiles situation
                profile = profile_info["last_used"]
        except Exception as e:
            logger.error("Most recent Chrome profile cannot be decided. ERROR: {}.".format(e))

        parent_dir = os.path.dirname(chrome_state_file_path)
        if profile is not None:
            profile_path = os.path.join(parent_dir, profile)

        return profile_path


    def get_most_recent_profile_path_chromium(self, chromium_state_file_path):
        logger = logging.getLogger(self.logger_name)
        profile_path = None
        profile = None
        try:
            with open(chromium_state_file_path, 'r') as f:
                content = f.read()
            state = json.loads(content)
            profile_info = state['profile']
            profiles = profile_info['info_cache']
            profile_names = profiles.keys()
            if len(profile_names) == 1:  # only one profile, last_used key will not show up, just use this one
                profile = profile_names[0]
            else:  # multiple profiles situation
                profile = profile_info["last_used"]
        except Exception as e:
            logger.error("Most recent Chromium profile cannot be decided. ERROR: {}.".format(e))

        parent_dir = os.path.dirname(chromium_state_file_path)
        if profile is not None:
            profile_path = os.path.join(parent_dir, profile)

        return profile_path

    def get_most_recent_profile_path_firefox(self, firefox_profiles_file_path):
        logger = logging.getLogger(self.logger_name)
        profile_path = None
        parent_dir = os.path.dirname(firefox_profiles_file_path)
        try:
            parser = ConfigParser.ConfigParser()
            parser.read(firefox_profiles_file_path)
            sections = parser.sections()

            if sections == ['General', 'Profile0']:  # edge case, only one profile, default key will not show up
                relative_profile_path = parser.get('Profile0', 'path', 0)
                profile_path = os.path.join(parent_dir, relative_profile_path)
            else:  # generic section case
                for section in sections:
                    if_default = parser.has_option(section, 'default')  # return bool
                    if if_default:
                        relative_profile_path = parser.get(section, 'path', 0)
                        profile_path = os.path.join(parent_dir, relative_profile_path)
                        break
        except Exception as e:
            logger.error("Most recent Firefox profile cannot be decided. ERROR: {}.".format(e))

        return profile_path

    def check_chrome_extension_installation(self, chrome_state_file_path, chrome_ext_id):
        logger = logging.getLogger(self.logger_name)

        result = False
        most_recent_profile_path = self.get_most_recent_profile_path_chrome(chrome_state_file_path)

        if most_recent_profile_path:
            ext_dir = os.path.join(most_recent_profile_path, "Extensions" ,chrome_ext_id)
            try:
                if os.path.isdir(ext_dir):
                    result = True
            except Exception as e:
                logger.error("Error happen when check if Chrome AEA extension is installed. ERROR: {}".format(e))
        return result


    def check_chromium_extension_installation(self, chromium_state_file_path, chromium_ext_id):
        logger = logging.getLogger(self.logger_name)

        result = False
        most_recent_profile_path = self.get_most_recent_profile_path_chromium(chromium_state_file_path)


        if most_recent_profile_path:
            ext_dir = os.path.join(most_recent_profile_path, "Extensions" ,chromium_ext_id)
            try:
                if os.path.isdir(ext_dir):
                    result = True
            except Exception as e:
                logger.error("Error happen when check if Chromium AEA extension is installed. ERROR: {}".format(e))
        return result

    def check_firefox_extension_installation(self, firefox_profiles_file_path, firefox_ext_id):
        logger = logging.getLogger(self.logger_name)

        result = False
        most_recent_profile_path = self.get_most_recent_profile_path_firefox(firefox_profiles_file_path)
        if most_recent_profile_path:
            ext_manifest = os.path.join(most_recent_profile_path, "extensions.json")
            try:
                data = {}
                with open(ext_manifest) as json_data:
                    data = json.load(json_data)
                addons = data.get("addons")
                for addon in addons:
                    if addon["id"] == self.firefox_ext_id and addon["active"]:
                        result = True
            except Exception as e:
                logger.error("Error happen when check if Firefox AEA extension is installed. ERROR: {}".format(e))
        return result

    def check_chrome_force_installed_policy(self):
        """
        Check force_installed key of Chrome extension policy.
        Returns True if enabled, false if disabled
        """
        
        logger = logging.getLogger(self.logger_name)
        policy_data = {}
        force_installed = False
        
        if self.platform == "ubuntu":
            policy_file = "/etc/opt/chrome/policies/managed/AEA.json"
            try:
                with open(policy_file) as json_data:
                    policy_data = json.load(json_data)
            except Exception as e:
                logger.error("Unable to read Chrome policy file: {}. Error: {}".format(policy_file, e))
        elif self.platform == "macos":
            policy_file = os.path.join("/Library/Managed Preferences", self.username, "com.google.Chrome.plist")
            cmd = ["/usr/bin/plutil","-convert","xml1","-o","-",policy_file]
            try:
                policy_string = subprocess.check_output(cmd)
                policy_data = plistlib.readPlistFromString(policy_string)
            except Exception as e:
                logger.error("Unable to read Chrome policy file: {}. Error: {}".format(policy_file, e))
        else:
            pass
        
        try:
            if policy_data["ExtensionSettings"][self.chrome_ext_id]["installation_mode"] == "force_installed":
                force_installed = True
        except KeyError as e:
            logger.error("Expected key: {} missing from Chrome policy file: {}.".format(e, policy_file))
                
        return force_installed
                    

    def check_chromium_force_installed_policy(self):
        """
        Check force_installed key of Chromium extension policy.
        Returns True if enabled, false if disabled
        """
        
        logger = logging.getLogger(self.logger_name)
        policy_data = {}
        force_installed = False
        
        if self.platform == "ubuntu":
            policy_file = "/etc/chromium-browser/policies/managed/AEA.json"
            try:
                with open(policy_file) as json_data:
                    policy_data = json.load(json_data)
            except Exception as e:
                logger.error("Unable to read Chromium policy file: {}. Error: {}".format(policy_file, e))
        else:
            pass
        
        try:
            if policy_data["ExtensionSettings"][self.chromium_ext_id]["installation_mode"] == "force_installed":
                force_installed = True
        except KeyError as e:
            logger.error("Expected key: {} missing from Chromium policy file: {}.".format(e, policy_file))
        
        return force_installed

    def configure_chrome_silent_installation(self, enable = True):
        """
        Configure silent installation of Chrome extension.
        Install Google Chrome AEA policy
        If enable is True,
            Install policy with installation_mode set to forced
        Else,
            Install policy with installation_mode set to allowed
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if self.check_chrome_force_installed_policy() != enable:
            if self.platform == "ubuntu":
                if enable:
                    logger.info("Enabling Chrome silent installation...")
                    self.ipc_client_request(action="EnableChromeAutoInstall")
                else:
                    logger.info("Disabling Chrome silent installation...")
                    self.ipc_client_request(action="DisableChromeAutoInstall")
            elif self.platform == "macos":
                if enable:
                    logger.info("Enabling Chrome silent installation...")
                    installation_mode = "force_installed"
                else:
                    logger.info("Disabling Chrome silent installation...")
                    installation_mode = "allowed"
                parsed_url = urlparse.urlparse(self.landing_page_root)
                parsed_url = parsed_url._replace(path='*')
                extension_install_sources = parsed_url.geturl()
                chrome_policy_plist = {'PayloadContent': [{'PayloadUUID': '368DD5F6-1128-4D44-AEA5-7066F01DB81E', 'PayloadType': 'com.apple.ManagedClient.preferences', 'PayloadDescription': '', 'PayloadEnabled': True, 'PayloadVersion': 1, 'PayloadContent': {'com.google.Chrome': {'Forced': [{'mcx_preference_settings': {'ExtensionInstallSources': [extension_install_sources], 'ExtensionInstallWhitelist': [self.chrome_ext_id], 'ExtensionSettings': {self.chrome_ext_id: {'update_url': os.path.join(self.landing_page_root, 'auto_update_chrome/updates.xml'), 'installation_mode': installation_mode}}}}]}}, 'PayloadOrganization': 'Amazon.com', 'PayloadIdentifier': '368DD5F6-1128-4D44-AEA5-7066F01DB81E', 'PayloadDisplayName': 'Custom'}], 'PayloadIdentifier': 'com.amazon.profile.Google-Chrome-AEA', 'PayloadDisplayName': 'Google Chrome AEA ', 'PayloadEnabled': True, 'PayloadOrganization': 'Amazon.com', 'PayloadRemovalDisallowed': False, 'PayloadUUID': '2C8AB183-A3B0-AEAD-A064-AFCF774E5354', 'PayloadDescription': 'Amazon managed AEA preferences for Google Chrome ', 'PayloadType': 'Configuration', 'PayloadScope': 'User', 'PayloadVersion': 201801171543}
                plist_file = tempfile.mktemp()
                plistlib.writePlist(chrome_policy_plist, plist_file)
                cmd = ["/usr/bin/profiles", "-I", "-F", plist_file]
                output = subprocess.check_output(cmd)
                if output:
                    logger.error("Problem importing profile. Output is: {}. File is {}".format(output, plist_file))
                try:
                    os.remove(plist_file)
                except OSError:
                    pass
            else:
                pass
        else:
            logger.info("Chrome extension installation preference unchanged, doing nothing.")
            

    def configure_chromium_silent_installation(self, enable = True):
        """
        Configure silent installation of Chromium extension.
        Install Google Chromium AEA policy
        If enable is True,
            Install policy with installation_mode set to forced
        Else,
            Install policy with installation_mode set to allowed
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if self.check_chromium_force_installed_policy() != enable:
            if self.platform == "ubuntu":
                if enable:
                    logger.info("Enabling Chromium silent installation...")
                    self.ipc_client_request(action="EnableChromiumAutoInstall")
                else:
                    logger.info("Disabling Chromium silent installation...")
                    self.ipc_client_request(action="DisableChromiumAutoInstall")
            else:
                pass
        else:
            logger.info("Chromium extension installation preference unchanged, doing nothing.")
 

    def configure_firefox_silent_installation(self, enable = True):
        """
        Configure silent installation of firefox extension.
        If not already installed and enable is True, 
            1.Download extension and stage it to standard extension dir
            2.Setup prefs js
            3.Setup autoconfig file
        Else,
            1. Remove extension
            2. Setup autoconfig file
        
        Steps can be found at : 
        https://developer.mozilla.org/en-US/Add-ons/WebExtensions/Alternative_distribution_options/Add-ons_in_the_enterprise#Controlling_automatic_installation 
        """
        
        logger = logging.getLogger(self.logger_name)
        extension_downloaded = False
        self.update_most_recent_profile_path()
        extension_installed = self.check_firefox_extension_installation(self.firefox_profiles_file_path, self.firefox_ext_id)

        if self.check_if_firefox_installed() and extension_installed != enable:
            if enable:
                logger.info("Enabling Firefox silent installation...")
            else:
                logger.info("Disabling Firefox silent installation...")
            self.firefox_std_extensions_path = os.path.join(self.most_recent_profile_path_firefox, self.firefox_std_extensions_path)
            if extension_installed or not enable:
                # Extension is already installed.
                logger.info("Firefox extension installed. Disabling side-loading.")
                self.ipc_client_request(action="ResetFirefoxExtensionsScope")
                if not enable:
                    try:
                        logger.info("Firefox auto-install disabled. Removing existing extension.")
                        os.remove(self.firefox_std_extensions_path)
                    except OSError:
                        pass
                
            # Fetch file from the link and stage it in the location. If already not staged.
            if not os.path.exists(self.firefox_std_extensions_path) and enable:
                logger.info("Firefox auto-install enabled. Downloading extension.")
                self.download_firefox_extension(self.firefox_ext_download_url, self.firefox_std_extensions_path)
                if os.path.exists(self.firefox_std_extensions_path):
                    extension_downloaded = True
            else:
                extension_downloaded = True

            # On successful download of extension file enabling scopes to install it silently
            if extension_downloaded and enable and not extension_installed:
                logger.info("Firefox extension downloaded but not installed. Enabling side-loading.")
                self.ipc_client_request(action="EnableFirefoxExtensionsScope")
        else:
            logger.info("Firefox extension installation preference unchanged, doing nothing.")

    def download_firefox_extension(self, ext_download_link, ext_download_path):
        """
        Method to fetch file from given link and save to given destination path
        """
        logger = logging.getLogger(self.logger_name)
        try:
            response = requests.get(ext_download_link)
            dest_dir = os.path.dirname(ext_download_path)

            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)

            with open(self.firefox_std_extensions_path, 'wb') as fd:
                for chunk in response.iter_content(chunk_size=128):
                    fd.write(chunk)
        except Exception as exp:
            logger.error("Failed to fetch from link: {0} and \
            save to destination: {1}.ERROR: {2}".format(self.firefox_ext_download_url, self.firefox_std_extensions_path, exp))
            raise exp

    def ipc_client_request(self, action, run_directory=None):
        logger = logging.getLogger(self.logger_name)
        response = None
        if not action:
            logger.error("Unable to request due to error: No action specified!")
            return response
        if not run_directory:
            run_directory = os.path.join(acme.BASE_DIR, "run")
        try:
            with ipc.Client(run_directory=run_directory) as c:
                r = ipc.Request(action=action)
                response = c.submit_request(r)
        except Exception as exp:
            logger.error("Unable to complete ipc client request for action '{}' due to error: {}"
                         .format(action, exp.message))
        return response
