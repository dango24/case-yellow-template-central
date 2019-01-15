"""
Created on Apr 17, 2018

@author: sbrg, thipperu
"""
import acme
from acme.core import ConfigurableObject
import datetime
import systemprofile
import pykarl
import logging
import base64
import json
import acme.core
import acme.ipc as ipc
import subprocess
from subprocess import CalledProcessError
import threading
import time
import requests
from acme.usher_utils import clean_directory
import os
import sys
import acme.usher_utils as usher_utils
BAD_VERSION_EXPIRY = datetime.timedelta(hours=24) # 24 hours

#MARK: Defaults
DEFAULT_HEALTH_CHECK_FREQUENCY = datetime.timedelta(minutes = 5)
DEFAULT_HEALTH_CHECK_SKEW = datetime.timedelta(seconds = 5)

DEFAULT_HEALTH_REPORT_FREQUENCY = datetime.timedelta(minutes = 60)
DEFAULT_HEALTH_REPORT_SKEW = datetime.timedelta(seconds = 5)

DEFAULT_REMEDIATE_FREQ = datetime.timedelta(minutes = 1)
DEFAULT_REMEDIATE_RETRY = datetime.timedelta(seconds = 30)
DEFAULT_REMEDIATE_RETRY_MAX = datetime.timedelta(hours = 1)

NUM_ALLOWED_WATCHER_RESTARTS = 3
NUM_ALLOWED_WATCHER_FAILED_INSTALL = 3

class UsherHealthStatus(object):
    """
    Class representing the usher health status
    """
    NONE=0
    ACME_INSTALLATION_HEALTHY=1<<0
    ACME_RUNNING_HEALTHY=1<<1
    ACME_REGISTRATION_HEALTHY=1<<2
    ACME_TELEMETRY_HEALTHY=1<<3
    
    WATCHER_INSTALLATION_HEALTHY = 1 << 16
    WATCHER_RUNNING_HEALTHY = 1 << 17
    WATCHER_TELEMETRY_HEALTHY = 1 << 18

class UsherControllerModule(object):
    """
    Class which is responsible for controlling all Usher reporting and health check and remediation.
    """
    def __init__(self,
                identity=None,
                registrant=None,
                karl_event_engine=None, 
                health_check_frequency=DEFAULT_HEALTH_CHECK_FREQUENCY, 
                health_check_skew = DEFAULT_HEALTH_CHECK_SKEW, 
                health_report_frequency = DEFAULT_HEALTH_REPORT_FREQUENCY, health_report_skew = DEFAULT_HEALTH_REPORT_SKEW,
                health_check_enabled = False, usher_load_path = None):
        self.identity = identity
        self.registrant = registrant
        self.karl_event_engine = karl_event_engine
        self.health_check_frequency = health_check_frequency
        self.health_check_skew = health_check_skew
        self.health_report_frequency = health_report_frequency
        self.health_report_skew = health_report_skew
        self.health_check_enabled = health_check_enabled
        self.health_check_timer = None
        self.health_report_timer = None
        self.latest_watcher = None
        self.usher_load_path = usher_load_path
        self.install_watcher_lock = threading.RLock()
        self.logger_name = "UsherHealthController"
        self.num_restart_failures = 0
        self.bad_watcher_versions = []
        self.remediate_watcher_timer = None
        self.remediate_watcher_timer_reset = False
        self.watcher_install_failed_attempts = {}
        self.developer_id = None
        self.health_status = UsherHealthStatus.NONE
        self.verify_codesign_enabled = None
        self.watcher_installers_state_dir = "/usr/local/amazon/var/acme/watcher/state/installers/"
        self.acme_installers_state_dir = "/usr/local/amazon/var/acme/state/installers/"
        self.installer_config_file_name = "InstallerConfig.json"
        self.installer_config_dir_name = "config"
        self.watcher_version = None
    
    def start(self):
        """
        Method to start Usher Controller
        """
        logger = logging.getLogger(self.logger_name)
        try:
            if self.health_check_enabled:
                if self.health_check_timer is None:
                    self.health_check_timer = acme.core.RecurringTimer(self.health_check_frequency,
                                                                       self.perform_health_check,
                                                                       name="UsherACMEHealthCheck")
                    self.health_check_timer.skew = self.health_check_skew
                    self.health_check_timer.use_zero_offset_skew = True
                self.health_check_timer.start(frequency=self.health_check_frequency)
                
                if self.health_report_timer is None:
                    self.health_report_timer = acme.core.RecurringTimer(self.health_report_frequency,
                                                                       self.report_health_status,
                                                                       name="UsherACMEHealthReport")
                    self.health_report_timer.skew = self.health_check_skew
                    self.health_report_timer.use_zero_offset_skew = True
                self.health_report_timer.start(frequency=self.health_report_frequency)
            
            self.watcher_load_path = os.path.join(self.usher_load_path, "Watcher")
        except Exception as exp:
            logger.error("Failed to start usher controller! Error:{}".format(exp))     
    
    def stop(self):
        """
        Method to stop Usher Controller
        """
        logger = logging.getLogger(self.logger_name)
        try:
            if self.health_check_timer:
                self.health_check_timer.cancel()
            if self.health_report_timer:
                self.health_report_timer.cancel()
            if self.remediate_watcher_timer:
                self.remediate_watcher_timer.cancel()
        except Exception as exp:
            logger.error("Failed to start usher controller! Error:{}".format(exp))
        
    def reload(self):
        """
        Method to reload Usher Controller
        """
        logger = logging.getLogger(self.logger_name)
        try:
            self.stop()
            self.start()
        except Exception as exp:
            logger.error("Failed to reload usher controller! Error:{}".format(exp))
    
    def perform_health_check(self):
        """
        Method that does health check for ACME and send Events to Usher
        """
        logger = logging.getLogger(self.logger_name)
        try:
            health_status = UsherHealthStatus.NONE
            watcher_health, watcher_version = self.get_watcher_health_info()
            self.watcher_version = watcher_version
            if watcher_health:
                health_status |= watcher_health
            else:
                logger.warn("ACMEGuardian is not running, will start remediation timer to start ACMEGuardian..")
                if self.remediate_watcher_timer is None:
                    self.num_restart_failures = 0
                    self.remediate_watcher_timer = acme.core.RecurringTimer(DEFAULT_REMEDIATE_FREQ,
                                                                       self._remediate_restart_watcher,
                                                                       name="RemediateWatcher")
                    self.remediate_watcher_timer.skew = self.health_check_skew
                    self.remediate_watcher_timer.use_zero_offset_skew = True
                    self.remediate_watcher_timer.retry_frequency = DEFAULT_REMEDIATE_RETRY
                    self.remediate_watcher_timer.max_retry_frequency = DEFAULT_REMEDIATE_RETRY_MAX
                    self.remediate_watcher_timer.start(frequency=DEFAULT_REMEDIATE_FREQ)
            #Check for 4 things. Installed, Running, Event Sending and Registration
            health_status |= UsherHealthStatus.ACME_INSTALLATION_HEALTHY
            health_status |= UsherHealthStatus.ACME_RUNNING_HEALTHY
            try:
                if self.registrant.is_registered():
                    health_status |= UsherHealthStatus.ACME_REGISTRATION_HEALTHY
            except Exception as exp:
                logger.error("Usher Registration health check failed with error:{0}".format(exp))
            try:
                # This is a chicken egg problem to report reporting failed.
                # If Problem might be one publisher failed, this health check in this event makes sense.
                # If all publisher's has failed, this event might be more of retrospective event.
                if self.karl_event_engine.num_failed_commits == 0:
                    health_status |= UsherHealthStatus.ACME_TELEMETRY_HEALTHY
            except Exception as exp:
                logger.error("Usher telemetry health check failed with error:{0}".format(exp))
                
            self.health_status = health_status
        except Exception as exp:
            logger.error("Sending UsherAcmeHealthReport failed with error:{0}".format(exp))
    
    def report_health_status(self):
        """
        Method that reports health status
        """
        logger = logging.getLogger(self.logger_name)
        if self.karl_event_engine.online() and self.health_status != UsherHealthStatus.NONE:
            karl_payload = {}
            karl_payload["current_watcher_version"]= self.watcher_version
            karl_payload["health_status"]= self.health_status
            self.send_usher_event(karl_payload, "UsherAcmeHealthReport")
        elif not self.karl_event_engine.online():
            logger.error("KARL event engine is not online, skipping sending of the usher event.")
        else:
            logger.info("Waiting for usher health check to occur, skipping sending of the usher event.")

    ##MARK: Methods solely used by usher configuration controller.
    def install(self, installer, filepath):
        """
        Method that installs installer at a given path
        """
        logger = logging.getLogger(self.logger_name)
        try:
            if installer.is_installable_by_acme:
                did_install = self.update_watcher(installer, filepath)
            else:
                did_install = self.update_acme(installer, filepath)
        except Exception as exp:
            logger.error(exp.message)
            raise InstallError(exp.message)
    
    def update_watcher(self, installer, filepath):
        """
        Method to install Watcher.
        :returns: :bool: True if successfully installs the installer given at path else False. 
        :raises: :py:InstallError: On failed installation
        """
        logger = logging.getLogger(self.logger_name)
        
        installer_dir = os.path.dirname(filepath)
        installer_config_file = os.path.join(installer_dir, self.installer_config_file_name)
        installer_config_dir = os.path.join(installer_dir, self.installer_config_dir_name)
        installer_state_config_file = os.path.join(self.watcher_installers_state_dir, self.installer_config_file_name)
        installer_state_config_dir = os.path.join(self.watcher_installers_state_dir, self.installer_config_dir_name)
        usher_utils.copyanything(installer_config_file, installer_state_config_file)
        usher_utils.copyanything(installer_config_dir, installer_state_config_dir)
        
        cmd = self.install_cmd.format(filepath)
        cmd_list = cmd.split()
        logger.info("Installing from path:{}".format(filepath))
        karl_payload = {}
        karl_payload["current_watcher_version"] = installer.old_version
        karl_payload["attempted_watcher_version"] = installer.version
        install_start_time = datetime.datetime.now()
        with self.install_watcher_lock:
            try:
                subprocess.check_output(cmd_list, env=systemprofile.get_english_env())
                if self.force_dependencies_cmd:
                    subprocess.check_output(self.force_dependencies_cmd, env=systemprofile.get_english_env())
                
                if not self.is_watcher_running_with_retry():
                    raise InstallError("ACMEGuardian ping failed after the install")
                
                install_time_millis= int((datetime.datetime.now() - install_start_time).total_seconds() * 1000.0)
                logger.info("Successfully upgraded ACMEGuardian from version:{} to version:{} and time taken to install: {}ms".format(installer.old_version, \
                                                    installer.version, install_time_millis))
                self.latest_watcher = installer
                karl_payload["install_time_millis"] = install_time_millis
                karl_payload["status"] = 1
                karl_payload["is_baseline"] = False
                self.send_usher_event(karl_payload, "UsherWatcherUpdate")
                return True
            except Exception as exp:
                self.conditionally_add_bad_watcher_version_report(installer, karl_payload, install_start_time, exp)
                raise InstallError(exp.message)
    
    def conditionally_add_bad_watcher_version_report(self, installer, karl_payload, install_start_time, exp):
        """
        Method which adds a watcher version to known bad versions if the number of failed installs is more than the allowed limit.
        return: bool: True: If added this version of Watcher to the bad version list and tried remediation
        """
        logger = logging.getLogger(self.logger_name)
        if not self.watcher_install_failed_attempts.get(installer.version):
            self.watcher_install_failed_attempts[installer.version] = 1
        else:
            self.watcher_install_failed_attempts[installer.version] +=1
        
        if self.watcher_install_failed_attempts[installer.version] > NUM_ALLOWED_WATCHER_FAILED_INSTALL:
            logger.error("Total number of failed ACMEGuardian installs:{} exceeded the threshold:{}, \
                        adding version:{} to bad ACMEGuardian versions".format\
                         (self.watcher_install_failed_attempts[installer.version], NUM_ALLOWED_WATCHER_FAILED_INSTALL, installer.old_version))
            self.add_to_bad_watcher_versions(installer)
                
            #Remediate by installing baseline version of Watcher
            try:
                self._remediate_install_watcher()
            except Exception as exp:
                logger.error("Failed to install baseline ACMEGuardian")
            return True
        return False

    def add_to_bad_watcher_versions(self, installer):
        """
        Method to add a version to bad_watcher_versions with datetime
        """
        index = -1
        for i, bad_version_dict in enumerate(self.bad_watcher_versions):
            if installer.version in bad_version_dict.keys():
                index = i 
        if index > -1:
            added_time = self.bad_watcher_versions[index][installer.version]
            if datetime.datetime.now() - datetime.datetime.strptime(added_time, '%Y-%m-%d %H:%M:%S.%f') > BAD_VERSION_EXPIRY:
                self.bad_watcher_versions[index][installer.version] = str(datetime.datetime.now())
        else:
            self.bad_watcher_versions.append({installer.version:str(datetime.datetime.now())})
               
    def update_acme(self, installer, filepath):
        """
        Method to install ACME at given path
        :raises: :py:InstallError: On failed IPC communication to install ACME
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("Sending IPC message to ACMEGuardian to install:{}.....".format(installer.identifier))

        installer_dir = os.path.dirname(filepath)
        installer_config_file = os.path.join(installer_dir, self.installer_config_file_name)
        installer_config_dir = os.path.join(installer_dir, self.installer_config_dir_name)
        installer_state_config_file = os.path.join(self.acme_installers_state_dir, self.installer_config_file_name)
        installer_state_config_dir = os.path.join(self.acme_installers_state_dir, self.installer_config_dir_name)
        usher_utils.copyanything(installer_config_file, installer_state_config_file)
        usher_utils.copyanything(installer_config_dir, installer_state_config_dir)
        
        resp = None
        req = ipc.Request()
        options = {}
        options["filepath"] = filepath
        options["installer"] = installer.to_dict()
        options["installer_name"] = self.acme_inst_name
        req.action="InstallACME"
        req.options=options
        try:
            resp = send_ipc_watcher(req)
        except Exception as exp:
            message = "ACME could not be updated because ACMEGuardian is unreachabled with error:{}".format(exp)
            raise InstallError(message)
        return True
    
    def send_usher_event(self, payload, evt_type):
        """
        Method to Send Usher ACME events to Usher backend.
        """
        logger = logging.getLogger(self.logger_name)
        on_corp = False
        payload["username"] = systemprofile.profiler.current_user()
        payload["hostname"] = systemprofile.profiler.hostname()
        payload["platform"] = systemprofile.profiler.platform()
        if systemprofile.profiler.platform().lower()=="os x":
            payload["platform"] = "macOS"
        payload["current_acme_version"] = acme.daemon.__version__
        payload["is_amazon_managed"] = systemprofile.profiler.is_amazon_managed() 
        
        try:
            on_corp = systemprofile.profiler.on_domain() or systemprofile.profiler.on_vpn()
            payload["on_corp"] = on_corp
        except Exception as exp:
            logger.warn("Could not find if the device is on corp or not, for the event:{} with exception: {}".format(evt_type, exp))
        
        logger.info("Usher event : {} sending data : {}".format(evt_type, payload))
        try:
            event = pykarl.event.Event(type=evt_type,
                                                    subject_area="Usher.Acme",
                                                    payload=base64.b64encode(json.dumps(payload)))
                    
            if self.karl_event_engine:
                self.karl_event_engine.commit_event(event)
            else:
                logger.error("KARL Event Engine is unavailable to send Usher Event with Event type:{0}".format(evt_type))
        except Exception as exp:
            logger.error("Usher event with type:{0} failed with error:{1}".format(evt_type,exp))
    
    #MARK: Methods used by remediation timer
    def _remediate_restart_watcher(self):
        """
        Method to restart watcher while remediating
        
        :raises: :py:Exception: Raise exception if not able to restart watcher, handled and used by the recurring timer to consider failure.
        """
        logger = logging.getLogger(self.logger_name)
        try:
            p = subprocess.Popen(self.restart_watcher_cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE, env=systemprofile.get_english_env())
            (out,err) = p.communicate()
            if not self.is_watcher_running_with_retry():
                self.num_restart_failures += 1
                if self.num_restart_failures > NUM_ALLOWED_WATCHER_RESTARTS:
                    logger.error("Failed to restart ACMEGuardian for {} times, will remediate by installing \
                                    baseline version of ACMEGuardian".format(self.num_restart_failures))
                    self._remediate_install_watcher()
                    if not self.remediate_watcher_timer_reset and self.remediate_watcher_timer:
                        self.remediate_watcher_timer.reset()
                        self.remediate_watcher_timer_reset = True
                else:
                    raise RestartError("Failed to restart ACMEGuardian.. will try again")
            else:
                logger.info("Successfully restarted ACMEGuardian!")
                if self.remediate_watcher_timer:
                    self.num_restart_failures = 0
                    self.remediate_watcher_timer.cancel()
                    self.remediate_watcher_timer = None
                    self.remediate_watcher_timer_reset = False
        except Exception as exp:
            logger.error("Failed to restart ACMEGuardian...")
            raise RestartError("Failed to restart ACME due to error : {}".format(exp)), None, sys.exc_info()[2]
                    
    def _remediate_install_watcher(self):
        """
        Method to install watcher while remediating
        
        :raises: :py:Exception: Raise exception if not able to install watcher, handled and used by the recurring timer to consider failure.
        """
        logger = logging.getLogger(self.logger_name)
        did_install = False
        watcher_status = self.is_watcher_running_with_retry()
        karl_payload = {}
        install_time_millis = 0
        if not watcher_status:
            logger.info("Found that ACMEGuardian is not running, downloading and installing baseline versions of ACMEGuardian")
            did_install, error, install_time_millis = self.download_install_watcher(self.watcher_baseline_url, self.watcher_load_path)
        if did_install:
            karl_payload["current_watcher_version"] = None
            karl_payload["attempted_watcher_version"] = None
            karl_payload["install_time_millis"] = install_time_millis
            karl_payload["status"] = 1
            karl_payload["is_baseline"] = True
            self.send_usher_event(karl_payload, "UsherWatcherUpdate")
            if self.remediate_watcher_timer:
                self.num_restart_failures = 0
                self.remediate_watcher_timer.cancel()
                self.remediate_watcher_timer = None
        else:
            karl_payload["current_watcher_version"] = None
            karl_payload["attempted_watcher_version"] = None
            karl_payload["error_message"] = error
            karl_payload["install_time_millis"] = int(install_time_millis * 1000.0)
            karl_payload["status"] = 0
            karl_payload["is_baseline"] = True
            self.send_usher_event(karl_payload, "UsherWatcherUpdate")
            raise InstallError("Failed to install ACMEGuardian..")
        
    def download_install_watcher(self, url, load_dir):
        """
        This method is solely called to install the baseline version of Watcher.
        Method to download and install an installer given its download url and load_dir
        
        :returns: :bool: True if able to download, verify and install watcher else False
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("Downloading the ACMEGuardian from url:{} to filepath:{}".format(url, load_dir))
        install_time_millis = 0
        try:
            with self.install_watcher_lock:
                clean_directory(self.usher_load_path)
                if not os.path.exists(load_dir):
                    # Permissions on the load_dir needs to executable.
                    os.makedirs(load_dir, mode = 0755)
                zip_file = os.path.join(load_dir, self.watcher_inst_name)
                self._fetch_s3file(url, zip_file)
                usher_utils.extract_zip(zip_file, load_dir)
                installer_package_path = self.find_installerpkg(load_dir)

                if self.verify_codesign_enabled:
                    is_verify, output = self.verify_installer_codesign(installer_package_path)
                    if not is_verify:
                        raise InstallError("Could not verify installer codesign, returned output; {}".format(output))
                    
                logger.info("Installing baseline version of ACMEGuardian installer...")
                
                installer_dir = os.path.dirname(installer_package_path)
                installer_config_file = os.path.join(installer_dir, self.installer_config_file_name)
                installer_config_dir = os.path.join(installer_dir, self.installer_config_dir_name)
                installer_state_config_file = os.path.join(self.watcher_installers_state_dir, self.installer_config_file_name)
                installer_state_config_dir = os.path.join(self.watcher_installers_state_dir, self.installer_config_dir_name)
                usher_utils.copyanything(installer_config_file, installer_state_config_file)
                usher_utils.copyanything(installer_config_dir, installer_state_config_dir)
                
                install_cmd = self.install_cmd.format(installer_package_path)
                install_cmd_list = install_cmd.split()
                install_start_time = datetime.datetime.now()
                subprocess.check_output(install_cmd_list, env=systemprofile.get_english_env())
                if self.force_dependencies_cmd:
                    subprocess.check_output(self.force_dependencies_cmd, env=systemprofile.get_english_env())
                install_time_millis = int((datetime.datetime.now() - install_start_time).total_seconds() * 1000.0)
                if self.is_watcher_running_with_retry():
                    logger.info("Successfully installed baseline version of ACMEGuardian, time taken to install:{} milli seconds".format(install_time_millis))
                else:
                    logger.error("Failed to install and start baseline version of ACMEGuardian, either the installer or the IPC communication is broken")
                    raise InstallError("Failed to install and start baseline version of ACMEGuardian, either the installer or the IPC communication is broken")
        except CalledProcessError as exp:
            exp_message = "Failed to install baseline version of ACMEGuardian while running install command, with exception CalledProcessError and error code:{}".format(exp.returncode)
            logger.error(exp_message)
            return False, str(exp_message), install_time_millis
        except Exception as exp:
            exp_message = "Failed to install baseline version of ACMEGuardian with error: {}".format(exp)
            logger.error(exp_message)
            return False, str(exp_message), install_time_millis
        return True, "", install_time_millis
    
    #MARK: Util methods used in this module and by usher configuration module.
    def get_installer_version(self, installer_identifier):
        """
        Method to find the version installed for the installer
        """
        if installer_identifier.lower() == "acme":
            return acme.daemon.__version__
        elif installer_identifier.lower() == "acmeguardian":
            return str(self.get_watcher_version())
    
    def get_bad_installer_version(self, installer_identifier):
        """
        Method to find the version installed for the installer. A particular version of installer is bad if we fail to install it configured number of times.
        It is used to avoid downloading of the bad version of installer again and again.
        """
        logger = logging.getLogger(self.logger_name)
        bad_versions = []
        result_bad_versions = []
        if installer_identifier == "ACMEGuardian":
            bad_versions = self.bad_watcher_versions
        
        #Asking Watcher about known bad versions of ACME
        elif installer_identifier == "ACME":
            req = ipc.Request()
            req.action="GetBadACMEVersions"
            try:
                resp = send_ipc_watcher(req)
                if resp.status_code == ipc.StatusCode.SUCCESS:
                    bad_versions = resp.data
            except Exception as exp:
                logger.warn("Could not find ACME versions which were failed to install earlier, with error:{}".format(exp))
                
        for version_dict in bad_versions:
            version, added_time = version_dict.items()[0]
            if BAD_VERSION_EXPIRY >  datetime.datetime.now() - datetime.datetime.strptime(added_time, '%Y-%m-%d %H:%M:%S.%f'):
                result_bad_versions.append(version)
                
        return list(set(result_bad_versions))
    
    def get_watcher_version(self):
        """
        Method that talks to watcher to find installed watcher version.
        """
        logger = logging.getLogger(self.logger_name)
        watcher_version = None
        req = ipc.Request()
        req.action="GetVersion"
        resp = None
        try:
            resp = send_ipc_watcher(req)
            if resp.status_code == ipc.StatusCode.SUCCESS:
                watcher_version = resp.data["ACMEGuardianLib"]
        except Exception as exp:
            logger.warn("Could not find ACMEGuardian's version, ACMEGuardian is unreachable with error:{}".format(exp))
        return watcher_version
    
    def is_watcher_running_with_retry(self):
        """
        Method to find if Watcher is running
        """
        logger = logging.getLogger(self.logger_name)
        is_watcher_running = False
        try:
            is_watcher_running = self.is_watcher_running()
        except Exception:
            time.sleep(5)
            try:
                is_watcher_running = self.is_watcher_running()
            except Exception as exp:
                logger.warn("ACMEGuardian is not reachable with error:{}".format(exp))
                pass
            pass
        return is_watcher_running
    
    def is_watcher_running(self):
        """
        Method that talks to watcher to find if watcher is running.
        """
        logger = logging.getLogger(self.logger_name)
        is_running = False
        resp = None
        req = ipc.Request()
        req.action="IsRunning"
        resp = send_ipc_watcher(req)
        if resp.status_code == ipc.StatusCode.SUCCESS:
            is_running = True
        
        return is_running
    
    def get_watcher_telemetry(self):
        """
        Method that talks to watcher to find if watcher can send events.
        """
        logger = logging.getLogger(self.logger_name)
        watcher_telemetry_status = False
        resp = None
        req = ipc.Request()
        req.action="GetKARLStatus"
        try:
            resp = send_ipc_watcher(req)
            if resp.data.get("num_failed_commits") == 0:
                watcher_telemetry_status = True
        except Exception as exp:
            logger.warn("ACMEGuardian is not reachable with error:{}".format(exp))
        
        return watcher_telemetry_status
    
    def get_watcher_health_info(self):
        """
        Method that talks to watcher to find if watcher can send events.
        """
        logger = logging.getLogger(self.logger_name)
        health = UsherHealthStatus.NONE
        watcher_version = None
        resp = None
        req = ipc.Request()
        req.action="GetWatcherHealthInfo"
        try:
            resp = send_ipc_watcher(req)
            health |= UsherHealthStatus.WATCHER_RUNNING_HEALTHY
            health |= UsherHealthStatus.WATCHER_INSTALLATION_HEALTHY
            if resp.data.get("ee_healthy"):
                health |= UsherHealthStatus.WATCHER_TELEMETRY_HEALTHY
            watcher_version = resp.data.get("watcher_version")
        except Exception as exp:
            logger.warn("ACMEGuardian is not reachable with error:{}".format(exp))
        return health, watcher_version

    def send_ipc_acme(self, request):  
        """
        Method to talk to watcher, throws error if IPC gets broken
        """
        resp = None
        request.secure = True
        auth_token_file_dir = "/usr/local/amazon/var/acme/run"
        request.create_auth_token(directory=auth_token_file_dir)
        with ipc.Client(run_directory="/usr/local/amazon/var/acme/run") as (c):
            resp = c.submit_request(request)
        request.delete_auth_token()
        return resp
    
    def _fetch_s3file(self, url, filepath):
        """
        Method to download the provided file from S3 to the specified path.
        Supports large objects.
        
        :raises: :py:class:`IOError` on filesystem problems
        :raises: :py:class:`request.exceptions.RequestException` on request errors.
        """
        
        response = requests.get(url)
        with open(filepath, "w") as fh:
            for chunk in response.iter_content():
                fh.write(chunk)
    
    def disable_watcher(self):
        """
        Method to disable Watcher program
        Returns the output,error return tuple of the command run.
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("Received a command to disable ACMEGuardian...")
        out = None
        err = None
        try:
            p = subprocess.Popen(self.disable_watcher_cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE, env=systemprofile.get_english_env())
            (out,err) = p.communicate()
            logger.info("Disabling ACMEGuardian complete with out: {} err:{}".format(out,err))
        except Exception as exp:
            logger.error("Disabling ACMEGuardian failed with error:{}".format(exp))
        return (out,err)
    
    def enable_watcher(self):
        """
        Method to enable Watcher program
        Returns the output,error return tuple of the command run.
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("Received a command to enable ACMEGuardian...")
        out = None
        err = None
        try:
            p = subprocess.Popen(self.enable_watcher_cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE, env=systemprofile.get_english_env())
            (out,err) = p.communicate()
            logger.info("Enabling ACMEGuardian complete with out: {} err:{}".format(out,err))
        except Exception as exp:
            logger.error("Enabling ACMEGuardian failed with error:{}".format(exp))
        return (out,err)
    
    def find_installerpkg(self, file_path):
        """
        Method which finds the installer package in a folder based on the extension of the installer.
        """
        return usher_utils.find_installerpkg(file_path, self.installer_ext)
    
#MARK: - Module logic
UsherController = UsherControllerModule

def send_ipc_watcher(request):  
    """
    Method to talk to watcher, throws error if IPC gets broken
    """
    resp = None
    request.secure = True
    auth_token_file_dir = "/usr/local/amazon/var/acme/run"
    request.create_auth_token(directory=auth_token_file_dir)
    with ipc.Client(run_directory="/usr/local/amazon/var/acme/watcher/run") as c:
        resp = c.submit_request(request)
    request.delete_auth_token()
    return resp
    
def _configure_macos():
    """
    Method to configure our compliance package for use with macOS
    """
    
    import usher_osx
    global UsherController
    UsherController = usher_osx.UsherControllerOSX

def _configure_ubuntu():
    """
    Method to configure our compliance package for use with Ubuntu
    """
    
    import usher_ubuntu
    global UsherController
    UsherController = usher_ubuntu.UsherControllerUbuntu

## OS Configuration
if acme.platform == "OS X" or acme.platform == "macOS":
    _configure_macos()
elif acme.platform == "Ubuntu":
    _configure_ubuntu()
    
class RestartError(Exception):
    """
    Exception thrown when ACME fails to restart Watcher
    """
    pass
    
class InstallError(Exception):
    """
    Exception thrown when ACME fails to install Watcher
    """
    pass
