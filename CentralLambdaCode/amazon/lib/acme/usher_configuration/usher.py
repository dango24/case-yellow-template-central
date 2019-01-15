"""
...package:: acme.configuration
    :synopsis: Module containing functionalities for updating installed installers.
    :platform: OSX, Ubuntu

.. moduleauthor:: Chethan Thipperudrappa <thipperu@amazon.com>
"""

import datetime
import os
import sys
import logging
import json
import shutil

import requests
from requests.models import Response

import systemprofile
import acme.core
import collections
import operator
import threading
import time
from . import *
from acme.usher_utils import clean_directory
import OpenSSL.crypto as crypto
import acme
import glob
import acme.usher_utils as usher_utils

#MARK: Defaults
USHER_UPDATE_FREQUENCY = datetime.timedelta(seconds = 21600)
USHER_UPDATE_SKEW = datetime.timedelta(seconds=300)

class UsherConfigModule(ConfigModule):
    """
    :py:class:`ConfigModule` subclass which provides functionality
    necessary for fetching installer configurations from Richter.
    
    #todo: update doc
    .. example:
        >>> cc = ConfigurationController(identity=d.identity,
        ...                                registrant=d.registrant)
        >>> file_dict = {   "interval": 15,
        >>>                 "execution_skew": 5,
        >>>                 "installers": {
        >>>                     "testfile.txt" : {
        >>>                         "file_name": "testfile.txt",
        >>>                         "update_frequency": 10,
        >>>                     }
        >>>                 }
        >>>             }
        >>> 
        >>> cf = UsherConfigModule(controller=cc,dict_data=file_dict)
        >>> 
        >>> cc.register_module(cf)
        >>> cc.start()
    
    """
    
    key_map = ConfigModule.key_map.copy()
    key_map["load_path"] = None
    key_map["staging_path"] = None
    
    key_map["installers"] = "<getter=get_installers,setter=set_installers>;"
    key_map["installers_state"] = "<getter=get_installer_state,setter=set_installer_state>;"
    
    settings_keys = ConfigModule.settings_keys[:]
    settings_keys.append("installers")
    
    state_keys = ConfigModule.state_keys[:]
    state_keys.extend(["installers_state"])
    
    installers = {}
    
    def __init__(self, usher_controller=None,key_map=None, 
                        settings_keys=None, state_keys=None,
                            load_path=None, staging_path=None, verify_codesign_enabled = None,
                                              *args, **kwargs):
        
        
        self.installers = {}
        self.load_path = load_path          #: Path for our module code 
        self.staging_path = staging_path    #: Path for our parcels
        
        self.url_path = "register/get_installer_targets"
        self.installer_url_path = "/usher/get_installer"
        self.installers_lock = threading.RLock()
        self.usher_controller = usher_controller
        if key_map is None:
            key_map = UsherConfigModule.key_map
        
        if settings_keys is None:
            settings_keys = UsherConfigModule.settings_keys[:]
        
        if state_keys is None:
            state_keys = UsherConfigModule.state_keys[:]
            
        super(UsherConfigModule, self).__init__( key_map=key_map,
                                                settings_keys=settings_keys,
                                                state_keys=state_keys,*args, **kwargs)
        self.installer_ext = self.get_installer_ext()
        self.sig_ext = "sig"
        self.verify_codesign_enabled = verify_codesign_enabled
        self.install_error_code = InstallErrorCode.SUCCESS

    #MARK: ConfigurationFileConfigModule Methods
    def get_installers(self):
        """
        Method to return our configured installers as a serializable dictionary,
        keyed by config name.
        """
        
        installers = {}
        
        with self.installers_lock:
            for identifier, installer in self.installers.iteritems():
                installers[identifier] = installer.to_dict()
            
        return installers
        
    def set_installers(self, installers):
        """
        Method which accepts a list of file data in dictionary form and
        update local installers.
        """
        
        logger = logging.getLogger(__name__)
        
        with self.installers_lock:
            for identifier, installer_data in installers.iteritems():
                try:
                    installer = UsherInstallerConfigEntry(dict_data=installer_data)
                    if not identifier in self.installers:
                        self.installers[identifier] = installer
                    else:
                        self.installers[identifier].load_dict(installer_data)
                    logger.debug("Loaded settings for file: {}".format(
                                                                identifier))
                except Exception as exp:
                    logger.error("Failed to de-serialize UsherInstallerConfigEntry. Data:'{}'.  Error: {}".format(
                                                            installer_data,
                                                            exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
            ## Remove missing files
            for key in [key for key in self.installers.keys() if key not in installers.keys()]:
                del(self.installers[key])

    def get_installer_state(self):
        """
        Method to return our configured files' state as a serializable dictionary,
        keyed by config name.
        """
        
        installers = {}
        
        with self.installers_lock:
            for identifier, installer in self.installers.iteritems():
                state_map = installer.key_map_for_keys(installer.state_keys)
                installers[identifier] = installer.to_dict(key_map=state_map)
                
        return installers
        
    def set_installer_state(self, installers):
        """
        Method which accepts a list of file data in dictionary form and
        update local files.
        """
        
        logger = logging.getLogger(__name__)
        
        with self.installers_lock:
            for identifier, installer_data in installers.iteritems():
                try:
                    installer = UsherInstallerConfigEntry(dict_data=installer_data)
                    state_map = installer.key_map_for_keys(installer.state_keys)
                    if identifier in self.installers:
                        installer = installers[identifier]
                        installer.load_dict(installer_data, key_map=state_map)
                    else:
                        self.installers[identifier] = installer
                    
                    logger.debug("Loaded state for file: {}".format(
                                                                identifier))
                except Exception as exp:
                    logger.error("Failed to de-serialize UsherInstallerConfigEntry. Data:'{}'.  Error: {}".format(
                                                            installer_data,
                                                            exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
            ## Remove missing files
            for key in [key for key in self.installers.keys() if key not in installers.keys()]:
                del(self.installers[key])
    
    #MARK: ConfigModule methods
    def should_run_immediately(self):
        """
        Method to determine whether or not our configuration module 
        has successfully completed an execution.
        
        :returns: (bool) True if we have installers which have never configured or
                        need immediate action taken.
        
        """
        
        needs_update = False
         
        with self.installers_lock:
            for installer in self.installers.values():
                try:
                    if installer.needs_update():
                        needs_update = True
                        break
                except Exception:
                    pass
        return needs_update

    def get_current_interval(self):
        """
        Method to return our timer's base interval. This will be the lowest
        update interval amongst all of our configured files.
        """
        
        logger = logging.getLogger(__name__)
                
        installer_next_activity = None
        next_activity = None
        with self.installers_lock:
            for installer in self.installers.values():
                installer_next_activity = installer.next_update()
                if next_activity is None:
                    next_activity = installer_next_activity
                elif next_activity > installer_next_activity:
                    next_activity = installer_next_activity
        if next_activity is not None:
            interval = next_activity - datetime.datetime.utcnow()
        else:
            interval = super(UsherConfigModule, self).get_current_interval()
        
        ## If we have a negative interval, return one shortly in the future
        if interval.total_seconds() < 0:
            logger.log(9, "UsherConfigModule calculated negative execution interval, will execute immediately.")
            interval = datetime.timedelta(seconds=60)
        
        logger.log(2, "get_current_interval() returns: {}".format(interval))
        
        return interval
    
    def run(self):
        """
        Our primary execution routine.
        """
        
        logger = logging.getLogger(__name__)
        
        ## Determine if it's time to renew, if not, defer
        
        self.last_update_attempt = datetime.datetime.utcnow()
        try:
            installers = self.update_usher_installers()
        except ThrottledRequestError as exp:
            logger.warning("Could not fetch installers configuration...")
            
            if exp.throttled_until is not None:
                ticks_till_relief = abs(exp.throttled_until 
                                            - datetime.datetime.utcnow())
            else:
                ticks_till_relief = self.get_current_interval()
            
            raise acme.core.DeferredTimerException(frequency=ticks_till_relief), None, sys.exc_info()[2]
        self.last_update = datetime.datetime.utcnow()
        
        ## Here if all intended executions succeeded. Update our
        ## timer interval to align with our next update
        self.timer.frequency = self.get_current_interval()
        
    #MARK: UsherConfigModule methods
    def update_usher_installers(self):
        """
        Method which will fetch our current installer configuration from Richter
        and update our active configuration.
        """
        logger = logging.getLogger(__name__)
        try:
            response = self._fetch_configuration()
            response_json = response.json()
        except (ThrottledRequestError, ConfigurationError):
            message = "Failed to fetch usher configuration due to throttling"
            logger.error(message)
            raise ResponseError(message), None, sys.exc_info()[2]
        except Exception as exp:
            message = "Failed to fetch usher configuration; {}".format(exp.message)
            logger.error(message)
            raise ResponseError(message), None, sys.exc_info()[2]
        
        installer_data = None
        if response_json and int(response_json['status']) == ResultStatus.SUCCESS:
            try:
                installer_data = response_json['data']['UsherInstallers']
            except KeyError as exp:
                logger.warning("No installer data sent by the server")
        elif response_json and int(str(response_json['status'])) == ResultStatus.FAILED:
            message = "Failed to fetch installer configuration; {}".format(
                                                response_json["message"])
            logger.error(message)
            
            raise ResponseError(message)
            
        self._process_configuration(installer_data)
    
    def _fetch_configuration(self):
        """
        Method that will fetch our installer configuration from
        our backend service and return our response
        
        :returns: :py:class:`requests.models.Response` instance.
        
        :raises: :py:class:`ConfigurationError` If we are not configured
        :raises: :py:class:`ThrottledRequestError` If our API is currently 
                                            throttled.
        :raises: :py:class:`ResponseError` If we recieved an API error.
        """
        
        response = None
        platform = systemprofile.profiler.platform() 
        user_or_host = systemprofile.profiler.current_user()
        data = {}
        data["platform"] = platform
        data["platform_version"] = systemprofile.profiler.system_version()
        response = self.controller.make_api_call(url_path=self.url_path,
                                                    params=data)
        return response
        
    def _process_configuration(self, usher_data):
        """
        This method is used to process the results of our 
        installer configuration. This method will compare active runtime
        configuration against the provide configuration. 
        
        It will unload and remove from disk any loaded installers which are not 
        specified, and will download installers which are missing, or which do 
        not have the same identifier+version as the respective installer 
        specified in the provided configuration. 
        
        :param configuration: Our configuration to process
        :type configuration: (dict)
        
        """
        
        logger = logging.getLogger(__name__)
        
        uc = self.usher_controller
        
        installers_to_fetch = collections.OrderedDict()
        installers = {}
        ## fetching installer data for each installer
        if usher_data:
            remote_installers = usher_data
            remote_installers = sorted(usher_data, key=operator.itemgetter('priority'), reverse=False)
            
            for installer_info in remote_installers:
                installer_info = self.dict_util(installer_info)
                remote_installer = UsherInstallerConfigEntry(identifier=installer_info["identifier"], 
                                                    dict_data=installer_info)
                
                last_updated = datetime.datetime.utcnow()
                remote_installer.last_update = last_updated
                remote_installer.last_update_attempt = last_updated
                    
                try:
                    all_bad_versions = []
                    installer_version = uc.get_installer_version(remote_installer.identifier)
                    remote_installer.old_version = installer_version
                    all_bad_versions = uc.get_bad_installer_version(remote_installer.identifier)
                    remote_installer.bad_versions = all_bad_versions
                        
                    if remote_installer.version in all_bad_versions:
                        logger.info("Targeted {} is one of the bad versions:{}, skipping updating to this version:{}".format(remote_installer.identifier, all_bad_versions, remote_installer.version))
                        continue
                    
                    if installer_version != remote_installer.version:
                        logger.info("Installed installer:'{}' does not match required version:'{}' (has '{}'), will update from server...".format(
                                            remote_installer.identifier, 
                                            remote_installer.version,
                                            installer_version))
                        installers_to_fetch[remote_installer.identifier] = remote_installer
                    else:
                        logger.info("Installed installer:'{}' matches required version:'{}'...".format(
                                            remote_installer.identifier, 
                                            remote_installer.version
                                            ))
                        
                    self.installers[remote_installer.identifier] = remote_installer
                except Exception as exp:
                    logger.warning("Failed to determine current version for installer:'{}', will update from server...".format(
                                                remote_installer.identifier))
                    logger.log(5,"Failure stack trace (handled cleanly)", 
                                                                exc_info=1)
                    if remote_installer.version in all_bad_versions:
                        logger.info("Targeted installer {} is one of the bad versions:{}, skipping updating to this version:{}".format(remote_installer.identifier, all_bad_versions, remote_installer.version))                                            
                    else:
                        installers_to_fetch[remote_installer.identifier] = remote_installer
        else:
            logger.warning("No installers found in configuration, leaving as is...")
        
        ## Fetch any installers which need to be fetched
        was_error = False
        karl_payload = {}
        for installer_name, installer in installers_to_fetch.iteritems():
            if installer_name.lower()=="acme":
                evt_type="UsherAcmeUpdate"
            elif installer_name.lower()=="acmeguardian":
                evt_type="UsherWatcherUpdate"
            try:
                if installer_name == "ACME":
                    is_watcher_running = uc.is_watcher_running_with_retry()
                    if not is_watcher_running:
                        logger.error("Skipping the download of ACME as ACMEGuardian is not running, will try to install ACMEGuardian")
                        continue
                is_installed = self.update_and_install_installer(installer)
                if is_installed:
                    self.installers[installer_name]["last_change"] = last_updated
            except Exception as exp:
                error_msg = "An error occurred installing installer:'{}' with error:{} and error code: {}".format(installer.identifier, exp.message, self.install_error_code)
                logger.error(error_msg)
                self.install_error_code = InstallErrorCode.SUCCESS
                karl_payload["current_"+installer_name.lower()+"_version"] = installer.old_version
                karl_payload["attempted_"+installer_name.lower()+"_version"] = installer.version
                karl_payload["is_baseline"] = False
                karl_payload["status"] = 0
                karl_payload["error_message"] = error_msg
                uc.send_usher_event(karl_payload, evt_type)
                raise PublishError(error_msg),None, sys.exc_info()[2]
        return
    
    #MARK: Delete this once we use real karlregistrar endpoint
    def dict_util(self,data):
        """
        This method converts the unicode elements in the dict keys and values to string.
        """
        if isinstance(data, basestring):
            return str(data)
        elif isinstance(data, collections.Mapping):
            return dict(map(self.dict_util, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(self.dict_util, data))
        else:
            return data
    
    def update_and_install_installer(self, installer):
        """
        This method will ensure the provided installer is on disk and up-to-date.
        It will query Richter and provide current installer data. If provided
        data does not match the active configuration in Richter, we will download
        the installer, cache it, and then load it into our usher controller.
        """
        #with INSTALL_LOCK:
        (staging_path, 
            load_path, 
            staging_filepath,
            staging_sig_filepath,
            load_filepath) = self.setup_directories(installer=installer)
        
        did_fetch = self.fetch_installer_package(installer=installer, 
                                                path=staging_filepath, 
                                                sig_path=staging_sig_filepath,
                                                reference_path=load_filepath)
        
        if (did_fetch):
            return self.install_software(installer=installer, 
                                            package_path=staging_filepath,
                                            cleanup_files=False)
        return did_fetch
        
    def fetch_installer_package(self, installer, path=None, sig_path=None, reference_path=None):
        """
        Method that will fetch the specified installer from
        our backend service and save it to the optional path. A reference
        file can also be specified to use for comparisons. 
        
        :param installer: Object specifying the installer to fetch
        :type installer: :py:class:`UsherinstallerConfigEntry`
        
        :param path: The path to download the installer to
        :type path: (string) 
        
        :param reference_path: Path to a reference file to use for string comparisons.
        
        :raises: :py:class:`ConfigurationError` If we are not configured
        :raises: :py:class:`ThrottledRequestError` If our API is currently 
                                            throttled.
        :raises: :py:class:`ResponseError` If we recieved an API error.
        
        returns: (bool) true if the file was downloaded
        """
        
        logger = logging.getLogger(__name__)
        
        result = False
                
        ## Ensure our directories are setup        
        (staging_path, 
            load_path, 
            staging_filepath,
            staging_sig_filepath,
            load_filepath) = self.setup_directories(installer=installer)
            
        if not path:
            path = staging_filepath
        
        if not sig_path:
            sig_path = staging_sig_filepath
            
        if not reference_path:
            reference_path = load_filepath
        
        #If staging path already contains some data clear it out.
        if len(os.listdir(staging_path)) > 0:
            for root, dirs, files in os.walk(staging_path):
                for f in files:
                    if os.path.join(staging_path, f) != path:
                        os.unlink(os.path.join(root, f))
                for d in dirs:
                    shutil.rmtree(os.path.join(root, d))
                    
        logger.info("Fetching installer:'{}' version:'{}' from Richter...".format(
                                                        installer.identifier,
                                                        installer.version))
        if installer:
            try:
                download_link = installer.download_link
                
                ## saving installer from download link to installer directory
                logger.debug("Starting to download installer:'{}' from url:'{}'".format(
                                                installer.identifier,
                                                download_link))
                
                self._fetch_s3file(download_link, path, hash_val = installer.file_hash)
                result = True
                logger.debug("Finished downloading installer:'{}' to file:'{}'".format(
                                                installer.identifier,
                                                path))
                
                sig_link = installer.download_link_sig
                
                self._fetch_s3file(sig_link, sig_path)
                result = True
                logger.debug("Finished downloading installer signature:'{}' to file:'{}'".format(
                                                installer.identifier,
                                                sig_path))
                
            except Exception as exp:
                message = "Failed to download installer:'{}'; {}".format(
                                                    installer.identifier, exp)
                self.install_error_code |= InstallErrorCode.DOWNLOAD_FAILED
                raise PublishError(message),None, sys.exc_info()[2]
        else:
            message = "Received empty response from server.".format()
            self.install_error_code |= InstallErrorCode.FETCH_CONFIGURATION_FAILED
            raise ResponseError(message), None, sys.exc_info()[2]
        
        return result
    
    def install_software(self, package_path=None, sig_package_path=None, installer=None,
                                                cleanup_files=False):
        """
        Method to install the installer at the provided path. 
        
        This method will perform following tasks:
        1. Verify installer.
        2. Publish installer on successful verification.
        3. Load installer into our usher controller.
        
        :param package_path: Filepath to the file which we wish to install
        :type  package_path: str
        
        :param installer: The UsherinstallerConfigEntry instance related to this install
        :type: installer: :py:class:`UsherinstallerConfigEntry`
        
        :param cleanup_files: If true, we will cleanup extracted files (default:False)
        :type cleanup_files: (bool)
        
        :raises: :py:class:`ConfigurationError` if we are not fully configured
        :raises: :py:class:`PackageVerificationError` if the package fails to verify
        :raises: :py:class:`PublishError` if the package fails to install
        
        """
        
        logger = logging.getLogger(__name__)
        
        if not installer:
            identifier = os.path.basename(package_path).splitext()[0]
            installer = UsherInstallerConfigEntry(identifier=identifier)
        
        ## Ensure our directories are setup        
        (staging_path, 
            load_path, 
            staging_filepath,
            staging_sig_filepath,
            load_filepath) = self.setup_directories(installer=installer)
        
        if package_path is None:
            package_path = staging_filepath
        
        if sig_package_path is None:
            sig_package_path = staging_sig_filepath
            
        if installer.version:
            message = "Installing installer:'{}' version:'{}'".format(
                                                installer.identifier,
                                                installer.version)
        else:
            message = "Installing installer:'{}'".format(installer.identifier)
        
        logger.info(message)
            
        if not package_path or not os.path.exists(package_path):
            raise PublishError("File does not exist at '{}'".format(
                                                        package_path))
        
        ## verifying installer content
        try:
            self.verify_signature_hash(package_path, sig_package_path, installer)
        finally:
            if cleanup_files and os.path.exists(staging_path):
                clean_directory(staging_path, raise_on_error=False)
        
        try:
            usher_utils.extract_zip(package_path, staging_path)
        except Exception as exp:
            self.install_error_code |= InstallErrorCode.ZIP_EXTRACTION_FAILED
            logger.error("Failed to extract the zip with error:{}".format(exp))
            raise PublishError("File extraction failed: '{}'".format(
                                                        package_path))
        finally:
            if cleanup_files and os.path.exists(staging_path):
                clean_directory(staging_path, raise_on_error=False)
        
        try:
            installer_package_path = self.usher_controller.find_installerpkg(staging_path)
        finally:
            if cleanup_files and os.path.exists(staging_path):
                clean_directory(staging_path, raise_on_error=False)
                       
        ## verifying codesign
        try:
            if self.verify_codesign_enabled:
                status = self.verify_codesign(installer_package_path)
                if not status:
                    self.install_error_code |= InstallErrorCode.CODE_SIGN_VERIFY_FAILED
                    raise PublishError("Failed to verify the codesigning of the installer:'{}'".format(
                                                        installer.identifier))
        finally:
            if cleanup_files and os.path.exists(staging_path):
                clean_directory(staging_path, raise_on_error=False)
                
        ## clearing out installer folder before publishing
        try:
            if not os.path.exists(load_path):
                os.makedirs(load_path, mode=0755)
            else:
                clean_directory(load_path)
        except Exception as exp:
            self.install_error_code |= InstallErrorCode.FAILED_TO_CLEAN
            raise PublishError("Failed to prepare destination directory:'{}'; {}".format(
                                        load_path,
                                        exp.message)), None, sys.exc_info()[2]
        finally:
            if cleanup_files and os.path.exists(staging_path):
                clean_directory(staging_path, raise_on_error=False)
        
        ## publishing installer
        try:
                
            ## Copy content to load path
            load_installerpkg = self._copy_package_content(staging_path, load_path)
            
            ## loading installer
            self.usher_install(path=load_installerpkg, installer = installer)
            
        except Exception as exp:
            self.install_error_code |= InstallErrorCode.INSTALL_FAILED
            raise PublishError(exp.message), None, sys.exc_info()[2]
        finally:
            if cleanup_files and os.path.exists(staging_path):
                clean_directory(staging_path, raise_on_error=False)
                
        return True
    
    def verify_codesign(self, package_path):
        """
        Method which is used to verify the codesign of our installer.
        
        """
        result = False
        
        is_verify, output = self.usher_controller.verify_installer_codesign(package_path)
        if not is_verify:
            raise ConfigurationError("Could not verify installer codesign, returned output; {}".format(
                                    output))
        return is_verify
        
        
    def verify_signature_hash(self, filepath, sigpath, installer):
        """
        Method which is used to verify the signature on our content.
        
        """
        
        result = False
        
        authority_pem = self.controller.registrant.config_signing_authority
        if not authority_pem:
            self.install_error_code |= InstallErrorCode.SIGN_HASH_VERIFY_FAILED
            raise ConfigurationError("Could not verify installer signature; "
                                        "no signing authority is available.")
        
        try:
            authority_cert = crypto.load_certificate(crypto.FILETYPE_PEM,
                                                    authority_pem)
        except Exception as exp:
            self.install_error_code |= InstallErrorCode.SIGN_HASH_VERIFY_FAILED
            raise ConfigurationError("Failed to load signing authority! "
                            "Could not verify installer signature; {}".format(
                                    exp.message)), None, sys.exc_info()[2]
        
        try:
            with open(filepath) as fp, open(sigpath) as fs:
                content = fp.read()
                signature = fs.read()
            
            result = crypto.verify(authority_cert, signature, content, "sha256")
            
        except Exception as exp:
            self.install_error_code |= InstallErrorCode.SIGN_HASH_VERIFY_FAILED
            raise ConfigurationError("Could not verify installer signature; {}".format(
                                    exp.message)), None, sys.exc_info()[2]
                                
        try:
            file_hash = acme.crypto.file_hash(filepath)
            if file_hash != installer.file_hash:
                raise ConfigurationError("Hash of the installer doesn't match")
        except Exception as exp:
            self.install_error_code |= InstallErrorCode.SIGN_HASH_VERIFY_FAILED
            raise ConfigurationError("Could not verify installer hash; {}".format(
                                    exp.message)), None, sys.exc_info()[2]
                                    
        return result
    
    def usher_install(self, path=None, installer=None):
        """
        This method will load the installer extracted at the provided path into
        our usher controller.
        
        .. warning:
            This method does not do any signing verification, you should use 
            :py:func:`self.install_software` instead.

        :param installer: object of UsherInstallerConfigEntry which needs installation
        :type  UsherInstallerConfigEntry

        :param path: path to installer directory from where installer will be loaded
        :type  path: str
        """
        
        usher_controller = self.usher_controller
        
        if path is None and installer is not None:
            (staging_path, 
                load_path, 
                staging_filepath,
                load_filepath) = self.setup_directories(installer=installer)
            path = load_filepath
        elif path is None:
            raise ValueError("No path or installer defined!")
                
        usher_controller.install(installer, path)
        
    
    def _fetch_s3file(self, url, filepath, hash_val=None):
        """
        Method to download the provided file from S3 to the specified path.
        Supports large objects.
        
        :raises: :py:class:`IOError` on filesystem problems
        :raises: :py:class:`request.exceptions.RequestException` on request errors.
        """
        logger = logging.getLogger(__name__)
        #If hash already matches
        if os.path.exists(filepath):
            file_hash = acme.crypto.file_hash(filepath)
            if hash_val == file_hash:
                logger.debug("Hash of the file on disk matches the one sent by the server, no need to download..")
                return
        
        response = requests.get(url)
        with open(filepath, "w") as fh:
            for chunk in response.iter_content():
                fh.write(chunk)
                
    def setup_directories(self, installer=None, zip_dir=None):
        """
        Method to setup our directory structures.
        """
        logger = logging.getLogger(__name__)
        if self.load_path is None:
            raise ConfigurationError("load_path is not defined!")
        
        if self.staging_path is None:
            raise ConfigurationError("staging_path is not defined!")
        
        
        if installer:
            load_path = os.path.join(self.load_path, installer.identifier)
            staging_path = os.path.join(self.staging_path, installer.identifier)
        else:
            load_path = self.load_path
            staging_path = self.staging_path
        
        if not os.path.exists(load_path):
            os.makedirs(load_path, mode=0755)
        
        if not os.path.exists(staging_path):
            os.makedirs(staging_path, mode=0755)
        
        if installer:
            filename = "{}.{}".format(installer.identifier, ".zip")
            sig_filename = "{}.{}".format(installer.identifier, self.sig_ext)
            staging_filepath = os.path.join(staging_path, filename)
            staging_sig_filepath = os.path.join(staging_path, sig_filename)
            load_filepath  = os.path.join(load_path, filename)
            if zip_dir:
                try:
                    ## Move zip to content folder
                    shutil.copyfile(os.path.join(zip_dir, filename), 
                                                        staging_path)
                except Exception as exp:
                    ## removing content directory
                    if os.path.exists(staging_path):
                        shutil.rmtree(staging_path)
                    message = "Unable to setup installer directories for installer:'{}'; {}".format(installer.identifier, exp)
                    logger.error(message)
                    raise
            
        else:
            staging_filepath = None
            load_filepath = None
            staging_sig_filepath = None
        return (staging_path, load_path, staging_filepath, staging_sig_filepath, load_filepath)
    
    def _compare_versions(self, first_version, second_version, normalize = False):
        """
        Todo: fix
        This method will compare two versions
        :param first_version str: first version to compare
        :param second_version str: second version to compare
        :param normalize bool: it will normalize version strings like .99 --> 0.99, .123 --> 0.123
                               (appending 0 at the beginning of string)
        :return: 1 if first_version > second_version
                -1 if first_version < second_version
                 0 if first_version == second_version
        """
        ret_val = None
        if normalize:
            if first_version.startswith("."):
                first_version = "0{}".format(first_version)
            if second_version.startswith("."):
                second_version = "0{}".format(second_version)
        if first_version > second_version:
            ret_val = 1
        elif first_version < second_version:
            ret_val = -1
        else:
            ret_val = 0
        return ret_val

    def _copy_package_content(self, source_folder, destination_folder,
                                            delete_on_error=False):
        """
        This method will copy all the zip extracted installers files
        in actual installers folder.

        :param source: Path to extracted package directory
        :type  source: string

        :param destination_installer_path: Path to destination install directory
        :type  destination_installer_path: string

        :param delete_on_error: If true, we will delete the destination if we encounter
        :type  delete_on_error: bool
        
        :raises: :py:class:`PublishError`
        
        """
        
        should_delete_files = False
        try:
            logger = logging.getLogger(__name__)
            logger.debug("Copying downloaded installer, source:'{}' destination:'{}'".format(
                                                        source_folder,
                                                        destination_folder))
            
            if os.path.exists(destination_folder):
                os.removedirs(destination_folder)
                
            ##copy file
            shutil.copytree(source_folder, destination_folder)
            
            ## If we succeed, make sure we delete files if we ever fail
            should_delete_files = True
            
            return self.usher_controller.find_installerpkg(destination_folder)

        except Exception as exp:
            if delete_on_error and should_delete_files:
                logger.warning("An error occured copying extracted package, deleting files at: '{}'. {}".format(
                                                            destination_folder,
                                                            exp.message))
                try:
                    if os.path.exists(destination_folder):
                        shutil.rmtree(destination_folder)
                except Exception as exp:
                    logger.error("Failed to delete destination '{}' after failed copy; {}".format(
                                                        destination_folder,
                                                        exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
                
            raise PublishError("Failed to copy package; {}".format(
                                        exp.message)), None, sys.exc_info()[2]
                                        
    def get_installer_ext(self):
        """
        Method returns the installer extension to be used based on the platform
        """
        if systemprofile.platform.lower() == "macos" or systemprofile.platform.lower() == "os x":
            return "pkg"
        elif systemprofile.platform.lower() == "ubuntu":
            return "deb"
        return "zip"
            
class UsherInstallerConfigEntry(acme.core.ConfigurableObject, acme.core.PersistentObject):
    """
    Class which represents a usher installer configuration.
    """
    
    
    key_map = {
                "file_hash": None, 
                "installer_dir": None,
                "identifier": None, 
                "id": None, 
                "version": None,
                "old_version": None,
                "download_link": None,
                "download_link_sig": None,
                "is_installable_by_acme": None,
                "bad_versions": None,
                "update_frequency": "<type=timedelta>;",
                "last_update": "<type=datetime>;",
                "last_update_attempt": "<type=datetime>;",
                "last_change": "<type=datetime>;"
                }
    settings_keys = ["identifier", "update_frequency"]
    state_keys = settings_keys[:]
    state_keys.extend(["last_update", "last_update_attempt", "bad_versions", "last_change"])
    
    
    def __init__(self, file_name=None, identifier=None,sigpath=None, 
                                                    update_frequency=None,
                                                    key_map=None, 
                                                    state_keys=None,
                                                    settings_keys=None,*args, **kwargs):
        
        if update_frequency is None:
            self.update_frequency = USHER_UPDATE_FREQUENCY
        else:
            self.update_frequency = update_frequency
        
        self.last_update = None
        self.last_update_attempt = None
                    
    
        file_name = file_name
        identifier = identifier
        id = None
        version = None
        self.old_version = None
        self.download_link = None
        self.download_link_sig = None
        is_installable_by_acme = None
        self.file_hash = None
        self.bad_versions = []
        if key_map is None:
            key_map = {}
            key_map.update(UsherInstallerConfigEntry.key_map)
        
        if state_keys is None:
            state_keys = UsherInstallerConfigEntry.state_keys[:]
        
        if settings_keys is None:
            settings_keys = UsherInstallerConfigEntry.settings_keys[:]
            
        self.file_name = file_name
        self.identifier = identifier
        super(UsherInstallerConfigEntry, self).__init__(key_map=key_map, state_keys=state_keys,
                                                settings_keys=settings_keys, *args, **kwargs)
        
    def next_update(self):
        """
        Method which returns the next time we should update our config.
        
        :returns: :py:class:`datetime.datetime` representing when we should next update
        
        :raises: ValueError if interval is misconfigured (must be positive number)
        """
        
        now = datetime.datetime.utcnow()
        
        last_run = self.last_update
        frequency = self.update_frequency
        default_frequency = USHER_UPDATE_FREQUENCY
        
        if last_run is None:
            return now
        
        if frequency is None:
            frequency = default_frequency
            
        if frequency.total_seconds() <= 0:
            raise ValueError("Cannot determine next update date for file:'{}', update_frequency must be positive value.".format(
                                                        self.file_name))
        
        return last_run + frequency
    
    def needs_update(self, verify_hash=None):
        """
        Method which returns whether or not our file needs an update.
        
        :returns: (bool) True if we need an immediate update of this file.
        
        :raises: ValueError if object is misconfigured
        """
        
        logger = logging.getLogger(__name__)
        
        result = False
        
        result = self.next_update() <= datetime.datetime.utcnow()
        
        return result
    
    def __str__(self):
        """
        Return object as a string.
        """
        
        return "<UsherInstallerConfigEntry name:'{}' update_frequency:'{}' path:'{}'>".format(
                                                    self.file_name,
                                                    self.update_frequency)

class InstallErrorCode:
    """
    Error Codes for installation failure
    """
    SUCCESS = 0
    FETCH_CONFIGURATION_FAILED = 1
    DOWNLOAD_FAILED = 1 << 1
    CODE_SIGN_VERIFY_FAILED = 1 << 2
    ZIP_EXTRACTION_FAILED = 1 << 3
    FAILED_TO_CLEAN = 1 << 4
    SIGN_HASH_VERIFY_FAILED = 1<<5
    INSTALL_FAILED = 1 << 6