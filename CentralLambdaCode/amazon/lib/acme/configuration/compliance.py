"""
...package:: acme.configuration
    :synopsis: Module containing functionalities for replace and rewrite manifest.
    :platform: OSX, Ubuntu

.. moduleauthor:: Jude Ning <huazning@amazon.com>
"""

import datetime
import os
import sys
import logging
import json
import zipfile
import shutil
import sys

import requests
import OpenSSL.crypto as crypto

import systemprofile
import acme.core
import acme.compliance

from . import *

#MARK: Defaults
COMPLIANCE_MODULE_CHECK_FREQUENCY = datetime.timedelta(hours=6)
COMPLIANCE_MODULE_CHECK_SKEW = datetime.timedelta(hours=1)
COMPLIANCE_MODULE_IDLE_RETRY = datetime.timedelta(seconds=30)

class ComplianceConfigModule(ConfigModule):
    """
    :py:class:`ConfigModule` subclass which provides functionality
    necessary for fetching compliance module configurations from Richter.
    
    #todo: update doc
    .. example:
        >>> cc = ConfigurationController(identity=d.identity,
        ...                                registrant=d.registrant)
        >>> file_dict = {   "interval": 15,
        >>>                 "execution_skew": 5,
        >>>                 "files": {
        >>>                     "testfile.txt" : {
        >>>                         "filename": "testfile.txt",
        >>>                         "filepath": "/tmp/testfile.txt",
        >>>                         "update_frequency": 10,
        >>>                     }
        >>>                 }
        >>>             }
        >>> 
        >>> cf = ConfigurationFileConfigModule(controller=cc,dict_data=file_dict)
        >>> 
        >>> cc.register_module(cf)
        >>> cc.start()
    
    """
    
    key_map = ConfigModule.key_map.copy()
    key_map["load_path"] = None
    key_map["staging_path"] = None
    
    interval = COMPLIANCE_MODULE_CHECK_FREQUENCY
    execution_skew = COMPLIANCE_MODULE_CHECK_SKEW
    
    def __init__(self, compliance_controller=None,
                                            load_path=None, 
                                            staging_path=None, 
                                            *args, **kwargs):
        
        self.load_path = load_path          #: Path for our module code 
        self.staging_path = staging_path    #: Path for our parcels
        
        self.url_path = "/configuration"
        self.module_url_path = "/config/get_module"
        
        self.compliance_controller = compliance_controller
        
        super(ComplianceConfigModule, self).__init__(*args, **kwargs)

    #MARK: ConfigModule methods        
    def should_run_immediately(self):
        """
        Method to determine whether or not our configuration module 
        has successfully completed an execution. This method should be 
        overriden by child objects if immediate configuration is not desirable
        on first run.
        
        :returns: (bool) True if we have have never configured.
        """
        
        if len(self.compliance_controller.modules) == 0:
            return True
        else:
            return super(ComplianceConfigModule, self).should_run_immediately()

    def run(self):
        """
        Our primary execution routine.
        """
        
        logger = logging.getLogger(__name__)
        
        ## Determine if it's time to renew, if not, defer
        
        self.last_update_attempt = datetime.datetime.utcnow()
        
        ## If we are actively evaluating, defer
        with self.compliance_controller.queue_lock:
            status = self.compliance_controller.status()
            if status == acme.compliance.ModuleStatus.IDLE:
                self.update_loaded_modules()
            else:
                raise acme.core.DeferredTimerException("Compliance controller is not idle (status:{}), deferring update for {}".format(
                                acme.compliance.ModuleStatus.to_string(status),
                                COMPLIANCE_MODULE_IDLE_RETRY),
                        COMPLIANCE_MODULE_IDLE_RETRY)
        
        self.last_update = datetime.datetime.utcnow()
        
        ## Here if all intended executions succeeded. Update our
        ## timer interval to align with our next update
        self.timer.frequency = self.get_current_interval()
        
    #MARK: ComplianceConfigModule methods
    def update_loaded_modules(self):
        """
        Method which will fetch our current module configuration from Richter
        and update our active configuration.
        """
        
        try:
            response = self._fetch_configuration()
            response_json = response.json()
        except (ThrottledRequestError, ConfigurationError):
            raise
        except Exception as exp:
            message = "Failed to fetch compliance configuration; {}".format(exp.message)
            logger.error(message)
            raise ResponseError(message), None, sys.exc_info()[2]
        
        module_data = None
        if response_json and int(response_json['status']) == ResultStatus.SUCCESS:
            try:
                module_data = response_json['data']['modules']
            except KeyError as exp:
                logger.warning("No compliance modules data sent by server.".format(uri))
        elif response_json and int(response_json['status']) == ResultStatus.FAILED:
            message = "Failed to fetch compliance configuration; {}".format(
                                                response_json["message"])
            logger.error(message)
            
            raise ResponseError(message)
            
        self._process_configuration(module_data)
    
    def _fetch_configuration(self):
        """
        Method that will fetch our compliance module configuration from
        our backend service and return our response
        
        :returns: :py:class:`requests.models.Response` instance.
        
        :raises: :py:class:`ConfigurationError` If we are not configured
        :raises: :py:class:`ThrottledRequestError` If our API is currently 
                                            throttled.
        :raises: :py:class:`ResponseError` If we recieved an API error.
        """
        
        response = None
        
        platform = systemprofile.profiler.platform() 
        params = {"platform":platform}
        response = self.controller.make_api_call(url_path=self.url_path,
                                                    params=params)
        return response
        
    def _process_configuration(self, module_data):
        """
        This method is used to process the results of our compliance 
        module configuration. This method will compare active runtime
        configuration against the provide configuration. 
        
        It will unload and remove from disk any loaded modules which are not 
        specified, and will download modules which are missing, or which do 
        not have the same identifier+version as the respective module 
        specified in the provided configuration. 
        
        :param configuration: Our configuration to process
        :type configuration: (dict)
        
        """
        
        logger = logging.getLogger(__name__)
        
        cc = self.compliance_controller
        
        ## syncing installed modules with modules we got from database
        loaded_modules = cc.modules.copy()
        modules_to_fetch = {}
        modules_to_remove = {}
        
        ## fetching compliance data for each module
        if "ComplianceModules" in module_data and module_data["ComplianceModules"]:
            remote_modules = module_data["ComplianceModules"]
            for module_name, module_info in remote_modules.iteritems():
                remote_module = ComplianceModuleConfigEntry(name=module_name, 
                                                    dict_data=module_info)
                try:
                    module = loaded_modules[remote_module.identifier]
                    if module.version != remote_module.version:
                        logger.debug("Loaded module:'{}' does not match required version:'{}' (has '{}'), will update from server...".format(
                                            module.identifier, 
                                            remote_module.version,
                                            module.version))
                        modules_to_fetch[module.identifier] = remote_module
                    else:
                        logger.log(9, "Loaded module:'{}' matches required version:'{}'...".format(
                                            module.identifier, 
                                            remote_module.version
                                            ))
                except KeyError:
                    logger.warning("Module:'{}' is not currently loaded, will load from server...".format(
                                                        remote_module.identifier))
                    modules_to_fetch[remote_module.identifier] = remote_module
                except Exception as exp:
                    logger.warning("Failed to determine current version for module:'{}', will update from server...".format(
                                                remote_module.identifier))
                    logger.log(5,"Failure stack trace (handled cleanly)", 
                                                                exc_info=1)
                                                                
                    modules_to_fetch[module.identifier] = remote_module
            
            ## Determine modules to remove
            remote_identifiers = map(lambda x: x["identifier"], module_data["ComplianceModules"].values())
            for key in [key for key in loaded_modules.keys() if key not in remote_identifiers]:
                modules_to_remove[key] = loaded_modules[key]
                
        else:
            logger.warning("No modules found in configuration, removing all running modules...")
            modules_to_remove = loaded_modules
        
        ## Unload and delete files for any modules meant for removal
        for key, module in modules_to_remove.iteritems():
            
            ## Unload the module from our compliance controller
            try:
                logger.debug("Module:'{}' qualifies for removal, unloading...".format(key))
                self.compliance_controller.unload_compliance_module_by_identifier(key)
            except Exception as exp:
                logger.error("Failed to unload compliance module:'{}'. Error: {}".format(
                                                            key))
                logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
            ## Remove filesystem components
            try:                
                module_path = os.path.join(self.load_path, key)
                if os.path.exists(module_path):
                    logger.debug("Removing module from path:'{}'".format(
                                                                module_path))
                    shutil.rmtree(module_path)
                else:
                    logger.warning("Could not remove module '{}', path '{}' does not exist!".format(
                                            key, module_path))
            except Exception as exp:
                logger.error("Failed to remove compliance module:'{}' from path:'{}'. Error: {}".format(
                                            key, module_path, exp.message))
                logger.log(5,"Failure stack trace (handled cleanly)", 
                                                                exc_info=1)
        
        ## Fetch any modules which need to be fetched
        was_error = False
        for module_name, module in modules_to_fetch.iteritems():
            try:
                self.update_and_install_module(module)
            except ThrottledRequestError:
                raise
            except Exception as exp:
                logger.error("An error occurred installing module:'{}'; {}".format(
                                                            module.identifier,
                                                            exp))
                logger.log(5,"Failure stack trace (handled cleanly)", 
                                                                exc_info=1)
                was_error = True
    
    def update_and_install_module(self, module):
        """
        This method will ensure the provided module is on disk and up-to-date.
        It will query Richter and provide current module data. If provided
        data does not match the active configuration in Richter, we will download
        the module, cache it, and then load it into our compliance controller.
        """
        
        (staging_path, 
            load_path, 
            staging_filepath,
            load_filepath) = self.setup_directories(module=module)
        
        did_fetch = self.fetch_module_package(module=module, 
                                                path=staging_filepath, 
                                                reference_path=load_filepath)
        
        if (did_fetch):
            self.install_module_package(module=module, 
                                            package_path=staging_filepath,
                                            cleanup_files=False)
        
    def fetch_module_package(self, module, path=None, reference_path=None):
        """
        Method that will fetch the specified compliance module from
        our backend service and save it to the optional path. A reference
        file can also be specified to use for comparisons. 
        
        :param module: Object specifying the module to fetch
        :type module: :py:class:`ComplianceModuleConfigEntry`
        
        :param path: The path to download the module to
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
            load_filepath) = self.setup_directories(module=module)
        
        if not path:
            path = staging_filepath
        
        if not reference_path:
            reference_path = load_filepath
        
        
        logger.info("Fetching module:'{}' version:'{}' from Richter...".format(
                                                        module.identifier,
                                                        module.version))
        response = None
        response_json = None
        try:
            response = self._fetch_module_payload_data(module, 
                                                            reference_path)
            if response:
                response_json = response.json()
        except Exception as exp:
            if response and response.status_code == 500 and int(response_json['status']) == ResultStatus.UNKNOWN_FILE:
                message = "Failed to fetch payload data for module:'{}'; module does not exist on server.".format(
                                                    module.identifier)
            else:
                message = "Failed to fetch payload data for module:'{}'; {}".format(
                                                            module.identifier,
                                                            exp.message)
            raise ResponseError(message), None, sys.exc_info()[2]
        
        if response_json and int(response_json['status']) == ResultStatus.SUCCESS:
            try:
                ## means we have received new zip with latest hash from KARLRegistrar
                download_link = response_json.get("download_link")
                
                ## saving module bundle from download link to module directory
                logger.debug("Downloading module:'{}' from url:'{}'".format(
                                                module.identifier,
                                                download_link))
                
                self._fetch_s3file(download_link, path)
                result = True
                logger.debug("Finished downloading module:'{}' to file:'{}'".format(
                                                module.identifier,
                                                path))
            except Exception as exp:
                message = "Failed to download module:'{}'; {}".format(
                                                    module.identifier, exp)
                raise PublishError(message),None, sys.exc_info()[2]
                
        elif response_json and int(response_json['status']) == ResultStatus.LATEST_VERSION:
            logger.info("Existing module:'{}' is up-to-date".format(
                                                        module.identifier))
        elif response_json:
            message = ("Failed to fetch module:'{}'; Server responded with status '{}' and message '{}'".format(
                                                    module.identifier,
                                                    response_json["status"], 
                                                    response_json["message"]))
            raise ResponseError(message), None, sys.exc_info()[2]
        else:
            message = "Received empty response from server.".format()
            raise ResponseError(message), None, sys.exc_info()[2]
        
        return result
    
    def _fetch_module_payload_data(self, module, reference_file=None):
        """
        Method that will fetch payload information for the provided
        module from our backend service and return our response. If a reference
        file is specified, we will provide a sha-256 the hash of this file
        in our request.
        
        :param module: The module to fetch information for.
        :type module: :py:class:`ComplianceModuleConfigEntry`
        :param reference_file: The path of the module if it exists on disk.
        :type reference_file: string
        
        :raises: :py:class:`ConfigurationError` If we are not configured
        :raises: :py:class:`ThrottledRequestError` If our API is currently 
                                            throttled.
        :raises: :py:class:`ResponseError` If we recieved an API error.
        """
        
        logger = logging.getLogger(__name__)
        
        logger.info("Fetching payload data configurations from Richter...")        
        response = None
        
        params = { 
                "module_identifier": module.identifier, 
                "module_version": module.version, 
        }
                
        if reference_file and os.path.isfile(reference_file):
            try:
                params["module_hash"] = acme.crypto.file_hash(reference_file)
            except Exception as exp:
                logger.warning("Failed to compute has for module file:'{}', will download new file. {}".format(
                                                                reference_file, 
                                                                exp.message))
                logger.log(5,"Failure stack trace (handled cleanly)", 
                                                    exc_info=1)
        elif reference_file and not os.path.isfile(reference_file):
            logger.debug("Will not provide hash for module file:'{}', file does not exist.. ".format(
                                                            reference_file))
        
        response = self.controller.make_api_call(
                                        url_path=self.module_url_path,
                                        params=params)
        
        return response
        
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
    
    
    def install_module_package(self, package_path=None, module=None,
                                                cleanup_files=False):
        """
        Method to install the module at the provided path. This should be 
        a .zip bundle. 
        
        This method will perform following tasks:
        1. Verify module bundle zip and its contents.
        2. Publish module on successful verification.
        3. Load module into our compliance controller.
        
        :param package_path: Filepath to the zip file which we wish to install
        :type  package_path: str
        
        :param module: The ComplianceModuleConfigEntry instance related to this install
        :type: module: :py:class:`ComplianceModuleConfigEntry`
        
        :param cleanup_files: If true, we will cleanup extracted files (default:False)
        :type cleanup_files: (bool)
        
        :raises: :py:class:`ConfigurationError` if we are not fully configured
        :raises: :py:class:`PackageVerificationError` if the package fails to verify
        
        """
        
        logger = logging.getLogger(__name__)
        
        if not module:
            identifier = os.path.basename(package_path).splitext()[0]
            module = ComplianceModuleConfigEntry(identifier=identifier)
        
        ## Ensure our directories are setup        
        (staging_path, 
            load_path, 
            staging_filepath,
            load_filepath) = self.setup_directories(module=module)
        
        if package_path is None:
            package_path = staging_filepath
            
        if module.version:
            message = "Installing module:'{}' version:'{}'".format(
                                                module.identifier,
                                                module.version)
        else:
            message = "Installing module:'{}'".format(module.identifier)
        
        logger.info(message)
            
        if not package_path or not os.path.exists(package_path):
            raise PublishError("Failed to extract package:'{}', file does not exist!".format(
                                                        package_path))
        
        ## extracting zip contents in extract_path directory
        logger.debug("Extracting package file:'{}' to directory:'{}'".format(
                                                        package_path,
                                                        staging_path))
        try:
            self._extract_zip(package_path, staging_path)
        except Exception as exp:
            raise PublishError("Failed to extract package:'{}' to path:'{}'; {}".format(
                                                package_path, 
                                                staging_path,
                                                exp.message)
                                            ), None, sys.exc_info()[2]
        finally:
            if cleanup_files and os.path.exists(staging_path):
                self._clean_directory(staging_path, raise_on_error=False)
        
        ## verifying module content
        try:
            self.verify_module_at_path(path=staging_path)
        finally:
            if cleanup_files and os.path.exists(staging_path):
                self._clean_directory(staging_path, raise_on_error=False)
        
        ## clearing out module folder before publishing
        try:
            if not os.path.exists(load_path):
                os.makedirs(load_path)
            else:
                self._clean_directory(load_path)
        except Exception as exp:
            raise PublishError("Failed to prepare destination directory:'{}'; {}".format(
                                        load_path,
                                        exp.message)), None, sys.exc_info()[2]
        finally:
            if cleanup_files and os.path.exists(staging_path):
                self._clean_directory(staging_path, raise_on_error=False)
        
        ## publishing module
        try:
            ## Copy content to load path
            self._copy_package_content(staging_path, load_path)
            
            ## loading module
            self.load_compliance_module(path=load_path)
        
        finally:
            if cleanup_files and os.path.exists(staging_path):
                self._clean_directory(staging_path, raise_on_error=False)
                
        return True

    
    def _extract_zip(self, zip_path, destination):
        """
        This helper method will extract contents of zip file in specified 
        destination directory.

        :param zip_path: path to zip file
        :type  zip_path: str

        :param destination: path to directory where zip has to be extracted (this should be an empty directory)
        :type  destination: str
        """
        
        ## creating module directory in content directory
        if not os.path.exists(destination):
            os.makedirs(destination)
        
        ## extracting zip contents 
        zip_ref = zipfile.ZipFile(zip_path, "r")
        zip_ref.extractall(destination)
        zip_ref.close()
    
    def load_compliance_module(self, path=None, module=None):
        """
        This method will load the module extracted at the provided path into
        our compliance controller.
        
        .. warning:
            This method does not do any signing verification, you should use 
            :py:func:`self.install_module_package` instead.

        :param compliance_controller: object of compliance module which holds functions related to load/unload compliance module
        :type  compliance_controller: object of acme.compliance

        :param path: path to module directory from where module will be loaded
        :type  path: str
        """
        
        logger = logging.getLogger(__name__)
        
        compliance_controller = self.compliance_controller
        
        if path is None and module is not None:
            (staging_path, 
                load_path, 
                staging_filepath,
                load_filepath) = self.setup_directories(module=module)
            path = load_path
        elif path is None:
            raise ValueError("No path or module defined!")
                
        
        try:
            compliance_controller.load_modules(path=path)
        except Exception as exp:
            logger.error("Failed to load module from directory '{}'. {}".format(
                                                            path, exp))
            logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)

    def setup_directories(self, module=None, zip_dir=None):
        """
        Method to setup our directory structures.
        """
        
        if self.load_path is None:
            raise ConfigurationError("load_path is not defined!")
        
        if self.staging_path is None:
            raise ConfigurationError("staging_path is not defined!")
        
        if module:
            load_path = os.path.join(self.load_path, module.identifier)
            staging_path = os.path.join(self.staging_path, module.identifier)
        else:
            load_path = self.load_path
            staging_path = self.staging_path
        
        if not os.path.exists(load_path):
            os.makedirs(load_path, mode=0700)
        
        if not os.path.exists(staging_path):
            os.makedirs(staging_path, mode=0700)
        
        if module:
            filename = "{}.zip".format(module.identifier)
            staging_filepath = os.path.join(staging_path, filename)
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
                    message = "Unable to setup module directories for module:'{}'; {}".format(module.identifier, exp)
                    logger.error(message)
                    raise
            
        else:
            staging_filepath = None
            load_filepath = None
        
        return (staging_path, load_path, staging_filepath, load_filepath)


    def load_compliance_module_by_name(self, module_path, content_path, name, compliance_controller, registration_data):
        """
        This method will be invoked by amce cli. It will load compliance module with provided name.

        :param module_path: path to module directory from where module will be loaded
        :type  module_path: str

        :param content_path: path of content/temporary extraction directory
        :type  content_path: str

        :param name: name of compliance module
        :type  name: str

        :param compliance_controller: object of compliance module which holds functions related to load/unload compliance module
        :type  compliance_controller: object of acme.compliance

        :param registration_data: object of our registration module holding registration information like certificate etc.
        :param registration_data: object of acme.registration
        """
        result = 0
        loaded_modules = {} 
        logger = logging.getLogger(__name__)

        ## setting up module directories
        content_dir_path, module_dir_path, content_file_path, module_file_path = self._setup_module_directories(name, content_path, module_path, content_path)

        ## processing module bundle along with verification
        try:
            result = self._process_and_verify_bundle(content_file_path, content_dir_path, registration_data, name, module_dir_path, compliance_controller)
        except Exception as exp:
            logger.error("Unable to load compliance module {} due to {}".format(name, exp))
            logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
            ## if content or module directory exists removing those
            if os.path.exists(content_dir_path):
                shutil.rmtree(content_dir_path)
            if os.path.exists(module_dir_path):
                shutil.rmtree(module_dir_path)

        if result:
            loaded_modules[name] = True

        return loaded_modules

    def unload_compliance_module_by_identifier(self, module_identifier, compliance_controller, module_dir_path):
        """
        This helper method will be invoed by our acme cli to unload module specified by its identifier.

        :param module_identifier: name of module
        :type  module_identifier: str

        :param compliance_controller: object of compliance module which holds functions related to load/unload compliance module
        :type  compliance_controller: object of acme.compliance

        :param module_dir_path: path of directory where our modules resides
        :type  module_dir_path: str
        """
        logger = logging.getLogger(__name__)
        try:
            result = False
            result = compliance_controller.unload_compliance_module_by_identifier(module_identifier)
            if result:
                ## cleanup module directory
                module_path = os.path.join(module_dir_path, module_identifier)
                if os.path.exists(module_path):
                    shutil.rmtree(module_path)
        except Exception as exp:
            logger.error("Unable to unload module {} due to {}".format(module_identifier, exp))
            logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
            result = False

        return result
    
    #MARK: Module verification
    def verify_module_at_path(self, path):
        """
        Method to verify the integrity of a compliance module expanded
        at the provided path.
        
        :param path: path to module to verify
        :type  path: str
        
        """
        
        logger = logging.getLogger(__name__)
        
        logger.debug("Verifying module at path:'{}'".format(path))
        
        logger.log(9,"Verifying package signature...")
        self._verify_module_signature(path=path)

        logger.log(9,"Verifying module content...")
        self._verify_module_content(path=path, delete_pyc_files=True)
        
    def _verify_module_signature(self, path):
        """
        Method which is used to verify the signature on our content.
        
        :raises: :py:class:`PackageVerificationError` On failure.
        
        """
        
        result = False
        
        authority_pem = self.controller.registrant.config_signing_authority
        if not authority_pem:
            raise ConfigurationError("Could not verify module signature; "
                                        "no signing authority is available.")
        
        try:
            authority_cert = crypto.load_certificate(crypto.FILETYPE_PEM,
                                                    authority_pem)
        except Exception as exp:
            raise ConfigurationError("Failed to load signing authority! "
                            "Could not verify module signature; {}".format(
                                    exp.message)), None, sys.exc_info()[2]
        
        bom_json_file_path = os.path.join(path, "bom.json")
        bom_sig_file_path  = os.path.join(path, "bom.signature")
        
        if not bom_json_file_path:
            raise PackageVerificationError("Could not verify module signature; "
                                        "bom.json file could not be found")
            
        if not bom_sig_file_path:
            raise PackageVerificationError("Could not verify module signature; "
                                        "bom.json file could not be found")
        
        try:
            with open(bom_json_file_path) as bj, open(bom_sig_file_path) as bs:
                content = bj.read()
                signature = bs.read()
            
            result = crypto.verify(authority_cert, signature, content, "sha256")
        except Exception as exp:
            raise ConfigurationError("Could not verify module signature; {}".format(
                                    exp.message)), None, sys.exc_info()[2]
        
        return result
    
    def _verify_module_content(self, path, delete_pyc_files=False):
        """
        This method will verify that content received in new module zip file
        is valid with existing module content.

        :param path: path to module download and extraction of new module
        :type  path: str
        
        :param delete_pyc_files: Flag to determine whether we cleanup .pyc files
        :type delete_pyc_files: (bool)
        
        :raises: :py:class:`PackageVerificationError` if verification fails.
        
        """
        verified = True
        identifier = os.path.basename(path)
        bundle_file = "{}.zip".format(identifier)
        
        metadata_files = ["bom.json", "bom.signature"]
        try:
            logger = logging.getLogger(__name__)
            module_file_contents = []
            
            ## reading bom.json file to get list of module's content
            bom_json_path = os.path.join(path,"bom.json")
            if not os.path.exists(bom_json_path):
                raise PackageVerificationError("bom.json file could not be found at path:'{}'".format(
                                                            bom_json_path))
            
            with open(bom_json_path) as fh:
                bom_json_content = json.load(fh)

            ## checking if all files mentioned in bom.json exists
            for bom_content in bom_json_content:
                rel_path = bom_content["path"]
                file_path = os.path.join(path, rel_path)
                module_file_contents.append(file_path)
                file_hash = bom_content["hash"]
                file_hash_algo = bom_content["alg"]
                if not os.path.exists(file_path):
                    raise PackageVerificationError("File '{}' specified in  bom '{}' does not exist!".format(
                                                        rel_path, 
                                                        bom_json_path))
                module_file_hash = acme.crypto.file_hash(file_path, 
                                                                file_hash_algo)
                if not module_file_hash or module_file_hash != file_hash:
                    raise PackageVerificationError("Invalid hash for file: '{}' (Expected:'{}' actual:'{}')".format(
                                            rel_path,
                                            file_hash,
                                            module_file_hash))

            ## Checking for files other than the ones listed in bom.json
            ## If file is any_file.pyc we will remove it and let it recreate on module load
            ## If any other file found we are flagging module as not verified and will not be loaded
            for root, directories, files in os.walk(path, topdown=False):
                for file in files:
                    ## skipping if file is from metadata files
                    if file in metadata_files or file == bundle_file:
                        continue
                    file_to_check = os.path.join(root, file)
                    rel_path = file_to_check.replace(path, "")
                    if file_to_check not in module_file_contents:
                        if file_to_check.endswith(".pyc") and delete_pyc_files:
                            logger.log(5, "Notice: file '{}' removed (will be recreated)...".format(
                                                            file_to_check))
                            os.remove(file_to_check)
                        else:
                            raise PackageVerificationError("Found unsanctioned file: '{}'".format(
                                                            rel_path))
                
        except PackageVerificationError:
            raise
        except Exception as exp:
            raise PackageVerificationError("Unknown verification failure; {}".format(
                                        exp.message)), None, sys.exc_info()[2]
        
        return True
    
    def unload_compliance_module(self, module, remove_files=None):
        """
        This method is used to unload the specifed compliance module
        and optionally remove it's files from our load path.

        :param module: The module to unload
        :type  module: :py:class:`ComplianceModuleConfigEntry`
        
        :param remove_files: Whether or not we remove files (default:False)
        :type remove_files: (bool)

        :raises: ValueError if module identifier is not specified
        :raises: Exception on error

        """
        
        logger = logging.getLogger(__name__)
        
        if not module.identifier:
            raise ValueError("Invalid module identifier specified!")
        
        logger.debug("Unloading compliance module:'{}'".format(module.identifier))
        
        result = self.compliance_controller.unload_compliance_module_by_identifier(
                                                        module.identifier)
        
        if not remove_files:
            return result
        
        ## Here if we are removing files    
        (staging_path, 
            load_path, 
            staging_filepath,
            load_filepath) = self.setup_directories(module=module)
            
        self._clean_directory(load_path)
        
        return result
    
    def load_compliance_modules(self):
        """
        Method to verify and load copied content. This will attempt to 
        sanitize the path as well
        """
        
        logger = logging.getLogger(__name__)
        
        path = self.load_path
        
        logger.debug("Loading compliance modules from path: '{}'".format(path))
        
        for entity in os.listdir(path):
            module_path = os.path.join(path,entity)
            if os.path.isdir(module_path):
                try:
                    self.verify_module_at_path(path=module_path)
                    self.load_compliance_module(path=module_path)
                except Exception as exp:
                    logger.error("Failed to verify module at path:'{}'. Error: {}".format(
                                                    module_path, exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
    
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

    def _clean_directory(self, path, raise_on_error=True):
        """
        This helper method will remove all the contents inside a directory.
        As a safety control, this method will only operate on paths with the 
        following roots:
        
        Allowed Paths
        ==============
        /private/tmp
        /tmp
        /var/folders
        /usr/local/amazon/var
        
        .. caution: 
            This will completely delete all contents in the provided directory.
            Use with discretion.

        :param path: path to the directory whose contents has to be removed
        :type  path: str
        
        :param raise_on_error: If False, we will mask all exceptions
        
        :returns: True if cleanup was successful
        
        
        
        """
        
        ## Safety control to prevent shooting oneself in foot
        allowed_roots = [ "/private/tmp", 
                                "/tmp",
                                "/var/folders",
                                "/usr/local/amazon/var", 
                                ]
        
        logger = logging.getLogger(__name__)
        logger.debug("Cleaning directory:'{}'".format(path))
        
        
        did_succeed = False
        try:
            if not os.listdir(path):
                logger.debug("Directory '{}' is empty, no need to clean for deployment.".format(path))
                return True
            else:
                logger.debug("Cleaning directory content at path: '{}'".format(path))
            
            valid_path = False
            for root in allowed_roots:
                if path.startswith(root):
                    valid_path = True
                    
            if not valid_path and os.listdir(path):
                raise ConfigurationError("Path:'{}' cannot be sanitized for deployment! (Valid roots: '{}')".format(
                                                    path,
                                                    "', '".join(allowed_roots)))
            
            for root, directories, files in os.walk(path, topdown=False):
                for file in files:
                    file_to_delete = os.path.join(root, file)
                    os.remove(file_to_delete)
                for directory in directories:
                    os.rmdir(os.path.join(root, directory))
            os.rmdir(path)
            did_succeed = True
        except Exception as exp:
            if raise_on_error:
                raise
            else:
                logger.error("Failed to cleanup directory:'{}'; {}".format(
                                                                exp.message))
                logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
              
        return did_succeed
    
    def _copy_package_content(self, source, destination,
                                            delete_on_error=False):
        """
        This method will copy all the zip extracted modules files
        in actual modules folder.

        :param source: Path to extracted package directory
        :type  source: string

        :param destination: Path to destination install directory
        :type  destination: string

        :param delete_on_error: If true, we will delete the destination if we encounter
        :type  delete_on_error: bool
        
        :raises: :py:class:`PublishError`
        
        """
        
        should_delete_files = False
        try:
            logger = logging.getLogger(__name__)
            logger.debug("Copying extracted package, source:'{}' destination:'{}'".format(
                                                        source,
                                                        destination))

            ## content metadata files
            source_bom_json_path = os.path.join(source,"bom.json")
            source_bom_signature_path = os.path.join(source, "bom.signature")
            
            ## module metadata files
            destination_bom_json_path = os.path.join(destination,"bom.json")
            destination_bom_signature_path = os.path.join(destination, "bom.signature")
            
            with open(source_bom_json_path) as fh:
                bom_json_content = json.load(fh)

            for bom_content in bom_json_content:
                src_file_path = os.path.join(source, bom_content["path"])
                destination_file_path = os.path.join(destination, 
                                                        bom_content["path"])
                ## checking if folder exists in module path
                destination_folder = os.path.dirname(destination_file_path)
                if not os.path.exists(destination_folder):
                    os.makedirs(destination_folder)
                
                ##copy file
                shutil.copyfile(src_file_path, destination_file_path)
                
                ## If we succeed, make sure we delete files if we every fail
                should_delete_files = True

            ## copying metadata files
            shutil.copyfile(source_bom_json_path, destination_bom_json_path)
            shutil.copyfile(source_bom_signature_path, destination_bom_signature_path)
        except Exception as exp:
            if delete_on_error and should_delete_files:
                logger.warning("An error occured copying extracted package, deleting files at: '{}'. {}".format(
                                                            destination,
                                                            exp.message))
                try:
                    if os.path.exists(destination):
                        shutil.rmtree(destination)
                except Exception as exp:
                    logger.error("Failed to delete destination '{}' after failed copy; {}".format(
                                                        destination,
                                                        exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
                
            raise PublishError("Failed to copy package; {}".format(
                                        exp.message)), None, sys.exc_info()[2]
    
    def update_loaded_modules(self):
        """
        Method which will fetch our current module configuration from Richter
        and update our active configuration.
        """
        
        logger = logging.getLogger(__name__)
        
        try:
            logger.info("Checking for latest compliance configurations from server...")    
            response = self._fetch_configuration()
            response_json = response.json()
        except (ThrottledRequestError, ConfigurationError):
            raise
        except Exception as exp:
            message = "Failed to fetch compliance configuration; {}".format(exp.message)
            logger.error(message)
            raise ResponseError(message), None, sys.exc_info()[2]
        
        module_data = None
        if response_json and int(response_json['status']) == ResultStatus.SUCCESS:
            try:
                module_data = response_json['data']['modules']
            except KeyError as exp:
                logger.warning("No compliance modules data sent by server.".format(uri))
        elif response_json and int(response_json['status']) == ResultStatus.FAILED:
            message = "Failed to fetch compliance configuration; {}".format(
                                                response_json["message"])
            logger.error(message)
            
            raise ResponseError(message)
            
        self._process_configuration(module_data)

class ComplianceModuleConfigEntry(acme.core.SerializedObject):
    """
    Class which represents a module configuration.
    """
    
    key_map = {
                "config_hash": None, 
                "identifier": None, 
                "config_file": None, 
                "id": None, 
                "version": None
                }
                
    name = None
    identifier = None
    id = None
    version = None
    config_file = None
    config_hash = None
    
    def __init__(self, name=None, identifier=None, *args, **kwargs):
        self.name = name
        self.identifier = identifier
        super(ComplianceModuleConfigEntry, self).__init__(*args, **kwargs)
        
    
        
 


