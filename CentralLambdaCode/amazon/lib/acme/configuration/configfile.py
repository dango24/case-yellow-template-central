"""
...package:: acme.configuration
    :synopsis: Module containing functionalities for replace and rewrite manifest.
    :platform: OSX, Ubuntu

.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>
"""

import base64
import datetime
import os
import sys
import logging
import hashlib
import json
import shutil
import sys
import threading

import OpenSSL.crypto as crypto

import acme
import acme.core

from acme.configuration import *

#MARK: Defaults
FILE_UPDATE_FREQUENCY = datetime.timedelta(hours=6)
FILE_UPDATE_SKEW = datetime.timedelta(hours=1)
FILE_VERIFY_FREQUENCY = datetime.timedelta(hours=1)

class ConfigurationFileConfigModule(ConfigModule):
    """
    :py:class:`ConfigModule` subclass which provides functionality
    necessary for fetching configuration files from Richter.
    
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
    key_map["files"] = "<getter=get_files,setter=set_files>;"
    key_map["file_state"] = "<getter=get_file_state,setter=set_file_state>;"
    
    settings_keys = ConfigModule.settings_keys[:]
    settings_keys.append("files")
    
    state_keys = ConfigModule.state_keys[:]
    state_keys.extend(["file_state"])
    
    files = {}
    
    def __init__(self, key_map=None, settings_keys=None, state_keys=None,
                                                            *args, **kwargs):
        self.files = {}
        
        self.url_path = "register/get_config_file"
        
        self.files_lock = threading.RLock()
        
        if key_map is None:
            key_map = ConfigurationFileConfigModule.key_map
        
        if settings_keys is None:
            settings_keys = ConfigurationFileConfigModule.settings_keys[:]
        
        if state_keys is None:
            state_keys = ConfigurationFileConfigModule.state_keys[:]
        
        super(ConfigurationFileConfigModule, self).__init__(key_map=key_map,
                                                settings_keys=settings_keys,
                                                state_keys=state_keys,
                                                *args, **kwargs)
    
    #MARK: ConfigurationFileConfigModule Methods
    def get_files(self):
        """
        Method to return our configured files as a serializable dictionary,
        keyed by config name.
        """
        
        files = {}
        
        with self.files_lock:
            for filename, file in self.files.iteritems():
                files[filename] = file.to_dict()
            
        return files
        
    def set_files(self, files):
        """
        Method which accepts a list of file data in dictionary form and
        update local files.
        """
        
        logger = logging.getLogger(__name__)
        
        new_files = {}
        with self.files_lock:
            for filename, filedata in files.iteritems():
                try:
                    config_file = ConfigFile(dict_data=filedata)
                    if not filename in self.files:
                        self.files[filename] = config_file
                    else:
                        self.files[filename].load_dict(filedata)
                        
                    if config_file.filename and not config_file.filepath:
                        config_file.filepath = os.path.join(self.manifest_dir,
                                                        config_file.filename)
                    logger.debug("Loaded settings for file: {}".format(
                                                                config_file))
                except Exception as exp:
                    logger.error("Failed to de-serialize ConfigFile. Data:'{}'.  Error: {}".format(
                                                            filedata,
                                                            exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
            ## Remove missing files
            for key in [key for key in self.files.keys() if key not in files.keys()]:
                del(self.files[key])

    def get_file_state(self):
        """
        Method to return our configured files' state as a serializable dictionary,
        keyed by config name.
        """
        
        files = {}
        
        with self.files_lock:
            for filename, file in self.files.iteritems():
                state_map = file.key_map_for_keys(file.state_keys)
                if file.exists() and not file.stored_hash:
                    file.stored_hash = file.hash()
                files[filename] = file.to_dict(key_map=state_map)
                
        return files
        
    def set_file_state(self, files):
        """
        Method which accepts a list of file data in dictionary form and
        update local files.
        """
        
        logger = logging.getLogger(__name__)
        
        new_files = {}
        with self.files_lock:
            for filename, filedata in files.iteritems():
                try:
                    config_file = ConfigFile(dict_data=filedata)
                    state_map = config_file.key_map_for_keys(config_file.state_keys)
                    if filename in self.files:
                        config_file = self.files[filename]
                        config_file.load_dict(filedata, key_map=state_map)
                    else:
                        self.files[filename] = config_file
                    
                    logger.debug("Loaded state for file: {}".format(
                                                                config_file))
                except Exception as exp:
                    logger.error("Failed to de-serialize ConfigFile. Data:'{}'.  Error: {}".format(
                                                            filedata,
                                                            exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
            ## Remove missing files
            for key in [key for key in self.files.keys() if key not in files.keys()]:
                del(self.files[key])

    
    def files_to_fetch(self):
        """
        Method which returns the files we should fetch, sorted by
        last_fetch_date.
        """
        
        logger = logging.getLogger(__name__)
        
        new_files = []
        stale_files = []
        with self.files_lock:
            for file in self.files.values():
                try:
                    if not file.exists():
                        new_files.append(file)
                    elif file.needs_update():
                        stale_files.append(file)
                except Exception as exp:
                    logger.error("Failed to check update for file:'{}', error:'{}'".format(
                                                file.filename,
                                                exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        
        ## Sort our stale files by last attempt
        get_last_attempt = lambda x: x.last_update_attempt if x.last_update_attempt is not None else datetime.datetime.utcfromtimestamp(0)
        stale_files = sorted(stale_files, key=get_last_attempt)
        
        return new_files + stale_files
               
    
    def fetch_file(self, file, update_only=None):
        """
        Method to fetch the specified file from Richter. 
        
        :param string filename: The file to request
        :param string filepath: The local path to save our file to
        :param string sigpath: The local path to save our signature path to.
                    This parameter is optional, by default we will save our
                    signature file to the same path as our file.
        :param bool update_only: If true, we will only fetch the file if 
                            the server-side version has changed (default:True)
        
        .. note:
            This is a blocking call, it will also block until any other
            active configuration calls have finished.
        
        :raises: :py:class:`ConfigurationError` If our object is not appropriately configured
        :raises: :py:class:`ThrottledRequestError` If we are currently under throttling
        :raises: :py:class:`ResponseError` If our request fails
        
        :returns: (bool) True if the file was updated
        
        """
        
        filename = file.filename
        filepath = file.filepath
        sigpath = file.sigpath
        
        logger= logging.getLogger(__name__)
        
        if sigpath is None:
            sigpath = "{}.sig".format(filepath)
        
        if update_only is None:
            update_only = True
        
        if not filename:   
            raise ConfigurationError("An error occurred attempting to fetch a configuration file: configuration filename was not provided in the request!")        
        
        if not self.url_path:
            raise ConfigurationError("Cannot fetch file '{}': no config-file url path defined.".format(filename))
        
        
        file_did_download = False
            
        logger.debug("Verifying file:'{}' with Richter. (path:'{}')".format(
                                                                    filename,
                                                                    filepath))
        
        ## Setup our request
        response = None
        response_json = None
        
        ## Build our payload
        data = {"filename":filename}
        
        if update_only and os.path.isfile(filepath):
            try:
                content_hash = acme.crypto.file_hash(filepath)
                data["content_hash"] = content_hash
                json_data = json.dumps(data,indent=4)
            except Exception as exp:
                logger.warning("Failed to determine hash for file: '{}', will download latest version ('{}'). Error: {}".format(filepath,
                                                            filename,
                                                            exp))
        try:
            logger.log(5, "Requesting configfile:'{}' from Richter. Data:\n'''\n {}\n'''".format(filename,json.dumps(data, indent=4)))
            response = self.controller.make_api_call(url_path=self.url_path,
                                                        params=data)
        except (ThrottledRequestError, ConfigurationError):
            raise
        except Exception as exp:
            if response and response.status_code == 500 and int(response_json['status']) == ResultStatus.UNKNOWN_FILE:
                message = "Failed to fetch config file:'{}'; Server error: File not Found.".format(filename)
            else:
                message ="Failed to fetch config file:'{}'; {}".format(filename,exp.message)
            raise ResponseError(message), None, sys.exc_info()[2]
        
        if response:
            response_json = response.json()
        
        if response_json and int(response_json["status"]) == ResultStatus.SUCCESS:
            logger.debug("Recieved success response from Richter, verifying signature...")
            
            try:
                self.verify_response_signature(response_json)
                
                logger.debug("Response signature successfully verified!")
                
                ## Write our main file
                logger.log(9, "Writing content for file:'{}' to path:'{}'".format(
                                                filename, filepath))
                
                self.write_data_to_file(base64.b64decode(
                                            response_json.get("content")), 
                                                            filepath)
                
                ## Write our signature
                logger.log(9, "Writing signature for file:'{}' to path:'{}'".format(
                                                filename, sigpath))
                self.write_data_to_file(base64.b64decode(
                                        response_json.get("signature")), 
                                                                sigpath)
                file_did_download = True
                
            except crypto.Error as exp:
                logger.error("Failed to fetch config file:'{}'; Could not verify response signature! Error:{}".format(filename, exp.message))
                raise
            except Exception as exp:
                logger.error("Failed to fetch config file:'{}'; {}".format(filename,exp))
                raise PublishError("Failed to publish {} due to error {}".format(filename,exp)), None, sys.exc_info()[2]
        elif response_json and int(response_json["status"]) == ResultStatus.LATEST_VERSION:
            logger.info("Did not fetch config file:'{}', on disk content is up-to-date.".format(
                                                                filename))
        return file_did_download
    
    def verify_response_signature(self, response, hash_algo="sha256"):
        """
        Method which will validate the server response for authenticity.
        
        :raises: :py:class:`OpenSSL.crypto.Error` If verification fails
        """
        
        try:
            content = base64.b64decode(response["content"])
            signature = base64.b64decode(response["signature"])
        except Exception as exp:
            raise SignatureVerificationError("Failed to determine response signature. Error: '{}'".format(exp.message)), None, sys.exc_info()[2]
        
        try:
            cert = crypto.load_certificate(crypto.FILETYPE_PEM,
                        self.controller.registrant.config_signing_authority)
            crypto.verify(cert, signature, content, hash_algo)
        except Exception as exp:
            raise SignatureVerificationError("Signature validation failed! '{}'".format(exp.message)), None, sys.exc_info()[2]
    
    def write_data_to_file(self, data, filepath):
        """
        Method to write the provided data to the prescribed file.
        This method will create directories as necessary.
        
        :param string data: Data to write
        :param string filepath: The path to write to
            
        :raises: :py:class:`IOError` on write error.
        """
        
        if not os.path.exists(filepath):
            dir = os.path.dirname(filepath)
            if not os.path.exists(dir):
                os.makedirs(dir)
        
        with open(filepath, "w") as fh:
            fh.write(data)    
    
    #MARK: ConfigModule methods
    def should_run_immediately(self):
        """
        Method to determine whether or not our configuration module 
        has successfully completed an execution.
        
        :returns: (bool) True if we have files which have never configured or
                        need immediate action taken.
        
        """
        
        needs_update = False
        
        with self.files_lock:
            for file in self.files.values():
                try:
                    if file.needs_update():
                        needs_update = True
                        break
                except Exception:
                    pass
        
        return needs_update
    
    def get_current_interval(self):
        """
        Method to return our timer's base interval. This will be the lowest
        update or verification interval amongst all of our configured files.
        """
        
        logger = logging.getLogger(__name__)
                
        next_activity = None
        with self.files_lock:
            for file in self.files.values():
                file_next_activity = None
                file_next_update = file.next_update()
                file_next_verification = file.next_verification()
                if file_next_verification and file_next_update < file_next_verification:
                    file_next_activity = file_next_update
                elif file_next_verification:
                    file_next_activity = file_next_verification
                
                if file_next_activity is not None:
                    if next_activity is None:
                        next_activity = file_next_activity
                    elif next_activity > file_next_activity:
                        next_activity = file_next_activity
                    
        if next_activity is not None:
            interval = next_activity - datetime.datetime.utcnow()
        else:
            interval = super(ConfigurationFileConfigModule, self).get_current_interval()
        
        ## If we have a negative interval, return one shortly in the future
        if interval.total_seconds() < 0:
            logger.log(9, "ConfigurationFileConfigModule calculated negative execution interval, will execute immediately.")
            interval = datetime.timedelta(seconds=.1)
        
        logger.log(2, "get_current_interval() returns: {}".format(interval))
        
        return interval
        
    def run(self):
        """
        Our primary execution routine.
        
        .. note:
            To take advantage of :py:class:`acme.core.RecurringTimer` backoff
            functionality, all exceptions are raised to the caller.
        
        """
        
        logger = logging.getLogger(__name__)
        
        files = self.files_to_fetch()
        
        logger.log(9, "Found {} files needing updates!".format(len(files)))
        
        for file in files:            
            file.last_update_attempt = datetime.datetime.utcnow()
            try:
                did_update = self.fetch_file(file, update_only=True)
            except ThrottledRequestError as exp:
                logger.warning("Could not fetch configfile: '{}'. {}".format(
                                                file.filename, exp.message))
                
                if exp.throttled_until is not None:
                    ticks_till_relief = abs(exp.throttled_until 
                                                - datetime.datetime.utcnow())
                else:
                    ticks_till_relief = self.get_current_interval()
                
                raise acme.core.DeferredTimerException(frequency=ticks_till_relief), None, sys.exc_info()[2]
                
            file.last_update = datetime.datetime.utcnow()
            if did_update:
                file.last_change = datetime.datetime.utcnow()
                try:
                    ## Update the hash on our file object
                    file.stored_hash = acme.crypto.file_hash(file.filepath)
                except Exception as exp:
                    logger.error("Failed to update file hash for file:'{}'. Error:'{}'".format(
                                                file.filepath,
                                                exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
        
        ## Here if all intended executions succeeded. Update our
        ## timer interval to align with our next update
        self.timer.frequency = self.get_current_interval()
            
class ConfigFile(acme.core.ConfigurableObject, acme.core.PersistentObject):
    """
    Class which represents a configuration file which will be 
    periodically updated from Richter.
    """
    
    key_map = {
                    "filename": None,
                    "filepath": None,
                    "sigpath": None,
                    "stored_hash": None,
                    "manifest_dir": None,
                    "update_frequency": "<type=timedelta>;",
                    "last_update": "<type=datetime>;",
                    "last_change": "<type=datetime>;",
                    "last_update_attempt": "<type=datetime>;",
                    "last_verified": "<type=datetime>;",
                    "verify_frequency": "<type=timedelta>;",

                }
    
    settings_keys = ["filename", "filepath", "sigpath", "update_frequency", "verify_frequency"]
    state_keys = settings_keys[:]
    state_keys.extend(["last_update", "last_change", "last_update_attempt",
                                                        "stored_hash",
                                                        "last_verified"])
    
    hash_check_frequency = datetime.timedelta(hours=1)  #: How often we will verify on-disk content
    
    def __init__(self, filename=None, filepath=None, sigpath=None, 
                                                    hash=None,
                                                    update_frequency=None,
                                                    verify_frequency=None,
                                                    key_map=None, 
                                                    state_keys=None,
                                                    settings_keys=None,
                                                    *args, **kwargs):
        
        self.filename = filename
        self.filepath = filepath
        self.sigpath = sigpath
        self.stored_hash = hash
        
        if update_frequency is None:
            self.update_frequency = FILE_UPDATE_FREQUENCY
        else:
            self.update_frequency = update_frequency
        
        self.last_update = None
        self.last_update_attempt = None
        
        self.last_verified = None
        
        if verify_frequency is None:
            self.verify_frequency = None
        else:
            self.verify_frequency = verify_frequency
        
        if key_map is None:
            key_map = {}
            key_map.update(ConfigFile.key_map)
        
        if state_keys is None:
            state_keys = ConfigFile.state_keys[:]
        
        if settings_keys is None:
            settings_keys = ConfigFile.settings_keys[:]
        
        super(ConfigFile, self).__init__(key_map=key_map, state_keys=state_keys,
                                                settings_keys=settings_keys,
                                                *args,**kwargs)
    
    def exists(self):
        """
        Method to return whether or not the specified file exists
        on disk.
        
        :returns: (bool) Whether or not this file, and it's signature
                    exists on disk.
        
        :raises: ValueError if filepath is not configured
        """
        
        exists = False
        if not self.filepath:
            raise ValueError("filepath is not defined!")
        
        exists = os.path.isfile(self.filepath)
            
        if exists and self.sigpath:
            if not os.path.isfile(self.sigpath):
                exists = False
        
        return exists 
    
    def verify(self):
        """
        Method which will compare the on-disk content of a file by hashing
        the file and comparing to our stored hash. If no hash is stored,
        we will return True
        
        :returns: (bool) True if the on-disk content matches our stored hash,
                        or if no hash is specified for the file.
        """
        
        logger = logging.getLogger(__name__)
        
        result = False
        
        if not self.stored_hash:
            logger.warning("No hash is stored for file:'{}', cannot verify.".format(
                                        self.filename))
            result = True
        else:
            logger.log(9, "Verifying file hash for file:'{}' at path:'{}'".format(
                                        self.filename,
                                        self.filepath))
            if self.stored_hash != self.hash():
                logger.debug("Failed to verify file hash for file:'{}' at path:'{}'".format(
                                        self.filename,
                                        self.filepath))
                result = False
            else:
                logger.debug("Successfully verified file hash for file:'{}' at path:'{}'".format(
                                        self.filename,
                                        self.filepath))
                result = True
            
            self.last_verified = datetime.datetime.utcnow()
        
        return result
    
    def needs_update(self, verify_hash=None):
        """
        Method which returns whether or not our file needs an update.
        
        :param verify_hash: If true, we will perform a hash comparison
                            of our saved hash to what's on disk, if false
                            we will simply verify if the file exists. 
        
        :returns: (bool) True if we need an immediate update of this file.
        
        :raises: ValueError if object is misconfigured
        """
        
        logger = logging.getLogger(__name__)
        
        result = False
        
        if not self.exists():
            result = True
        else:
            if verify_hash is None:
                verify_hash = self.needs_verification()
            
            if verify_hash:
                result = not self.verify()
            
            ## If we have not yet determined a need for an update, do a date check
            if not result:
                result = self.next_update() <= datetime.datetime.utcnow()
        
        return result
    
    def needs_verification(self):
        """
        Method which returns whether or not our file needs a verification
        ran.
        
        :returns: (bool) True if we need to verify this file.
        
        :raises: ValueError if object is misconfigured
        """
        
        result = False
        
        if self.stored_hash:
            result = self.next_verification() <= datetime.datetime.utcnow()
        
        return result
    
    def next_update(self):
        """
        Method which returns the next time we should update our config.
        
        :returns: :py:class:`datetime.datetime` representing when we should next update
        
        :raises: ValueError if interval is misconfigured (must be positive number)
        """
        
        now = datetime.datetime.utcnow()
        
        last_run = self.last_update
        frequency = self.update_frequency
        default_frequency = FILE_UPDATE_FREQUENCY
        
        if last_run is None:
            return now
        elif self.filepath and not self.exists():
            return now
        elif not self.filepath:
            return now
        
        if frequency is None:
            frequency = default_frequency
            
        if frequency.total_seconds() <= 0:
            raise ValueError("Cannot determine next update date for file:'{}', update_frequency must be positive value.".format(
                                                        self.filename))
        
        return last_run + frequency
        
    def next_verification(self):
        """
        Method which returns the next time we should validate our file.
        
        :returns: :py:class:`datetime.datetime` representing when we should next update
        :returns: None  If no filepath is defined, or if we don't have a stored hash for the file
        
        :raises: ValueError if interval is misconfigured (must be positive number)
        """
        
        ## If we don't have a stored hash for the file, we can't verify.
        if not self.stored_hash:
            return None
            
        ## If we don't have a filepath, we can't verify
        if not self.filepath or not self.exists():
            return None
        
        now = datetime.datetime.utcnow()
        
        last_run = self.last_verified
        frequency = self.verify_frequency
        default_frequency = FILE_VERIFY_FREQUENCY
        
        ## If we've never verified, verify now
        if last_run is None:
            return now
        
        if frequency is None:
            frequency = default_frequency
        
        if frequency.total_seconds() <= 0:
            raise ValueError("Cannot determine next verification date for file:'{}', verify_frequency must be positive value.".format(
                                                        self.filename))
        
        return last_run + frequency
    
    def content(self):
        """
        Method to return the content of our config file.
        
        :returns: Content of our file 
        
        :raises: :py:class:`ValueError` if no filepath is set for this object
        :raises: :py:class:`IOError` if the file does not exist or fails to be read
        
        """
        
        data = None
        if not self.filepath:
            name = self.filename
            raise ValueError("Cannot read file content, no file specified!")
            
        with open(self.filepath) as fh:
            data = fh.read()
        
        return data
    
    def hash(self, hash=None):
        """
        Method to return a hash our file on disk
        
        :param hash: The hashing algorithm to use
        :type hash: :py:class:`hashlib.HASH` object (default 'hashlib.sha256')
        
        :returns: string - hash 
        
        :raises: IOError on standard filesystem errors
        :raises: Exception on misc error
        """
        
        return acme.crypto.file_hash(self.filepath, hash=hash)
    
    def __str__(self):
        """
        Return object as a string.
        """
        
        return "<ConfigFile name:'{}' update_frequency:'{}' path:'{}'>".format(
                                                    self.filename,
                                                    self.update_frequency,
                                                    self.filepath)


       
