"""
...package:: acme.configuration
    :synopsis: Module containing functionalities for replace and rewrite manifest.
    :platform: OSX, Ubuntu

.. moduleauthor:: Jude Ning <huazning@amazon.com>
"""

import base64
import datetime
import os
import sys
import logging
import hashlib
import json
import zipfile
import shutil
import sys
import threading

import requests
import OpenSSL.crypto as crypto

import systemprofile
import acme.utils
import acme.requests as re
import acme
import acme.core
import acme.usher
from acme.configuration import *
import acme.ipc as ipc
import acme.crypto

#MARK: Defaults
STSTOKEN_RENEWAL_THRESHOLD=.5

class STSTokenConfigModule(ConfigModule):
    """
    :py:class:`ConfigModule` subclass which provides functionality
    necessary for fetching STS Authentication Tokens from Richter.
    
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
    key_map["renewal_percentage"] = None
    key_map["token_path"] = None       
     
    def __init__(self, karl_engine=None, routes_dir=None, *args, **kwargs):
        
        self.karl_engine = karl_engine
        self.routes_dir = routes_dir
        self.token = None
        self.token_path = None
        self.renewal_percentage = STSTOKEN_RENEWAL_THRESHOLD
        
        self.url_path = "register/get_credentials"
        
        super(STSTokenConfigModule, self).__init__(*args, **kwargs)
        
        ## If we don't have a token path specified yet, tried to construct
        if self.token_path is None and self.state_dir is not None:
            self.token_path = os.path.join(self.state_dir, "aws.data")
    
    #MARK: ConfigModule methods
    def should_run_immediately(self):
        """
        Method to determine whether or not our configuration module 
        has successfully completed an execution.
        
        :returns: (bool) True if we do not have a valid STS Token, or
                if our current STS Token is expired.
        
        """
        
        logger = logging.getLogger(__name__)
        needs_update = False
        
        if not self.token:
            if self.token_path and os.path.exists(self.token_path):
                try:
                    self.load_token()
                    self.configure_karl_engine()
                except Exception as exp:
                    needs_update = True
                    logger.info("STS Token failed to load from disk, will fetch as soon as possible...")
            elif self.token_path and not os.path.exists(self.token_path):
                logger.info("STS Token is not cached, will fetch as soon as possible...")
                needs_update = True
        
        if self.token and self.should_renew():
            logger.info("Cached STS Token is expired, will fetch as soon as possible...")
            needs_update = True
        
        return needs_update
    
    def get_current_interval(self):
        """
        Method to return our timer's interval. This will be based upon
        the renewal date of our timer.
        """
        
        interval = None
        
        try:
            renewal_date = self.renewal_date()
            interval = datetime.datetime.utcnow() - renewal_date
        except Exception as exp:
            logger = logging.getLogger(__name__)
            logger.error("Failed to determine timer interval: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
        
        ## If renewal_date is in the past, change our timer to our configured
        if interval is None or interval.total_seconds() <= 0:
            interval = super(STSTokenConfigModule, self).get_current_interval()
        
        return interval
    
    def run(self):
        """
        Our primary execution routine.
        """
        
        logger = logging.getLogger(__name__)
        
        ## Determine if it's time to renew, if not, defer
        defer_until = None
        renewal_date = None
        try:
            renewal_date = self.renewal_date()
        except:
            pass
        
        if renewal_date:
            renew_delta = renewal_date - datetime.datetime.utcnow()
            if renew_delta.total_seconds() >= 1:
                defer_until = renew_delta
                
        ## If we should defer STS renewal, at least verify that KARL
        ## has loaded credentials and load them if not
        if defer_until:
            if not self.karl_engine.has_credentials:
                self.configure_karl_engine()
            
            self.timer.frequency = defer_until
            return
        
        self.last_update_attempt = datetime.datetime.utcnow()
        
        response = self.fetch_credentials()
        
        processed_credentials = False
        processed_routes = False
        #Process creds from the response.
        credentials = response.get("credentials")
        if not credentials:
            logger.error("KARL did not send any credentials in the response..")
        else:
            try:
                self.process_credentials(credentials)
                processed_credentials = True
            except Exception as exp:
                logger.error("Error processing routes with error:{}".format(exp))
        
        #Process routes from the response
        routes = response.get("routes")
        if not routes:
            logger.error("KARL did not send any routes in the response..")
        else:
            try:
                self.process_routes(routes)
                processed_routes = True
            except Exception as exp:
                logger.error("Error processing routes with error:{}".format(exp))
        
        #Raise exception to allow recurring timer to calculate backoff retry.
        if not (processed_credentials and processed_routes):
            raise Exception("Did not process credentials and routes")
        
        ## Here if all intended executions succeeded. Update our
        ## timer interval to align with our next update
        logger.info("STSToken Fetching module completed executing successfully.")
        self.timer.frequency = self.get_current_interval()
    
    def process_routes(self, routes):
        """
        Process the routes file and saves them if the contents change.
        """
        logger = logging.getLogger(__name__)
        for route_entry in routes:
            try:
                if route_entry.get("target") == "acme":
                    route_filename = route_entry.get("file_name")
                    route_filehash = route_entry.get("data_hash")
                    route_filecontent = route_entry.get("data")
                    logger.info("Received route file from KARL:{}".format(route_filename))
                    target_route_filepath = os.path.join(self.routes_dir,route_filename)
                    if os.path.exists(target_route_filepath) and os.path.isfile(target_route_filepath):
                        existing_route_filecontent = ''
                        with open(target_route_filepath,"rb") as route_file:
                            existing_route_filecontent = base64.b64encode(route_file.read())
                        existing_route_filehash = acme.crypto.string_hash(existing_route_filecontent)
                        if existing_route_filehash == route_filehash:
                            logger.info("Route file:{} already matches the file in {}".format(route_filename,target_route_filepath))
                            continue
                    logger.info("Writing new/updated route file:{} in location {}".format(route_filename,target_route_filepath))
                    with open(target_route_filepath,"w") as route_file:
                        route_file.write(base64.b64decode(route_filecontent))
                elif route_entry.get("target") == "watcher":
                    # Get the cred targeted for watcher and send an IPC request to watcher. 
                    logger.info("Got a route:{} for watcher. Sending IPC to watcher..".format(route_entry.get("file_name")))
                    req = ipc.Request()
                    options = {}
                    options["route"] = route_entry
                    req.action ="ProcessRoutes"
                    req.options = options
                    resp = acme.usher.send_ipc_watcher(req)
                    logger.info("IPC Request to watcher to save credentials returned with status : {}".format(resp.status))
            except Exception as exp:
                    logger.error("Error processing route with error:{}".format(exp))        
        
    def process_credentials(self, credentials):
        """
        Process the credentials. Saves it to file, pass the creds to watcher.
        """
        logger = logging.getLogger(__name__)
        acme_credential_received = False
        watcher_credential_received =False
        for credential in credentials:
            try:
                if credential.get("target") == "acme":
                    token = STSToken(dict_data=credential.get("credential"))
                    self.token = token       
                    self.last_update = datetime.datetime.utcnow()                
                    ## Load our STS Token into our KARL event engine
                    self.configure_karl_engine()                
                    ## Update our Timer
                    self.timer.frequency = self.get_current_interval()                
                    ## Save our STS Token
                    self.save_token()
                    logger.info("Saved ACME Credentials!!")
                    acme_credential_received = True
                if credential.get("target") == "watcher":
                    watcher_cred = credential.get("credential")
                    # Get the cred targeted for watcher and send an IPC request to watcher. 
                    req = ipc.Request()
                    options = {}
                    options["credentials"] = watcher_cred
                    req.action ="RotateWatcherCredentials"
                    req.options = options
                    resp = acme.usher.send_ipc_watcher(req)
                    logger.info("IPC Request to watcher to save credentials returned with status : {}".format(resp.status))
                    watcher_credential_received = True    
            except Exception as exp:
                logger.error("Error processing credentials with error:{}".format(exp))
        
        logger.info("ACME Credential received:{0}\nWatcher Credential received:{1}".format(acme_credential_received,watcher_credential_received))
        if not acme_credential_received and not watcher_credential_received:
            raise Exception("Unable to process ACME and Watcher credential")
    
    #MARK: STSTokenConfigModule methods
    def renewal_date(self, renewal_percentage=None):
        """
        This method will examine a provided token and return the 
        ideal renewal time for our loaded token.
        
        :param renewal_percentage: The percentage lifetime of the ticket
        :type renewal_percentage: (float)
                
        :returns: :py:class:`datetime.datetime` object specifying the 
                                        preferred renewal time of our token
        
        """
        
        now = datetime.datetime.utcnow()
        
        if not renewal_percentage:
            renewal_percentage = self.renewal_percentage
        
        token = self.token
        
        if not token:
            return now
        elif not token.is_valid():
            if token.expiration:
                return token.expiration
            else:
                return now
        
        ## Here if we have a valid token. 
        token_lifetime = token.expiration - token.creation_datetime
        
        seconds = token_lifetime.total_seconds() * renewal_percentage
        
        token_renewal_date = token.creation_datetime + datetime.timedelta(
                                                            seconds=seconds)
        
        return token_renewal_date
    
    def should_renew(self):
        """
        Method which returns whether our current token should be immediately
        renewed.
        
        :returns: (bool) True if we should immediately renew.
        
        """
        
        if self.renewal_date() <= datetime.datetime.utcnow():
            return True
        else:
            return False
     
    def configure_karl_engine(self):
        """
        Method to configure our KARL engine with our credentials
        """
        
        if self.token and self.karl_engine:
            self.karl_engine.credentials = self.token
            self.karl_engine.load_default_routes_map(self.routes_dir)
            self.karl_engine.load_routes_map(self.routes_dir)
            self.karl_engine.reload()
        
    def load_token(self):
        """
        Method to load our token from disk.
        
        :raises: :py:class:`ConfigurationError` if token paths are not configured
        :raises: :py:class:`IOError` on filesystem error

        """
        
        if not self.token_path:
            raise ConfigurationError("Cannot load STS Token, token_path is not configured!")
        
        logger = logging.getLogger(__name__)
        
        token = STSToken()
        
        logger.log(9, "Attempting to load STS token from path:'{}'".format(
                                                            self.token_path))
        token.load_from_file(self.token_path)
        
        logger.debug("Successfully loaded STS Token from disk.")
        
        self.token = token
    
    def save_token(self):
        """
        Method to save our token to disk.
        
        :raises: :py:class:`ConfigurationError` if token paths are not configured
        :raises: :py:class:`IOError` on filesystem error
        """
        
        if not self.token:
            raise ConfigurationError("Cannot save STS Token, no token is loaded in runtime.")
        
        if not self.token_path:
            raise ConfigurationError("Cannot save STS Token, token_path is not configured!")
        
        self.token.save_to_file(self.token_path)
    
    def fetch_credentials(self):
        """
        Method to fetch a new STS Token from Richter..
        
        :raises ConfigurationError: If we are not appropriatel configured for this action.
        :raises ResponseError: If our call fails
        
        :returns: py:dictionary: Response from KARLRegistrar
        
        """
        logger = logging.getLogger(__name__)
        
        ## Fetch our STS Token from Richter
        logger.info("Fetching STS Token from Richter...")
        
        response = None
        response_json = None
        try:
            response = self.controller.make_api_call(url_path=self.url_path,
                                                    handler=requests.post)
            logger.info("Response from STS Token fetching endpoint {0}".format(response))
        except (ThrottledRequestError, ConfigurationError):
            raise
        except Exception as exp:
            message = "Failed to fetch STS token due to error {}".format(
                                                                exp.message)
            raise ResponseError(message), None, sys.exc_info()[2]
        
        response_json = response.json()
        
        return response_json

class STSToken(acme.core.SerializedObject):
    """
    Class which represents an STSToken used by KARL
    
    .. note:
        As STSTokens are a KARL construct, this class really
        belongs in the PyKARL library. However, due to dependency
        on acme.core, we would have a circular reference.
        Need to refactor a "PyACMECore" library that both ACME and KARL
        can consume without circular references. 
    
    """
    
    key_map = { "SecretAccessKey": "secret_access_key",
                    "SessionToken": "session_token",
                    "AccessKeyId": "access_key_id",
                    "Expiration": "<type=datetime>;expiration",
                    "CreationDateTime": "<type=datetime>;creation_datetime"
                }
    
    def __init__(self, *args, **kwargs):
        
        self.creation_datetime = datetime.datetime.utcnow()
        self.secret_access_key = None
        self.session_token = None
        self.access_key_id = None
        self.expiration = None
        
        super(STSToken, self).__init__(*args, **kwargs)
    
    def is_valid(self):
        """
        Method which determines wheter or not this token is valid. By valid,
        we mean we have populated credentials and we have not expired
        
        :returns bool: True if the token is populated and not expired
        
        """
        
        is_valid = True
        
        if (not self.secret_access_key 
                        or not self.session_token 
                        or not self.access_key_id):
            is_valid = False
        elif not self.expiration or self.expiration <= datetime.datetime.utcnow():
            is_valid = False
        
        return is_valid

    

