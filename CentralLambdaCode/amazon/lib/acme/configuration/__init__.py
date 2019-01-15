"""
...package:: acme.configuration
    :synopsis: Module containing functionalities for replace and rewrite manifest.
    :platform: OSX, Ubuntu

.. moduleauthor:: Jude Ning <huazning@amazon.com>
"""

import base64
import datetime
import os
import random
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
import acme.requests
import acme
import acme.core

#MARK: Defaults
DEFAULT_MODULE_INTERVAL = datetime.timedelta(hours=6)
DEFAULT_MODULE_SKEW = datetime.timedelta(minutes=30)

RICHTER_MAX_RETRY_FREQUENCY = datetime.timedelta(hours=1)
RICHTER_RETRY_FREQUENCY = datetime.timedelta(seconds=2)

class ResultStatus(acme.Enum):
    '''
    Class which represents Registrar response status
    '''
    #it also can contain some exception message information
    FAILED = 1 #don't use it directly
    UNKNOWN_ERROR = 1 << 1 | FAILED
    KNOWN_ERROR = 1 << 2 | FAILED
    ERROR_JSON_CONTENT = 1<<3 | FAILED
    SUCCESS = 1<<11
    LATEST_VERSION = 1<<14
    UNKNOWN_FILE = 1<<15 | FAILED

class ConfigurationController(acme.core.ConfigurableObject):
    """
    Class which is responsible for ensuring our client is running
    the latest configurations, as designated by our backend service.
    """
    
    
    key_map = {
                "start_skew": "<type=timedelta>;",
                "retry_frequency": "<type=timedelta>;",
                "max_retry_frequency": "<type=timedelta>;",
                "retry_skew": "<type=timedelta>;",
                "retry_base_num": None,
                "api_url_template": None,
                }
    settings_keys = key_map.keys()
    
    def __init__(self, registrant=None, identity=None, key_map=None,
                                                        settings_keys=None,
                                                        *args, **kwargs):
        
        self.registrant = registrant  #: Var representing our registration data.:py:class:`acme.registration.Applicant` object
        
        self.identity = identity    #: Var representing our identity. :py:class:`acme.crypto.Identity` object
        
        self.start_skew = datetime.timedelta(seconds=5)
        
        self.retry_frequency = RICHTER_RETRY_FREQUENCY  #: If a checkin has failed, retry_frequency
                                #: defines the base period of time which we
                                #: will wait before retrying. 
        self.max_retry_frequency = RICHTER_MAX_RETRY_FREQUENCY #: The maximum interval we will
                                #: delay before attempting another checkin
        self.retry_base_num = 2 #: Our base number when doing polynomial
                                #: fallback
        self.retry_skew = None  #: Defines the skew we add when attempting
                                #: to fallback. The provided skew will 
                                #: result in our trigger executing with increased or 
                                #: decresaed frequency, based on a random draw
                                #: can be float or :py:class:`datetime.timedelta` object
        
        self.api_url_template = "https://{}/api/cert"
        
        self.checkin_lock = threading.RLock()
        
        self.modules = {}
        self._module_lock = threading.RLock()
        
        self._retry_skew = self.roll_skew()
        self._should_run = False
        self._num_consecutive_failures = 0
        self._last_failure_date = None
        
        if key_map is None:
            key_map = {}
            key_map.update(ConfigurationController.key_map)
        if settings_keys is None:
            settings_keys = ConfigurationController.settings_keys[:]
            
        acme.core.ConfigurableObject.__init__(self, key_map=key_map,
                                                settings_keys=settings_keys,
                                                *args, **kwargs)
    
    #MARK: Control
    def start(self):
        """
        Method to start our configuration controller.
        
        """
        logger = logging.getLogger(__name__)
        
        logger.info("Starting configuration controller...")
        
        self._should_run = True
        
        for module in self.modules.values():
            try:
                self.start_module(module)
            except Exception as exp:
                logger.error("Failed to start module: {}. Error: {}".format(
                                                        module.name,
                                                        exp.message))
                logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)
    
    def stop(self):
        """
        Method to shut down our configuration controller.
        """
        
        logger = logging.getLogger(__name__)
        logger.info("Stopping configuration controller...")
        
        self._should_run = False
        for module in self.modules.values():
            try:
                self.stop_module(module)
            except Exception as exp:
                logger.error("Failed to stop module: {}. Error: {}".format(
                                                        module.name,
                                                        exp.message))
                logger.log(5,"Failure stack trace (handled cleanly)", 
                                                        exc_info=1)    
    
    #MARK: State Management
    def next_checkin(self):
        """
        Method which will return the date that we are next allowed to
        checkin to Richter. If we are not throttled, we will return 
        epoch date (1970-01-01).
        
        :returns: :py:class:`datetime.datetime` 
        """
        
        logger = logging.getLogger(__name__)
        
        next_checkin = datetime.datetime.utcfromtimestamp(0)
        
        frequency = None
        try:
            if (self._num_consecutive_failures and self._last_failure_date 
                                                    and self.retry_frequency):
                try:
                    frequency = datetime.timedelta(
                                seconds=self.retry_frequency.total_seconds() 
                                    * (self.retry_base_num ** (self._num_consecutive_failures-1)))
                except OverflowError:
                    if self.max_retry_frequency:
                        frequency = self.max_retry_frequency
                    else:
                        frequency = RICHTER_MAX_RETRY_FREQUENCY
                    
                ## Constrain against our max frequency
                if self.max_retry_frequency:
                    max = self.max_retry_frequency
                    if frequency > max:
                        frequency = max
                
                next_checkin = self._last_failure_date + frequency
                
                ## If we haven't passed our next_checkin, adjust for skew
                if next_checkin >= datetime.datetime.utcnow():
                    next_checkin += self._retry_skew
                
        except Exception as exp:
            logger.error("An error occurred while calculating next checkin time. Error: {}".format(
                                                        exp.message))
            logger.log(5,"Failure stack trace (handled cleanly)", exc_info=1)
        
        return next_checkin
    
    def roll_skew(self, zero_offset=None, max_skew=None):
        """
        Method which can be ran to determine a new skew.
        
        :returns: :py:class:`datetime.timedelta` object
        """
        
        logger = logging.getLogger(__name__)
        
        result = datetime.timedelta(seconds=0)
        
        if not self.retry_skew:
            return result
        
        if max_skew is None:
            skew = self["retry_skew"]
        else:
            skew = acme.core.DataFormatter.convert_timedelta(
                                            max_skew, 
                                            format="float")
        
        ## Check for integer skew, translate to milliseconds for better
        ## accuracy.
        max_ms = None
        try:
            max_ms = abs(skew * 1000.0)
        except (AttributeError, TypeError):
            pass
        
        if max_ms is None:
            logger.warning("Failed to set skew, could not interpret configured skew value:'{}'".format(self.name, self.retry_skew))
            return result
        
        try:
            rand_num = float(random.randint(0, int(max_ms)) / 1000.0)
            if not zero_offset:
                rand_skew = rand_num - (max_ms / 2000.0)
            else:
                rand_skew = rand_num
            result = datetime.timedelta(seconds=rand_skew)
        except Exception as exp:
            logger.warning("Failed to determine skew. Error:{}".format(self.handler, exp))
            logger.log(5,"Failure stack trace (handled cleanly)", exc_info=1)
        
        return result
    
    def register_module(self, module):
        """
        Method to register the provided configuration module.
        
        :param module: The module to register
        
        """
        
        with self._module_lock:
            if not module.name in self.modules:
                self.modules[module.name] = module
                module.controller = self
            
            ## If we're running, start our module
            if self._should_run:
                try:
                    self.start_module(module)
                except Exception as exp:
                    logger.error("Failed to start timer: {}. Error: {}".format(
                                                            module.timer,
                                                            exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly)", 
                                                            exc_info=1)
    
    def start_module(self, module):
        """
        Method to start the provided module.
        """
        
        logger = logging.getLogger(__name__)
        
        logger.info("Starting configuration module: '{}'".format(module.name))
        
        ## Load state and settings for our module
        module.try_load_state()
        module.try_load_settings()
        
        ## Determine if we should run immediately, and if so apply a start skew
        start_frequency = None
        if module.should_run_immediately():
            if self.start_skew:
                start_frequency = self.roll_skew(zero_offset=True, 
                                                    max_skew=self.start_skew)
        else:
            start_frequency = module.get_current_interval()
        
        module.timer.start(frequency=start_frequency)
    
    def stop_module(self, module):
        """
        Method to stop the provided module
        """
        
        logger = logging.getLogger(__name__)
        
        logger.info("Stopping configuration module: '{}'".format(module.name))
        
        module.try_save_state()
        
        if module.timer:
            module.timer.cancel()
        
    #MARK: Processing methods
    def run_sanity_check(self):
        """
        Method which runs a sanity check to verify we are configured
        to a point where we can perform an API call.
        
        :raises: :py:class:`ConfigurationError` If we are not configured
        :raises: :py:class:`ThrottledRequestError` If our API is currently 
                                            throttled.
        """
        
        ## Verify we're configured
        if not self.registrant:
            raise ConfigurationError("Cannot fetch file: no registration data is available!")
        elif not self.registrant.is_registered():
            raise ConfigurationError("Cannot fetch file: system is not registered!")            
        
        if not self.identity:
            raise ConfigurationError("Cannot fetch file: no authentication identity is loaded.")
        
        if not self.api_url_template:
            raise ConfigurationError("Cannot fetch file: no API URL template defined.")
        
        ## Verify we're not throttled, our next checkin should be within
        ## our allowed skew.
        next_call = self.next_checkin()
        if next_call > datetime.datetime.utcnow():
            raise ThrottledRequestError(throttled_until=next_call)
    
    def make_api_call(self, url_path, params=None, handler=None, *args, **kwargs):
        """
        Method to make an API call to Richter. 
        
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
        
        .. note:
            This method calls :py:function:`run_sanity_checks` and will 
            raise appropriate exceptions
        
        :raises: :py:class:`ConfigurationError` If our object is not appropriately configured
        :raises: :py:class:`ThrottledRequestError` If we are currently under throttling
        :raises: :py:class:`ResponseError` If our request fails
        
        :returns: :py:class:`requests.models.Response` instance.
        
        """
        
        if handler is None:
            handler = requests.get
        
        logger = logging.getLogger(__name__)
        
        with self.checkin_lock:
            
            ## Run API sanity checks
            self.run_sanity_check()
        
            ## Build our URL
            base_uri = self.api_url_template.format(self.registrant.config_server)
            
            ## Strip off any slashes
            if base_uri[-1:] == "/":
                base_uri = base_uri[:-1]
            if url_path[0:1] == "/":
                url_path = url_path[1:]
            
            uri = "{}/{}".format(base_uri, url_path)
            
            logger.log(5, "Making API call:'{}' from url:'{}'".format(handler,uri))
            
            ## Setup our request
            response = None
            
            try:
                with acme.requests.RequestsContextManager(self.identity) as cm:
                    response = handler(uri,cert=cm.temp_file.name,
                                                            params=params,
                                                            *args,
                                                            **kwargs)
                self._num_consecutive_failures = 0
                self._last_failure_date = None
            except Exception:
                self._num_consecutive_failures += 1
                self._last_failure_date = datetime.datetime.utcnow()
                raise
            
            self.last_response = response
            
            return response

class ConfigModule(acme.core.ConfigurableObject, 
                                                acme.core.PersistentObject):
    """
    ConfigModule serves as a base class which provides functionality
    necessary for performing configuration activities against Richter. 
    This classe functions as an abstract and should not be used directly.
    
    :param name: Friendly name to use for our module.
    :type name: (string)
    :param controllor: Our controller object
    :type controller: :py:class:`ConfigurationController` instance
    :param timer: Our timer object
    :type timer: :py:class:`acme.core.RecurringTimer` object
    
    .. example:
        >>> cc = ConfigurationController(
        ...                                identity=d.identity,
        ...                                registrant=d.registrant)
        >>> settings = {    "name": "BaseConfigModule",
        >>>                 "interval": 15,
        >>>                 "execution_skew": 5,
        >>>             }
        >>> 
        >>> cf = acme.ConfigModule(controller=cc, dict_data=settings)
        >>> 
        >>> cc.register_module(cf)
        >>> cc.start()
    
    """
    
    key_map = {
                "name": None,
                "interval": "<type=timedelta>;",
                "manifest_dir": None,
                "state_dir": None,
                "last_update": "<type=datetime>;",
                "last_update_attempt": "<type=datetime>;",
                "execution_skew": "<type=timedelta>;"
            }
    
    settings_keys = ["interval"]
    state_keys = ["last_update", "last_update_attempt"]
    
    execution_skew = DEFAULT_MODULE_SKEW
    interval = DEFAULT_MODULE_INTERVAL
    
    def __init__(self, name=None, controller=None, timer=None, key_map=None, 
                                                    state_keys=None, 
                                                    settings_keys=None,
                                                    manifest_dir=None,
                                                    state_dir=None,
                                                    *args, **kwargs):
        if name is not None:
            self.name = name
        else:
            self.name = self.__class__.__name__
        self.controller = controller
        
        if manifest_dir:
            self.manifest_dir = manifest_dir
        else:
            self.manifest_dir = os.path.join(acme.core.BASE_DIR, "manifests")
        
        if state_dir:
            self.state_dir = state_dir
        else:
            self.state_dir = os.path.join(acme.core.BASE_DIR, "state")
        
        self.files = {}
        self.last_update = None
        self.last_update_attempt = None
        self.interval = ConfigModule.interval
        
        self.files_lock = threading.RLock()
        
        if key_map is None:
            key_map = {}
            key_map.update(self.__class__.key_map)
        
        if state_keys is None:
            state_keys = self.__class__.state_keys[:]
        
        if settings_keys is None:
            settings_keys = self.__class__.settings_keys[:]
        
        ## Call super constructors to finalize configuration
        super(ConfigModule, self).__init__(key_map=key_map, 
                                                state_keys=state_keys,
                                                settings_keys=settings_keys,
                                                *args,**kwargs)
        if timer is None:            
            self.timer = acme.core.RecurringTimer(self.interval, 
                                                    self.run,
                                                    name=self.name)
            self.timer.skew = self.execution_skew
            self.timer.use_zero_offset_skew = True
            self.timer.retry_frequency = datetime.timedelta(seconds=5)
        elif timer is not None:
            self.timer = timer
    
    def get_current_interval(self):
        """
        Method to return our timer's base interval. This method should be 
        overridden by child objects if you wish to inject custom execution 
        scheduling.
        The output of this method controls the execution frequency of 
        our timer
        """
        
        interval = None
        
        if self.interval:
            interval = self.interval
        else:
            interval = DEFAULT_MODULE_INTERVAL
        
        logger = logging.getLogger(__name__)
        logger.log(2, "get_current_interval_base() returns: {}".format(interval))
        
        return interval
        
    def should_run_immediately(self):
        """
        Method to determine whether or not our configuration module 
        has successfully completed an execution. This method should be 
        overriden by child objects if immediate configuration is not desirable
        on first run.
        
        :returns: (bool) True if we have have never configured.
        """
        
        run_now = False
        if self.last_update:
            if self.last_update + self.interval <= datetime.datetime.utcnow():
                run_now = True
        elif not self.last_update and not self.last_update_attempt:
            run_now = True
        
        return run_now
        
    def run(self):
        """
        Method to execute our configuration action. In this instance, this
        is essentiall a no-op. This method should be overridden by child
        objects to perform their desired action.
        """
        
        ## Run custom code here.
        
        return
        

#MARK: Exceptions
class ResponseError(Exception):
    """
    Exception thrown when files fetching fails.
    """
    def __init__(self,message=None,payload=None,*args,**kwargs):
        if message is None and payload:
            message = "Fetching files failed!"
        elif message is None:
            message = "Fetching files failed due to an unknown error!"

        self.payload = payload

        super(ResponseError, self).__init__(message)

class PublishError(Exception):
    """
    Exception thrown when publishing files failes.
    """

    def __init__(self,message=None,payload=None,*args,**kwargs):
        if message is None and payload:
            message = "Publishing files failed!"
        elif message is None:
            message = "Publishing files failed due to an unknown error!"

        self.payload = payload

        super(PublishError, self).__init__(message, *args, **kwargs)

class ConfigurationError(Exception):
    """
    Exception which is raised when we attempt to perform configuration actions
    without required setup.
    """
    pass
    
class ThrottledRequestError(Exception):
    """
    Exception which is thrown in the event that our request has been
    throttled.
    """
    
    def __init__(self, message=None, throttled_until=None, *args, **kwargs):
        if message is None and throttled_until:
            message = "Request faied! Call is throttled until '{}'".format(
                                                    throttled_until)
        elif message is None:
            message = "Request faied! Call was throtttled."

        self.throttled_until = throttled_until

        super(ThrottledRequestError, self).__init__(message, *args, **kwargs)

class PackageVerificationError(Exception):
    """
    Exception which is thrown in the event that verification of package
    content fails.
    """
    pass

class SignatureVerificationError(Exception):
    """
    Exception which is thrown in the event that signature verification of
    request content fails.
    """
    pass
    

