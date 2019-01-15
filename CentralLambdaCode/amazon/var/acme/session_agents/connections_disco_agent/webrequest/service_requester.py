# System modules
import sys
sys.path.insert(0, '/System/Library/Frameworks/Python.framework/Versions/2.7/Extras/lib/python')
import requests
import ConfigParser
import os
import threading
from utilities import getConfigPath

# utilities could be in the parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
# User modules
import utilities

from datetime import datetime as dt, timedelta
from os.path import join as pjoin
from requests.exceptions import RequestException
from threading import Thread
from urlparse import urljoin
from xml.etree import ElementTree

from ..config.agentversion import __version__
from decorators import retry, synchronized, synchronizedWith


class StageEnum(object):
    INTEG = "integ"
    PROD = "prod"

# TODO As a PyACME agent this will be harder to use, should switch to python platform module calls
class PlatformEnum(object):
    LINUX = "Linux"
    MAC = "Darwin"

class RequestParamEnum(object):
    CLIENT_IDENTITY = "clientIdentity"
    HOSTNAME = "hostname"

class ConfigOptions(object):
    _LOGGER = utilities.user_logger(loggerName="ConfigOptions")

    CONFIG_DIR = "config/"
    CONFIG_PATH = pjoin(CONFIG_DIR, "webrequest.config")
    CONFIG_OVERRIDE_PATH = pjoin(CONFIG_DIR, "webrequest_override.config")

    DEFAULT_STAGE = StageEnum.PROD
    DEFAULT_BASE_URLS = {StageEnum.INTEG: "https://expresssurveyservice.integ.amazon.com/",
                         StageEnum.PROD: "https://expresssurveyservice-iad.amazon.com/"}

    MAIN_SECTION = "webrequest"
    STAGE_OPTION = "stage"

    BASE_URL_SECTION = "service_urls"

    def __init__(self, stage=None, configPaths=None):
        self.logger = ConfigOptions._LOGGER
        self.stage = stage
        self.baseUrls = None
        if configPaths is None:
            # ConfigParser read will read the override file if it exists
            # Files later in the list take precedence if the same options are in both (so override should not be first)
            configPaths = [ConfigOptions.CONFIG_PATH, ConfigOptions.CONFIG_OVERRIDE_PATH]
        self.configPaths = configPaths
        self.initConfig(configPaths)

    def pathsJoin(self, base, paths):
        joinedPaths = []
        for path in paths:
            joinedPaths.append(pjoin(base, path))
        return joinedPaths

    def getFullPaths(self, paths):
        moduleDir = os.path.dirname(os.path.realpath(__file__))
        return self.pathsJoin(moduleDir, paths)

    def initConfig(self, configPaths):
        try:
            self.config = ConfigParser.SafeConfigParser()
            # Note this is a list of filenames read correctly
            self.readResults = self.config.read(self.getFullPaths(configPaths))
            if not self.readResults:
                raise ConfigParser.ParsingError("Was not able to read any config file")
        except ConfigParser.ParsingError as e:
            self.logger.error("Error parsing config file: {0}".format(e))
        return self.readResults

    def getStage(self):
        if self.stage is None:
            try:
                self.stage = self.config.get(self.MAIN_SECTION, self.STAGE_OPTION)
            # From: https://docs.python.org/2/library/configparser.html#ConfigParser.Error
            except ConfigParser.Error as e:
                self.logger.error("Error getting stage from config. Error: {0}".format(e))
                self.stage = self.DEFAULT_STAGE
        return self.stage

    def getBaseUrl(self, stage=None):
        if stage is None:
            stage = self.getStage()
        if self.baseUrls is None:
            self.baseUrls = self.DEFAULT_BASE_URLS.copy()
            try:
                configUrls = dict(self.config.items(self.BASE_URL_SECTION))
                self.baseUrls.update(configUrls)
            except ConfigParser.Error as e:
                self.logger.error("Error getting base urls from service_urls section. Error: {0}".format(e))

        if not (stage in self.baseUrls):
            self.logger.error("Error, could not find stage '{0}' in base urls dict {1}".format(stage, self.baseUrls))
            stage = self.DEFAULT_STAGE
        return self.baseUrls[stage]

    def isDevMode(self):
        try:
            return self.config.getboolean(self.MAIN_SECTION, "dev")
        except ConfigParser.Error:
            return False

class ServiceRequester(object):
    _LOGGER = None
    _DEFAULT_CONFIG = ConfigOptions()

    # Set in child classes
    REQUEST_URI = None

    CLIENT_ID_PREFIX = "ConnectionsDaemonQ1-"

    # One hour by default
    POLL_INTERVAL_SECS = 60 * 60
    # The max wait time is (backoff factor)^(retries-1) * delay . There is a summation of all wait times so the total
    # max wait time is at most twice the max single wait. Given that retry picks random times between 0 and the max
    # then the total expected max wait time is about half the summation so about equal to the max single wait time
    # With 5 retries, a delay of 2, and a backoff of 2 the expected total wait is about 30 seconds
    POLL_RETRIES = 5
    POLL_DELAY_SECS = 2
    POLL_BACKOFF_FACTOR = 2

    REQUEST_TIMEOUT_SECS = 10

    # Just to prevent queued up threads from re-making the same requests after a long running thread unblocks
    MIN_REQUEST_TIMEDELTA = timedelta(seconds=15)

    # Threshold of threads to start cleaning stored array
    CLEAN_THREADS_THRESHOLD = 100

    USER_NOT_FOUND_TAG = "UserNotFoundException"


    def __init__(self, userLogin, platform=PlatformEnum.LINUX, stage=None, clientId=None):
        self.userLogin = userLogin
        self.platform = platform
        if clientId is None:
            self.clientId = self.CLIENT_ID_PREFIX + platform + "-" + __version__
        else:
            self.clientId = clientId
        if self._LOGGER is None:
            # Setting self.__class__._LOGGER in the constructor allows us to set the class variable _LOGGER of child
            # classes instead of ServiceRequester's class variable
            self.__class__._LOGGER = utilities.user_logger(modulePath=self._MODULE)
        self.logger = self._LOGGER
        # Reentrant locks (RLock) allow the same thread to acquire the lock more than once,
        # but they only allow 1 thread to have the lock at a time
        # This allows for synchronization but prevents deadlocking on recursive / nested function calls
        # See: https://docs.python.org/2/library/threading.html#rlock-objects
        # and http://effbot.org/zone/thread-synchronization.htm for more info
        self._lock = threading.RLock()
        self._event = threading.Event()
        self._threadLock = threading.RLock()
        self.threads = []

        if stage is None:
            self.configOptions = self._DEFAULT_CONFIG
        else:
            self.configOptions = ConfigOptions(stage=stage)

    def _getBaseParams(self):
        params = {RequestParamEnum.CLIENT_IDENTITY: self.clientId,
                  RequestParamEnum.HOSTNAME: utilities.getHostname()}
        return params

    def _joinUrl(self, *parts):
        joinedUrl = ""
        for i, part in enumerate(parts):
            # Add a trailing slash if not present and not the last element or urljoin will overwrite it
            addSlash = not part.endswith("/") and i < len(parts)-1
            part = part + "/" if addSlash else part
            joinedUrl = urljoin(joinedUrl, part)
        return joinedUrl

    def _getServiceUrl(self, uri, userLogin, stage=None):
        baseUrl = self.configOptions.getBaseUrl(stage)
        serviceUrl = self._joinUrl(baseUrl, uri, userLogin)
        # serviceUrl = "http://getstatuscode.com/403" # For testing
        return serviceUrl

    def getFullRequestUrl(self, requestUri=None):
        if requestUri is None:
            # Use self because we want to get the class variable in our child
            requestUri = self.REQUEST_URI
        return self._getServiceUrl(requestUri, self.userLogin)

    def handleResponse(self, response):
        responseCode = response.status_code
        # Requests does not throw an exception for codes >= 400 unless you call response.raise_for_status()
        if responseCode == 404:
            try:
                xmlObj = ElementTree.fromstring(response.content)
                if xmlObj.tag == self.USER_NOT_FOUND_TAG:
                    # If user is not found this may be a local account, may want to decrease check frequency
                    raise UserNotFoundException("User was not found")
            except ElementTree.ParseError:
                # This will be raised as a RequestException below so we can pass here
                pass
        # Raise any other status exception
        response.raise_for_status()
        return response

    def handleException(self, e):
        # With the requests library you can handle the status codes before throwing an exception,
        # so we don't need to do anything here
        return e

    def _makeServiceCallWithRetries(self, requestUrl, data, params, headers, ca):
        C = self.__class__ # for convenience of static field access
        # Nested function allows us to use self in decorator
        @retry(RequestException, C.POLL_RETRIES, C.POLL_DELAY_SECS, C.POLL_BACKOFF_FACTOR, self.logger,
               handler=self.handleResponse, excptHandler=self.handleException)
        def makeCallWithSelf():
            response = None
            queryParams = self._getBaseParams()
            # Requests escapes parameters, so we can just pass them in
            queryParams.update(params)
            method = "GET" if data is None else "POST"
            try:
                self.logger.info("Attempt 1: system provided ca-list")
                response = requests.request(method, requestUrl, data=data, params=queryParams, headers=headers,
                                            timeout=self.REQUEST_TIMEOUT_SECS)
                self.logger.info("Response received")
            except requests.exceptions.SSLError:
                self.logger.info("Attempt 2: local package ca-list")
                self.logger.info("Using local ca-path: " + ca)
                try:
                    response = requests.request(method, requestUrl, data=data, params=queryParams, headers=headers,
                                                timeout=self.REQUEST_TIMEOUT_SECS,
                                                verify=ca)
                except Exception as e:
                    self.logger.error("An error occurred during the request: {0}. Error: {1}".format(requestUrl, e))
                self.logger.info("Response received")
            except Exception as e:
                self.logger.error("An error occurred during the request: {0}. Error: {1}".format(requestUrl, e))

            if response is not None:
                self.logger.info("Submitted response with code: {0}, reason: {1}"
                                 .format(response.status_code, response.reason))
            return response
        return makeCallWithSelf()

    def _makeServiceCall(self, onSuccess, requestUrl=None, data=None, params={}, headers={}, default=None):
        if requestUrl is None:
            requestUrl = self.getFullRequestUrl()
        self.logger.info("Requesting URL: {0}".format(requestUrl))

        response = None
        retVal = default
        caList = getConfigPath(self) + "/amazon-internal-ca.pem"
        try:
            response = self._makeServiceCallWithRetries(requestUrl, data, params, headers, caList)
            retVal = onSuccess(response)
        except UserNotFoundException as e:
            self.logger.warn("{0}. User: {1}".format(e, self.userLogin))
        except RequestException as e:
            self.logger.error("An error occurred during the request: {0}. Error: {1}".format(requestUrl, e))

        return retVal, response

    def makeServiceCall(self, *args, **kwargs):
        retVal, _ = self._makeServiceCall(*args, **kwargs)
        return retVal

    def makeServiceCallAsync(self, *args, **kwargs):
        return self._initThread(self.makeServiceCall, args, kwargs)

    def getMinTimedelta(self, minTimedelta=None):
        if minTimedelta is None:
            minTimedelta = self.MIN_REQUEST_TIMEDELTA
        return minTimedelta

    @staticmethod
    def _getInitialTime():
        # Unix epoch, so basically the difference will always be greater than min timedelta
        return dt.utcfromtimestamp(0)

    @staticmethod
    def getCurrentTime():
        return dt.utcnow()

    def _timedeltaSince(self, lastTime):
        if lastTime is None:
            return timedelta(0)
        return self.getCurrentTime() - lastTime

    def _timeSince(self, lastTime, minTimedelta=None):
        minTimedelta = self.getMinTimedelta(minTimedelta)
        delta = self._timedeltaSince(lastTime)
        enough = delta >= minTimedelta
        return delta, enough

    def _enoughTimeSince(self, *args, **kwargs):
        enough = self._timeSince(*args, **kwargs)[1]
        return enough

    def raiseUnexpectedResponse(self, responseStr):
        unexpectedResp = "Response did not contain any of the expected tags"
        raise ValueError("{0}. Response raw string: {1}".format(unexpectedResp, responseStr))

    def _initThread(self, target, args=(), kwargs=None, daemonic=False):
        t = Thread(target=target, args=args, kwargs=kwargs)
        t.setDaemon(daemonic)
        t.start()
        self._addThread(t)
        return t

    def _initThreadJoin(self, target, timeout=None, *args, **kwargs):
        t = self._initThread(target, *args, **kwargs)
        t.join(timeout)
        return t

    # To be replaced in child classes with the request to be made
    def makeRequest(self):
        pass

    @synchronizedWith("_threadLock")
    def _addThread(self, thread):
        self.threads.append(thread)
        if len(self.threads) > self.CLEAN_THREADS_THRESHOLD:
            self._cleanThreads()

    @synchronizedWith("_threadLock")
    def _cleanThreads(self):
        # Iterate backwards so that popping does not affect the loop
        for i in reversed(xrange(len(self.threads))):
            thread = self.threads[i]
            if not thread.isAlive():
                self.threads.pop(i)
                # Can also just use remove in forward order, but less efficient
                #self.threads.remove(thread)

class UserNotFoundException(Exception):
    pass
