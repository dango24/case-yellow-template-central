import sys
sys.path.insert(0, '/System/Library/Frameworks/Python.framework/Versions/2.7/Extras/lib/python')
import requests
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement
from datetime import datetime
import base64
from collections import namedtuple
from threading import Thread
from utilities import getConfigPath

import utilities
from ..config.agentversion import __version__
from service_requester import StageEnum, PlatformEnum, ConfigOptions

ClientEventTypeTuple = namedtuple("ClientEventTypeTuple",
                                  ["DAEMON_START", "DAEMON_NOT_ON_DOMAIN", "DAEMON_ON_DOMAIN", "DAEMON_COMPLETE",
                                   "DAEMON_HOST_IP", "WEBAPP_START_LAUNCH", "WEBAPP_IN_DELTA_HOLD", "WEBAPP_RESPAWN",
                                   "WEBAPP_RESPAWN_MAX", "WEBAPP_START_DISPLAY", "WEBAPP_SHUTDOWN_NORMAL", "WEBAPP_SHUTDOWN_UNEXPECTED",
                                   "WEBAPP_TIMED_OUT", "SURVEY_FOUND", "SURVEY_NOT_FOUND", "SURVEY_FIND_EXCEPTION"])
ClientEventType = ClientEventTypeTuple(DAEMON_START="daemon_start",
                                       DAEMON_NOT_ON_DOMAIN="daemon_not_on_domain",
                                       DAEMON_ON_DOMAIN="daemon_on_domain",
                                       DAEMON_COMPLETE="daemon_complete",
                                       DAEMON_HOST_IP="daemon_host_ip",
                                       WEBAPP_START_LAUNCH="webapp_start_launch",
                                       WEBAPP_IN_DELTA_HOLD="webapp_in_delta_hold",
                                       WEBAPP_RESPAWN="webapp_respawn",
                                       WEBAPP_RESPAWN_MAX="webapp_respawn_max",
                                       WEBAPP_START_DISPLAY="webapp_start_display",
                                       WEBAPP_SHUTDOWN_NORMAL="webapp_shutdown_normal",
                                       WEBAPP_SHUTDOWN_UNEXPECTED="webapp_shutdown_unexpected",
                                       WEBAPP_TIMED_OUT="webapp_timeout",
                                       SURVEY_FOUND="survey_found",
                                       SURVEY_NOT_FOUND="survey_not_found",
                                       SURVEY_FIND_EXCEPTION="survey_find_exception")


ClientEventCommonParamTuple = namedtuple("ClientEventCommonParamTuple",
                                    ["EMPLOYEE_LOGON", "IDENTITY", "DEVICE", "VERSION", "HOSTNAME"])
ClientEventCommonParam = ClientEventCommonParamTuple(EMPLOYEE_LOGON="employeeLogon",
                                                IDENTITY="clientIdentity",
                                                DEVICE="clientDevice",
                                                VERSION="clientVersion",
                                                HOSTNAME="hostname")


ClientEventLevelTuple = namedtuple("ClientEventLevel", ["TRACE", "INFO", "WARN", "ERROR", "FATAL"])
ClientEventLevel = ClientEventLevelTuple(TRACE="trace", INFO="info", WARN="warning", ERROR="error", FATAL="fatal")

ClientEventParamTuple = namedtuple("ClientEventParamTuple",
                                   ["LOCAL_TIME", "UTC_TIME", "TYPE", "LEVEL", "DESCRIPTION",
                                    "PARAMETER1", "PARAMETER2", "PARAMETER3", "PARAMETER4"])
ClientEventParam = ClientEventParamTuple(LOCAL_TIME="localTime",
					 UTC_TIME="utcTime",
                                         TYPE="type",
                                         LEVEL="level",
                                         DESCRIPTION="description",
                                         PARAMETER1="parameter1",
                                         PARAMETER2="parameter2",
                                         PARAMETER3="parameter3",
                                         PARAMETER4="parameter4")

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class ClientEventsReporter(object):
    __metaclass__ = Singleton

    POST_HEADERS = {"ContentType": "application/xml"}
    REQUEST_TIMEOUT_SECS = 10
    REQUEST_POST_METHOD = "POST"

    CLIENT_EVENT_BASE_URLS = {StageEnum.INTEG: "aHR0cDovL2Njc20tenFmLXBkeC1kLmludGVnLmFtYXpvbi5jb20vZXZlbnRz",
                                StageEnum.PROD: "aHR0cHM6Ly9jY3NtLXpxZi1wcmltYXJ5LW5hLXAtaWFkLmlhZC5wcm94eS5hbWF6b24uY29tL2V2ZW50cw=="}

    XML_ROOT = "ReportEventsRequest"
    XML_EVENTS_NODE = "events"
    XML_EVENT_NODE = "element"

    def __init__(self, userLogin, platform=PlatformEnum.MAC):
        self.userLogin = userLogin
        self.logger = utilities.user_logger()
        stage = ConfigOptions().getStage()
        self.baseUrl = base64.b64decode(self.CLIENT_EVENT_BASE_URLS[stage])
        self.clientIdentify = "ConnectionsDaemon"
        self.clientEventsCommonParams = {ClientEventCommonParam.EMPLOYEE_LOGON: self.userLogin ,
                                         ClientEventCommonParam.IDENTITY: self.clientIdentify,
                                         ClientEventCommonParam.DEVICE: platform,
                                         ClientEventCommonParam.VERSION: __version__,
                                         ClientEventCommonParam.HOSTNAME: utilities.getHostname()}

    def addEvent(self, type, level, description, events=None, param1="", param2="", param3="", param4=""):
        if events is None:
            events = []
        # python datetime object's ISO format doesn't include Z,
        # when http request it complained with "timestamp must follow ISO8601"
        # see https://stackoverflow.com/questions/19654578/python-utc-datetime-objects-iso-format-doesnt-include-z-zulu-or-zero-offset
        localTime = datetime.now().isoformat() 
        systemTime = datetime.utcnow().isoformat()
	if not localTime.endswith("Z"):
            localTime += "Z"
        if not systemTime.endswith("Z"):
            systemTime += "Z"

        event = {ClientEventParam.LOCAL_TIME: localTime,
		 ClientEventParam.UTC_TIME: systemTime,
                 ClientEventParam.TYPE: type,
                 ClientEventParam.LEVEL: level,
                 ClientEventParam.DESCRIPTION: description,
                 ClientEventParam.PARAMETER1: param1,
                 ClientEventParam.PARAMETER2: param2,
                 ClientEventParam.PARAMETER3: param3,
                 ClientEventParam.PARAMETER4: param4}
        events.append(event)
        return events


    def reportEventsSync(self, events):
        root = Element(self.XML_ROOT)
        for param, val in self.clientEventsCommonParams.iteritems():
            commonChild = SubElement(root, param)
            commonChild.text = val

        eventsNode = Element(self.XML_EVENTS_NODE)
        for event in events:
            eventNode = SubElement(eventsNode, self.XML_EVENT_NODE)
            for param, val in event.iteritems():
                eventChild = SubElement(eventNode, param)
                eventChild.text = val

        root.append(eventsNode)
        data = ElementTree.tostring(root)

        self.logger.info("Submitting client event for user: {0}, data: {1}".format(self.userLogin, data))
        response = None
        try:
            self.logger.info("Attempt 1: system provided ca-list")
            response = requests.request(self.REQUEST_POST_METHOD, self.baseUrl, data=data,
                                        headers=self.POST_HEADERS, timeout=self.REQUEST_TIMEOUT_SECS)

        except requests.exceptions.SSLError:
            self.logger.info("Attempt 2: local package ca-list")
            self.logger.info("Using local ca-path: " + getConfigPath(self) + "/amazon-internal-ca.pem")
            try:
                response = requests.request(self.REQUEST_POST_METHOD, self.baseUrl, data=data,
                                        headers=self.POST_HEADERS, timeout=self.REQUEST_TIMEOUT_SECS,
                                        verify=getConfigPath(self) + "/amazon-internal-ca.pem")
            except Exception as e:
                self.logger.error("An error occurred during the request: {0}. Error: {1}".format(self.baseUrl, e))

        except Exception as e:
            self.logger.warn("Exception {0}. User: {1}".format(e, self.userLogin))

        if response is not None:
            self.logger.info("Submitted client event with code: {0}, reason: {1}, content: {2}"
                             .format(response.status_code, response.reason, response.content))
        return response



    def reportEvents(self, *args, **kwargs):
        # Report client events asynchronously so they don't block (since they are not crucial)
        t = Thread(target=self.reportEventsSync, args=args, kwargs=kwargs)
        t.start()
        return t
