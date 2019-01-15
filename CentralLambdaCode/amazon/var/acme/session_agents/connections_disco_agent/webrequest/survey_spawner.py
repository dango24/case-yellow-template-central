# System modules
import os
import socket

from datetime import timedelta
from subprocess import Popen, PIPE
from xml.etree import ElementTree

from decorators import synchronized, synchronizedNoBlocking
from event_reporter import EventReporter, EventEnum
from service_requester import ServiceRequester, StageEnum, PlatformEnum
from client_events_reporter import ClientEventsReporter, ClientEventLevel, ClientEventType

class SurveySpawner(ServiceRequester):
    _MODULE = __file__

    REQUEST_URI = "has-question/"

    MIN_DISPLAY_TIMEDELTA = timedelta(hours=1)

    WEBAPP_REL_PATH = {PlatformEnum.LINUX: "../webapp/AmazonConnections",
                       PlatformEnum.MAC: "../webapp/AmazonConnections.app/Contents/MacOS/AmazonConnections"}
    WEBAPP_RESPAWN_WAIT_SECS = 5 # Wait between respawns
    WEBAPP_SPAWN_MAX_ATTEMPTS = 4
    WEBAPP_MAX_TIME_OPEN = timedelta(hours=3)

    MAX_TIME_SINCE_CHECK_START = timedelta(minutes=5)

    def __init__(self, *args, **kwargs):
        super(SurveySpawner, self).__init__(*args, **kwargs)
        self.lastDisplayed = self._getInitialTime()
        self.postponeTimedelta = SurveySpawner.MIN_DISPLAY_TIMEDELTA
        self.initialCheckStart = None
        self.eventReporter = EventReporter(*args, **kwargs)
        self.clientEventsReporter = ClientEventsReporter(*args, **kwargs)

        # log connections service IP
        baseUrl = self.configOptions.getBaseUrl(self.configOptions.getStage())
        serviceDomain = baseUrl.split("//")[-1].split("/")[0]
        serviceIP = socket.gethostbyname(serviceDomain)
        logMessage = "Connections service url is {0}, IP is {1}".format(baseUrl, serviceIP)
        self.logger.info(logMessage)
        clientEvents = self.clientEventsReporter.addEvent(ClientEventType.DAEMON_HOST_IP, ClientEventLevel.INFO,
                                                    logMessage, None, serviceIP)
        self.clientEventsReporter.reportEvents(clientEvents)

    def isSurveyScheduled(self):

        def onSuccess(response):
            xmlStr = response.content
            xmlObj = ElementTree.fromstring(xmlStr)
            hasQuestion = xmlObj.find("hasQuestion")
            clientEvents = None
            if xmlObj.tag == "DoesUserHaveScheduledQuestionResponse" and hasQuestion is not None:
                if hasQuestion.text == "true":
                    isScheduled = True
                    logMessage = "Survey found for user: {0}".format(self.userLogin)
                    self.logger.info(logMessage)
                    clientEvents = self.clientEventsReporter.addEvent(ClientEventType.SURVEY_FOUND, ClientEventLevel.INFO,
                                                                      logMessage, clientEvents)
                else:
                    isScheduled = False
                    logMessage = "No pending surveys for user: {0}".format(self.userLogin)
                    self.logger.info(logMessage)
                    clientEvents = self.clientEventsReporter.addEvent(ClientEventType.SURVEY_NOT_FOUND, ClientEventLevel.INFO,
                                                                      logMessage, clientEvents)
            else:
                logMessage = "Exception when calling DoesUserHaveScheduledQuestions with response: {0}".format(xmlStr)
                self.logger.error(logMessage)
                clientEvents = self.clientEventsReporter.addEvent(ClientEventType.SURVEY_FIND_EXCEPTION, ClientEventLevel.ERROR,
                                                                  logMessage, clientEvents)
                self.raiseUnexpectedResponse(xmlStr)

            self.clientEventsReporter.reportEvents(clientEvents)

            return isScheduled

        isScheduled = self.makeServiceCall(onSuccess, default=False)

        return isScheduled

    def timeSinceLastDisplay(self, minTimedelta=MIN_DISPLAY_TIMEDELTA):
        return self._timeSince(self.lastDisplayed, minTimedelta)

    @synchronized
    def _launchWebappIfSched(self):
        respawn = True
        spawnAttempt = 1
        maxAttempts = SurveySpawner.WEBAPP_SPAWN_MAX_ATTEMPTS
        while respawn and spawnAttempt <= maxAttempts and not self._event.is_set():
            respawn = False
            # To be safe check if scheduled even if it is a respawn
            isScheduled = self.isSurveyScheduled()
            # If initialCheckStart is None then timedeltaSince returns timedelta(0)
            timeSinceCheckStart = self._timedeltaSince(self.initialCheckStart)
            if isScheduled and timeSinceCheckStart <= self.MAX_TIME_SINCE_CHECK_START:
                respawn = self._launchWebapp()
                if respawn:
                    spawnAttempt += 1
                    if spawnAttempt <= maxAttempts:
                        # App was forcefully killed or crashed, wait and respawn
                        waitSecs = SurveySpawner.WEBAPP_RESPAWN_WAIT_SECS
                        logMessage = "Waiting {0} seconds before respawn attempt".format(waitSecs)
                        self.logger.info(logMessage)
                        clientEvents = self.clientEventsReporter.addEvent(ClientEventType.WEBAPP_RESPAWN, ClientEventLevel.INFO,
                                                                    logMessage, None)
                        self._event.wait(waitSecs)
                        logMessage = "Spawn attempt {0} of {1}".format(spawnAttempt, maxAttempts)
                        self.logger.info(logMessage)
                        clientEvents = self.clientEventsReporter.addEvent(ClientEventType.WEBAPP_RESPAWN, ClientEventLevel.INFO,
                                                                    logMessage, clientEvents)
                        self.clientEventsReporter.reportEvents(clientEvents)
                    else:
                        logMessage = "Max spawn count of {0} reached. Not retrying".format(maxAttempts)
                        self.logger.warn(logMessage)
                        clientEvents = self.clientEventsReporter.addEvent(ClientEventType.WEBAPP_RESPAWN_MAX, ClientEventLevel.WARN,
                                                                    logMessage, None)
                        self.clientEventsReporter.reportEvents(clientEvents)
                else:
                    self.lastDisplayed = self.getCurrentTime()
        # Reset the time since we started checking for a survey
        self.initialCheckStart = None

    @synchronized
    def launchWebappIfSched(self):
        minTimeDelta = self.postponeTimedelta
        delta, enoughTimeSinceLastDisplay = self.timeSinceLastDisplay(minTimeDelta)

        logMessage = "Time delta since last display: {0}. Enough time? {1}".format(delta, enoughTimeSinceLastDisplay)
        self.logger.info(logMessage)
        clientEvents = self.clientEventsReporter.addEvent(ClientEventType.WEBAPP_START_LAUNCH, ClientEventLevel.INFO,
                                                    logMessage, None, str(enoughTimeSinceLastDisplay))
        self.clientEventsReporter.reportEvents(clientEvents)

        if enoughTimeSinceLastDisplay:
            self._launchWebappIfSched()
        else:
            logMessage = "Not enough time since last display, avoiding launch"
            self.logger.info(logMessage)
            clientEvents = self.clientEventsReporter.addEvent(ClientEventType.WEBAPP_IN_DELTA_HOLD, ClientEventLevel.INFO,
                                                        logMessage, None)
            self.clientEventsReporter.reportEvents(clientEvents)

    def _getWebappPath(self):
        appRelativePath = SurveySpawner.WEBAPP_REL_PATH[self.platform]
        workingDir = os.path.dirname(os.path.realpath(__file__))
        appPath = os.path.join(workingDir, appRelativePath)
        return appPath


    def _killWebappAfterMaxTimeOpen(self, process, timedOut):
        maxWaitTime = self.WEBAPP_MAX_TIME_OPEN
        # process.wait waits for the process to exit
        waitThread = self._initThreadJoin(process.wait, timeout=maxWaitTime.total_seconds(), daemonic=True)
        if waitThread.is_alive():
            process.terminate()
            timedOut[0] = True

    def _launchWebapp(self):
        # This should not be called directly as we need to check a few things first
        # Use launchWebappIfSched instead
        args = ["--username=" + self.userLogin]
        stage = self.configOptions.getStage()
        args += ["--stage={0}".format(stage)]
        if stage == StageEnum.INTEG and self.configOptions.isDevMode():
            args += ["--dev"]

        appPath = self._getWebappPath()
        cmd = [appPath] + args
        logMessage = "Display WebApp by running command: {0}".format(cmd)
        self.logger.info(logMessage)
        clientEvents = self.clientEventsReporter.addEvent(ClientEventType.WEBAPP_START_DISPLAY, ClientEventLevel.INFO,
                                                          logMessage, None)
        self.clientEventsReporter.reportEvents(clientEvents)

        try:
            self.eventReporter.reportEvent(EventEnum.ATTEMPTING_DISPLAY_APP_LAUNCH)
            p = Popen(cmd, stdout=PIPE, stderr=PIPE)
            # Pass in timedOut argument as list to thread so that it can modify the reference as a way of
            # returning a value.
            timedOut = [False]
            self._initThread(self._killWebappAfterMaxTimeOpen, args=(p, timedOut), daemonic=True)
            # Should run this in a thread and have a timeout or it could block
            out, err = p.communicate()
            out = out.splitlines()
            if any(x in out for x in ["postponed" , "completed"]):
                self.eventReporter.reportEvent(EventEnum.DISPLAY_APP_SHUTDOWN_NORMAL)
                logMessage = "WebApp shut down with normal action: {0}".format(out)
                self.logger.info(logMessage)
                clientEvents = self.clientEventsReporter.addEvent(ClientEventType.WEBAPP_SHUTDOWN_NORMAL, ClientEventLevel.INFO,
                                                                  logMessage, None, "{0}".format(out))
                self.clientEventsReporter.reportEvents(clientEvents)
            else:
                if timedOut[0]:
                    appOpenTooLongMsg = ("App was killed by daemon as it was open longer than the max time of: " +
                        str(self.WEBAPP_MAX_TIME_OPEN))
                    self.logger.warn(appOpenTooLongMsg)
                    self.eventReporter.reportEvent(EventEnum.DISPLAY_APP_TIMED_OUT, appOpenTooLongMsg)
                    clientEvents = self.clientEventsReporter.addEvent(ClientEventType.WEBAPP_TIMED_OUT, ClientEventLevel.WARN,
                                                                      appOpenTooLongMsg, None)
                    self.clientEventsReporter.reportEvents(clientEvents)
                else:
                    # App was forcefully killed or crashed, return true to indicate to respawn
                    appCrashedMsg = "App crashed, will respawn"
                    self.logger.warn(appCrashedMsg)
                    self.eventReporter.reportEvent(EventEnum.DISPLAY_APP_SHUTDOWN_UNEXPECTED, appCrashedMsg)
                    clientEvents = self.clientEventsReporter.addEvent(ClientEventType.WEBAPP_SHUTDOWN_UNEXPECTED, ClientEventLevel.WARN,
                                                                      appCrashedMsg, None)
                    self.clientEventsReporter.reportEvents(clientEvents)

                    return True
        # Popen raises OSError for standard errors: https://docs.python.org/2/library/subprocess.html#exceptions
        except OSError as e:
            exceptionMsg = "Error while launching webapp, error: {0}".format(e)
            self.logger.error(exceptionMsg)
            self.eventReporter.reportEvent(EventEnum.DISPLAY_APP_SHUTDOWN_UNEXPECTED, exceptionMsg)
            clientEvents = self.clientEventsReporter.addEvent(ClientEventType.WEBAPP_SHUTDOWN_UNEXPECTED, ClientEventLevel.WARN,
                                                              exceptionMsg, None)
            self.clientEventsReporter.reportEvents(clientEvents)

        return False

    # If a thread is already running this method and another thread tries to, just return right away
    @synchronizedNoBlocking()
    def _singleSurveyCheck(self, checkStart=None):
        if checkStart is None:
            self.initialCheckStart = self.getCurrentTime()
        else:
            self.initialCheckStart = checkStart
        self.launchWebappIfSched()

    def singleSurveyCheck(self, timeout=None, *args, **kwargs):
        # Single synchronous survey check, can be triggered by events for immediate survey showing
        self._initThreadJoin(target=self._singleSurveyCheck, timeout=timeout, args=args, kwargs=kwargs)

    # Overrides superclass method
    def makeRequest(self):
       return self._singleSurveyCheck()
