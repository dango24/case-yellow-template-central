import acme.agent as agent
import logging
import platform
import systemprofile
import time
import datetime

from webrequest.survey_spawner import SurveySpawner
from webrequest.client_events_reporter import ClientEventsReporter, ClientEventLevel, ClientEventType

class ConnectionsDiscoAgent(agent.BaseAgent):
    # Wait times between checking the network status for on domain
    NETWORK_WAIT_TIMES = [10, 20, 60, 60, 60]
    # Wait for the display for a max of a minute, could take a while if the user leaves the window open
    # Note that this timeout won't close the display, just let the agent execute method finish
    # while keeping the other threads running
    MAX_WAIT = 60

    spawner = None

    def __init__(self, *args, **kwargs):
        """
        Configure our default agent behavior
        """

        identifier = "ConnectionsDiscoAgent"
        self.identifier = identifier  # : This MUST be unique
        self.name = identifier  # : This SHOULD be unique

        self.run_frequency = datetime.timedelta(hours=3)
        #: Run skew is recommended for scheduled agents to distribute load on dependent systems
        self.run_frequency_skew = datetime.timedelta(minutes=5)

        self.prerequisites = agent.AGENT_STATE_NONE
        # SESSIONSTART is when the user first logs on
        self.triggers = agent.AGENT_TRIGGER_SESSIONUNLOCK | agent.AGENT_TRIGGER_SESSIONSTART | agent.AGENT_TRIGGER_SCHEDULED

        # When subclassing, always init superclasses
        super(ConnectionsDiscoAgent, self).__init__(name=self.name, identifier=self.identifier, *args, **kwargs)

    def execute(self, trigger=None, data=None):
        """
        Our primary execution method. This method will be called by
        our scheduler or during events as registered by our triggers.

        :param trigger: The trigger executing the action
        :type trigger: int: bitwise mask value. See AGENT_TRIGGER_*
        """

        logger = logging.getLogger(self.logger_name)

        username = None

        if data is not None:
            # For AGENT_TRIGGER_SESSIONUNLOCK and AGENT_TRIGGER_SESSIONSTART event
            username = data["username"]
        else:
            # For AGENT_TRIGGER_SCHEDULED event
            username = systemprofile.profiler.current_user()

        localPlatform = platform.system()

        clientEventsReporter = ClientEventsReporter(username, localPlatform)

        logMessage = "Executing Connections Disco agent with user: {0}".format(username)
        logger.info(logMessage)
        clientEvents = clientEventsReporter.addEvent(ClientEventType.DAEMON_START, ClientEventLevel.INFO, logMessage, None)

        unlockTime = SurveySpawner.getCurrentTime()

        i = 0
        onDomain = systemprofile.profiler.on_domain()
        while not onDomain and i < len(self.NETWORK_WAIT_TIMES):
            waitTime = self.NETWORK_WAIT_TIMES[i]
            logMessage = "Not on Amazon network, waiting {0} seconds before checking again".format(waitTime)
            logger.info(logMessage)
            clientEvents = clientEventsReporter.addEvent(ClientEventType.DAEMON_NOT_ON_DOMAIN, ClientEventLevel.INFO,
                                                         logMessage, clientEvents)

            time.sleep(waitTime)
            i += 1
            onDomain = systemprofile.profiler.on_domain()

        if onDomain:
            if self.spawner is None:
                self.__class__.spawner = SurveySpawner(username, localPlatform)

            logMessage = "On Amazon network, proceeding with agent execution with user: {0}".format(username)
            logger.info(logMessage)
            clientEvents = clientEventsReporter.addEvent(ClientEventType.DAEMON_ON_DOMAIN, ClientEventLevel.INFO, logMessage, clientEvents)
            clientEventsReporter.reportEvents(clientEvents)

            self.spawner.singleSurveyCheck(timeout=self.MAX_WAIT, checkStart=unlockTime)
        else:
            totalWait = SurveySpawner.getCurrentTime() - unlockTime
            logger.warn("The maximum network wait time was exceeded. Not running agent. Total wait: {0}"
                        .format(totalWait))

        logMessage = "Done Connections Disco agent with user: {0}".format(username)
        logger.info(logMessage)

        if onDomain:
            clientEvents = clientEventsReporter.addEvent(ClientEventType.DAEMON_COMPLETE, ClientEventLevel.INFO, logMessage, None)
            clientEventsReporter.reportEvents(clientEvents)

        # Cleanup
        self.last_execution_status = agent.AGENT_EXECUTION_STATUS_SUCCESS
