"""**IdleStatus** - which is responsible for checking and commit KARL event of system idle status.

  :platform: macOS, Ubuntu
  :synopsis: This module is used to send idle start time to KARL.

  .. codeauthor:: Xin Du <duxin@amazon.com>
"""

#MARK: Imports
import acme
import acme.agent as agent
import logging
import datetime
import pykarl

#MARK: Classes
class IdleStatusAgent(agent.BaseAgent):

    def __init__(self,*args,**kwargs):
        """
        Configure our default agent behavior
        """
        self.identifier = "IdleStatusAgent"           #: This MUST be unique
        self.name = "IdleStatusAgent"                 #: This SHOULD be unique
        self.triggers = agent.AGENT_TRIGGER_SESSIONLOCK | agent.AGENT_TRIGGER_SESSIONEND
        #: When subclassing, always init superclasses
        super(IdleStatusAgent,self).__init__(name=self.name,*args,**kwargs)

    def execute(self, trigger=None, *args, **kwargs):
        """
        Our primary execution method. This method will be called by
        our scheduler or during events as registered by our triggers.
        :param trigger: The trigger executing the action
        :type trigger: int: bitwise mask value. See AGENT_TRIGGER_*
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("{} Executing!".format(self.identifier))
        result = agent.AGENT_EXECUTION_STATUS_NONE
        try:
            dispatcher = pykarl.event.dispatcher
            if dispatcher.is_configured():
                evt = pykarl.event.Event(type = "IdleStatusEvent", subject_area = "ACME")
                evt.payload = {}
                idle_start_time = datetime.datetime.strftime(datetime.datetime.utcnow(), '%Y-%m-%d %H:%M:%S')
                evt.payload["idle_start_time"] = idle_start_time
                dispatcher.dispatch(evt)
                logger.info("Successfully commit the KARL event of system idle start time to KARL rds. Idle start time is {}".format(idle_start_time))
            result = agent.AGENT_EXECUTION_STATUS_SUCCESS
        except Exception as exp:
            result = agent.AGENT_EXECUTION_STATUS_ERROR
            logger.error("Failed to report idle status to KARL: {}".format(exp))
            
        ## Cleanup
        self.last_execution_status = result 
        self.last_execution = datetime.datetime.utcnow()
        logger.info("{} Finished Executing!".format(self.identifier))
