"""
**acme_agent_contractor_logoff** - Provides custom data handling for ACME agent for contractor log off event.

..   module:: **acme_agent_contractor_logoff** Provides data handling for ACMEAgent subject area
    :platform: RHEL5
    :synopsis: Module plugin to capture ACME data for usernames when users get logged off from WorkSpaces.

.. codeauthor:: Abhinav Srivastava <srabhina@amazon.com>



"""

import datetime
import logging
import json

import pykarl.core
from pykarl.modules.base import BaseModel,BasePublisher,BaseEventHandler



class ContractorLogOffEvent(BaseModel):
    """
    Class representing contractor log off event
    """

    def __init__(self,event=None,data=None,payload_map=None,export_map=None):
        """
        Our constructor.

        :param event: Our event to process.
        :type event: `:py:class:Event`
        :param payload_map: Key=>Property map for data imports
        :type payload_map: dict(string,string)
        :param export_map: Key=>Property map for data exports
        :type export_map: dict(string,string)
        :export_map concerns DB -> mapping of columns:val
        :payload_map concerns creating string val -> mapping of payload : instance var
        """

	self.username = None

        if payload_map is None:
            payload_map = {
                "UserName": "username",
                        }

        if export_map is None:
            export_map = {
				"username" : "username",
                		"date" : "<type=datetime>",
                		"source_uuid" : "source",
                     		"type" : None,
                     		"event_uuid" : None,
                        }
    
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)



####start of main
module_name = "acme_agent_contractor_logoff"   #: The name of our module, used for filtering operations

event_handler = BaseEventHandler(name=module_name)  #: Our modules event handler, this MUST be
                                    #: defined if our module is intended to be
                                    #: called.

event_handler.subject_areas = ["ACMEAgent"]
event_handler.action_map = {
                "ContractorLogOffEvent" : {
                    "obj_class":ContractorLogOffEvent,
                    "rds_table" : "contractor_logoff_ws_event",
                    "archive_table" : "contractor_logoff_ws_event",
                    "rds_action" : "insert",
                    "s3key_prefix" : "event/contractor_logoff_ws_event",
                    }
                }
