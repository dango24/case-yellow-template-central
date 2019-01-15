"""
**claims** - Provides custom data handling for claims subject area

.. module:: **claims** Provides custom data handling for claims subject area

    :platform: RHEL5
    :synopsis: Module plugin that provides claims data and publishing models.

.. codeauthor:: Jude Ning <huazning@amazon.com>

"""

import datetime
import logging
import json


import pykarl.core
from .base import BaseModel,BasePublisher,BaseEventHandler

class ClaimsUpdateEvent(BaseModel):
    """
    Class representing a ClaimsUpdateEvent model
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
        self.token_state = None
        self.type = 'ClaimsUpdateEvent'

        if payload_map is None:
            payload_map = {
                                "token_state" : None,
                                "last_token_generation_attempt": None,
        }
        
        if export_map is None:
            export_map = {
                                "source_uuid" : "source",
                                "type" : "type",
                                "event_uuid" : None,
                                "token_state" : None,
                                "last_token_generation_attempt": "datestamp",
                        }
        
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)

module_name = "claims"
event_handler = BaseEventHandler(name = module_name)
event_handler.subject_areas=["Claims"]

event_handler.action_map = {"ClaimsUpdateEvent":{
    "obj_class":ClaimsUpdateEvent,
    "rds_key":"source_uuid",
    "rds_table":"claims_status",
    "archive_table":"claims_status",
    "rds_action":"insert",
    "s3key_prefix":"claims_status/karl_claims_status",
    "update_device":False,
    }
}

