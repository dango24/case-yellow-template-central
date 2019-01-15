""":q
**patch** - Provides custom data handling for ACME patching events

.. module:: **patch** Provides data handling for ACME subject area
    :platform: RHEL5
    :synopsis: Module plugin that provides ACME data and publishing models
     :       representing patching activity.

.. codeauthor:: Abhinav Srivastava <srabhina@amazon.com>



"""

import datetime
import logging
import json

import pykarl.core
from pykarl.modules.base import BaseModel,BasePublisher,BaseEventHandler



class RebootFlagSetEvent(BaseModel):
    """
    Class representing reboot flag set  event
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

        if export_map is None:
            export_map = {
                "date" : "<type=datetime>",
                     "source_uuid" : "source",
                     "type" : None,
                     "event_uuid" : None,
                        }
    
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)


class RebootFlagUnSetEvent(BaseModel):
    """
    Class representing reboot flag unset event
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
        """

        if export_map is None:
            export_map = {
                "date" : "<type=datetime>",
                                "source_uuid" : "source",
                                "type" : None,
                                "event_uuid" : None,
                        }

        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)



class RebootACMEInitiatedEvent(BaseModel):
    """
    Class representing ACME initiated reboot event
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
        """

        if export_map is None:
            export_map = {
                                "date" : "<type=datetime>",
                                "source_uuid" : "source",
                                "type" : None,
                                "event_uuid" : None,
                        }

        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)



class RebootEvent(BaseModel):
    """
    Class representing all reboot events beyond ACME as well.
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
        """

        self.reboot_string = None
        self.reboot_date = None
        self.reboot_duration = None
        self.boot_duration = None

        if payload_map is None:
            payload_map = {
                "DateTime": "<type=datetime>;reboot_date",
                "RebootString":"reboot_string",
                "RebootDuration" : "<type=float>;reboot_duration",
                "BootDuration" : "boot_duration",
                "platform" : None,
                "reboot_in_maintenance_window" : None,
                "acme_reboot": None,
                        }
        if export_map is None:
            export_map = {
                "date":"<type=datetime>;reboot_date",
                "source_uuid" : "source",
                "type" : None,
                "event_uuid" : None,
                "reboot_message":"reboot_string",
                "reboot_duration" : "reboot_duration",
                "boot_duration" : "boot_duration",
                "platform": None,
                "reboot_in_maintenance_window": None,
                "acme_reboot": None,

                        }

        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)
       
####start of main
module_name = "acme_reboot_instrument"   #: The name of our module, used for filtering operations

event_handler = BaseEventHandler(name=module_name)  #: Our modules event handler, this MUST be
                                    #: defined if our module is intended to be
                                    #: called.

event_handler.subject_areas = ["ACME","System"]
event_handler.action_map = {
                "RebootFlagSetEvent" : {
                    "obj_class":RebootFlagSetEvent,
                    "rds_table" : "reboot_event",
                    "archive_table" : "reboot_event",
                    "rds_action" : "insert",
                    "s3key_prefix" : "event/system_reboot_event",
                    },

                "RebootACMEInitiatedEvent" : {
                    "obj_class":RebootACMEInitiatedEvent,
                    "rds_table" : "reboot_event",
                    "archive_table" : "reboot_event",
                    "rds_action" : "insert",
                    "s3key_prefix" : "event/system_reboot_event",
                    },
                "RebootFlagUnSetEvent" : {
                    "obj_class":RebootFlagUnSetEvent,
                    "rds_table" : "reboot_event",
                    "archive_table" : "reboot_event",
                    "rds_action" : "insert",
                    "s3key_prefix" : "event/system_reboot_event",
                    },
                "RebootEvent" : {
                   "obj_class":RebootEvent,
                    "rds_table" : "reboot_event",
                    "archive_table" : "reboot_event",
                    "rds_action" : "insert",
                    "s3key_prefix" : "event/system_reboot_event",
                    },
}

