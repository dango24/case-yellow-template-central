"""
**aea_plugin** - Provides custom data handling for Amazon Enterprise Access Plugin

.. module:: **aea_plugin** Provides data handling for AEA Plugin subject area
    :platform: RHEL5
    :synopsis: Module plugin that provides AEA Plugin data and publishing models
     :       representing patching activity.

.. codeauthor:: Jason Simmons <jasosimm@amazon.com>



"""

import datetime
import logging
import json

import pykarl.core
from pykarl.modules.base import BaseModel,BasePublisher,BaseEventHandler



class AEAPluginStatus(BaseModel):
    """
    Class representing Amazon Enterprise Access browser plugin status.
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

        self.browser = None
        self.browser_status = None
        self.username = None
        
        if payload_map is None:
            payload_map = {
                "Browser" : "browser",
                "Extension_Version" : "extension_version",
                "Installation_Status" : "installation_status",
                "Username" : "username",
                }
        if export_map is None:
            export_map = {
                "date":"<type=datetime>",
                "source_uuid" : "source",
                "username" : "username",
                "browser":"browser",
                "extension_version" : "extension_version",
                "installation_status" : "installation_status",
                }
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)
       
####start of main
module_name = "aea_plugin"   #: The name of our module, used for filtering operations

event_handler = BaseEventHandler(name=module_name)  #: Our modules event handler, this MUST be
                                    #: defined if our module is intended to be
                                    #: called.

event_handler.subject_areas = ["ACME"]
event_handler.action_map = {
                "AEAPluginStatus" : {
                   "obj_class":AEAPluginStatus,
                    "rds_table" : "aea_plugin",
                    "archive_table" : "aea_plugin",
                    "rds_action" : "insert",
                    "s3key_prefix" : "event/aea_plugin",
                    }
                }

