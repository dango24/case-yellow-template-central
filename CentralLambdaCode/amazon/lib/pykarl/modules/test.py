"""
.. module:: test
   :platform: RHEL5
   :synopsis: Module plugin that provides testing facilities.
   
.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

import datetime
import logging
import json
import pykarl
import base64
from .base import BaseModel,BasePublisher,BaseEventHandler

class GenericEvent(BaseModel):
    """Class representing a generic event (used for prototyping)"""

    def __init__(self,event=None,data=None):
        """
        Our constructor.
        
        :param event: Our event to process.
        :type event: `:py:class:Event`
        """
        
        BaseModel.__init__(self,event,data)

        self.payload = {}
        
        self.s3_base = "acme/karl_%s" % self.type.lower()
        
        self.payload_map = {}
        
        if event is not None:
            self.load_event(event)
        
        if data is not None:
            self.load_dict(data)

    def load_event(self,event,payload_map=None):
        """
        Method to load data from a karl event.
        
        :param pykarl.event.Event event: Our event to load
        
        :param dict payload_map: Key=>Value mappings to use for loading
        """
        
        BaseModel.load_event(self,event,payload_map)
        self.payload = event.payload

    def to_event(self):
        """
        Method which will output a KARL Event based on our object.
        """
        
        event = BaseModel.to_event(self)
        event.payload = self.payload

        return event


class GenericEventPublisher(BasePublisher):

    def process_model(self,model=None):
        logger = logging.getLogger(__name__)
        e = model.to_event()
        logger.info("New Event Data:%s" % e.to_json())
    
    def string_for_event(self,json_data):
        """Returns a string representing our event in JSON"""

        dict = {}
        for key,value in json.loads(json_data).iteritems():
            if key == "data":
                dict[key] = json.loads(base64.b64decode(value))
            else:
                dict[key] = value

        return json.dumps(dict)


module_name = "test"
event_handler = BaseEventHandler()
event_handler.subject_areas = ["__debug__"]
event_handler.action_map = {"default" : {"obj_class":GenericEvent,
                                            "pub_class": GenericEventPublisher
                                            },
                }
