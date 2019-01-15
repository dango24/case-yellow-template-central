"""**auth** - Module plugin that provides custom authentication event processing

.. module::
    :platform: RHEL5
    :synopsis: Module plugin that provides authentication data models.
   
.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

.. testsetup:: *
    
    import pykarl
    from pykarl.event import Event


"""

import datetime
import logging
import json

import pykarl
import pykarl.core
from pykarl.event import Event,EventTypeError
from pykarl.modules.base import BaseModel,BaseEventHandler,BasePublisher

EVENTTYPE_LOGIN = "LoginEvent"
EVENTTYPE_AUTH = "AuthEvent"

class AuthRecord(BaseModel):
    """
    Class representing a login record.

    :Example:

        >>> event = Event(type="AuthEvent",subject_area="Auth")
        >>> event.uuid = "E-UUID-XXX-XXX-YYY"
        >>> event.source = "S-UUID-XXX-XXX-XXX"
        >>> event.payload["Username"] = "beauhunt"
        >>> event.date = datetime.datetime.strptime("2014-02-01","%Y-%m-%d")
        >>> 
        >>> model = auth.AuthRecord(event=event)
        >>> print model.export_as_redshift_json()
        {
             "source_uuid": "UUID-XXX-XXX-XXX", 
             "username": "beauhunt", 
             "authtype": "AuthEvent", 
             "event_uuid": "E-UUID-XXX-XXX-YYY", 
             "date": "2014-02-01 00:00:00"
        }
        >>>
        
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
        
        self.username = None
        
        if payload_map is None:
            payload_map = {"Username":"username",}
        
        if export_map is None:
            export_map = {"source_uuid" : "source",
                                "event_uuid" : None,
                                "username" : None,
                                "authtype" : "type",
                                "date" : "datestamp",
                                }
        
        BaseModel.__init__(self,event=event,data=data,
                                            payload_map=payload_map,
                                            export_map=export_map)
        
    def load_event(self,event):
        """Method to load data from a karl event."""
        
        BaseModel.load_event(self,event)
        
        if event.type.lower() == "authevent":
            self.type = EVENTTYPE_AUTH
        elif event.type.lower() == "loginevent":
            self.type = EVENTTYPE_LOGIN
        else:
            raise EventTypeError("Unknown event type:%s" % event.type)

class AuthEventPublisher(BasePublisher):
    """
    Class used to orchestrate publishing AuthEvent information to RDS and S3 
    (for RedShift import)
    """
    name = "AuthEventPublisher"
    
    can_target_rds = True      #: We publish to RDS
    can_target_s3 = True       #: We publish to S3
    
    def commit_to_rds(self,model=None,table="auth_event",
                                        device_table="device_instance"):
        """
        Method which will commit the provided event to RDS.
        
        :param model: The event to commit
        :type model: :py:class:`AuthRecord` object
        :param table: The name of the table to publish
        :type table: str
        :param device_table: The name of the device table to publish
        :type device_table: str
        
        """
        
        auth_record = model
        
        if model is None:
            model = self.queued_model
        
        if model is None:
            raise AttributeError("Model is not specified, cannot commit to RDS")
        
        if table is None:
            raise AttributeError("rds_table is not specified, cannot commit to RDS")
        
        if device_table is None:
            raise AttributeError("rds_table is not specified, cannot commit to RDS")
        
        logger = logging.getLogger("commit_to_rds()")
        
        logger.debug("Commiting 'AuthRecord' to RDS table:%s for source:'%s'"
                                                    % (table,model.source))
        rds = self.rds()
        
        ## Attempt to update our device record
        try:
            last_seen = model.event.submit_date.strftime(pykarl.core.DATE_FORMAT)
        except (AttributeError) as exp:
            last_seen = datetime.datetime.utcnow().strftime(pykarl.core.DATE_FORMAT) 
            
        query = "UPDATE %s SET last_seen = $1 WHERE uuid = $2" % device_table
        values = (last_seen,model.source)
        
        logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
        result = int(rds.query(query,values))
        
        if result > 0:
            logger.debug("Successfully updated %s record for event:%s (%s) in RDS." % (device_table,model.event_uuid,model.type))
        else: 
            instance_id = rds.query("INSERT INTO %s(uuid, username, first_seen, last_seen) "
                    "VALUES($1,$2,$3,$4) RETURNING instance_id" % device_table,
                                                        (model.source,
                                                        model.username,
                                                        model.datestamp,
                                                        last_seen))
        
        ## Update our logininfo table
        query = ("UPDATE %s SET username = $1, event_uuid = $2,"
                "authtype = $3, date = $4 WHERE source_uuid = $5" % table)
        
        values = (model.username,model.event_uuid,model.type,model.datestamp,
                                                                model.source)
        
        logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
        result = int(rds.query(query,values))
       
        if result > 0:
            logger.info("Successfully updated %s record for event:%s (%s) in RDS." % (table,model.event_uuid,model.type))
            
        else:
            query = ("INSERT INTO %s (source_uuid,event_uuid,username,authtype,date) VALUES($1,$2,$3,$4,$5)" % table)
            values = (model.source,model.event_uuid,model.username,model.type,
                                            model.datestamp)
            
            logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,
                                                                    values))
            rds.query(query,values)

module_name = "auth"    #: The name of our module, used for filtering operations

event_handler = BaseEventHandler(name=module_name)
event_handler.subject_areas = ["Auth"]

event_handler.action_map = {"default" : {"obj_class":AuthRecord,
                                            "pub_class": AuthEventPublisher,
                                            "archive_table" : "auth_event",
                                            "s3key_prefix" : "auth/karl_authevent"
                                            },
                           }

