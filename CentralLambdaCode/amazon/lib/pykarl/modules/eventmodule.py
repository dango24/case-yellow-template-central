"""**eventmodule** - Provides standard routing and storage facilities for all KARL event

.. module:: eventmodule
    :platform: RHEL5
    :synopsis: Module plugin that provides ACME data models. This is the default
           data handler for all events received.
   
.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

import datetime
import logging
import boto
import boto.s3
import re
import json
import uuid

import pykarl
import pykarl.core
from .base import BaseModel,BasePublisher,BaseEventHandler,BaseArchiver

publish_event = True
publish_event_payload = True

class GenericEvent(BaseModel):
    """
    Class used to represent any event.
    
    :Example:

            >>> event = Event(type="SystemInfo",subject_area="ACME")
            >>> event.uuid = "E-UUID-XXX-XXX-YYY"
            >>> event.source = "S-UUID-XXX-XXX-XXX"
            >>> event.payload["key1"] = "value1"
            >>> event.date = datetime.datetime.strptime("2014-02-01","%Y-%m-%d")
            >>> event.submit_date = datetime.datetime.strptime("2014-02-02","%Y-%m-%d")
            >>> 
            >>> generic_event = GenericEvent(event=event)
            >>> print generic_event.export_as_redshift_json()
            {
                 "source_uuid": "S-UUID-XXX-XXX-XXX", 
                 "uuid": "E-UUID-XXX-XXX-YYY", 
                 "submit_date": "2014-02-02 00:00:00", 
                 "date": "2014-02-01 00:00:00", 
                 "type": "SystemInfo", 
                 "size": 226
            }
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
        
        if payload_map is None:
            payload_map = {}
        if export_map is None:
            export_map = {  "uuid" : "event_uuid",
                            "source_uuid" : "source",
                            "type" : None,
                            "size":None,
                            "date":"datestamp",
                            "submit_date" : "submit_datestamp"
                        }                
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)

class EventPublisher(BasePublisher):
    """
    Class used to orchestrate publishing information to RDS, S3, and Redshift
    """
    
    name = "EventPublisher"
    
    can_target_rds = True
    can_target_s3 = True
    
    
    s3key_prefix = "event/karl_event_"
    payload_s3key_prefix = "event_payload/"
    payload_s3key_suffix = ".txt"
    
    def __init__(self,name="EventPublisher",model=None,karl=None,targets=None):
        """
        Primary constructor.
        """
        
        BasePublisher.__init__(self,name=name,model=model,karl=karl,targets=targets)

        self.publish_event = True
        self.publish_event_payload = True
        self.s3key_prefix = EventPublisher.s3key_prefix
                
    def commit_to_s3(self,model=None,models=None,
                                            s3key_prefix=None,
                                            s3key_suffix=None,
                                            payload_s3key_prefix=None,
                                            payload_s3key_suffix=None,
                                            s3file_name=None,
                                            process_callbacks=None):
        """
        Method which commits data to S3.
        
        :param model: Model to commit
        :type model: :py:class:`BaseModel` descendent
        :param models: List of models to commit
        :type models: List of :py:class:`BaseModel` descendents
        :param karl: The KARL object used for resource access
        :type karl: :py:class:`pykarl.core.KARL` object
        """
        
        logger = logging.getLogger(self.__class__.__name__)
        
        if model is None:
            model = self.queued_model
        
        if models is None:
            models = self.queued_models
        
        if model is None and models is None:
            raise AttributeError("Model(s) not specified, cannot commit to s3")
        elif models is None:
            models = []
        
        if model is not None:
            with self.lock:
                models.append(model)
        
        if process_callbacks is None:
            process_callbacks = True
        
        now = datetime.datetime.utcnow()
        starttime = now
        
        if len(models) > 1:
            is_batch = True
        elif len(models) == 1:
            is_batch = False
        else:
            ## No records to commit
            logger.debug("Module:{} No events to commit.".format(self.name))
            return
        
        logger.log(25,"Commiting {} events to S3.".format(len(models))) 
        
        if is_batch:
            my_uuid = uuid.uuid1()
            file_id = "batch"
        else:
            my_uuid = models[0].event_uuid
            file_id = "event"
        
        export_map = {      "uuid" : "event_uuid",
                            "source_uuid" : "source",
                            "type" : None,
                            "size":None,
                            "date":"datestamp",
                            "submit_date" : "submit_datestamp"
                        } 
        
        if s3file_name is None:
            if s3key_prefix is None:
                if self.s3key_prefix is not None:
                    s3key_prefix = self.s3key_prefix
                else:
                    s3key_prefix = "model_data/karl_%s_" % self.type
            if s3key_suffix is None:
                if self.s3key_suffix is not None:
                    s3key_suffix = self.s3key_suffix
                else:
                    s3key_suffix = ".txt"
            
            s3file_name = "{}_{}_{}_{}{}".format(s3key_prefix,file_id,
                                my_uuid,
                                now.strftime(pykarl.core.FILE_DATE_FORMAT),
                                s3key_suffix)
                
        ## Block to see if we're referencing our current models. If so
        ## copy their contents for our commit and flush the ivar
        was_local_reference = False
        with self.lock:
            if models is self.queued_models:
                was_local_reference = True
                local_models = models[:]
                self.queued_models = []
                if self.queued_model in local_models:
                    self.queued_model = None
            else:
                local_models = models
        
        try:
            events = map(lambda m: m.event,local_models)
            data = "\n".join(map(lambda m: m.export_as_redshift_json(
                                            export_map=export_map),models))
        
            uploaded_keys = []
            
            if self.publish_event:
                bucket = self.s3_bucket()
                
                my_key = boto.s3.key.Key(bucket)
                my_key.key = s3file_name
                my_key.set_contents_from_string(data)
                
                s3_path = "s3://%s/%s" % (self.s3_bucketname,my_key.key)
                
                uploaded_keys.append(my_key.key)
                
                logger.debug("Wrote S3 Import data to:'%s'" % s3_path)

            if self.publish_event_payload:
                try:
                    self.commit_event_payloads_to_s3(events,
                                            s3key_prefix=payload_s3key_prefix,
                                            s3key_suffix=payload_s3key_suffix,
                                            batch_uuid=my_uuid)
                except Exception as exp:
                    ## Rollback
                    if len(uploaded_keys) > 0:
                        logger.error("Failed to commit payload to S3 for batch:{} ({} events), rolling back. Error: {}".format(my_uuid,len(events),exp.message))
                        try:
                            bucket = self.s3_bucket()
                            bucket.delete_keys(uploaded_keys)
                        except Exception as exp2:
                            logger.error("Failed to rollback event publication to S3 for batch:{} after failure. Error: {}".format(my_uuid,exp2.message))
                    raise
        
            endtime = datetime.datetime.utcnow()
        
            for model in local_models:
                try:
                    if process_callbacks and model.process_callback is not None:
                        options = {"process_date" : starttime,
                                        "process_time" : endtime - starttime}
                        model.process_callback(model=model,
                                                        publisher=self,
                                                        target="s3",
                                                        options=options)
                except TypeError:
                    pass
                except KeyError:
                    pass
                except AttributeError as exp:
                    logger.error("Failed to report model to callback:{}. Error:{}".format(
                                                        `model.process_callback`,exp),
                                                    exc_info=True)
            
            logger.debug("Wrote S3 Import data to:'{}' ({} records)".format(
                                                                s3_path,
                                                                len(local_models)))
                                                                
        except Exception:
            ## Catch any errors, determine if we had previosuly flushed our
            ## queues, if so, repopulate them)
            if was_local_reference:
                with self.lock:
                    self.queued_models += local_models
            raise
            
    
    def commit_to_rds(self,model=None,table="event",
                                        device_table="device_instance"):
        """
        Method which will commit the provided event to RDS.
        
        :param model: The event to commit
        :type model: :py:class:`BaseModel` descendant
        :param table: The name of the table to publish
        :type table: str
        :param device_table: The name of the device table to publish
        :type device_table: str
        
        """
        
        if model is None:
            model = self.queued_model
        
        if model is None:
            raise AttributeError("Model is not specified, cannot commit to RDS")
        
        if table is None:
            raise AttributeError("rds_table is not specified, cannot commit to RDS")
        
        if device_table is None:
            raise AttributeError("rds_table is not specified, cannot commit to RDS")
        
        logger = logging.getLogger("commit_to_rds()")
        
        logger.debug("Commiting '%s' event to RDS table:%s for source:'%s'"
                                            % (model.type,table,model.source))
        
        rds = self.rds()
        
        """ 05/07/15: beauhunt@ This code has been implemented in base model.
        if device_table is not None:
            ## Attempt to update our device record
            
            query = "SELECT instance_id FROM %s WHERE uuid = $1" % device_table
            values = (model.source)
            
            logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
            result = rds.query(query,values).dictresult()
            
            if len(result) > 0:
                try:
                    device_id = result[0]["instance_id"]
                except KeyError:
                    logger.error("Failed to load device id for identifier:%s"
                                                                % model.source)
                    return
            else:
                device_id = rds.query("INSERT INTO %s (uuid, first_seen) "
                            "VALUES($1,$2) RETURNING instance_id" % device_table,(
                                                            model.source,
                                                            model.datestamp))
        """
    
        ## Update our logininfo table
        my_map = model.export_map
        
        import_keys = ()
        import_values = ()
        
        export_dict = model.to_dict(key_map=my_map)
        for key,value in export_dict.iteritems():
            if value is not None:
                import_keys += (key,)
                import_values += (value,)
                
        if len(import_keys) > 0:
            count = 1
            value_string = None
            while count <= len(import_keys):
                if value_string is None:
                    value_string = "$1"
                else:
                    value_string += ",$%s" % count
                count += 1
                
            query = ("INSERT INTO %s(%s) VALUES(%s)" % (table,
                                                        ", ".join(import_keys),
                                                        value_string))
                
            logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,import_values))
            rds.query(query,import_values)
        
    def commit_event_payloads_to_s3(self,events,s3key_prefix=None,
                                                        s3key_suffix=None,
                                                        batch_uuid=None):
        """
        Method to commit event payloads to S3, collating data based on
        datatype.
        """
        
        
        if s3key_prefix is None:
            s3key_prefix = self.payload_s3key_prefix
        
        if s3key_suffix is None:
            s3key_suffix = self.payload_s3key_suffix
        
        if batch_uuid is None:
            batch_uuid = uuid.uuid1()
        
        logger = logging.getLogger("commit_eventpayload_to_s3()")
                
        payload_data = {}
        
        for event in events:
            payload_data[event.uuid] = self.format_payload(event.payload)
        
        
        datatypes = ["int","str","bigstr"]
        
        uploaded_keys = []
        was_error = False
        
        for datatype in datatypes:
            data_entries = []
            for e_uuid,e_payload in payload_data.iteritems():
                if not datatype in e_payload:
                    continue
                
                for key,value in e_payload[datatype].iteritems():
                    entry_dict = { "event_uuid": e_uuid, "key":key, "value":value}
                    data_entries.append(entry_dict)
            
            if len(data_entries) == 0:
                ## If we have no entries for this datatype, continue to next
                continue
                 
            json_data = "\n".join(map(lambda e: json.dumps(e),data_entries))
            
            s3key = ("{}{}/karl_eventpayload_{}_{}{}".format(s3key_prefix,
                                    datatype,batch_uuid,
                                    event.date.strftime(pykarl.core.FILE_DATE_FORMAT),
                                    s3key_suffix))
            
            s3_path = "s3://{}/{}".format(self.s3_bucketname,s3key)
            
            try:
                s3 = self.s3()
                bucket = s3.get_bucket(self.s3_bucketname)
                
                my_key = boto.s3.key.Key(bucket)
                my_key.key = s3key
                
                my_key.set_contents_from_string(json_data)
                
                uploaded_keys.append(s3key)
                
            except Exception as exp:
                ## Rollback
                if len(uploaded_keys) > 0:
                    logger.error("Failed to commit payload_payload to S3 for event:%s, rolling back. Error: %s" % (event.uuid,exp.message))
                    try:
                        s3 = self.s3()
                        bucket = s3.get_bucket(self.s3_bucketname)
                        bucket.delete_keys(uploaded_keys)
                    except Exception as exp2:
                        logger.error("Failed to rollback event payload to S3 for event:%s after failure. Error: %s" % (event.uuid,exp2.message))
                raise
            
            logger.debug("Wrote S3 Import data to:'%s'" % s3_path)
            
        
    def format_payload(self,payload):
        """
        Method which returns a dictionary representing payload data, keyed
        by data type.
        """
        
        results = {}
        
        for key,value in payload.iteritems():
            try:
                data = self.format_payload_entry(key,value)
                for dict_type, dict_entry in data.iteritems():
                    if not dict_type in results.keys():
                        results[dict_type] = dict_entry
                    else:
                        results[dict_type].update(dict_entry)
            except (ValueError,OverflowError) as exp:
                logger = logging.getLogger(self.__class__.__name__)
                str_value = ""
                try:
                    str_value = "{}".format(value)
                except:
                    pass
                
                logger.error("Failed to convert value for key:{} value:{}. Error:{}".format(
                                                                    key,
                                                                    str_value,
                                                                    exp))
        #
        return results
    
    def format_payload_entry(self,key,value):
        """
        Method which returns a dictionary, keyed by data type,
        representing our payload data in a format to be published
        to our database.
        """
        
        results = {}
        
        type = self.datatype_for_value(value)
            
        if type == "dict":
            for dict_key, dict_value in value.iteritems():
                data = self.format_payload_entry(key="%s.%s" % (key,dict_key),
                                                            value=dict_value)
                for dict_type, dict_entry in data.iteritems():
                    if not dict_type in results.keys():
                        results[dict_type] = dict_entry
                    else:
                        results[dict_type].update(dict_entry)
        elif type == "list":
            count = 0
            for list_value in value:
                data = self.format_payload_entry(key="%s.%s" % (key,count),
                                                        value=list_value)
                count += 1
                for dict_type, dict_entry in data.iteritems():
                    if not dict_type in results.keys():
                        results[dict_type] = dict_entry
                    else:
                        results[dict_type].update(dict_entry)
        else:
            if type == "int":
                if not type in results:
                    results[type] = {}
                if value:
                    results[type][key] = int(value)
                else:
                    results[type][key] = 0
            elif type == "float":
                # We convert floats to ints here
                if not "int" in results:
                    results["int"] = {}
                if value:
                    results["int"][key] = int(float(value))
                else:
                    results["int"][key] = 0
            elif type == "long":
                if not "str" in results:
                    results["str"] = {}
                results["str"][key] = "{}".format(value)
            elif type == "other":
                if not "str" in results:
                    results["str"] = {}
                if value:
                    results["str"][key] = "{}".format(value)
                else:
                    results["str"][key] = value
            else:
                if not type in results:
                    results[type] = {}
                    
                results[type][key] = value
        
        return results
    
    def datatype_for_value(self,entry):
        """
        Method which returns the payload type for our entry. Possible values:
        
        * dict
        * list
        * int
        * long
        * float
        * str (0-255 chars)
        * bigstr (255-64000 chars)
        * toobigstr (> 64000 chars)
        * other
        
        """
        type = None
        
        if entry is None:
            type = "str"
        if isinstance(entry,dict):
            type = "dict"
        elif isinstance(entry,list):
            type = "list"
        elif isinstance(entry,long):
            type = "long"
        elif isinstance(entry,int):
            type="int"
        elif isinstance(entry,float):
            type="float"
        elif isinstance(entry,basestring):
            m = re.match("^[0-9]+$",entry)
            if m is not None:
                type = "int"
            else:
                try:
                    d = float(entry)
                    type = "float"
                except ValueError:
                    if len(entry) <= 255:
                        type = "str"
                    elif len(entry) <= 64000:
                        type = "bigstr"
                    else:
                        type = "toobigstr"
        else:
            type="other"
        #
        return type

class EventPayloadArchiver(BaseArchiver):
    """
    Class used to commit event payload data stored on S3 into our redshift 
    database
    """
    
    name = "EventPayloadArchiver"
    
    def import_s3files(self,s3key_prefix=None,s3key_suffix=None,
                                                table_name=None,
                                                bucket_name=None,
                                                max_count=None,
                                                delete=True):
        """
        Method to import a file from s3 into the
        Redshift database.
        
        :param str s3key_prefix: Prefix string to determine matches (if an s3
            object's key begins with this it's a match)
        :param str s3key_suffix: Suffix string to determine matches (if an s3
            object's key ends with this it's a match)
        :param str bucket_name: The name of the s3 bucket
        :param int max_count: The maximum number of records to return.
        :param str table_name: The name of the redshift table to import into
        :param bool delete: If true, we will delete files after upload (default:true)
        
        """
        
        logger = logging.getLogger(self.__class__.__name__)
    
        if not table_name:
            table_name = self.redshift_table
        
        if s3key_prefix is None:
            s3key_prefix = self.s3key_prefix
        
        if not table_name:
            raise AttributeError("No table name was provided, cannot import")
        
        ## Import typed payload data
        datatypes = ["int","str","bigstr"]
        
        for datatype in datatypes:
            logger.log(5,"Importing event {} payload history...".format(datatype))
            payload_table_name = "{}_{}".format(table_name,datatype)
            prefix = "{}{}/karl_eventpayload_".format(s3key_prefix,datatype)
            BaseArchiver.import_s3files(self,s3key_prefix=prefix,
                                        s3key_suffix=s3key_suffix,
                                        table_name=payload_table_name,
                                        bucket_name=bucket_name,
                                        max_count=max_count,
                                        delete=delete)
            
module_name = "eventmodule"  #: The name of our module, used for filtering operations           

event_handler = BaseEventHandler(name=module_name)
event_handler.action_map = {
                "default" : {"obj_class" : GenericEvent,
                                "pub_class" : EventPublisher,
                                "rds_table" : "event",
                                "archive_table" : "event",
                                "s3key_prefix" : "event/karl_event",
                                "archive_payload" : True,
                            },
                "__event_process_history__" : {
                        "archive_table" : "event_process_history",
                        "s3key_prefix" : "event_process_history/karl_event",
                    },
                "__event_payload__" : {
                        "archive_class" : EventPayloadArchiver,
                        "archive_table" : "event_payload",
                        "s3key_prefix" : "event_payload/",
                },
                ## Note: I am defining these here to prevent this generic module
                ## from archiving payloads for the following events (each of
                ## these archives their own data). 
                ## This should be revisited, as I should be able to define
                ## this behavior at the module level. 
                "__no_archived_payload__" : {
                        "obj_class" : GenericEvent,
                        "pub_class" : EventPublisher,
                        "rds_table" : "event",
                        "s3key_prefix" : "event/karl_event",
                        "archive_table" : "event",
                        "update_device" : False,
                        "archive_payload" : False,
                        "archive_class" : None,
                },
                "LocalPasswordRotation" : {
                        "obj_class" : GenericEvent,
                        "pub_class" : EventPublisher,
                        "rds_table" : "event",
                        "s3key_prefix" : "event/karl_event",
                        "archive_payload" : False,
                },
            }
            
event_handler.action_map["LocalPasswordEscrow"] = event_handler.action_map["LocalPasswordRotation"]
event_handler.action_map["NetworkChange"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["NetworkSessionStart"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["NetworkSessionEnd"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["HeartBeat"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["PluginLoadEvent"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["AuthEvent"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["LoginEvent"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["UpdateDiscovered"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["UpdateResolved"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["DownloadStarted"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["DownloadFinished"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["UpdateCached"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["InstallationStarted"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["InstallationFinished"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["ComplianceStatusDidChange"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["ModuleComplianceStatusDidChange"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["ComplianceModuleLoadEvent"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["ComplianceModuleUnloadEvent"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["RemediationStarted"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["RemediationFinished"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["EvaluationFinished"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["EcsiAgent"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["RemediationEvent"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["MaintenanceWindowEvaluationEvent"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["MaintenanceWindowRemediationEvent"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["MaintenanceWindowUINotificationEvent"] = event_handler.action_map["__no_archived_payload__"]
event_handler.action_map["MaintenanceWindowEvent"] = event_handler.action_map["__no_archived_payload__"]

## Completely ignore EvaluationStarted events for now.
event_handler.action_map["EvaluationStarted"] = {"publish" : False,}
