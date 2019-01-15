"""
**modules** - Package providing facilities for extending KARL processing capabilites.
   
:platform: RHEL5
:synopsis: Provides a collection of modules and subpackages which provide
            a variety of stream processing capabilities. Includes root classes 
            which facilitate KARL event processing, KARL data 
            modeling, and data publishing. All pykarl modules will be 
            structured off of root classes provided by this module.

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""
import pkgutil
import os
import socket
import logging
import json
import datetime
import boto.s3
    
import uuid
import threading


from acme import SerializedObject
import pykarl.modules
import pykarl.core

from pykarl.core import DATE_FORMAT, FILE_DATE_FORMAT

## Best effort imports
DB_AVAILABLE = False
try:
    import pg
    DB_AVAILABLE = True
except:
    pg = None
    pass

#MARK: Defaults
DEFAULT_S3PUBLISH_BATCH_SIZE=0          #: The number of records we will batch before publishing to S3
DEFAULT_S3IMPORT_MAXRECORDCOUNT=10000   #: The value used to represent the maximum number of S3 files that will be imported in a single run
DEFAULT_S3HISTORY_KEY_PREFIX="event_process_history/karl_event"
DEFAULT_S3HISTORY_KEY_SUFFIX=".txt"

#MARK: Classes
class BaseModel(SerializedObject):
    """
    Class representing a model constructed by a Kinesis stream. This class 
    is a basic model object and is responsible for data representation 
    of a submitted event. This class and decsendent classes should have 
    capabilities to process a :py:class:`pykarl.event.Event` object and appropriately 
    populate it's internal state based on data provided in the event payload. 

    
    :Example:
        
        >>> event = Event(type="MyTestEvent")
        >>> event.uuid = "E-UUID-XXX-XXX-YYY"
        >>> event.source = "S-UUID-XXX-XXX-XXX"
        >>> event.date = datetime.datetime.strptime("2014-02-01","%Y-%m-%d")
        >>> event.payload["key1"] = "value1"
        >>> event.payload["key2"] = "value2"
        >>> event.payload["key3"] = "value3"
        >>> 
        >>> m = BaseModel()
        >>> 
        >>> ## Set our import and export maps
        ... m.payload_map = {"key1": "a", "key2": "b", "key3": None}
        >>> m.export_map = {"event_uuid": None, "source_uuid": "source", 
        ...                                            "field_1": "a", 
        ...                                            "field_2": "b"}
        >>> 
        >>> ## Load our event
        ... m.load_event(event)
        >>> 
        >>> m.a
        'value1'
        >>> m.b
        'value2'
        >>> m.key3
        'value3'
        >>>
        >>>  ## Reference non-existent key
        ... m.key1
        Traceback (most recent call last):
          File "<stdin>", line 2, in <module>
        AttributeError: 'BaseModel' object has no attribute 'key1'
        >>> 
        >>> print m.export_as_redshift_json()
        {
             "source_uuid": "UUID-XXX-XXX-XXX", 
             "field_2": "value2", 
             "event_uuid": "E-UUID-XXX-XXX-YYY", 
             "field_1": "value1"
        }
        >>> ## Export with custom mapping
        ... print m.export_as_redshift_json(export_map={"key1": "a","b": None})
        {
             "key1": "value1", 
             "b": "value2"
        }
        >>>     

    """

    source = None               #: The source/device identifier
    type = "BaseModelObject"    #: The type of record
    date = None                 #: The date of our record
    submit_date = None          #: The date our record was submitted
    event_uuid = None           #: The identifier of the event
    event = None                #: The source event
    payload_map = None          #: Key=>value mapping used during event import
    export_map = None           #: Key=>value mapping used during export
    rds_export_map = None       #: Key=>value mapping used during RDS export (defaults to export_map)
    redshift_export_map = None  #: Key=>value mapping used during Redshift export (defaults to export_map)
    
    process_callback = None     #: A reference to a callback method to be invoked
                                #: whenever this model is processed by a subsystem
                                #: The referenced method should maintain the signature:
                                #: def model_did_publish(model,publisher,target,options=None):
    
    _size = None
    
    @property
    def size(self): 
        """
        Property which returns the size of our event.
        """
        if self._size is None:
            if self.event is not None:
                self._size = self.event.get_size()
                
        return self._size
    
    @size.setter
    def size(self,value):
        """
        Setter for our size propert
        """
        self._size = value
                
    @property
    def datestamp(self):
        """
        Property which returns a datestamp formatted for
        SQL use.
        """
        
        if self.date:
            return self.date.strftime(pykarl.core.DATE_FORMAT)
        else:
            return None
            
    @property
    def submit_datestamp(self):
        """Property which returns a datestamp representing submisison date,
        formatted for SQL use."""
        
        if self.submit_date:
            return self.submit_date.strftime(pykarl.core.DATE_FORMAT)
        else:
            return None
    
    @property
    def payload_map(self):
        """
        Shim property to map acme.SerializedObject.key_map to BaseModel.payload_map.
        """
        return self.key_map
        
    @payload_map.setter
    def payload_map(self,value):
        """
        Shim property to map acme.SerializedObject.key_map to BaseModel.payload_map.
        """
        self.key_map = value
    
    def __init__(self,event=None,data=None,payload_map=None,
                                                    export_map=None,
                                                    rds_export_map=None,
                                                    redshift_export_map=None,
                                                    require_validation=None,
                                                    *args,**kwargs):
        """
        Our constructor.
        
        :param event: Our event to process.
        :type event: :py:class:`pykarl.event.Event`
        :param payload_map: Key=>Property map for data imports
        :type payload_map: dict(string,string)
        :param export_map: Key=>Property map for data exports
        :type export_map: dict(string,string)
        :param rds_export_map: RDS specific export mappings
        :type rds_export_map: dict(string,string)
        :param redshift_export_map: Redshift specific export mappings
        :type redshift_export_map: dict(string,string)
        :param require_validation: Whether or not this model needs to be loaded from a validated source
        :type: (bool)
        """
        
        if export_map is None:
            self.export_map = {
                            "source" : None,
                            "type" : None,
                            "datestamp" : None,
                        }
        else:
            self.export_map = export_map
            
        if rds_export_map is not None:
            self.rds_export_map = rds_export_map
            
        if redshift_export_map is not None:
            self.redshift_export_map = redshift_export_map
        
        if payload_map is None:
            self.payload_map = {}
        else:
            self.payload_map = payload_map
            
        kwargs["key_map"] = self.payload_map
            
        self.date = datetime.datetime.utcnow()
        self.submit_date = datetime.datetime.utcnow()
        
        self.require_validation = require_validation
        
        if event is not None:
            self.load_event(event)
        
        if data is not None:
            self.load_dict(data)
        
        SerializedObject.__init__(self,*args,**kwargs)

    def load_event(self,event,payload_map=None):
        """
        Method to load data from a karl event.
        
        :param pykarl.event.Event event: Our event to load
        
        :param dict payload_map: Key=>Value mappings to use for loading
        """
        
        if payload_map is None:
            payload_map = self.payload_map
        
        self.source = event.source
        self.event_uuid = event.uuid
        self.date = event.date
        self.submit_date = event.submit_date
        self.type = event.type
        
        self.event = event
        
        self.load_dict(data=event.payload,key_map=payload_map)
        
    
    def to_dict(self,key_map=None,output_null=True):
        """
        Method to export our record in key=>value dictionary form,
        as prescribed by our key_map. If no key_map is provided, ond
        our object has a mapping defined at self.export_map, we will use that. 
        Otherwise we will fall back to self.payload_map
        
        :param dict key_map: Dictionary of key->attribute mappings
                which represent local properties used in our output
        :param bool output_null: If False, we will omit attributes with value
            of 'None' (default: True)
        """
        
        my_map = None
        
        if key_map is not None:
            my_map = key_map
        elif self.export_map:
            my_map = self.export_map
            
        return SerializedObject.to_dict(self,key_map=my_map,
                                                    output_null=output_null)

    def to_event(self):
        """
        Method which will output a KARL Event based on our object.
        """
        
        logger = logging.getLogger("to_event()")
        
        if self.event:
            return self.event
        
        event = pykarl.event.Event()
        event.source = self.source
        event.date = self.date
        event.submit_date = self.submit_date
        event.type = self.type
        
        event.payload = self.to_dict(key_map=self.payload_map)
        
        return event
            
    def export_as_csv(self,delimiter="|",export_keys=None,export_map=None):
        """
        Method to export our record as a delimited text record.
        This will use the property `self.export_map` to determine
        data members to include in this output.
        
        :param str delimeter: The delimeter to use in our output
        :param list export_keys: The keys used for export. As export_map
            is a dict, it does not supported ordered keys. export_keys can
            be provided, which ensures proper ordering of csv values.
        :param dict export_map: Dictionary of key->attribute mappings
                which represent local properties used in our output.
        
        """
        
        csv = None
        
        export_dict = self.to_dict(key_map=export_map)
                
        for key in export_keys:
            if key in export_dict.keys():
                value = export_dict[key]                
            if value is None:
                value = ""
                
            if not csv:
                csv = value
            else:
                csv = "%s%s%s" % (csv,delimiter,value)
            
        return csv
    
    def export_as_redshift_json(self,export_map=None):
        """
        Method to export our record as a json file meant for consumption
        into Redshift.
        This will use the property `self.export_map` to determine
        data members to include in this output.
        
        :param str delimeter: The delimeter to use in our output
        :param list export_map: Dictionary of key->attribute mappings
                which represent local properties used in our output
        
        """

        json_dict = {}
        
        if export_map is None:
            if self.redshift_export_map is not None:
                export_map = self.redshift_export_map
            else:
                export_map = self.export_map
        
        export_dict = self.to_dict(key_map=export_map)
        
        for key,value in export_dict.iteritems():
                if value is not None:
                    json_dict[key] = value
        
        return json.dumps(json_dict,indent=5)

    def load_data(self,data):
        """Method to load data from a record.
        
        .. warning:
            This is a shim method to preserve interface compatability. Moving
            forward you should use self.load_dict()
        """
        
        return self.load_dict(data=data)
        
class BasePublisher(pykarl.core.KARL,SerializedObject):
    """
    Class used to publish module data to various data stores such as RDS and S3.
    This class will be commonly overridden to provide custom processing behavior.
    """
    
    name = None                #: The name of our publisher
    model = None                #: The :py:class:`BaseModel` descendent that we will be publishing
    targets = []                #: List of targets that we should publish to. 
    
    batch_deletion = None     #: Flag used by descendents to specify
    
    can_target_rds = None      #: Flag used by descendents to specify 
                                #: whether they publish to RDS
    
    can_target_s3 = None       #: Flag used by descendents to specify
                                #: whether they publish to S3
    can_target_dynamo = None

    updates_rds_device_table = True         #: Flag denoting whether we attempt to
                                            #: update a device record in RDS.
                                            
   
    rds_action = None        #: Flag to define whether to update or insert into RDS table. 
    rds_table = None         #: The name of the RDS table to update
    rds_key = None           #: The name of the RDS column key to use for updates 
    rds_keys = None          #: The name of the RDS column keys to use for updates
    rds_update_null = None   #: If set to true, we will overwrite RDS field values with empty data, if provided
    
    rds_device_table = "device_instance"    #: Name of device table that 
                                            #: we key to.
    
    rds_device_table_map = {"uuid" : "source",
                            "hostname" : None,
                            "hardware_uuid" : None,
                            "platform" : None,
                            "username" : None,
                            "last_seen" : "datestamp",
			    "platform_version" : None,
                            }   #: Mapping which denotes what attributes to
                                #: look for when updating our device table 
    
    s3batch_size = 0            #: The size of our s3 commit batch. A size of 0 indicates that all s3 commits will be batched and manually committed
    s3key_prefix = None         #: The s3key prefix to use for posting event data to S3
    s3key_suffix = None         #: The s3key suffix appended to keys when posting event to S3

    dynamo_table = None

    batch_uuid = None           #: Our current batch uuid
    
    queued_model = None         #: The data model object to use for publishing. This will
                                #: be a descendant of :py:class:`BaseModel`. 
                                #: The object stored in this variable will be
                                #: flushed on successful commit.
    queued_models = None        #: A list of models used for batching actions 
                                #: This list will be flushed on successful commit
                                
    lock = None                 #: :py:class:`threading.RLock` object used for 
                                #: managing access to model lists
    
    
    @property
    def derived_name(self):
        """
        Property to return the derived name of our publisher. If we are 
        explicitely defined, we return that, otherwise, we derive it.
        """
        
        if self.name is not None:
            return self.name
            
        derived_name = "BasePublisher"
                
        if self.__class__.__name__ != "BasePublisher":
            derived_name = self.__class__.__name__
        elif self.rds_table:
            derived_name = "publisher_{}".format(self.rds_table)
        elif self.s3key_prefix:
            derived_name = "s3publisher_{}".format(self.s3key_prefix)
        elif self.dynamo_table:
            derived_name = "dynamo_publisher_{}".format(self.dynamo_table)
        
        return derived_name
        
    
    
    def __init__(self,name=None,model=None,models=None,s3batch_size=None,
                                                    karl=None,targets=None):
        """
        Primary constructor.
        """

        if name is not None:
            self.name = name
        
        if model is not None:
            self.queued_model = model

        if models is not None:
            self.queued_models = models
        
        if self.queued_models is None:
            self.queued_models = []
                
        if s3batch_size is None:
            self.s3batch_size = DEFAULT_S3PUBLISH_BATCH_SIZE
        else:
            self.s3batch_size = s3batch_size
            
        self.batch_uuid = uuid.uuid1()
            
        self.s3key_prefix = "model_data/karl_%s" % self.__class__.__name__
        self.s3key_suffix = ".txt"
        
        if targets is not None:
            self.targets = targets
        else:
            if self.can_target_rds:
                self.targets.append("rds")
            if self.can_target_s3:
                self.targets.append("s3")
            if self.can_target_dynamo:
                self.targets.append("dynamo")
    
        self.lock = threading.RLock()
        
        key_map = { "name" : None,
                    "s3batch_size" : None,
                    "s3key_prefix" : None,
                    "s3key_suffix" : None,
                    "rds_table" : None,
                    "rds_key" : None,
                    "rds_keys": None,
                    "dynamo_table" : None,
                    "batch_deletion": None,
                    "rds_update_null" : None,
                    "updates_rds_device_table" : None,
                    "rds_action" : None,
                    "can_target_rds" : None,
                    "can_target_s3" : None,
                    "can_target_dynamo" : None,
                    "targets" : None,
                }
                
        SerializedObject.__init__(self,key_map=key_map)
        pykarl.core.KARL.__init__(self,karl=karl)
    
    def should_publish_to_target(self,target):
        """
        Method which returns whether or not we should publish to the provided
        target.
        
        :returns true: If the provided target is in the publishers targeting list
        
        """
        
        result = False
        
        if target.lower() == "s3" and self.can_target_s3:
            result = True
        elif target.lower() == "rds" and self.can_target_rds:
            result = True
        elif target.lower() == 'dynamo' and self.can_target_dynamo:
            result = True

        if result:
            if not target.lower() in map(lambda x: x.lower(),self.targets):
                result = False
        
        return result
    
    def process_model(self,model=None,targets=None):
        """
        Method which will process our event, wherever that may lead. By default
        we attempt to publish to rds and s3.
        """
        
        logger = logging.getLogger("{}:process_model()".format(self.name))
        
        if model is None:
            model = self.queued_model
            
        if targets is None:
            my_targets = map(lambda t: t.lower(),self.targets)
        else:
            my_targets = map(lambda t: t.lower(),targets)
        
        if "rds" in my_targets and self.should_publish_to_target("rds"):
            starttime = datetime.datetime.utcnow()
            status = None
            try:
                logger.log(15,"Committing event: {} ({}) to RDS".format(
                                                model.event_uuid,
                                                    model.type))
                if self.updates_rds_device_table:
                    try:
                        logger.debug("Updating device in table:{} for event:{} ({})".format(self.rds_device_table,
                                                    model.event_uuid,
                                                    model.type))
                        r = self.update_rds_entry(model=model,
                                            table=self.rds_device_table,
                                            key_name="uuid",
                                            key_map=self.rds_device_table_map)
                        if not r:
                            logger.debug("Creating device entry in table:{} for event:{} ({})".format(self.rds_device_table,
                                                    model.event_uuid,
                                                    model.type))

                            r = self.create_rds_entry(model=model,
                                                table=self.rds_device_table,
                                                key_map=self.rds_device_table_map)
                    except pg.ProgrammingError as exp:
                        logger.error("Failed to update/create RDS device entry in table:{t} for event:{e} ({s})".format(
                                        t=self.rds_device_table,
                                        e=model.event_uuid,
                                        s=model.type),exc_info=True)
                
                ## If we are using BasePublisher, only call commit_to_rds if
                ## we have an established rds_table.
                if (self.__class__.__name__ != "BasePublisher" or 
                                                self.rds_table is not None):
                    self.commit_to_rds(model=model)
                status = "Success"
                
            except Exception as exp:
                logger.error("Failed to commit event to RDS: %s" % exp,exc_info=True)
                status = "Error"

            endtime = datetime.datetime.utcnow()
            
            try:
                if model.process_callback is not None:
                    options = {"process_date" : starttime,
                                "process_time" : endtime - starttime,
                                "status" : status}
                    model.process_callback(model=model,
                                                publisher=self,
                                                target="rds",
                                                options=options)
            except (AttributeError,KeyError):
                pass
            except TypeError as exp:
                logger.error("Failed to report model to callback:{}. Error:{}".format(
                                                    `model.process_callback`,exp),
                                                exc_info=True)

        if "s3" in my_targets and self.should_publish_to_target("s3"):
            if self.s3batch_size == 0 or self.s3batch_size > 1:
                models = self.queued_models
                if len(models) < (self.s3batch_size - 1) or self.s3batch_size == 0:
                    logger.debug("Adding model for batching to target: 's3' (BatchID:{})".format(self.batch_uuid))
                    models.append(model)
                else:
                    models.append(model)
                    logger.log(15,"Committing batch to S3 ({} records, BatchID:{})".format(self.s3batch_size,
                        self.batch_uuid))
                    self.commit_to_s3()
            elif self.s3batch_size == 1:
                try:
                    logger.log(15,"Committing event to S3")
                    self.commit_to_s3(model=model)
                    try:
                        if model is self.queued_model:
                            self.queued_model = None
                    
                    except (AttributeError,KeyError):
                        pass
                except Exception as exp:
                    logger.error("Failed to commit event to S3: %s" % exp,exc_info=True)

        if "dynamo" in my_targets and self.should_publish_to_target("dynamo"):
            logger.debug("Committing event: {} ({}) to Dynamo".format(model.event_uuid, model.type))

            starttime = datetime.datetime.utcnow()
            status = None

            try:
                self.commit_to_dynamo(model=model, table=self.dynamo_table)
                status = "Success"
            except Exception as exp:
                logger.error("Failed to commit event: {} ({}) to Dynamo. {}".format(model.event_uuid, model.type, exp))
                status = "Error"

            endtime = datetime.datetime.utcnow()
            
            try:
                logger.debug("Calling callback event for {} Dynamo".format(model.event_uuid))
                if model.process_callback is not None:
                    options = {"process_date" : starttime,
                                "process_time" : endtime - starttime,
                                "status" : status}
                    model.process_callback(model=model,
                                                publisher=self,
                                                target="dynamo",
                                                options=options)
            except (AttributeError,KeyError):
                pass
            except TypeError as exp:
                logger.error("Failed to report model to callback:{}. Error:{}".format(
                                                    `model.process_callback`,exp),
                                                exc_info=True)

    def update_rds_entry(self,model=None,key_name=None,key_names=None,table=None,key_map=None):
        """
        Method which will update an entry in RDS based on the provided table
        and update map
        
        :param model: The event to commit
        :type model: :py:class:`BaseModel` descendant
        :param table: The name of the table to publish to
        :type table: string
        :param str key_name: The name of the key used to determine the record to update.
        :param key_map: Dictionary that maps db field names to properties
        :type key_map: Dictionary<string,string>
        
        :returns True: If entry was updated
        
        """
        
        logger = logging.getLogger()
        
        if model is None:
            model = self.queued_model
    
        if model is None:
            raise AttributeError("Model is not specified, cannot commit to RDS")
        
        if key_name is None and key_names is None:
            if self.rds_key is not None:
                key_name = self.rds_key
            else:
                raise AttributeError("Key is not specified, cannot commit to RDS")  
        
        if key_names is None:
            if self.rds_keys:
                key_names = self.rds_keys
            else:
                key_names = []
            
        if key_name is not None and not key_name in key_names:
            key_names.append(key_name)
        
        if table is None:
            if self.rds_table is not None:
                table = self.rds_table
            else:
                raise AttributeError("rds_table is not specified, cannot commit to RDS")
        
        if key_map is None:
            if model.rds_export_map is not None:
                key_map = model.rds_export_map
            else:
                key_map = model.export_map

        the_keys = ()
        the_values = ()
        result = False
        
        export_dict = model.to_dict(key_map=key_map)
        
        ## If our update key is not in our export_dict, bail
        for key_name in key_names:
            if not key_name in export_dict.keys() or export_dict[key_name] is None:
                logger.warning("Cannot update RDS entry: invalid key:{} was provided".format(key_name))
                return False
        
        ## Attempt to update only populated values
        for k,v in export_dict.iteritems():
            if (self.rds_update_null or v is not None) and k not in key_names:
                the_keys += (k,)
                the_values += (v,)
        
        ## If we have no keys, bail
        if len(the_keys) == 0:
            return result
            
        count = 1
        value_string = None
        for k in the_keys:
            if value_string is None:
                value_string = "SET {} = $1".format(k)
            else:
                value_string += ", {} = ${}".format(k,count)
            count += 1
            
        where_string = ""
        for key_name in key_names:
            if where_string:
                where_string += " AND "
            where_string += "{k} = ${c}".format(k=key_name,c=count)
            the_values += (export_dict[key_name],)
            count += 1
        
        query = "UPDATE {t} {str} WHERE {w}".format(t=table,
                                                    str=value_string,
                                                    w=where_string)
                                                    
        logger.log(2,"Running Update Query:\"\"\"{}\"\"\", {}".format(
                                                    query,the_values))
        rds = self.rds()
        update_count = int(rds.query(query,the_values))
        if update_count > 0:
            logger.debug("Updated RDS entry in table:{t} for event:{e} ({s})".format(
                                                    t=table,
                                                    e=model.event_uuid,
                                                    s=model.type))
            result = True
                
        
        return result

    def remove_rds_entry(self,model=None,key_name=None,key_names=None,table=None,key_map=None,batch_deletion=None):
        """
        Method which will remove an entry in RDS based on the provided table
        
        :param model: The event to commit
        :type model: :py:class:`BaseModel` descendant
        :param table: The name of the table to publish to
        :type table: string
        :param str key_name: The name of the key used to determine the record to remove.
        :param key_map: Dictionary that maps db field names to properties
        :type key_map: Dictionary<string,string>
        
        :returns num of removed entry: If entry was removed 
        
        """
        
        logger = logging.getLogger()
        
        if model is None:
            model = self.queued_model
    
        if model is None:
            raise AttributeError("Model is not specified, cannot commit to RDS")
        
        if key_name is None and key_names is None:
            if self.rds_key is not None:
                key_name = self.rds_key
            else:
                raise AttributeError("Key is not specified, cannot commit to RDS")  
        
        if key_names is None:
            if self.rds_keys:
                key_names = self.rds_keys
            else:
                key_names = []
            
        if key_name is not None and not key_name in key_names:
            key_names.append(key_name)

        if batch_deletion is None:
           if self.batch_deletion is not None:
               batch_deletion = self.batch_deletion
        
        if table is None:
            if self.rds_table is not None:
                table = self.rds_table
            else:
                raise AttributeError("rds_table is not specified, cannot commit to RDS")
        
        if key_map is None:
            if model.rds_export_map is not None:
                key_map = model.rds_export_map
            else:
                key_map = model.export_map

        the_values = ()
        result = 0 
        
        export_dict = model.to_dict(key_map=key_map)
        
        ## If our remove key is not in our export_dict, bail
        for key_name in key_names:
            if not key_name in export_dict.keys() or export_dict[key_name] is None:
                logger.debug(export_dict)
                logger.warning("Cannot remove RDS entry: invalid key:{} was provided".format(key_name))
                return result 

        count = 1
        where_string = ""
        for key_name in key_names:
            if where_string:
                where_string += " AND "
            where_string += "{k} = ${c}".format(k=key_name,c=count)
            the_values += (export_dict[key_name],)
            count += 1
        
        query = "DELETE FROM {t} WHERE {w}".format(t=table,
                                                    w=where_string)
        select_query = "SELECT * FROM {t} WHERE {w}".format(t=table,
                                                             w=where_string)                                            
        logger.log(2,"Checking if multiple records are involved")
        rds = self.rds()
        r = rds.query(select_query,the_values)
        if not r.dictresult():
           logger.warning("cannot remove RDS entry: no record found with Query : {s},({v})  ".format(s=select_query,v=the_values))
           return result
        pre_remove_count = len(r.dictresult())
        if  pre_remove_count>1 and not batch_deletion:
            logger.warning("Cannot remove RDS entry: multiple records will be invovled in deletion. Please set batch_deletion to True if you mean to delete multiple records")
            return result  

        logger.log(2,"Running Delete Query:\"\"\"{}\"\"\", {}".format(
                                                    query,the_values))
        r = rds.query(query,the_values)
        remove_count = int(r)
        if remove_count > 0:
            logger.debug("Deleted {n} RDS entry in table:{t} for event:{e} ({s})".format(n=remove_count,
                                                    t=table,
                                                    e=model.event_uuid,
                                                    s=model.type))
        result = max(result,remove_count)
        return result
        
    def create_rds_entry(self,model=None,table=None,key_map=None):
        """
        Method which will create an entry in RDS based on the provided table
        and update map
        
        :param model: The event to commit
        :type model: :py:class:`BaseModel` descendant
        :param table: The name of the table to publish to
        :type table: string
        :param key_map: Dictionary that maps db field names to properties
        :type key_map: Dictionary<string,string>
        
        :returns True: If entry was created
        
        """
        
        logger = logging.getLogger()
                        
        if model is None:
            model = self.queued_model
    
        if model is None:
            raise AttributeError("Model is not specified, cannot commit to RDS")
           
        if table is None:
            if self.rds_table is not None:
                table = self.rds_table
            else:
                raise AttributeError("rds_table is not specified, cannot commit to RDS")
        
        if key_map is None:
            if model.rds_export_map is not None:
                key_map = model.rds_export_map
            else:
                key_map = model.export_map
                
        import_keys = ()
        import_values = ()
        
        result = False
        
        export_dict = model.to_dict(key_map=key_map)
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
                
            logger.log(5,"Running Insert Query:\"\"\"{}\"\"\", {}".format(query,import_values))
            rds = self.rds()
            r = rds.query(query,import_values)
            if r > 0:
                logger.debug("Created RDS entry in table:{t} for event:{e} ({s})".format(
                                                        t=table,
                                                        e=model.event_uuid,
                                                        s=model.type))
                result = True
        
        return result
                
    def commit_to_rds(self,model=None,table=None,key_name=None,key_names=None,
                                                export_map=None,action=None):

        """
        Method which will commit the provided event to RDS.
        
        :param model: The event to commit
        :type model: :py:class:`BaseModel` descendant
        :param table: The name of the table to publish to
        :type table: string
        :param str key_name: The name of the key used to determine the record to update.
        :param update_map: Dictionary that maps db field names to properties
        :type update_map: Dictionary<string,string>
                        
        """
        
        logger = logging.getLogger("commit_to_rds()")
        
        if action is None:
            action = self.rds_action

        if action and action.lower() == "none":
            logger.debug("RDS Action is {}, exiting...".format(action))
            return
        
        if model is None:
            model = self.queued_model
    
        if model is None:
            raise AttributeError("Model is not specified, cannot commit to RDS")
        
        if table is None:
            if self.rds_table is not None:
                table = self.rds_table
            elif action and action.lower() == "none":
                return
            else:
                raise AttributeError("rds_table is not specified, cannot commit to RDS")
        
        
        if action and action.lower() == "insert":
            pass
        else:
            if key_name is None and key_names is None:
                if self.rds_key is not None:
                    key_name = self.rds_key
                elif self.rds_key is None and self.rds_keys is not None:
                    key_names = self.rds_keys
                else:
                    raise AttributeError("Key is not specified, cannot commit to RDS")
        
        logger.debug("Commiting '%s' event to RDS table:'%s' for source:'%s'"
                                            % (model.type,table,model.source))
        
        rds = self.rds()
        
        if action and action.lower() == "insert":
            r = self.create_rds_entry(model=model,table=table,key_map=export_map)
        elif action and action.lower() == "remove":
            r = self.remove_rds_entry(model=model,key_name=key_name,
                                            key_names=key_names,table=table,
                                            key_map=export_map)
        else:
            ## Update our own record
            r = self.update_rds_entry(model=model,key_name=key_name,
                                            key_names=key_names,table=table,
                                            key_map=export_map)
            if not r:
                logger.debug("No existing RDS record could be found, creating...")
                r = self.create_rds_entry(model=model,table=table,key_map=export_map)
    
    def commit_to_s3(self,model=None,models=None,
                                            export_map=None,
                                            s3key_prefix=None,
                                            s3key_suffix=None,
                                            s3file_name=None,
                                            process_callbacks=None):
        """
        Method which commits data to S3.
        
        :param model: Model to commit
        :type model: :py:class:`BaseModel` descendent
        :param models: List of models to commit. If ommitted we will reference self.queued_models
        :type models: List of :py:class:`BaseModel` descendents
        :param karl: The KARL object used for resource access
        :type karl: :py:class:`pykarl.core.KARL` object
        :param bool process_callbacks: If set to False, we will not make
                            any callbacks on processed modules. (Default
                            behavior is True)
                            
        :raises EmptyQueueError: if no model data is provided or queued.
        
        """
        
        logger = logging.getLogger("{}:commit_to_s3()".format(
                                                self.__class__.__name__))
        
        starttime = datetime.datetime.utcnow()
        
        if model is None:
            model = self.queued_model
        
        if models is None:
            models = self.queued_models
        
        if model is None and models is None or len(models) == 0:
            raise EmptyQueueError("Model(s) not specified nor queud, cannot commit to s3")
        elif models is None:
            models = []
            
        if model is not None:
            with self.lock:
                models.append(model)
                
        if process_callbacks is None:
            process_callbacks = True
        
        now = starttime
        
        is_batch = len(models) > 1
        
        if len(models) == 0:
            logger.debug("Publisher:{} No events to commit to s3.".format(
                                                            self.derived_name))
            return
            
        if is_batch:
            my_uuid = self.batch_uuid
            file_id = "batch"
        else:
            my_uuid = models[0].event_uuid
            file_id = "event"
        
        logger.log(25,"Commiting {} events to S3.".format(len(models)))        
        
        if s3file_name is None:
            if s3key_prefix is None:
                if self.s3key_prefix is not None:
                    s3key_prefix = self.s3key_prefix
                else:
                    s3key_prefix = "model_data/karl_%s" % self.type
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
        local_models = None
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
            data = "\n".join(map(lambda m:m.export_as_redshift_json(
                                        export_map=export_map),local_models))
            
            bucket = self.s3_bucket()
            
            my_key = boto.s3.key.Key(bucket)
            my_key.key = s3file_name
            my_key.set_contents_from_string(data,encrypt_key=True)
            
            s3_path = "s3://%s/%s" % (self.s3_bucketname,my_key.key)
        
        except Exception:
            ## If we failed the transfer, restore our queues
            if was_local_reference:
                with self.lock:
                    self.queued_models += local_models
            raise
        
        endtime = datetime.datetime.utcnow()
        
        ## If this was a batch, reset our batch id
        if is_batch:
            new_batch_uuid = uuid.uuid1()
            logger.debug("Finished committing records to S3 new batch ID:{} (previous:{}) ".format(
                                                            new_batch_uuid,
                                                            self.batch_uuid))
            self.batch_uuid = new_batch_uuid

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
            
        logger.debug("Wrote S3 Import data to:'{}' ({} commits)".format(
                                                            s3_path,
                                                            len(local_models)))

    def commit_to_dynamo(self, model=None, table=None):
        """
        commit data to dynamo db
        :param model: The event data to be committed
        :type model: base:BaseModel
        :param table: The dynamo table to commit to
        :type table: string
        :return:
        """

        if not model:
            raise Exception("model is not set!")

        if not table:
            raise Exception("dynamo table is not set!")

        item = model.to_dict(model.export_map)

        try:
            dynamo = self.ddb_resource()
            table = dynamo.Table(table)
            table.put_item(Item=item)
        except Exception as ex:
            detail = "Table: {}, Item: {}".format(table, item)
            s = "commit_to_dynamo() FAILED. Detail: {}. Exception: {}".format(detail, ex)
            raise Exception(s)

    def __eq__(self,other):
        """
        Equality checker for our publisher. To be considered equal,
        comparitors must be the same class, have the same mappings,
        rds info, and s3 info
        """
        
        attributes_to_check = ["name","targets","rds_table","rds_key",
                        "rds_device_table","rds_device_table_map",
                        "s3key_prefix","s3key_suffix", "dynamo_table"]
        
        if not self.__class__.__name__ == other.__class__.__name__:
            return False
        
        for attribute in attributes_to_check:
            my_value = getattr(self,attribute)
            their_value = getattr(other,attribute)
            if my_value != their_value:
                return False
        
        return True
            
    def __ne__(self,other):
        return not self.__eq__(other)

class BaseEventHandler(object):
    """
    Class which provides per-module event dispatching capabilities. Each module
    must define a var at the module scope named ''event_handler'', which
    will be an instantiated descendent of this object Each module will populate
    subject_areas and action_mappings, which will define which events are
    routed to our module. The event handler is responsible for defining which
    events a module can process as well as instantiating and configuring the
    necessary objects to process an event. The event handler is also responsible
    for tracking event process state and ensuring that duplicate events are
    not published twice.
    
    ..example:
        event_handler = BaseEventHandler()
        event_handler.subject_areas = ["MYSUBJECT"]
        
        event_handler.action_map = {"myevent" : {"obj_class":MyEvent,
                                            "pub_class": MyEventPublisher
                                            },
                }
                
    The example above would be defined at the bottom of a module file. This
    configuration would ensure that only events with subject area "MYSUBJECT"
    with type "myevent". To subscribe to all subject areas, simply leave
    it undefined. To subscribe to all types, define an action map using 
    key ''default'' (replacing ''myevent'' in the code above)
    
    """
    
    name = None             #: The name of our handler
    action_map = None       #: Mapping of event types to models, publisher, and archiver
    subject_areas = None    #: Subject areas that we handle
    
    events = None           #: Dictionary of queued events, keyed by event ID
    event_state_data = None #: Dictionary containing event history data, keyed by event ID
    
    targets = None          #: List of targets which we will dispatch
    
    publishers = None       #: List of publishers
    
    lock = threading.RLock() #: Our lock object to ensure consistency    
    
    def __init__(self,name=None,action_map=None,subject_areas=None,targets=None,
                                                            karl=None):
        """
        Constructor.
        
        :param action_map: Dictionary of dictionaries keyed by event types, 
                    each sub dict mapping ''obj'' to our object class and 
                    ''pub'' to our publisher class
        :type action_map: Dictionary of dictionaries.
        :param subject_areas: List of subject areas that we will handle
        :type subject_areas: List of strings
        :param targets: The publishing targets to process
        :type targets: (str) target name (i.e. 's3', 'rds')
        :param karl: Our karl server object.
        :type karl: :py:class:`pykarl.core.KARL`
        
        """
        
        if name is not None:
            self.name = name
        else:
            self.name = "eventhandler"
        
        self.karl = pykarl.core.KARL()
        
        if action_map is None:
            self.action_map = {}
        else:
            self.action_map = action_map
        
        if subject_areas is None:
            self.subject_areas = []
        else:
            self.subject_areas = subject_areas
        
        self.events = {}
        self.event_state_data = {}
        
        self.publishers = []
        
        if targets is None:
            self.targets = ["s3","rds","dynamo"]
        else:
            self.targets = targets
        
        if karl is not None:
            self.karl.load_karl_settings(karl)
        else:
            self.karl = pykarl.core.KARL()
    
    def handles_subject_areas(self,subject_areas,include_debug=False):
        """
        Method which interogates the provided module to determine if it 
        provides services for the provided subject areas.
        
        :param subject_areas: string list of subject areas we are interested in
        :type subject_areas: list of strings
        :praam include_debug: Flag that determines whether we include debug 
                    modules. By default these will not be included when loading 
                    modules unless "__debug__" is explicitely specified as a
                    subject_area when loading the module.
        
        :returns: True if our module provides for one or more of the subject_areas
        """
            
        if not self.subject_areas or len(self.subject_areas) == 0:
            return True
        
        try:
            for subject_area in self.subject_areas:
                if subject_area.lower() == "__global__":
                    return True
                elif subject_areas is None:
                    if subject_area.lower() == "__debug__" and include_debug:
                        return True
                else:
                    for test_subject_area in subject_areas:
                        if (include_debug and test_subject_area.lower() == "__debug__"):
                            return True
                        elif subject_area.lower() == test_subject_area.lower():
                            return True
                
        except AttributeError:
            pass
        
        return False
    
    def handles_eventtype(self, eventtype):
        """
        Method which interogates the provided module to determine if it 
        provides services for the provided eventtype (or provides a default
        handler).
        
        :param event: The event type that we are interested in
        :type event: list of strings
        
        :returns: True if our module provides for the requested eventtype
        """
        if not eventtype:
            return True
            
        found_default = False
        for mapped_event_type in self.action_map.keys():
            if eventtype.lower() == mapped_event_type.lower():
                publish_event = True
                try:
                    publish_event = self.action_map[mapped_event_type]["publish"]
                except KeyError:
                    pass
                if publish_event is None or publish_event:
                    return True
            elif mapped_event_type.lower() == "default":
                found_default = True
        
        if found_default:
            return True
        
        return False
    
    def event_is_processing(self,event):
        """
        Method which will determine if a particular event is currently being
        processed. That is to say, it is either actively publishing, awaiting
        publishing, or awaiting publishing of historic processing data.
        
        :param event: The event or event uuid to check
        :type event: (str) event uuid, or :py:class:`pykarl.event.Event`
        
        :returns: (bool) True if the event is currently being processed
        
        """
        
        result = False
        
        if isinstance(event,basestring):
            event_uuid = event
        else:
            event_uuid = event.uuid
            
        if not event_uuid in self.event_state_data.keys():
            result = False
        else:
            if self.has_unpublished_events(event=event_uuid):
                result = True
            elif self.has_unpublished_process_history(event=event_uuid):
                result = True
        
        return result
        
    def has_processed_event(self,event,target=None,data=None):
        """
        Method which returns whether or not this handler has 
        processed the provided event. If target is provided, we will check
        against that specific target.
        
        :param event: Our event to check.
        :type event: :py:class:`Event`
        :param target: The name of the target to check for
        :type target: string
        :param data: The data set to look at
        :type data: List of SQL named results to consult
        
        :returns: bool - True if the provided module has previously processed 
            the provided event and target (if specified)
        """
        
        result = False
        
        ## Todo: eventually need to do more effecient history caching system
        ## It's unclear at this point under which circumstances duplicates
        ## will be caught. Need more data on this
        if data is None:
            history = self.load_event_processing_history(event=event)
        else:
            history = data
        
        my_targets = self.targets[:]
        
        found_targets = []
        for entry in history:
            if entry["module"].lower() == self.name.lower():
                result = True
                break
            elif target is not None:
                my_target = target.lower()
                target_name = "{}.{}".format(self.name.lower(),my_target)
                if entry["module"].lower() == target_name:
                    result = True
                    break
            elif target is None:
                for my_target in my_targets:
                    my_target = my_target.lower()
                    target_name = "{}.{}".format(self.name.lower(),my_target)
                    if entry["module"].lower() == target_name and my_target not in found_targets:
                        found_targets.append(my_target)
                        break
        
        if not result and target is None:
            if len(found_targets) == len(my_targets) and len(found_targets) > 0:
                result = True
        
        return result
        
    def load_event_processing_history(self,event):
        """
        Method which loads processing history for the event.
        """
    
        ## Todo: eventually need to do more efficient caching system
        ## It's unclear at this point under which circumstances duplicates
        ## will occur. Need more data on this
    
        logger = logging.getLogger(self.name)
        rds = self.karl.rds()
        try:
            d = rds.query("SELECT * FROM event_process_history WHERE event_process_history.event_uuid = $1",event.uuid)
        except Exception as exp:
            logger.error("Failed to load event processing history from RDS. Error: {error}".format(error=exp),
                                                                    exc_info=1)
            return None
        
        return d.dictresult()
      
    def commit_all_events(self,report_history=True):
        """
        Method that will immediately publish any queued events, and optionally
        commit process_history as well.
        """
        
        logger = logging.getLogger(self.name)
                
        ## Commit any batched s3 submissions
        for publisher in self.publishers:
            try:
                count = len(publisher.queued_models)
                if publisher.queued_model:
                    count += 1
                
                if count > 0:
                    logger.debug("Instructing publisher:{} to commit all ({}) events to S3...".format(
                                                        publisher.derived_name,
                                                        count))
                    publisher.commit_to_s3()
                else:
                    logger.debug("Skipping commit for publisher:{}, no models queued...".format(
                                                        publisher.derived_name))
            except Exception as exp:
                logger.error("Publisher:{} failed to commit events to S3. Error: {}".format(
                                        publisher.derived_name,exp),exc_info=1)
            
        if report_history:
            ## Commit our process history
            self.commit_process_history_to_s3()
    
    def commit_process_history_to_rds_for_events(self,events,target=None):
        """
        Method which commits record processing history for a series of
        events.
        """
        
        ## Build our QUERY
        query = "INSERT INTO event_process_history(event_uuid, module, process_date, process_time) VALUES "
        values = ()
        
        current_row_count = 0
        
        query_substring = None
        for event in events:
            uuid = event.uuid
            if target:
                module = "{}.{}".format(self.name,target)
            else:
                module = self.name
            
            try:
                process_date = event.process_date.strftime(DATE_FORMAT)
            except Exception:
                process_date = datetime.datetime.utcnow().strftime(DATE_FORMAT)
            
            try:
                process_time = int(event.process_time)    
            except Exception:
                process_time = None
            
            start_index = (current_row_count * 4) + 1
            
            my_substring = None
            for i in range(start_index,start_index + 4):
                if my_substring is not None:
                    my_substring += ","
                else:
                    my_substring = ""
                
                my_substring += "${}".format(i)
            
            if query_substring is None:
                query_substring = "({})".format(my_substring)
            else:
                query_substring += ",({})".format(my_substring)
                        
            values += (uuid,module,process_date,process_time)
            
            current_row_count += 1
            
        query += "{}".format(query_substring)
        
        return query,values
    
    def commit_process_history_to_s3(self,targets=None,module_name=None,
                                s3key_prefix=None,
                                s3key_suffix=None):
        """
        Method which commits our process history to S3 for any outstanding
        events (which will subsequently be consumed into Redshift). 
        
        :param targets: The publishing targets to report having processed
        :type targets: (str) target name (i.e. 's3', 'rds')
        :param str module_name: The name of the module to commit
        :param str s3key_prefix: The s3 key prefix to prepend when writing to s3
        :param str s3key_suffix: The s3 key suffix to append when writing to s3
        
        """
        
        logger = logging.getLogger(self.name)
        
        if module_name is None:
            module_name = self.name
        
        if s3key_prefix is None:
            s3key_prefix = DEFAULT_S3HISTORY_KEY_PREFIX
        
        if s3key_suffix is None:
            s3key_suffix = DEFAULT_S3HISTORY_KEY_SUFFIX
        
        records = []
        
        now = datetime.datetime.utcnow()
        batch_guid = uuid.uuid1()
        
        if targets is None:
            targets = ["rds","s3","dynamo"]
        
        pub_references_to_delete = []
        with self.lock:
            for e_uuid in self.event_state_data.keys():            
                event_data = self.event_state_data[e_uuid]
                
                for pub,pub_data in event_data.iteritems():
                    for target in map(lambda t: t.lower(),targets):
                        target_name = "{}.{}".format(module_name,target)
                        
                        if target in pub_data.keys():
                            target_dict = pub_data[target]
                            if "publish" in target_dict:
                                event_type = None
                                try:
                                    event_type = self.event[e_uuid].type
                                except (KeyError,AttributeError):
                                    pass
                                
                                pub_is_processing = False
                                for model in pub.queued_models:
                                    if model.event_uuid == e_uuid:
                                        pub_is_processing = True
                                
                                if pub_is_processing:
                                    logger.debug("Cannot commit process history for event:{} ({}) with target:{}, event has not yet been published.".format(e_uuid,
                                                                event_type,
                                                                target))
                                    continue
                                else:
                                    logger.warning("Event {} ({}) has not confirmed publishing but is no longer queued, treating event as published!".format(
                                                            e_uuid,
                                                            event_type))
                            
                            if "history_s3" in target_dict:
                                evt_pub_data = {"module" : target_name,
                                                        "event_uuid" : e_uuid}
                                try:
                                    process_date = target_dict["history_s3"]["process_date"]
                                except Exception:
                                    process_date = datetime.datetime.utcnow()
                                
                                try:
                                    process_time = target_dict["history_s3"]["process_time"]
                                except Exception:
                                    process_time = datetime.timedelta(seconds=0)
                                
                                evt_pub_data["process_date"] = process_date.strftime(DATE_FORMAT)
                                evt_pub_data["process_time"] = int(round(process_time.total_seconds() * 1000.0))
                                pub_references_to_delete.append(target_dict)
                                records.append(evt_pub_data)
        
        if len(records) == 0:
            logger.log(15,"No suitable records were found, no history to publish!")
            return
        
        record_data = ""
        for record in records:
            record_data += "{}\n".format(json.dumps(record))
        
        s3key = ("{p}_batch_{b}_{d}{s}".format(
                    p=s3key_prefix,
                    b=batch_guid,
                    d=now.strftime(FILE_DATE_FORMAT),
                    s=s3key_suffix))
                
        uploaded_keys = []
        
        bucket = self.karl.s3_bucket()
        
        my_key = boto.s3.key.Key(bucket)
        my_key.key = s3key
        my_key.set_contents_from_string(record_data)
        
        s3_path = "s3://{}/{}".format(self.karl.s3_bucketname,my_key.key)
        
        uploaded_keys = s3key
        
        logger.debug("Wrote S3 Event Process Import data to:'{}' ({} records)".format(s3_path,len(records)))
            
        ## Remove our references
        with self.lock:
            for ref in pub_references_to_delete:
                if "history_s3" in ref.keys():
                    del(ref["history_s3"])
                    try:
                        ## If we are deleting history, make sure our process
                        ## dict is clear as well.
                        del(ref["publish"]) 
                    except KeyError:
                        pass
                    except Exception:
                        logger.error("Failed to delete publish history.",exc_info=1)
                        
                    
        ## Prune our list
        self.prune_event_data()
                    
    
    def commit_process_history_to_s3_for_events(self,events,target=None,
                                                            module_name=None):
        """
        Method which commits our process history to S3. (which will 
        subsequently be consumed into Redshift).
        
        :param event: The event to process
        :type event: :py:class:`Event`
        :param process_date: The date the event started to be processed
        :type process_date: :py:class:`datetime.datetime`
        :param process_time: The time spent processing the event
        :type process_time: :py:class:`datetime.timedelta`
        :param module_name: The name of the module
        :type module_name: (string)
        """
        
        logger = logging.getLogger(self.name)
        
        if events is None:
            raise AttributeError("Events are not specified, cannot commit to s3")
        
        if module_name is None:
            module_name = self.name
        
        records = []
        
        now = datetime.datetime.utcnow()
        batch_guid = uuid.uuid1()
        
        if target:
            target_name = "{}.{}".format(module_name,target)
        else:
            target_name = module_name
        
        for event in events:
            e_uuid = event.uuid
            
            try:
                process_date = event.process_date.strftime(DATE_FORMAT)
            except Exception:
                process_date = datetime.datetime.utcnow().strftime(DATE_FORMAT)
            
            try:
                process_time = int(event.process_time.total_seconds() * 1000.0)    
            except Exception:
                process_time = None
            
            evt_data = {}
            evt_data["event_uuid"] = e_uuid
            evt_data["module"] = module_name
            evt_data["process_date"] = process_date
            evt_data["process_time"] = process_time
            
            records.append(evt_data)
        
        record_data = ""
        for record in records:
            record_data += "{}\n".format(json.dumps(record))
        
        s3key = ("event_process_history/karl_event_%s_batch_%s_%s.txt"
                    % (target_name,batch_guid,now.strftime(FILE_DATE_FORMAT)))
                
        uploaded_keys = []

        bucket = self.s3_bucket()
        
        my_key = boto.s3.key.Key(bucket)
        my_key.key = s3key
        my_key.set_contents_from_string(record_data)
        
        s3_path = "s3://{}/{}".format(self.s3_bucketname,my_key.key)
        
        uploaded_keys = s3key
        
        logger.debug("Wrote S3 Event Process Import data to:'{}' ({} events)".format(s3_path,len(events)))           
        
    def commit_process_history_to_rds_for_event(self,event,process_date,
                                                            process_time,
                                                            target=None):
        """
        Method which records our event processing history to our RDS database.
        
        :param event: The event to process
        :type event: :py:class:`Event`
        :param 
        :param process_date: The date the event started to be processed
        :type process_date: :py:class:`datetime.datetime`
        :param process_time: The time spent processing the event
        :type process_time: :py:class:`datetime.timedelta`
        
        
        """
        
        rds = self.karl.rds()
        module_name = self.name
        
        if target is not None:
            my_name = "{}.{}".format(module_name,target)
        else:
            my_name = module_name
        
        query = "INSERT INTO event_process_history(event_uuid, module, process_date, process_time) VALUES($1,$2,$3,$4)"
        
        datestamp = process_date.strftime(DATE_FORMAT)
        timestamp = int(process_time.total_seconds() * 1000)
        
        d = rds.query(query,event.uuid,my_name,datestamp,timestamp)
        
    def commit_process_history_to_s3_for_event(self,event,
                                                module_name,
                                                process_date,
                                                process_time):
        """
        Method which commits our process history to S3 for the provided event
        
        :param event: The event to process
        :type event: :py:class:`Event`
        :param process_date: The date the event started to be processed
        :type process_date: :py:class:`datetime.datetime`
        :param process_time: The time spent processing the event
        :type process_time: :py:class:`datetime.timedelta`
        :param module_name: The name of the module
        :type module_name: (string)
        """
        
        logger = logging.getLogger(self.name)
        
        if event is None:
            raise AttributeError("Event is not specified, cannot commit to s3")
        
        data = {}
        
        data["event_uuid"] = event.uuid
        data["module"] = module_name
        data["process_date"] = process_date.strftime(DATE_FORMAT)
        data["process_time"] = int(process_time.total_seconds() * 1000)
        
        s3key = ("event_process_history/karl_event_%s_%s_%s.txt"
                    % (module_name,event.uuid,event.date.strftime(FILE_DATE_FORMAT)))
                
        uploaded_keys = []
        
        bucket = self.s3_bucket()
        
        my_key = boto.s3.key.Key(bucket)
        my_key.key = s3key
        my_key.set_contents_from_string(json.dumps(data))
        
        s3_path = "s3://{}/{}".format(self.s3_bucketname,my_key.key)
        
        uploaded_keys = s3key
        
        logger.debug("Wrote S3 Event Process Import data to:'{}'".format(s3_path))
    
    def model_for_event_type(self,event_type,action_map=None):
        """
        Method which returns a data model for our event type, using our
        event handler registration system.
        
        :param event: The event to lookup
        :type event: :py:class:`pykarl.event.Event`
        
        :returns: Our mapped class representing our data model. Returns None 
            if no mapping matches the provided event.
        
        """
        logger = logging.getLogger(__name__)
        
        the_class = self.get_key_for_event_type("obj_class",
                                                        event_type=event_type,
                                                        action_map=action_map)
        
        require_validation = self.get_key_for_event_type("require_validation",
                                                        event_type=event_type,
                                                        action_map=action_map)
        
        obj = None
        if the_class:
            try:
                obj = the_class()
                obj.process_callback = self.model_did_publish
            except TypeError:
                logger.error("Failed to load data model using class:'%s', using standerd Event model." % the_class)
        
        obj.require_validation = require_validation
        
        return obj
    
    def publisher_for_event_type(self,event_type,action_map=None):
        """
        Returns a publisher instance for the applicable event type.
        
        :param str event_type: The event to lookup
        :param action_map: A multi-level dictionary used to map event types
                    to objects and settings
        
        :returns: :py:class:`BasePublisher` descendant. None if no publisher could  
                                    be determined.
        """
        
        my_class = self.get_key_for_event_type(key="pub_class",
                                                        event_type=event_type,
                                                        action_map=action_map)
        archive_payload = self.get_key_for_event_type(key="archive_payload",
                                                        event_type=event_type,
                                                        action_map=action_map)
        batch_deletion = self.get_key_for_event_type(key="batch_deletion",
                                                        event_type=event_type,
                                                        action_map=action_map)
        s3prefix = self.get_key_for_event_type(key="s3key_prefix",
                                                        event_type=event_type,
                                                        action_map=action_map)
        s3suffix = self.get_key_for_event_type(key="s3key_suffix",
                                                        event_type=event_type,
                                                        action_map=action_map)                       
        rds_table = self.get_key_for_event_type("rds_table",
                                                        event_type=event_type,
                                                        action_map=action_map)
        rds_key = self.get_key_for_event_type("rds_key",
                                                        event_type=event_type,
                                                        action_map=action_map)
        rds_keys = self.get_key_for_event_type("rds_keys",
                                                        event_type=event_type,
                                                        action_map=action_map)
        rds_update_null = self.get_key_for_event_type("rds_update_null",
                                                        event_type=event_type,
                                                        action_map=action_map)
        
        update_device = self.get_key_for_event_type("update_device",
                                                        event_type=event_type,
                                                        action_map=action_map)
        publish_event = self.get_key_for_event_type(key="publish",
                                                        event_type=event_type,
                                                        action_map=action_map)
        dynamo_table = self.get_key_for_event_type(key="dynamo_table",
                                                        event_type=event_type,
                                                        action_map=action_map)
       
        ### adding new key for rds action (update/insert/remove)
        rds_action = self.get_key_for_event_type(key="rds_action",
                                event_type=event_type,
                                action_map=action_map)
        
        setup_for_rds = False
        setup_for_s3 = False
        setup_for_dynamo = False

        if publish_event is not None and not publish_event:
            return
        
        if my_class is None:
            my_class = BasePublisher
        elif not my_class:
            return
            
        my_obj = my_class()
        
        if self.karl:
            my_obj.load_karl_settings(self.karl)
        
        if archive_payload is not None:
            my_obj.publish_event_payload = archive_payload

        if batch_deletion is not None and str(batch_deletion).lower() != "false":
            my_obj.batch_deletion = True
        
        if s3prefix is not None:
            my_obj.s3key_prefix = s3prefix
            setup_for_s3 = True
            
        if s3suffix is not None:
            my_obj.s3key_suffix = s3suffix
            setup_for_s3 = True
            
        if rds_table is not None:
            my_obj.rds_table = rds_table
            setup_for_rds = True
            
        if rds_key is not None:
            my_obj.rds_key = rds_key

        if rds_keys is not None:
            my_obj.rds_keys = rds_keys
        
        if rds_update_null is not None:
            my_obj.rds_update_null = rds_update_null
        
        if update_device is not None:
            my_obj.updates_rds_device_table = update_device
            setup_for_rds = True
        
        if rds_action is not None:
            my_obj.rds_action = rds_action

        if dynamo_table:
            my_obj.dynamo_table = dynamo_table
            setup_for_dynamo = True

        if setup_for_rds and my_obj.can_target_rds is None:
            my_obj.can_target_rds = True
        
        if setup_for_s3 and my_obj.can_target_s3 is None:
            my_obj.can_target_s3 = True

        if setup_for_dynamo and my_obj.can_target_dynamo is None:
            my_obj.can_target_dynamo = True

        my_obj.targets = self.targets
        
        ## See if our publisher is already in our cached list
        existing_publisher = None
        for publisher in self.publishers:   
            if publisher == my_obj:
                existing_publisher = publisher
                
        if existing_publisher is None:
            existing_publisher = my_obj
            self.publishers.append(my_obj)
        
        return existing_publisher
    
    def prune_event_data(self):
        """
        Method that will prune stored event data and remove any entries
        that have been fully processed.
        """
        
        uuids_to_prune = []
        with self.lock:
            result = False
            
            for event_uuid,event_pub_dict in self.event_state_data.iteritems():
                pubs_have_actions = False
                for pub,pub_dict in event_pub_dict.iteritems():
                    targets_have_actions = False
                    for target in pub_dict.keys():
                        if "publish" in pub_dict[target]:
                            targets_have_actions = True
                    
                    if targets_have_actions:
                        pubs_have_actions = True
                if not pubs_have_actions:
                    uuids_to_prune.append(event_uuid)
            
            for uuid in uuids_to_prune:
                if uuid in self.events:
                    del(self.events[uuid])
                if uuid in self.event_state_data.keys():
                    del(self.event_state_data[uuid])
                    
    def archivers(self,action_map=None):
        """
        Returns a list of :py:class:`BaseArchiver` decsendants that provide coverage for this module
        """
        archivers = []
        
        if action_map is None:
            action_map = self.action_map
        
        for key in action_map.keys():
            archiver = self.archiver_for_event_type(key)
            if archiver is not None and archiver not in archivers:
                archivers.append(archiver)
        
        return archivers
    
    def archiver_for_event_type(self,event_type,action_map=None):
        """
        Returns an :py:class:`BaseArchiver` instance for the applicable event 
        type. :py:class:`BaseArchiver` are used to import files from S3 into
        Redshift.
        
        :param str event_type: The event to lookup
        :param action_map: A multi-level dictionary used to map event types
                    to objects and settings
        
        :returns: :py:class:`BaseArchiver` descendant. None if no archiver could  
                                    be determined.
        """
        
        my_class = self.get_key_for_event_type(key="archive_class",
                                                        event_type=event_type,
                                                        action_map=action_map)
        s3prefix = self.get_key_for_event_type(key="s3key_prefix",
                                                        event_type=event_type,
                                                        action_map=action_map)
        s3suffix = self.get_key_for_event_type(key="s3key_suffix",
                                                        event_type=event_type,
                                                        action_map=action_map)
        archive_table = self.get_key_for_event_type(key="archive_table",
                                                        event_type=event_type,
                                                        action_map=action_map)
        
        if my_class is None and archive_table is None:
            return None
            
        if my_class is None:
            my_class = BaseArchiver
        
        my_archiver = my_class(name=self.name,arg_list=[])
        
        if self.karl:
            my_archiver.load_karl_settings(self.karl)
        
        if s3prefix is not None:
            my_archiver.s3key_prefix = s3prefix
            
        if s3suffix is not None:
            my_archiver.s3key_suffix = s3suffix
            
        if archive_table is not None:
            my_archiver.redshift_table = archive_table
            
        return my_archiver
    
    def get_key_for_event_type(self,key,event_type,action_map=None):
        """
        Method which returns the provided value for the provided event_type
        
        """
        the_value = None
        
        if action_map is None:
            action_map = self.action_map
        
        use_default = True
        for action_key in action_map.keys():
            if action_key.lower() == event_type.lower():
                use_default = False
                for subkey in action_map[action_key].keys():
                    if subkey.lower() == key.lower():
                        the_value = action_map[action_key][subkey]
        
        if use_default and "default" in action_map.keys():
            for subkey in action_map["default"].keys():
                if subkey.lower() == key.lower():
                    the_value = action_map["default"][subkey]
        
        return the_value  
    
    def process_event(self,event,karl=None,history=None):
        """
        Function which will process an event for our module. If this is not
        an event we care about, we silently die.
        
        :param event: The event to process.
        :type event: :py:class:`pykarl.event.Event`
        :param karl: Our karl server object.
        :type karl: :py:class:`pykarl.core.KARL`
        :param history: A data set as that returned by load_event_processing_history()
        :type history: Array of dictionaries.
        
        """
        
        event.process_date = datetime.datetime.utcnow()
        
        if karl is None:
            karl = self.karl
        
        logger = logging.getLogger(__name__)
        
        action_map = self.action_map
        
        event_type = event.type.lower()
        
        use_default = True
        
        ## Instantiate and load our data model
        obj = self.model_for_event_type(event_type=event.type)
        if obj is not None:
            obj.load_event(event)
        else:
            obj = event
        
        ## Instantiate and load our processing model
        publisher = None
        
        publisher = self.publisher_for_event_type(event_type=event_type)
        if publisher is None:
            logger.warning("Failed to determine publisher for event:{uuid} ((type={t} subject={s}), will not publish.".format(
                                uuid=event.uuid,
                                t=event.type,
                                s=event.subject_area))
            return
        
        publisher.targets = self.targets
        
        logger.debug("Processing event:{} ({}) using publisher:'{}', model:'{}'".format(
                                                event.uuid,
                                                event.type,
                                                publisher.__class__.__name__,
                                                obj.__class__.__name__))
        
        event.process_date = datetime.datetime.utcnow()
        
        if history is None:
            event_process_data = self.load_event_processing_history(event)
        else:
            event_process_data = history
        
        with self.lock:
            my_targets = []
            for target in self.targets:
                ## Determine if we have previously processed the event
                my_target_name = "{}".format(target)
                if self.has_processed_event(event,target=my_target_name,
                                                    data=event_process_data):
                    
                    logger.info("Event:{e}({e_type}) has been previously processed by  module:'{m}' for target:'{t}', skipping...".
                                format(e=event.uuid,e_type=event.type,m=self.name.lower(),t=target))
                    continue
                
                my_targets.append(target)
                self.events[event.uuid] = event
                batch_data = self.event_state_data
                if not event.uuid in batch_data.keys():
                    batch_data[event.uuid] = {}
                
                if not publisher in batch_data[event.uuid]:
                    batch_data[event.uuid][publisher] = {}
                if publisher.should_publish_to_target(target):
                    batch_data[event.uuid][publisher][target] = {
                                                        "publish" : {},
                                                        "history_rds": {},
                                                        "history_s3" : {},
                                                        }
        if len(my_targets) > 0:
            publisher.process_model(obj,targets=my_targets)
        
    def model_did_publish(self,model,publisher,target,options=None):
        """
        Callback function invoked when an event has been processed.
        
        :param publisher: The processor that processed the event
        :type publisher: :py:class:`BasePublisher`
        :param target: The target which was processed (i.e. 's3' 
        :type target: string
        :param options: Optional data to be passed by publisher
        :type options: :py:class:`object`
        """
        
        logger = logging.getLogger("{}:model_did_publish()".format(__name__))
        
        if model.event:
            with self.lock:
                event = model.event
                
                logger.debug("Model:{} did publish for target:{}".format(model.type,target))
                
                ## Remove our publish target from our batch data
                try:
                    del(self.event_state_data[event.uuid][publisher][target]["publish"])
                except (KeyError,AttributeError):
                    pass
                
                try:
                    process_date = options["process_date"]
                except (KeyError,AttributeError,TypeError):
                    process_date = event.process_date
                    
                try:
                    process_time = options["process_time"]
                except (KeyError,AttributeError,TypeError):
                    process_time = datetime.datetime.utcnow() - process_date
                
                ## Commit the event history to RDS immediately
                if ("status" not in options or 
                            ("status" in options and options["status"] 
                                and options["status"].lower() == "success")):
                    
                    self.commit_process_history_to_rds_for_event(model.event,
                                                        target=target.lower(),
                                                        process_date=process_date,
                                                        process_time=process_time)
                elif "status" in options and options["status"] == "Buffered":
                    logger.debug("Model:{} deferred publishing, item has been buffered.".format(model.event.uuid))
                else:
                    logger.error("Model:{} failed to publish with status:{}, will not write rds process history!".format(model.event.uuid, options["status"]))
                    ## Todo: We should archive the failed event somewhere for analysis.
                
                
                ## Remove event history target from our batch data
                try:
                    del(self.event_state_data[event.uuid][publisher][target]["history_rds"])
                except (KeyError,AttributeError):
                    pass
                    
                try:
                    d = {"process_date" : process_date,
                            "process_time" : process_time,
                            }
                    self.event_state_data[event.uuid][publisher][target]["history_s3"] = d
                except Exception:
                    pass
        else:
            logger.warning("Could not process model:{}, it has no event data!".format(
                                                                    model.type))
       
    
                                  
    def has_unpublished_process_history(self,targets=None,event=None):
        """
        Method which determines whether we have batched models that have
        not yet reported processing history (this may because they have not
        yet been processed, or because they have been processed in batch but
        not yet logged)
                
        :param targets: List of targets to check against
        :type targets: List of strings
        :param event: The event or event uuid to check (if ommitted we will check against any events)
        :type event: (str) event uuid, or :py:class:`pykarl.event.Event`
               
        :returns True: If we have outstanding tasked dispatched.
        """
        
        result = False
        
        event_state_data = self.event_state_data
        
        if event is not None:
            if isinstance(event,basestring):
                event_uuid = event
            else:
                event_uuid = event.uuid
            
            if event_uuid in self.event_state_data.keys():
                event_state_data = {event_uuid : self.event_state_data[event_uuid]}
            else:
                return False
        
        if targets is None:
            targets = ["s3","rds"]
        
        for event_uuid,event_pub_dict in event_state_data.iteritems():
            for pub,pub_dict in event_pub_dict.iteritems():
                for target in map(lambda t: t.lower(),targets):
                    if target in pub_dict.keys():
                        for action in pub_dict[target].keys():
                            if action[0:8] == "history_":
                                result = True
                                break
                    if result:
                        break
                if result:
                    break
            if result:
                break
                    
        return result
        
    
    def has_unpublished_events(self,targets=None,event=None):
        """
        Method which determines whether we have batched models that are 
        awaiting work from publishers.
        
        :param targets: List of targets to check against
        :type targets: List of strings
        :param event: The event or event uuid to check (if ommitted we will check against any events)
        :type event: (str) event uuid, or :py:class:`pykarl.event.Event`
        
        :returns True: If we have outstanding tasked dispatched.
        """
        result = False
        
        event_state_data = self.event_state_data
        
        if event is not None:
            if isinstance(event,basestring):
                event_uuid = event
            else:
                event_uuid = event.uuid
            
            if event_uuid in self.event_state_data.keys():
                event_state_data = {event_uuid : self.event_state_data[event_uuid]}
            else:
                return False
        
        if targets is None:
            targets = ["s3","rds","dynamo"]
        
        for event_uuid,event_pub_dict in event_state_data.iteritems():
            for pub,pub_dict in event_pub_dict.iteritems():
                for target in map(lambda t: t.lower(),targets):
                    if target in pub_dict.keys():
                        for action in pub_dict[target].keys():
                            if action[0:7] == "publish":
                                result = True
                                break
                    if result:
                        break
                if result:
                    break
            if result:
                break
        
        return result
        
    def log_summary(self):
        """
        Method which logs our current status.
        """
        
        logger = logging.getLogger(self.name)
        
        summary = "Log summary for event handler: {}\n".format(self.name)
        summary += "  - Number of events: {}\n".format(len(self.events))
        summary += "  - Has unprocessed events (rds): {}\n".format(self.has_unpublished_events(targets=["rds"]))
        summary += "  - Has unprocessed event history (rds): {}\n".format(self.has_unpublished_process_history(targets=["rds"]))
        summary += "  - Has unprocessed events (s3): {}\n".format(self.has_unpublished_events(targets=["s3"]))
        summary += "  - Has unprocessed event history (s3): {}\n".format(self.has_unpublished_process_history(targets=["s3"]))
        summary += "  - Has unprocessed events (dynamo): {}\n".format(self.has_unpublished_events(targets=["dynamo"]))
        summary += "  - Has unprocessed event history (dynamo): {}\n".format(self.has_unpublished_process_history(targets=["dynamo"]))
        
        if logger.level <= logging.DEBUG:
            summary += "  Held events:\n"
            for event in self.events.values():
                summary += "    * {} ({})\n".format(event.uuid,event.type)
        
        summary += "Publishers:\n"
        for publisher in self.publishers:
            queue_len = len(publisher.queued_models)
            if publisher.queued_model:
                queue_len += 1
            summary += "  {} - queued models:{}\n".format(publisher.derived_name,
                                                        queue_len)
            if logger.level <= logging.DEBUG:
                for model in publisher.queued_models:
                    summary += "  {} * {} ({})\n".format(
                                            " " * len(publisher.derived_name),
                                            model.event_uuid,
                                            model.type)
                if publisher.queued_model:
                    model = publisher.queued_model
                    summary += "  {} *q {} ({})\n".format(
                                            " " * len(publisher.derived_name),
                                            model.event_uuid,
                                            model.type)
                    
        logger.info(summary)
                
class BaseArchiver(pykarl.core.KARLCollector,pykarl.core.Benchmarker):
    """
    Class which provides several convenience methods for dealing with
    archival of Data from RDS to redshift.
    
    """
    
    name = None                 #: The name of our archiver
    redshift_table = None       #: The name of the redshift table for imports
    
            
    def __init__(self,name=None,s3key_prefix=None,s3key_suffix=".txt",
                                                        redshift_table=None,
                                                        **kwargs):
        """
        Our class constructor which includes two options for configuration.
        Configurations specified in env_config will override those specified
        in config file at env_filepath.
        
        :param env_config: Provide configuration data.
        :type env_config: (dict) Dictionary object keyed at the top level by Tanium environment
        :param env_filepath: Path to a configuration file
        :type env_filepath: (str) Filesystem path
        :param str s3key_prefix: The prefix string to use when searching for applicable S3 keys
        :param str s3key_suffix: The suffix string to use when searching for applicable S3 keys
        :param karl: :py:class:`KARL` object to load from
        :type karl: :py:class:`KARL`
        
        """
        
        if name is not None:
            self.name = name
        
        if redshift_table is not None:
            self.redshift_table = redshift_table
           
        if s3key_prefix is not None:
            self.s3key_prefix = s3key_prefix
        
        if s3key_suffix is not None:
            self.s3key_suffix = s3key_suffix
        
        pykarl.core.KARLCollector.__init__(self,**kwargs)
    
    def __eq__(self,other):
        """
        Method to determine equality. Two Archivers are considered equal
        if they have the same class, name, table, bucket, and s3 prefix/suffix
        """
        
        logger = logging.getLogger("BaseArchiverDebug")
        
        are_equal = True
        
        should_run_tests = True
        if other is None:
            should_run_tests = False
        
        while should_run_tests:
            should_run_tests = False
            try:
                if self.__class__.__name__ != other.__class__.__name__:
                    are_equal = False
                    break
                
                if self.name != other.name:
                    are_equal = False
                    break
                    
                if self.redshift_table != other.redshift_table:
                    are_equal = False
                    break
                
                if self.s3_bucketname != other.s3_bucketname:
                    are_equal = False
                    break
                
                if self.s3key_prefix != other.s3key_prefix:
                    are_equal = False
                    break
                    
                if self.s3key_suffix != other.s3key_suffix:
                    are_equal = False
                    break
            except:
                are_equal = False
        
        return are_equal
    
    def __ne__(self,other):
        """
        Method to determine inequality
        """
        
        return not self.__eq__(other)
        
    def run(self):
        """
        Our Primary CLI subroutine to process passed arguments, take appropriate
        action, and return an integer result code.
        
        :returns: (int) exit code
        """
        
        self.benchmark_start_timer("Runtime")
        
        
        ## Parse our arguments
        args = self.args
        
        ## Intro logging
        log_message = ""
        
        question = None
        
        s3file_localpath = None
        s3file_key = None
        
        ## If an import file was not specified, generate one now.
        if args.s3file:
            if os.path.exists(args.s3file):
                now = datetime.datetime.utcnow()
                datestamp = now.strftime(DATE_FORMAT)
            
                s3file_localpath = self.generate_s3file(date=now)
            
                s3file_key = "%s_%s%s" % (self.s3key_prefix,
                                        now.strftime(pykarl.core.FILE_DATE_FORMAT),
                                        self.s3key_suffix)
            
                self.upload_s3file(filepath=s3file_localpath)
        
        elif args.s3file:
            if os.path.exists(args.s3file):
                s3file_localpath = args.s3file
                s3file_key = "%s_%s%s" % (self.s3key_prefix,
                                        now.strftime(pykarl.core.FILE_DATE_FORMAT),
                                        self.s3key_suffix)
                self.upload_s3file(filepath=s3file_localpath)
            else:
                s3file_key = args.s3file
        
        ## At this point we have a file (or files) in s3 at s3filekey
        if not args.no_log:
            max_count = None
            try:
                max_count = args.max_batchsize
            except:
                pass
                
            if s3file_key:
                self.import_s3files(s3key_prefix=s3file_key,s3key_suffix=None,
                                                max_count=max_count)
            else:
                self.import_s3files(s3key_prefix=self.s3key_prefix,
                                            s3key_suffix=self.s3key_suffix,
                                            max_count=max_count)
        
        self.benchmark_end_timer("Runtime")
        
        ## Report our Results
        if args.stats or args.verbose > 0:
            self.print_benchmarks(args=args)
        
    def setup_parser(self):
        """
        Subroutine where we establish our Command line arguments. Populates
        self.parser. You may wish to override this if you want to implement
        any custom CLI arguments.
        """
        
        ## If we don't have a configured parser, set it up
        if self.parser is None:
            pykarl.core.KARLCollector.setup_parser(self)
        
        parser = self.parser
            
        parser.description='''Query RDS, export, process, and post data to RedShift'''
        parser.add_argument("--redshift-table",help="The RedShift table name")
        parser.add_argument("--max-batchsize",help="The maximum number of S3 Keys that will be imported in any given run",
                                        default=DEFAULT_S3IMPORT_MAXRECORDCOUNT)
    
    def generate_csv_file(self,filepath=None,date=None):
        """
        Method which will load data from RDS, generate a delimited file, and
        save it to the specified location. You will need to override this
        method when writing new archival scripts.
        
        :param str filepath: The path to save our file to. If ommitted we will 
                    a temporary file.
        :param date: The date to use for our commit. If none is
                supplied we will use utcnow()
        :param type date: py:class:`datetime.datetime`
        
        :returns: Path to delimited file for upload to s3
        
        """
        
        raise RuntimeError("generate_csv_file() not implemented.")
    
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
        
        datestamp = datetime.datetime.utcnow().strftime(pykarl.core.FILE_DATE_FORMAT)
        
        if not bucket_name:
            bucket_name = self.s3_bucketname
        
        if not table_name:
            table_name = self.redshift_table
        
        if not table_name:
            raise AttributeError("No table name was provided, cannot import")
        
        if s3key_prefix is None:
            s3key_prefix = self.s3key_prefix
            
        if s3key_suffix is None:
            s3key_suffix = self.s3key_suffix
        
        self.benchmark_start_timer("S3Import:%s" % table_name,"FileImport")
        
        self.logger.debug("Importing s3 files from bucket:'%s' prefix:'%s' suffix:'%s' into table:'%s' (batch size:%s)" 
                    % (bucket_name,s3key_prefix,s3key_suffix,table_name,max_count))
        
        redshift = self.redshift()
        bucket = self.s3_bucket()
        
        s3keys = self.get_s3keys(s3key_prefix=s3key_prefix,
                                                s3key_suffix=s3key_suffix,
                                                bucket_name=bucket_name,
                                                max_count=max_count)
        
        self.benchmark_counter("S3Import:%s" % table_name,"DataFiles",len(s3keys))
        
        if len(s3keys) == 0:
            self.logger.info("No %s files were found to import..." % table_name)
            self.benchmark_end_timer("S3Import:%s" % table_name,"FileImport")
            return
        
        self.logger.log(5,"Importing s3 objects: '%s'" % "','".join(s3keys))
        
        manifest_string = self.redshift_manifest_data(s3keys)
        manifest_keyname = "%s_%s.manifest" % (self.s3key_prefix,datestamp)
        my_key = boto.s3.key.Key(bucket)
        my_key.key = manifest_keyname
        my_key.set_contents_from_string(manifest_string)
        
        s3path = "s3://%s/%s" % (bucket_name,manifest_keyname)
        
        credential_string = ("aws_access_key_id=%s;aws_secret_access_key=%s"
                                        % (self.s3_access_key_id,self.s3_secret_key))
        
        self.logger.info("Importing Ingest File:%s (table:%s)..." % (s3path,table_name))
        
        query_string = ("COPY %s FROM '%s' CREDENTIALS '%s' manifest json 'auto'" % (table_name,s3path, credential_string))
        
        # Note: query contains credentials.
        self.logger.log(1,"Running SQL Query:\"%s\"" % query_string)
        redshift.query(query_string)
        
        self.benchmark_end_timer("S3Import:%s" % table_name,"FileImport")
        
        filecount = len(s3keys)
        self.benchmark_counter("S3Import:%s" % table_name,"FileCount",
                                                                filecount)
        
        
        process_time_ms = int(1000 * self.duration_for_timer(
                                                    "S3Import:%s" % table_name,
                                                    "FileImport"))
                                        
        self.benchmark_start_timer("S3Import:%s" % table_name,"DeleteFiles")
        if delete:
            s3keys.append(manifest_keyname)
            self.logger.debug("Deleting {} s3 keys.".format(len(s3keys)))
            bucket.delete_keys(s3keys)
        
        self.benchmark_end_timer("S3Import:%s" % table_name,"DeleteFiles")
    
        try:
            self.report_archive_results(module_name=self.name,table_name=table_name,
                                        record_count=filecount,
                                        process_time=process_time_ms)
        except Exception as exp:
            self.logger.error("Failed to report archive results: {}".format(exp))
    
    def upload_s3file(self,filepath,s3key):
        """
        Method to upload an s3file.
            
        :param str filepath: Filesystem path to our file to upload.
        :param str s3key: The s3 key to upload the file to.
        
        """
        
        self.benchmark_start_timer("S3Import","Upload")
        self.logger.info("Uploading import file to s3...")
        
        bucket = self.s3_bucket()
        my_key = boto.s3.key.Key(bucket)
        my_key.key = s3key
        my_key.set_contents_from_filename(filepath,encrypt_key=True)
        
        s3_path = "s3://%s/%s" % (self.s3_bucketname,my_key.key)
        
        if self.args.verbose > 0:
            self.logger.info("Wrote S3 Import data to:'%s'" % s3_path)
        
        self.benchmark_end_timer("S3Import","Upload")
    
    def get_s3keys(self,s3key_prefix=None,s3key_suffix=None,bucket_name=None,
                                                            max_count=None):
        """
        Method to return a list of s3 keys matching the given criteria.
        
        :param str s3key_prefix: Prefix string to determine matches (if an s3
            object's key begins with this it's a match)
        :param str s3key_suffix: Suffix string to determine matches (if an s3
            object's key ends with this it's a match)
        :param str bucket_name: The name of the s3 bucket
        :param int max_count: The maximum number of records to return.
        
        """
        
        logger = logging.getLogger()
        logger.log(5,"Fetching s3 keys prefix:{} suffix:{}, bucket:{}, batchsize:{}".format(
                            s3key_prefix,
                            s3key_suffix,
                            bucket_name,
                            max_count))
        
        if s3key_prefix is None:
            s3key_prefix = self.s3key_prefix
            
        if s3key_suffix is None:
            s3key_suffix = self.s3key_suffix
        
        if bucket_name is None:
            bucket_name = self.s3_bucketname
        
        s3keys = []
        
        bucket = self.s3_bucket()
        
        list = bucket.list(prefix=s3key_prefix)
        
        for key in list:
            if s3key_suffix is not None:
                if not key.name.endswith(s3key_suffix):
                    continue
            s3keys.append(key.name)
            
            if max_count is not None:
                if int(len(s3keys)) >= int(max_count):
                    logger.debug("Max record limit hit, returning {} s3 keys.".format(max_count))
                    break
                
        return s3keys
    
    def report_archive_results(self,module_name,table_name,record_count,
                                process_time,
                                archive_history_table_name="archive_history"):
        """
        Method to report our archive activity to RDS and S3.
        
        """
        logger = logging.getLogger(self.__class__.__name__)
                                            
        query = ("INSERT INTO %s(module, table_name, record_count, process_date, process_time, process_host) "
                        "VALUES($1,$2,$3,$4,$5,$6)" % archive_history_table_name)
        
        hostname = socket.gethostname()
        datestring = datetime.datetime.utcnow().strftime(pykarl.core.DATE_FORMAT)
        values = (module_name,table_name,record_count,datestring,process_time,hostname)
            
        logger.info("Reporting archive results to RDS...")
        rds = self.rds()
        logger.log(5,"Running RDS Query:\"\"\"{}\"\"\", {}".format(query,values))
        try:
            rds.query(query,values)
        except Exception as exp:
            logger.error("Failed to record RDS archive history for module:{}. Error: {}".format(
                                                    module_name,exp))

        logger.info("Reporting archive results to Redshift...")        
        rs = self.redshift()
        logger.debug("Running Redshift Query:\"\"\"{}\"\"\", {}".format(query,values))
        try:
            rs.query(query,values)
        except Exception as exp:
            logger.error("Failed to record Redshift archive history for module:{}. Error: {}".format(
                                                    module_name,exp))
    
    def redshift_manifest_data(self,keys,bucket_name=None):
        """
        Method which will generate redshift manifest data for the
        given keys and return as a string.
        
        :param keys: The s3 key names to include.
        
        :returns: str - manifest content
        
        """
        
        if not bucket_name:
            bucket_name = self.s3_bucketname
    
        d = {"entries" : []}
    
        for key in keys:
            d["entries"].append({"url" : "s3://%s/%s" % (bucket_name,key)})
            
        return json.dumps(d)
    
    def setup(self,env=None,redshift_host=None,redshift_name=None,redshift_port=None,
                            redshift_ms=None,rds_host=None,
                            rds_name=None,rds_port=None,
                            rds_ms=None,s3_bucketname=None,s3_ms=None,
                            args=None,
                            redshift_table=None,
                            opconfig=None):
        
        """
        Method which sets up our object by loading the appropriate
        credentials from odin.
        """
                            
        if args is None:
            args = self.args
            
        if not redshift_table:
            try:
                redshift_table = args.redshift_table
            except AttributeError:
                pass
            
        if redshift_table:
            self.redshift_table = redshift_table
        
        pykarl.core.KARLCollector.setup(self,env=env,redshift_host=redshift_host,
                                    redshift_name=redshift_name,
                                    redshift_port=redshift_port,
                                    redshift_ms=redshift_ms,
                                    rds_host=rds_host,
                                    rds_name=rds_name,
                                    rds_port=rds_port,
                                    rds_ms=rds_ms,
                                    s3_bucketname=s3_bucketname,
                                    s3_ms=s3_ms,
                                    args=args,
                                    opconfig=opconfig)
                                    
#MARK: Module Functions
allowed_modules = []       #: List of module names that are allowed to load
ignored_modules = []        #: List of module names that are ignored

modules = []                #: Var which represents loaded modules

module_name = __name__      #: Name reference

def registered_subject_areas():
    """
    Method which outputs a list of registered subject areas.
    """
    subjects = []
    for module in modules:
        for subject_area in module.subject_areas:
            if not subject_area in subjects:
                subjects.append(subject_area)
    return subjects

def find_modules(subject_areas=None,event_types=None):
    """
    Method which can be used to find qualifying modules based on
    passed criteria.

    :param subject_areas: string list of subject areas we are interested in
    :type subject_areas:  string or list of strings
    :param event_types: string list of event types we are interested in
    :type event_types: string or list of strings

    :returns: List of modules.

    """
    
    my_subject_areas = []
    my_event_types = []
    
    if subject_areas is None and event_types is None:
        return modules
    
    if isinstance(subject_areas,basestring):
        my_subject_areas = [subject_areas]
    else:
        my_subject_areas = subject_areas
    
    if isinstance(event_types,basestring):
        my_event_types = [event_types]
    else:
        my_event_types = event_types
    
    my_modules = []
    for module in modules:
        try:
            handler = module.event_handler
            if handler.handles_subject_areas(my_subject_areas):
                if my_event_types and len(my_event_types) > 0:
                    for event_type in my_event_types:
                        if handler.handles_eventtype(event_type):
                            my_modules.append(module)
                else:
                    my_modules.append(module)
        except AttributeError:
            pass
    
    return my_modules

def load_modules(subject_areas=None,namespace=pykarl.modules):
    """
    Method which will load any modules or modules of sub-packages which
    match the provided subject_areas.
    
    :param subject_areas: string list of subject areas we are interested in
    :type subject_areas: List of strings
    :param module_paths: List of paths to import
    :type module_paths: list<string>
    """
    
    logger = logging.getLogger("modules")
    
    module_paths=namespace.__path__
    
    if not subject_areas:
        logger.info("Loading Modules with all subject areas")
    else:
        logger.info("Loading Modules with subject areas:{}".format(", ".join(subject_areas)))
    
    logger.debug(" - Import paths:'{}'".format("', '".join(module_paths)))
    
    for module_path in module_paths:
        for module in modules_from_path(path=module_path,
                                                namespace=namespace.__name__,
                                                subject_areas=subject_areas):
            logger.log(5," - Testing module:{}".format(module))
            qualifies = False
            if len(ignored_modules) > 0 or len(allowed_modules) > 0:
                if len(ignored_modules) > 0:
                    if module.module_name.lower() in (name.lower() for name in ignored_modules):
                        logger.debug("Ignored module: {}".format(module.module_name))
                        continue
                    else:
                        try:
                            if module.module_name.split(".")[-1].lower() in (name.lower() for name in ignored_modules):
                                logger.debug("Ignored module: {}".format(module.module_name))
                                continue
                        except (AttributeError,IndexError):
                            pass
            
                if len(allowed_modules) > 0:
                    if module.module_name in (name.lower() for name in allowed_modules):
                        qualifies = True
                    else:
                        try:
                            if module.module_name.split(".")[-1] in (name.lower() for name in allowed_modules):
                                qualifies = True
                        except (AttributeError, IndexError):
                            pass
                else:
                    qualifies = True
            else:
                qualifies = True
                    
            if qualifies and module not in modules:
                logger.info("Found Qualified Module: {}".format(module.module_name))
                logger.log(5," - Module Detail:{}".format(module))
                modules.append(module)
            elif not qualifies:
                logger.info("Skipping Unqualified Module: {}".format(module.module_name))
                logger.log(5," - Module Detail:{}".format(module))


def modules_from_path(path,namespace,subject_areas=None):
    """
    Method which will load any modules or modules of sub-packages which
    match the provided subject_areas at the given path.
    
    :param str path: Filesystem path for the package/module to load
    :param str namespace: Python namespace representing the modules.
    :param subject_areas: string list of subject areas we are interested in
    :type subject_areas: List of strings
    
    :returns: List of Modules
    """
    
    logger = logging.getLogger("modules")
    
    my_modules = []
    
    logger.debug("Searching for modules in path: '{}'".format(path))
    for importer, modname, ispkg in pkgutil.iter_modules([path],"{}.".format(namespace)):
        logger.debug("Found submodule {} (is a package: {})".format(modname, ispkg,path))
        if ispkg:
            pkg_path = os.path.join(path,modname.split(".")[-1])
            pkg_modules = modules_from_path(path=pkg_path,namespace=modname,
                                                subject_areas=subject_areas)
            my_modules.extend(pkg_modules)
        else:
            m = __import__(modname,fromlist=[namespace])
            try:
                handler = m.event_handler
                if subject_areas is None:
                    my_modules.append(m)
                elif handler.handles_subject_areas(subject_areas,False):
                    my_modules.append(m)
                else:
                    logger.info("Submodule %s does not handle subject_area:%s" % (modname,subject_areas))
            except AttributeError:
                logger.debug("Submodule %s has no event handlers, ignoring..." % (modname))
                pass
                
    return my_modules
    

#MARK: Module Exceptions
class EmptyQueueError(Exception):
    """
    Exception thrown when no applicable objects are queued
    """
    pass
