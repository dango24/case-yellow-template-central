"""
**compliance** - Provides custom data handling for Compliance subject area

.. module:: **compliance** Provides data handling for Compliance subject area
    :platform: RHEL5
    :synopsis: Module plugin that provides Compliance data and publishing models.
   
.. codeauthor:: Beau Hunter <beauhunt@amazon.com>



"""

import datetime
import logging
import json

import pykarl.core
from .base import BaseModel,BasePublisher,BaseEventHandler

COMPLIANCE_STATUS_UNKNOWN = 0
COMPLIANCE_STATUS_EXEMPT = 1 << 1
COMPLIANCE_STATUS_COMPLIANT = 1 << 2
COMPLIANCE_STATUS_ERROR = 1 << 3
COMPLIANCE_STATUS_INGRACETIME = 1 << 4
COMPLIANCE_STATUS_NONCOMPLIANT = 1 << 5
COMPLIANCE_STATUS_ISOLATIONCANDIDATE = 1 << 6
COMPLIANCE_STATUS_ISOLATED = 1 << 7

class EvaluationStartedEvent(BaseModel):
    """
    Class representing evaluation starting event
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

        self.module_name = None

        if payload_map is None:
            payload_map = {
                                "ModuleName": "module_name",
        }

        if export_map is None:
            export_map = {
                                "module_name" : "module_name",
                                "date" : "<type=datetime>",
                                "source_uuid" : "source",
                                "type" : None,
                                "event_uuid" : None,
                        }

        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)



class EvaluationFinishedEvent(BaseModel):
    """
    Class representing evaluation finished  event
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

        self.module_name = None
        self.duration = None

        if payload_map is None:
            payload_map = {
                                "ModuleName": "module_name",
                                "Duration" : "duration",
        }

        if export_map is None:
            export_map = {
                                "module_name" : "module_name",
                                "duration" : "duration",
                                "date" : "<type=datetime>",
                                "source_uuid" : "source",
                                "type" : None,
                                "event_uuid" : None,
                        }

        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)


class RemediationStartedEvent(BaseModel):
    """
    Class representing remediation started event
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


        self.module_name = None
        self.subtype = None

        if payload_map is None:
            payload_map = {
                                "ModuleName" : "module_name",
                                "Force" : "subtype",
        }


        if export_map is None:
            export_map = {
                                "module_name" : "module_name",
                                "subtype" : "subtype",
                                "date" : "<type=datetime>",
                                "source_uuid" : "source",
                                "type" : None,
                                "event_uuid" : None,
                        }

        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)


class RemediationFinishedEvent(BaseModel):
    """
    Class representing remediation finished event
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

        self.duration = None
        self.module_name = None
        self.subtype = None

        if payload_map is None:
            payload_map = {
                                "Duration" : "duration",
                                "ModuleName" : "module_name",
                                "Force" : "subtype",
        }

        if export_map is None:
            export_map = {
                                "module_name" : "module_name",
                                "subtype" : "subtype",
                                "duration" : "duration",
                                "date" : "<type=datetime>",
                                "source_uuid" : "source",
                                "type" : None,
                                "event_uuid" : None,
                        }

        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)

class ModuleLoadEvent(BaseModel):
    """
    Class representing compliance module laod event.

    :Example:

        >>> event = e.Event(type="ComplianceModuleUnloadEvent",subject_area="Compliance")
        >>> event.source = "UUID-really-uniqye"
        >>> event.payload["version"] = "1.7.8"
        >>> event.payload["hash"] = "HASH######cjethan"
        >>> event.payload["unload_uuid"] = "UUID-unload"
        >>> event.payload["domain"] = "ServiceDomain"
        >>> event.payload["identifier"] = "MyModuleOk1"
        >>> event.payload["num_failures"] = 0
        >>> event.payload["loadtime"] = 140
        >>> event.date = datetime.datetime.strptime("2014-03-01","%Y-%m-%d")
        >>> model = compliance.ModuleUnloadEvent(event=event)
        >>> print model.export_as_redshift_json()
        {
             "source_uuid": "UUID-really-uniqye", 
             "domain": "ServiceDomain", 
             "load_uuid": "UUID-unload", 
             "hash": "HASH######cjethan", 
             "loadtime": 140, 
             "module": "MyModuleOk1", 
             "version": "1.7.8", 
             "num_failures": 0, 
             "date": "2014-03-01 00:00:00", 
             "event_uuid": "954d0f96-9ac2-4bb6-bcc5-4d3276ad8e7c"
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
        
        self.modules = []
        self.domain = None
        self.type = "ComplianceModuleLoadEvent"
        self.num_failures = None
        self.loadtime = None
        self.load_date = None
        self.load_uuid = None
        self.was_error = None
        
        # Current design does not allow ACME to send the below fields because, ACME sends a list of modules loaded and the below fields will not be
        #able to point to each one of them
        self.hash = None
        self.filename = None
        self.version = None
        
        if payload_map is None:
            payload_map = {
                            "domain":"domain",
                            "file":"filename",
                            "hash":"hash",
                            "load_time":"loadtime",
                            "load_date": "load_date",
                            "version":"version",
                            "num_failures":"num_failures",
                            "num_modules" : "num_modules",
                            "load_uuid" : "load_uuid",
                            "error": "was_error",
                        }
        if export_map is None:
            export_map = {
                            "source_uuid" : "source",
                            "event_uuid" : "event_uuid",
                            "load_uuid" : "load_uuid",
                            "domain":"domain",
                            "file":"filename",
                            "hash":"hash",
                            "loadtime":"loadtime",
                            "load_date": "load_date",
                            "version":"version",
                            "num_failures":"num_failures"
                        }     
        
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)
        
    def load_event(self,event):
        """
        Method to load data from a karl event. Relies heavily on 
        :py:func:`BaseModel.load_event`
        
        """
        
        BaseModel.load_event(self,event)
        
        try:
            self.modules = event.payload["modules"].split(", ")
        except KeyError:
            pass
    
    def to_event(self):
        """
        Method which will output a KARL Event based on our object.
        Relies heavily on :py:func:`BaseModel.to_event`

        """

        event = BaseModel.to_event(self)
    
        event.payload["modules"] = ", ".join(self.modules)
    
        return event
    
    def export_as_csv(self,delimiter="|"):
        """
        Method to export our record as a delimited text record.
        uuid|domain|module|file|hash|loadtime|version|num_failures|date
        """
        
        csv = None
        
        for module in self.modules:
            my_csv = None
            
            data = {}
            
            for key,value in self.export_map.iteritems():
                prop_value = None
                if value is not None:
                    attribute_name = value
                else:
                    attribute_name = key
                try:
                    prop_value = getattr(self,attribute_name)
                except (TypeError, AttributeError) as exp:
                    logger = logging.getLogger()
                    logger.warning("Failed to export attribute:%s for event:%s. Error:%s" 
                                    % (attribute_name,self.event_uuid,exp.message))
                
                if prop_value is None:
                    prop_value = ""
                elif key == "date":
                    prop_value = self.date.strftime(pykarl.core.DATE_FORMAT)
                elif key == "module":
                    prop_value = module
                
             
                if not my_csv:
                    my_csv = prop_value
                else:
                    my_csv = "%s%s%s" % (my_csv,delimiter,prop_value)
            
            if not csv:
                csv = my_csv
            else:
                csv = "%s\n%s" % (csv,my_csv)
        
        return csv

    def export_as_redshift_json(self,export_map=None):
        """
        Method which outputs a JSON representation of our model meant
        for S3 import.
        
        :returns str: JSON string representing our ModuleLoadEvent
        """
        
        logger = logging.getLogger("export_as_redshift_json()")
        
        my_string = ""
        
        if export_map is None:
            export_map = self.export_map
        
        for module in self.modules:
            data = {}
            
            for key,value in export_map.iteritems():
                if value is not None:
                    attribute_name = value
                else:
                    attribute_name = key
                try:
                    data[key] = getattr(self,attribute_name)
                except (TypeError, AttributeError) as exp:
                    logger.warning("Failed to export attribute:%s for event:%s. Error:%s" 
                                    % (attribute_name,self.event_uuid,exp.message))
             
            data["date"] = self.date.strftime(pykarl.core.DATE_FORMAT)
            data["module"] = module
            
            my_string = "%s\n%s" % (my_string,json.dumps(data,indent=3))
            
        return my_string


    

class ModuleLoadEventPublisher(BasePublisher):
    """
    Class used to orchestrate client data reported from ACME involving 
    module loading details loaded by Compliance module and publishing to RDS, S3, and Redshift.
    Inserts entries into transaction table namely compliance_module_activity and current state to  loaded_modules table
    """
    
    can_target_rds = True  #: Flag denoting whether our class interacts with RDS, 
                        #: if true, :py:func:`commit_to_rds` will be called.
    can_target_s3 = True   #: Flag denoting whether our class interacts with S3
                        #: if true, :py:func:`can_target_s3` will be called.
    
    def commit_to_rds(self,model=None,table="compliance_module_activity",
                                        device_table="device_instance"):
        """
        Method which will commit the provided event to RDS.
        
        :param model: The event to commit
        :type model: :py:class:`ModuleLoadEvent` object
        :param table: The name of the table to publish
        :type table: str
        :param device_table: The name of the device table to publish
        :type device_table: str
        
        :raises :py:class:`AttributeError`: If model or table information is missing.
        :raises :py:class:`pg.ProgrammingError`: In the event of an SQL query exception
        
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
        
        logger.debug("Commiting 'ComplianceModuleLoad' to RDS table:%s for source:'%s'"
                                                    % (table,model.source))
        
        rds = self.rds()
        ## Insert a row into our DB for each module
        if model.modules:
            for module in model.modules:
                if not module:
                    module= "Unknown"
                query = ("INSERT INTO %s (module_name, file, hash, loadtime, version, num_failures,date,"
                                " source_uuid, event_uuid, load_uuid, domain, load_date, num_modules, type) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)" % table)
                values = (module,model.filename,
                                model.hash,model.loadtime,model.version,
                                model.num_failures,model.datestamp,model.source,
                                model.event_uuid,
                                model.load_uuid,model.domain, model.load_date, model.num_modules, model.type)
            
                logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
                rds.query(query,values)
        
        # Compliance module load event, needs to be stored in loaded_modules table in order to know the active loaded modules for a device.
        table = "loaded_modules"
        if model.modules:
            for module in model.modules:
                if not module:
                    module = "Unknown"
                    
                # Deleting the existing load event for the same device, module and domain.
                # This helps to avoid duplicate load events in loaded_modules table.
                query = "DELETE FROM %s WHERE source_uuid = $1 AND module_name = $2 AND domain=$3" % table
                values = (model.source, module, model.domain )
                logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
                rds.query(query,values)
                
                query = ("INSERT INTO %s (module_name, file, hash, loadtime, version, num_failures,date,"
                                " source_uuid, event_uuid, load_uuid, domain, load_date, num_modules) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)" % table)
                values = (module,model.filename,
                                model.hash,model.loadtime,model.version,
                                model.num_failures,model.datestamp,model.source,
                                model.event_uuid,
                                model.load_uuid,model.domain, model.load_date, model.num_modules)
                
                logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
                rds.query(query,values)
                


class ModuleUnloadEvent(BaseModel):
    """
    Class representing a network change event.

    :Example:

        >>> event = e.Event(type="ComplianceModuleUnloadEvent",subject_area="Compliance")
        >>> event.source = "UUID-really-uniqye"
        >>> event.payload["version"] = "1.7.8"
        >>> event.payload["hash"] = "HASH######cjethan"
        >>> event.payload["unload_uuid"] = "UUID-unload"
        >>> event.payload["domain"] = "ServiceDomain"
        >>> event.payload["identifier"] = "MyModuleOk1"
        >>> event.payload["num_failures"] = 0
        >>> event.payload["loadtime"] = 140
        >>> event.date = datetime.datetime.strptime("2014-03-01","%Y-%m-%d")
        >>> model = compliance.ModuleUnloadEvent(event=event)
        >>> print model.export_as_redshift_json()
        {
             "source_uuid": "UUID-really-uniqye", 
             "domain": "ServiceDomain", 
             "load_uuid": "UUID-unload", 
             "hash": "HASH######cjethan", 
             "loadtime": 140, 
             "module": "MyModuleOk1", 
             "version": "1.7.8", 
             "num_failures": 0, 
             "date": "2014-03-01 00:00:00", 
             "event_uuid": "954d0f96-9ac2-4bb6-bcc5-4d3276ad8e7c"
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
        
        self.module_name = None
        self.domain = None
        self.type = "ComplianceModuleUnloadEvent"
        self.load_date = None
        self.load_uuid = None
        
        #ACME is not sending the below values currently.
        self.filename = None
        self.hash = None
        self.version = None
        self.num_failures = None
        self.loadtime = None
        self.was_error = None
        
        if payload_map is None:
            payload_map = {
                            "identifier" : "module_name",
                            "domain":"domain",
                            "file":"filename",
                            "hash":"hash",
                            "loadtime":"loadtime",
                            "unload_date":"load_date",
                            "version":"version",
                            "num_failures":"num_failures",
                            "num_modules" : "num_modules",
                            "unload_uuid" : "load_uuid",
                            "error": "was_error",
                        }
        if export_map is None:
            export_map = {
                            "source_uuid" : "source",
                            "event_uuid" : None,
                            "load_uuid" : "load_uuid",
                            "domain":"domain",
                            "file":"filename",
                            "hash":"hash",
                            "loadtime":"loadtime",
                            "version":"version",
                            "num_failures":"num_failures",
                            "date" : "<type=datetime>",
                        }     
        
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)
    
    def export_as_redshift_json(self,export_map=None):
        """
        Method which outputs a JSON representation of our model meant
        for S3 import.
        
        :returns str: JSON string representing our ModuleLoadEvent
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
        
        json_dict["date"] = self.date.strftime(pykarl.core.DATE_FORMAT)
        json_dict["module"] = self.module_name
        return json.dumps(json_dict,indent=5)

class ModuleUnloadEventPublisher(BasePublisher):
    """
    Class used to orchestrate client data reported from ACME involving 
    compliance module unloading details loaded by Compliance module and publishing to RDS, S3, and Redshift.
    Inserts entries into transaction table namely compliance_module_activity and deletes entry in loaded_modules table
    """
    
    can_target_rds = True  #: Flag denoting whether our class interacts with RDS, 
                        #: if true, :py:func:`commit_to_rds` will be called.
    can_target_s3 = True   #: Flag denoting whether our class interacts with S3
                        #: if true, :py:func:`can_target_s3` will be called.
    
    def commit_to_rds(self,model=None,table="compliance_module_activity",
                                        device_table="device_instance"):
        """
        Method which will commit the provided event to RDS.
        
        :param model: The event to commit
        :type model: :py:class:`ModuleUnloadEvent` object
        :param table: The name of the table to publish
        :type table: str
        :param device_table: The name of the device table to publish
        :type device_table: str
        
        :raises :py:class:`AttributeError`: If model or table information is missing.
        :raises :py:class:`pg.ProgrammingError`: In the event of an SQL query exception
        
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
        
        logger.debug("Commiting 'compliancemoduleload' to RDS table:%s for source:'%s'"
                                                    % (table,model.source))
        
        rds = self.rds()
        if model:
            if not model.module_name:
                model.module_name = "Unknown"
            query = ("INSERT INTO %s (module_name, file, hash, loadtime, version, num_failures,date,"
                                    " source_uuid, event_uuid, load_uuid, domain, load_date, type) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, $12, $13)" % table)
            values = (model.module_name,model.filename,
                        model.hash,model.loadtime,model.version,
                        model.num_failures,model.datestamp,model.source,
                        model.event_uuid,
                        model.load_uuid,model.domain, model.load_date, model.type)
                    
            logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
            rds.query(query,values)
        
        
        # Compliance module load event, needs to be stored in loaded_modules table in order to know the active loaded modules for a device.
        table = "loaded_modules"    
        ## Insert a row into our DB for each module
        if model:
            if not model.module_name:
                model.module_name = "Unknown"
            ## Delete the records which correspond to unloaded modules.
            query = "DELETE FROM %s WHERE source_uuid = $1 AND module_name = $2 and domain=$3" % table
            values = (model.source, model.module_name, model.domain)
            logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
            rds.query(query,values)
                  
class ComplianceStatusChangeEvent(BaseModel):
    """
    Class representing a change in a compliance module status.
    
    :Example:

        >>> event = Event(type="ComplianceStatusDidChange",subject_area="Compliance")
        >>> event.source = "UUID-XXX-XXX-XXX"
        >>> event.payload["old_status"] = 48  ## (NonCompliant, InGracetime)
        >>> event.payload["new_status"] = 32  ## (NonCompliant)
        >>> event.payload["deadline"] = 1433288387
        >>> event.payload["isolation_deadline"] = 1433461187
        >>>  
        >>> model = ComplianceStatusChangeEvent(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "source_uuid": "UUID-XXX-XXX-XXX", 
             "date": "2015-10-29 22:16:19", 
             "old_status": 48, 
             "event_uuid": "c8994d09-c167-4ceb-8740-93258709e047", 
             "new_status": 32
        }
        >>> 
    
    """
    
    type = "ComplianceStatusDidChange"
    
    def __init__(self,event=None,data=None,payload_map=None,export_map=None,
                rds_export_map=None,*args,**kwargs):
        """
        Our constructor.
        
        :param event: Our event to process.
        :type event: `:py:class:Event`
        :param payload_map: Key=>Property map for data imports
        :type payload_map: dict(string,string)
        :param export_map: Key=>Property map for data exports
        :type export_map: dict(string,string)
        """
        
        self.old_status = None
        self.new_status = None
        self.deadline = None
        self.isolation_deadline = None
        
        if payload_map is None:
            payload_map = {
                            "old_status": None,
                            "new_status" : None,
                            "deadline" : "deadline_datestamp",
                            "isolation_deadline" : "isolation_datestamp",
                        }
        if export_map is None:
            export_map = {
                            "source_uuid" : "source",
                            "event_uuid" : None,
                            "old_status" : None,
                            "new_status" : None,
                            "date" : "datestamp",
                            "deadline" : "deadline_datestamp",
                            "isolation_deadline" : "isolation_datestamp",
                        }     
        
        if rds_export_map is None:
            rds_export_map = { 
                        "source_uuid" : "source",
                        "event_uuid" : None,
                        "status" : "new_status",
                        "date" : "datestamp",
                        "deadline" : "deadline_datestamp",
                        "isolation_deadline" : "isolation_datestamp"
                    }

        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                export_map=export_map,
                                                rds_export_map=rds_export_map,
                                                *args,
                                                **kwargs)
        
    @property
    def deadline_datestamp(self):
        """
        Property which returns a datestamp formatted for
        SQL use.
        """
        logger = logging.getLogger()
        
        deadline = None
        if self.deadline:
            try:
                deadline = self.deadline.strftime(pykarl.core.DATE_FORMAT)
            except Exception as exp:
                logger.error("Failed to return deadline for date:{}. Error:{}".format(self.deadline,exp))
                
        return deadline
        
    @deadline_datestamp.setter
    def deadline_datestamp(self,value):
        """
        Setter for our datestamp
        """
        the_date = None
        
        if isinstance(value,datetime.datetime):
            the_date = value
        elif value is not None:
            try:
                the_date = datetime.datetime.utcfromtimestamp(float(value))
            except TypeError as exp:
                try:
                    the_date = datetime.datetime.strptime(value,pykarl.core.DATE_FORMAT)
                except ValueError as exp:
                    logger = logging.getLogger(self.__class__.__name__)
                    logger.warning("Could not import deadline_datestamp value:{v} for model:{m}".format(v=value,m=self.__class__.__name__)) 

        self.deadline = the_date

    @property
    def isolation_datestamp(self):
        """
        Property which returns a datestamp formatted for
        SQL use.
        """
        
        logger = logging.getLogger()
        
        deadline = None
        if self.isolation_deadline:
            try:
                deadline = self.isolation_deadline.strftime(pykarl.core.DATE_FORMAT)
            except Exception as exp:
                logger.error("Failed to return deadline for date:{}. Error:{}".format(self.deadline,exp))
                
        return deadline
        
    @isolation_datestamp.setter
    def isolation_datestamp(self,value):
        """
        Setter for our datestamp
        """
        the_date = None
        
        if isinstance(value,datetime.datetime):
            the_date = value
        elif value is not None:
            try:
                the_date = datetime.datetime.utcfromtimestamp(float(value))
            except TypeError as exp:
                try:
                    the_date = datetime.datetime.strptime(value,pykarl.core.DATE_FORMAT)
                except ValueError as exp:
                    logger = logging.getLogger(self.__class__.__name__)
                    logger.warning("Could not import isolation_datestamp value:{v} for model:{m}".format(v=value,m=self.__class__.__name__)) 

        self.isolation_deadline = the_date

class ModuleComplianceStatusChangeEvent(ComplianceStatusChangeEvent):
    """
    Class representing a change in a compliance module status.
    
    :Example:

        >>> event = Event(type="ModuleStatusDidChange",subject_area="Compliance")
        >>> event.source = "UUID-XXX-XXX-XXX"
        >>> event.payload["module_name"] = "WinUpdate"
        >>> event.payload["old_status"] = 48  ## (NonCompliant, InGracetime)
        >>> event.payload["new_status"] = 32  ## (NonCompliant)
        >>> event.payload["deadline"] = 1433288387
        >>> event.payload["isolation_deadline"] = 1433461187
        >>>  
        >>> model = ModuleComplianceStatusChangeEvent(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "source_uuid": "UUID-XXX-XXX-XXX", 
             "new_status": 32, 
             "date": "2015-10-29 22:17:29", 
             "module_name": "WinUpdate", 
             "old_status": 48, 
             "event_uuid": "25d983bd-5453-48c5-84cf-1bd3a3b5c040"
        }
        >>> 
    
    """
    
    type = "ModuleComplianceStatusDidChange"
    
    def __init__(self,event=None,data=None,payload_map=None,export_map=None,
                                                    rds_export_map=None,
                                                    *args,**kwargs):
        """
        Our constructor.
        
        :param event: Our event to process.
        :type event: `:py:class:Event`
        :param payload_map: Key=>Property map for data imports
        :type payload_map: dict(string,string)
        :param export_map: Key=>Property map for data exports
        :type export_map: dict(string,string)
        """
        
        self.module_name = None
        
        if payload_map is None:
            payload_map = {
                            "module_name": None,
                            "old_status": None,
                            "new_status" : None,
                            "deadline" : "deadline_datestamp",
                            "isolation_deadline" : "isolation_datestamp",
                        }
        if export_map is None:
            export_map = {
                            "source_uuid" : "source",
                            "event_uuid" : None,
                            "module_name" : None,
                            "old_status" : None,
                            "new_status" : None,
                            "date" : "datestamp",
                            "deadline" : "deadline_datestamp",
                            "isolation_deadline" : "isolation_datestamp",
                        }   
        
        if rds_export_map is None:
            rds_export_map = { 
                        "source_uuid" : "source",
                        "event_uuid" : None,
                        "module_name" : None,
                        "status" : "new_status",
                        "deadline" : "deadline_datestamp",
                        "isolation_deadline" : "isolation_datestamp"
                    }
        
        ComplianceStatusChangeEvent.__init__(self,event=event,
                                            data=data,
                                            payload_map=payload_map,
                                            export_map=export_map,
                                            rds_export_map=rds_export_map,
                                            *args,
                                            **kwargs)



class ModuleComplianceStatusChangePublisher(BasePublisher):
    """
    Class used to orchestrate publishing AuthEvent information to RDS and S3 
    (for RedShift import)
    """
    
    name = "ModuleComplianceStatusChangePublisher"
    
    can_target_rds = True      #: We publish to RDS
    can_target_s3 = True       #: We publish to S3
    
    def commit_to_rds(self,model=None,table="module_compliance",
                                        *args,**kwargs):
        """
        Method which will commit the provided event to RDS.
        
        :param model: The event to commit
        :type model: :py:class:`ModuleComplianceStatusChangeEvent` object
        :param table: The name of the table to publish
        :type table: str
                
        """
        
        record = model
        
        if model is None:
            model = self.queued_model
        
        if model is None:
            raise AttributeError("Model is not specified, cannot commit to RDS")
        
        if table is None:
            raise AttributeError("rds_table is not specified, cannot commit to RDS")
        
        
        
        logger = logging.getLogger("commit_to_rds()")
        
        logger.debug("Commiting 'ModuleComplianceStatusChangeEvent' to RDS table:%s for source:'%s'"
                                                    % (table,model.source))
        rds = self.rds()
        
        ## Update our table
        query = ("UPDATE %s SET status = $1, date = $2,"
                "deadline = $3, isolation_deadline = $4, event_uuid = $5 WHERE source_uuid = $6 AND module_name = $7" % table)
        
        values = (model.new_status,model.datestamp,model.deadline_datestamp,
                                            model.isolation_datestamp,
                                            model.event_uuid,
                                            model.source,
                                            model.module_name)
        
        logger.log(5,"Running Query:\"\"\"{}\"\"\", {}".format(query,values))
        result = int(rds.query(query,values))
       
        if result > 0:
            logger.info("Successfully updated %s record for event:%s (%s) in RDS." % (table,model.event_uuid,model.type))
            
        else:
            query = ("INSERT INTO %s (source_uuid,event_uuid,status,date,deadline,isolation_deadline,module_name) VALUES($1,$2,$3,$4,$5,$6,$7)" % table)
            values = (model.source,model.event_uuid,model.new_status,model.datestamp,
                                            model.deadline_datestamp,
                                            model.isolation_datestamp,
                                            model.module_name)
            
            logger.log(5,"Running Query:\"\"\"{}\"\"\", {}".format(query,
                                                                    values))
            rds.query(query,values)

module_name = "compliance"   #: The name of our module, used for filtering operations
                                
event_handler = BaseEventHandler(name=module_name)  #: Our modules event handler, this MUST be 
                                    #: defined if our module is intended to be
                                    #: called.
                                    
event_handler.subject_areas = ["Compliance"]
event_handler.action_map = {"ComplianceStatusDidChange" : {
                                "obj_class":ComplianceStatusChangeEvent,
                                "rds_table" : "device_compliance",
                                "rds_key" : "source_uuid",
                                "rds_update_null" : True,
                                "s3key_prefix" : "compliance/karl_compliancestatuschange_event",
                                "archive_table" : "device_compliance",
                                "update_device" : False,
                                },
                            "ModuleComplianceStatusDidChange" : {
                                "obj_class":ModuleComplianceStatusChangeEvent,
                                "pub_class":ModuleComplianceStatusChangePublisher,
                                "rds_table" : "module_compliance",
                                "rds_key" : "source_uuid",
                                "rds_update_null" : True,
                                "s3key_prefix" : "compliance/karl_modulestatuschange_event",
                                "archive_table" : "module_compliance",
                                "update_device" : False,
                                },
                            "RemediationStarted" : {
                                "obj_class" : RemediationStartedEvent,
                                "s3key_prefix" : "compliance/karl_module_activity",
                                "rds_action" : "insert",
                                "rds_table" : "compliance_module_activity",
                                "archive_table" : "compliance_module_activity",
                                "update_device" : False,
                                },
                            "RemediationFinished" : {
                                "obj_class" : RemediationFinishedEvent,
                                "s3key_prefix" : "compliance/karl_module_activity",
                                "rds_action" : "insert",
                                "rds_table" : "compliance_module_activity",
                                "archive_table" : "compliance_module_activity",
                                "update_device" : False,
                                },
                            "EvaluationFinished" : {
                                "obj_class" : EvaluationFinishedEvent,
                                "s3key_prefix" : "compliance/karl_module_activity",
                                "rds_action" : "insert",
                                "rds_table" : "compliance_module_activity",
                                "archive_table" : "compliance_module_activity",
                                "update_device" : False,
                                },
                            "ComplianceModuleLoadEvent" : {
                                "obj_class" : ModuleLoadEvent,
                                "pub_class": ModuleLoadEventPublisher,
                                "s3key_prefix" : "compliance/karl_module_activity",
                                "archive_table" : "compliance_module_activity",
                                },
                            "ComplianceModuleUnloadEvent" : {
                                "obj_class" : ModuleUnloadEvent,
                                "pub_class": ModuleUnloadEventPublisher,
                                "s3key_prefix" : "compliance/karl_module_activity",
                                "archive_table" : "compliance_module_activity",
                                },
                            }
                

