"""
**softwareupdate** - Provides custom data handling for ACME patching events

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
from .base import BaseModel,BasePublisher,BaseEventHandler

class SoftwareUpdateEvent(BaseModel):
    """
    Class representing a SoftwareUpdate model
    """
    
    _update_identifiers = []
    
    @property
    def update_identifiers(self):
        return self._update_identifiers
    
    @update_identifiers.setter
    def update_identifiers(self,value):
        if isinstance(value,basestring):
            self._update_identifiers = value.split(",")
        else:
            self._update_identifiers = value
    
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
        
        self.update_name = None
        self.update_identifier = None
        self.module_name = None
        self.update_identifiers = None
        
        if payload_map is None:
            payload_map = {
                                "UpdateName" : "update_name",
                                "ModuleName" : "module_name",
                                "UpdateIdentifier" : "update_identifier",
                                "UpdateIdentifiers" : "update_identifiers",
        }
        
        if export_map is None:
            export_map = {
                                "update_name" : "update_name",
                                "module_name" : "module_name",
                                "update_identifier" : "update_identifier",
                                "date" : "<type=datetime>",
                                "source_uuid" : "source",
                                "type" : None,
                                "event_uuid" : None,
                        }
        
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)

class UpdateDiscoveredEvent(SoftwareUpdateEvent):
    """
    Class which represents a new update discovery. This is the only event
    for a software update that contains detailed information.
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
        
        self.update_missing = True
        self.update_size = 0
        self.current_version = None
        self.release_date = None
        self.deadline = None
        self.version = None
        self.required_version = None
        self.url = None
        self.is_exempt = None
        self.exempt_until = None
        self.kb_article = None
        self.size = None
        self.security_bulletins = None
        self.cve_ids = None
        self.reason = None
        
        SoftwareUpdateEvent.__init__(self,event=event,data=data)
        
        if payload_map is None:
            payload_map = self.payload_map
            payload_map["Identifier"] = "update_identifier"
            payload_map["Name"] = "update_name"
            payload_map["ReleaseDate"] = "<type=datetime>;release_date"
            payload_map["Deadline"] = "<type=datetime>;deadline"
            payload_map["Version"] = "version"
            payload_map["RequiredVersion"] = "required_version"
            payload_map["URL"] = "url"
            payload_map["IsExempt"] = "is_exempt"
            payload_map["ExemptUntil"] = "<type=datetime>;exempt_until"
            payload_map["KBArticle"] = "kb_article"
            payload_map["Size"] = "<type=int>;update_size"
            payload_map["SecurityBulletins"] = "security_bulletins"
            payload_map["CVEIds"] = "cve_ids"
            payload_map["Reason"] = "reason"
        
        if export_map is None:
            export_map = self.export_map
            export_map["update_missing"] = "<type=bool>;"
            export_map["release_date"] = "<type=datetime>;"
            export_map["deadline"] = "<type=datetime>;"
            export_map["version"] = None
            export_map["required_version"] = None
            export_map["url"] = None
            export_map["is_exempt"] = "<type=bool>;"
            export_map["exempt_until"] = "<type=datetime>;"
            export_map["kb_article"] = None
            export_map["size"] = "<type=int>;update_size"
            export_map["security_bulletins"] = None
            export_map["cve_ids"] = None
        
        
        
        
        ## Create export mappping for our software update fact table 
        self.reference_update_export_map = {}
        for map in ["update_identifier",
                    "update_name",
                    "module_name",
                    "release_date",
                    "version",
                    "url",
                    "kb_article",
                    "size",
                    "security_bulletins",
                    "cve_ids"]:
            if map in export_map:
                self.reference_update_export_map[map] = export_map[map]
            self.reference_update_export_map["version"] = "required_version"
        
        ## Create export mapping for our update missing measurement table
        self.device_update_export_map = {}
        for map in ["update_identifier",
                        "source_uuid",
                        "event_uuid",
                        "date",
                        "deadline",
                        "version",
                        "required_version",
                        "is_exempt",
                        "exempt_until",
                        "update_missing"]:
            if map in export_map:
                self.device_update_export_map[map] = export_map[map]

class UpdateResolvedEvent(UpdateDiscoveredEvent):
    """
    Class which represents an update resolved event.
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
                        
        UpdateDiscoveredEvent.__init__(self,event=event,data=data,
                                                    payload_map=payload_map,
                                                    export_map=export_map)
        
        self.update_missing = False
        
        ## Create export mapping for our update missing measurement table
        self.device_update_export_map = {}
        for map in ["update_identifier",
                        "source_uuid",
                        "event_uuid",
                        "date",
                        "update_missing"]:
            if map in self.export_map:
                self.device_update_export_map[map] = self.export_map[map]

class DownloadStartedEvent(SoftwareUpdateEvent):
    """
    Class representing download started  event
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
        
        if payload_map is None:
            payload_map = {
                                "UpdateName" : "update_name",
                                "ModuleName" : "module_name", 
                                "UpdateIdentifier" : "update_identifier",
                                "UpdateIdentifiers" : "update_identifiers",
                        }
        
        if export_map is None:
            export_map = {
                                "update_name" : "update_name",
                                "module_name" : "module_name",
                                "update_identifier" : "update_identifier",
                                "date" : "<type=datetime>",
                                "source_uuid" : "source",
                                "type" : None,
                                "event_uuid" : None,
                        }
        
        SoftwareUpdateEvent.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)

class DownloadFinishedEvent(SoftwareUpdateEvent):
    """
    Class representing download finished event
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
        self.download_result = None

        if payload_map is None:
            payload_map = {
                                "UpdateName": "update_name",
                                "UpdateIdentifiers" : "update_identifiers",
                                "ModuleName" : "module_name",
                                "UpdateIdentifier" : "update_identifier",
                                "Duration" : "<type=timedelta>;duration",
                                "DownloadResult" : "download_result",
        }

        if export_map is None:
            export_map = {
                                "module_name" : "module_name",
                                "update_name" : "update_name",
                                "result" : "download_result",
                                "duration" : "<type=timedelta,format=float>;",
                                "update_identifier" : "update_identifier",
                                "date" : "<type=datetime>",
                                "source_uuid" : "source",
                                "type" : None,
                                "event_uuid" : None,
                        }

        SoftwareUpdateEvent.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)

class UpdateCachedEvent(BaseModel):
    """
    Class representing caching  event
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

        self.update_name = None
        self.update_identifier = None

        if payload_map is None:
            payload_map = {
                            "UpdateName": "update_name",
                            "UpdateIdentifier" : "update_identifier",
                        }

        if export_map is None:
            export_map = {
                            "update_name" : "update_name",
                            "update_identifier" : "update_identifier",
                            "date" : "<type=datetime>",
                            "source_uuid" : "source",
                            "type" : None,
                            "event_uuid" : None,
                        }

        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)

class InstallationStartedEvent(SoftwareUpdateEvent):
    """
    Class representing installation started  event
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

        if payload_map is None:
            payload_map = {
                            "ModuleName" : "module_name",
                            "UpdateName": "update_name",
                            "UpdateIdentifier" : "update_identifier",
                            "UpdateIdentifiers" : "update_identifiers",
                        }

        if export_map is None:
            export_map = {
                            "module_name" : "module_name",
                            "update_name" : "update_name",
                            "update_identifier" : "update_identifier",
                            "date" : "<type=datetime>",
                            "source_uuid" : "source",
                            "type" : None,
                            "event_uuid" : None,
                        }


        SoftwareUpdateEvent.__init__(self,event=event,data=data,
                                                    payload_map=payload_map,
                                                    export_map=export_map)

class InstallationFinishedEvent(SoftwareUpdateEvent):
    """
    Class representing installation finished event
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
        
        self.install_result = None
        self.duration = None
        
        if payload_map is None:
            payload_map = {
                            "ModuleName" : "module_name",
                            "UpdateName" : "update_name",
                            "UpdateIdentifier" : "update_identifier",
                            "UpdateIdentifiers" : "update_identifiers",
                            "InstallResult" : "install_result",
                            "Duration" : "<type=timedelta>;duration",
        }
        
        if export_map is None:
            export_map = {
                            "module_name" : "module_name",
                            "update_name" : "update_name",
                            "result" : "install_result",
                            "update_identifier" : "update_identifier",
                            "duration" : "<type=timedelta,format=float>;",
                            "date" : "<type=datetime>",
                            "source_uuid" : "source",
                            "type" : None,
                            "event_uuid" : None,
                        }

        SoftwareUpdateEvent.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)

class UpdateActivityPublisher(BasePublisher):
    """
    Class used to orchestrate publishing information to RDS, S3, and Redshift
    This class extends our :py:class:`Basepublisher` class to provide the 
    ability to de-collate a single event with multiple identifiers (via
    model.update_identifiers), and creates a separate record for each.
    """

    name = "UpdateActivityPublisher"

    def commit_to_rds(self,model=None,table=None,
                                        *args,**kwargs):
        """
        Method which will commit the provided event to RDS. If multiple 
        identifiers are specified in our model via update_identifiers,
        we will commit a row for each identifier.
        
        """
        
        identifiers_list = model.update_identifiers
        
        did_publish = False
        if model.update_identifier:
            did_publish = True
            BasePublisher.commit_to_rds(self,model,table,*args,**kwargs)
        
        if identifiers_list:
            for identifier in identifiers_list:
                did_publish = True
                model.update_identifier = identifier
                BasePublisher.commit_to_rds(self,model,table,*args,**kwargs)
        
        if not did_publish:
            BasePublisher.commit_to_rds(self,model,table,*args,**kwargs)
        
    def commit_to_s3(self,model=None,models=None,s3key_prefix=None,
                                                        process_callbacks=None,
                                                        *args,**kwargs):
        """
        Method which will commit the provided event to S3 to facilitate delivery
        to Redshift. If multiple identifiers are specified in our model via 
        update_identifiers, we will commit a row for each identifier.
        
        """
        
        logger = logging.getLogger(self.name)
            
        starttime = datetime.datetime.utcnow()
        
        uses_local_reference = False
        
        if models is None and model is None:
            uses_local_reference = True
            models = self.queued_models
            model = self.queued_model
        elif models is None:
            models = []
        
        if model is not None:
            models.append(model)
        
        if process_callbacks is None:
            process_callbacks = True
        
        my_models = []

        for my_model in models:
            
            identifiers_list = my_model.update_identifiers
            
            did_publish = False
            if my_model.update_identifier:
                my_models.append(my_model)
                did_publish = True

            if identifiers_list:
                for identifier in identifiers_list:
                    did_publish = True
                    new_model = my_model.__class__()
                    new_model.event = my_model.event
                    new_model.key_map["type"] = None
                    new_model.load_data(data=my_model.to_dict())
                    new_model.update_identifier = identifier
                    new_model.update_identifiers = []
                    new_model.process_callback = my_model.process_callback
                    my_models.append(new_model)
        
            if not did_publish:
                my_models.append(my_model)
            
        
        logger.debug("Commiting {} records for {} models!".format(
                                                        len(my_models),
                                                        len(models)))

        if my_models:
            if uses_local_reference:
                self.queued_models = my_models
                my_models = None
            
            BasePublisher.commit_to_s3(self,models=my_models,
                                s3key_prefix=s3key_prefix,
                                process_callbacks=False,*args,**kwargs)
            
            endtime = datetime.datetime.utcnow()
            
            ## Process callbacks for our original models
            if uses_local_reference:
                for model in models:
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

                    

class UpdateDiscoveredResolvedPublisher(BasePublisher):
    """
    Class used to orchestrate the publishing of event data provided
    by UpdateDiscovered and UpdateResolved events..
    """
    
    name = "UpdateDiscoveredResolvedPublisher"
    software_update_table = "software_update"
    software_update_status_table = "software_update_status"
    update_activity_publisher = None
    
    can_target_rds = True
    can_target_s3 = True
    
    def build_update_activity_publisher(self):
        """
        Method which generates an update_activity publisher to facilitate
        multi-targeted publishing.
        """
        
        publisher_keys = self.key_map.keys()
        publisher_map = {}
        try:
            publisher_keys.remove("name")
        except ValueError:
            pass
        
        publisher_map = self.key_map_for_keys(publisher_keys)
        
        if self.update_activity_publisher is None:
            update_activity_publisher = UpdateActivityPublisher(karl=self)
            
            update_activity_publisher.load_dict(self.to_dict(),
                                                        key_map=publisher_map)
        else:
            update_activity_publisher = self.update_activity_publisher
            update_activity_publisher.load_dict(self.to_dict(),
                                                        key_map=publisher_map)
                                                        
        self.update_activity_publisher = update_activity_publisher
        
        return update_activity_publisher
        
    
    def commit_to_rds(self,model=None,table=None,
                                        *args,**kwargs):
        """
        Method which will commit the provided event to RDS. Specifically,
        this method is responsible for populating tables 'software_update'
        and 'software_update_status', in addition to 'software_update_activity' 
        
        """
        
        ## Publish our event using our custom UpdateActivityPublisher class
        
        
        update_activity_publisher = self.build_update_activity_publisher()
        
        generic_event = SoftwareUpdateEvent()
        
        update_activity_publisher.commit_to_rds(model=model,table=table,
                                        export_map=generic_event.export_map,
                                        *args,
                                        **kwargs)
        
        ## Update our software_update table
        try:
            key_map = model.reference_update_export_map
        except AttributeError:
            key_map = None
        
        key_names = ["update_identifier"]
        
        ## Note: we store 'required_version' in software_update table as
        ## 'version'
        if model.required_version is not None: 
            key_names.append("version")
               
        BasePublisher.commit_to_rds(self,model=model,
                                        table=self.software_update_table,
                                        key_names=key_names,
                                        export_map=key_map,
                                        action="update",
                                        *args,
                                        **kwargs)
        
        ## Update our software_update_status table
        try:
            key_map = model.device_update_export_map
        except AttributeError:
            key_map = None
        
        key_names=["update_identifier", "source_uuid"]
               
        BasePublisher.commit_to_rds(self,model=model,
                                        table=self.software_update_status_table,
                                        key_names=key_names,
                                        export_map=key_map,
                                        action="update",
                                        *args,
                                        **kwargs)
    
    def commit_to_s3(self,model=None,models=None,s3key_prefix=None,*args,**kwargs):
        """
        Method which will commit the provided event to S3 for ingestion into
        Redshift. Specifically, this method is responsible for uploading files
        to populating Redshift tables 'software_update_activity' and 
        'software_update_data'        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        ## Publish our event using our custom UpdateActivityPublisher class,
        ## This will populate the standard software_update_activity Redshift
        ## table.
        update_activity_publisher = self.build_update_activity_publisher()
                
        logger.debug("Committing to s3 using publisher:{} on behalf of:{}".format(
                                        update_activity_publisher.derived_name,
                                        self.derived_name))
            
        generic_event = SoftwareUpdateEvent()
        
        if model is None:
            model = self.queued_model
        
        if models is None:
            models = self.queued_models
        
        update_activity_publisher.commit_to_s3(model=model,models=models,
                                        s3key_prefix=s3key_prefix,
                                        export_map=generic_event.export_map,
                                        process_callbacks=False,
                                        *args,
                                        **kwargs)
        
        ## Publish our event containing data for our software_update_data
        ## Redshift table
        logger.debug("Committing to s3 using publisher:{}".format(self.derived_name))
                                            
        BasePublisher.commit_to_s3(self,model=model,models=models,
                                    s3key_prefix="event/software_update_data",
                                    *args,**kwargs)
        
       


####start of main
module_name = "softwareupdate"   #: The name of our module, used for filtering operations

event_handler = BaseEventHandler(name=module_name)  #: Our modules event handler, this MUST be
                                    #: defined if our module is intended to be
                                    #: called.

event_handler.subject_areas = ["Compliance","UpdateActivity"]
event_handler.action_map = {
                "UpdateDiscovered" : {
                        "obj_class" : UpdateDiscoveredEvent,
                        "pub_class" : UpdateDiscoveredResolvedPublisher,
                        "s3key_prefix" : "event/software_update_activity",
                        "rds_action" : "insert", 
                        "rds_table" : "software_update_activity",
                        "archive_table" : "software_update_activity",
                        "update_device" : False,
                        },
                "DownloadStarted" : {
                        "obj_class" : DownloadStartedEvent,
                        "pub_class" : UpdateActivityPublisher,
                        "s3key_prefix" : "event/software_update_activity",
                        "rds_action" : "insert", 
                        "rds_table" : "software_update_activity",
                        "archive_table" : "software_update_activity",
                        "update_device" : False,
                        },
                "DownloadFinished" : {
                        "obj_class" : DownloadFinishedEvent,
                        "pub_class" : UpdateActivityPublisher,
                        "s3key_prefix" : "event/software_update_activity",
                        "rds_action" : "insert", 
                        "rds_table" : "software_update_activity",
                        "archive_table" : "software_update_activity",
                        "update_device" : False,
                        },
                "UpdateCached" : {
                        "obj_class" : UpdateCachedEvent,
                        "s3key_prefix" : "event/software_update_activity",
                        "rds_action" : "insert",
                        "rds_table" : "software_update_activity",
                        "archive_table" : "software_update_activity",
                        "update_device" : False,
                        },
                "CachedUpdateCopyError" : {
                        "obj_class" : UpdateCachedEvent,
                        "s3key_prefix" : "event/software_update_activity",
                        "rds_action" : "insert",
                        "rds_table" : "software_update_activity",
                        "archive_table" : "software_update_activity",
                        "update_device" : False,
                        },
                "CachedUpdateCopyCrashed" : {
                        "obj_class" : UpdateCachedEvent,
                        "s3key_prefix" : "event/software_update_activity",
                        "rds_action" : "insert",
                        "rds_table" : "software_update_activity",
                        "archive_table" : "software_update_activity",
                        "update_device" : False,
                        },
                "InstallationStarted" : {
                        "obj_class" : InstallationStartedEvent,
                        "pub_class" : UpdateActivityPublisher,
                        "s3key_prefix" : "event/software_update_activity",
                        "rds_action" : "insert",
                        "rds_table" : "software_update_activity",
                        "archive_table" : "software_update_activity",
                        "update_device" : False,
                        },
                "InstallationFinished" : {
                        "obj_class" : InstallationFinishedEvent,
                        "pub_class" : UpdateActivityPublisher,
                        "s3key_prefix" : "event/software_update_activity",
                        "rds_action" : "insert",
                        "rds_table" : "software_update_activity",
                        "archive_table" : "software_update_activity",
                        "update_device" : False,
                        },
                "UpdateResolved" : {
                        "obj_class" : UpdateResolvedEvent,
                        "pub_class" : UpdateDiscoveredResolvedPublisher,
                        "s3key_prefix" : "event/software_update_activity",
                        "rds_action" : "insert", 
                        "rds_table" : "software_update_activity",
                        "archive_table" : "software_update_activity",
                        "update_device" : False,
                        },
                "UpdateDiscoveredResolvedArchiver" : {
                        "obj_class" : UpdateDiscoveredEvent,
                        "s3key_prefix" : "event/software_update_data",
                        "rds_action" : "None", 
                        "archive_table" : "software_update_data",
                        "update_device" : False,
                        },
                
            }

