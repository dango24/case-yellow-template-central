#!/apollo/sbin/envroot $ENVROOT/bin/python

"""
**acme** - Provides custom data handling for ACME subject area

.. module:: **acme** Provides data handling for ACME subject area
    :platform: RHEL5
    :synopsis: Module plugin that provides ACME data and publishing models.
   
.. codeauthor:: Beau Hunter <beauhunt@amazon.com>



"""

import datetime
import logging
import json

import pykarl.core
from pykarl.modules.base import BaseModel,BasePublisher,BaseEventHandler
import pg
HDD_TYPE     = 0
SSD_TYPE     = 1
TYPE_UNKNOWN = 2

class OwnerReportEvent(BaseModel):
    """
    Class representing an ownership reporting event.
    
    :Example:

        >>> event = Event(type="ReportOwner",subject_area="ACME")
        >>> event.source = "UUID-XXX-XXX-XXX"
        >>> event.payload["Owner"] = "Mr. Bigglesworth"
        >>>  
        >>> model = OwnerReportEvent(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "source_uuid": "UUID-XXX-XXX-XXX", 
             "owner": "Mr. Bigglesworth", 
             "event_uuid": "11bdc456-f10a-499d-bff3-3ceaa539d64a"
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
        
        self.owner = None
        
        if payload_map is None:
            payload_map = {
                            "Owner":"owner",
                        }
        if export_map is None:
            export_map = {
                            "source_uuid" : "source",
                            "event_uuid" : None,
                            "owner" : None,
                        }     
        
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)

class OwnerEventPublisher(BasePublisher):
    """
    Class used to orchestrate publishing ownership information to RDS, S3, and Redshift
    """
    
    name = "OwnerEventPublisher"
    
    can_target_rds = True
    can_target_s3 = False 
    
    def commit_to_rds(self,model=None,table="device_instance",
                                        device_table="device_instance"):
        """
        Method which will commit the provided event to RDS.
        In this case, we simply update the 'username' value on the 
        'device_instance' table in RDS.
        
        :param model: The event to commit
        :type model: :py:class:`OwnerReportEvent` object
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
        
        logger.debug("Commiting event %s (%s) to RDS table:%s for source:'%s'"
                            % (model.event_uuid,model.type,table,model.source))
        
        rds = self.rds()
        
        ## Attempt to update our device record        
        try:
            last_seen = model.event.submit_date.strftime(pykarl.core.DATE_FORMAT)
        except (AttributeError) as exp:
            last_seen = datetime.datetime.utcnow().strftime(pykarl.core.DATE_FORMAT) 
        
        query = ("UPDATE %s SET username = $1, last_seen = $2 WHERE uuid = $3" % table)
        values = (model.owner,last_seen,model.source)
        
        logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
        result = int(rds.query(query,values))
        
        if result > 0:
            logger.info("Successfully updated record for event:%s (%s) in RDS." % (model.event_uuid,model.type))
            
        else:
            try:
                device_id = rds.query("INSERT INTO %s (uuid, username, first_seen, last_seen) "
                        "VALUES($1,$2,$3,$4) RETURNING instance_id" % device_table,(
                                                        model.source,
                                                        model.owner,
                                                        model.datestamp,
                                                        last_seen))
            except pg.ProgrammingError as exp:
                logger.warning("Failed to update device record: {e}".format(e=exp))

class HeartBeat(BaseModel):
    """
    Class representing a heartbeat from ACME. Heartbeats have no 
    payload. It's primary purpose is to update the `last seen` paraameter
    on our device record. Historic archival will be performed by 
    our standard archiver.
    
    :Example:

        >>> event = Event(type="HeartBeat",subject_area="ACME")
        >>> event.uuid = "E-UUID-XXX-XXX-YYY"
        >>> event.source = "S-UUID-XXX-XXX-XXX"
        >>>
        >>>  
        >>> model = HeartBeat(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "uuid": "S-UUID-XXX-XXX-XXX", 
        }
    
    """
    
    def __init__(self,event=None,data=None,payload_map=None,export_map=None,
                                                    rds_export_map=None,
                                                    redshift_export_map=None):
        """
        Our constructor.
        
        :param event: Our event to process.
        :type event: `:py:class:Event`
        :param payload_map: Key=>Property map for data imports
        :type payload_map: dict(string,string)
        :param export_map: Key=>Property map for data exports
        :type export_map: dict(string,string)
        :param rds_export_map: RDS specific export mappings
        :type rds_export_map: dict(string,string)
        :param redshift_export_map: RDS specific export mappings
        :type redshift_export_map: dict(string,string)
        """
        
       
        if rds_export_map is None:
            rds_export_map = {
                    "uuid" : None,
                    "last_seen" : "submit_datestamp",
        
            }
        
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                        export_map=export_map,
                                        rds_export_map=rds_export_map,
                                        redshift_export_map=redshift_export_map)

class SystemReport(BaseModel):
    """
    Class representing an ownership reporting event.
    
    :Example:

        >>> event = Event(type="SystemInfo",subject_area="ACME")
        >>> event.uuid = "E-UUID-XXX-XXX-YYY"
        >>> event.source = "S-UUID-XXX-XXX-XXX"
        >>> event.payload["platform"] = "Mac"
        >>> event.payload["mac_address"] = "3c:15:c2:de:04:80"
        >>> event.payload["hardware_id"] = "HW-UUID-XXX-XXX-ZZZ"
        >>> event.payload["hostname"] = "mycoolhost"
        >>> event.payload["physical_memory"] = 549755813888
        >>> event.payload["cpu_type"] = "Intel(R) Core(TM) i5-4590"
        >>> event.payload["cpu_cores"] = 2
        >>> event.payload["ssd"] = 1
        >>> event.payload["hdd"] = 0
        >>>  
        >>> model = SystemReport(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
            "uuid": "S-UUID-XXX-XXX-XXX",
            "physical_memory": 549755813888,
            "hostname": "mycoolhost",
            "cpu_cores": 2,
            "platform": "Mac",
            "hdd": 0,
            "cpu_type": "Intel(R) Core(TM) i5-4590",
            "date": "2017-04-30 22:43:41",
            "ssd": 1,
            "hardware_uuid": "HW-UUID-XXX-XXX-ZZZ",
            "event_uuid": "E-UUID-XXX-XXX-YYY"
        }
        >>> 

    
    """
    
    mac_address = None      #: Primary MAC address on the system
    harware_uuid = None     #: Unique identifier for our hardware
    serial_number = None    #: Serial number of the mother board
    make = None             #: The manufacturer of the system
    model = None            #: The model label of the system
    asset_tag = None        #: The asset tag of the system
    system_type = None      #: The type of system. Common values
                            #: * Laptop
                            #: * Desktop
                            #: * Tablet
                            #: * Server
                            #: * Appliance
    architecture = None     #: System Architecture (x86)
    hostname = None         #: Hostname of the system
    platform = None         #: The OS platform
                            #: * Windows
                            #: * macOS
                            #: * Linux
   
    platform_version = None #: version of Operating System
    physical_memory = None  #: Physical memory of system
    cpu_type = None         #: Type of cpu example- Intel(R) Core(TM) i5-4590
    cpu_cores = None        #: Number of cpu cores on system
    ssd = None              #: Does system has ssd(solid state drivie) or not. If system has ssd present then 1 else 0
                            #: It will help if querying for ssds, check if ssd present then only query disk_information table
    hdd = None              #: Does system has hdd(hard disk drive) or not. If system has hdd present then 1 else 0
                            #: It will help if querying for hdds, check if hdd present then only query disk_information table

    def __init__(self,event=None,data=None,payload_map=None,export_map=None,
                                                    rds_export_map=None,
                                                    redshift_export_map=None):
        """
        Our constructor.
        
        :param event: Our event to process.
        :type event: `:py:class:Event`
        :param payload_map: Key=>Property map for data imports
        :type payload_map: dict(string,string)
        :param export_map: Key=>Property map for data exports
        :type export_map: dict(string,string)
        :param rds_export_map: RDS specific export mappings
        :type rds_export_map: dict(string,string)
        :param redshift_export_map: RDS specific export mappings
        :type redshift_export_map: dict(string,string)
        """
        
        self.type = "SystemReport"
        
        if payload_map is None:
            payload_map = {
                            "mac_address": None,
                            "hardware_id" : "hardware_uuid",
                            "serial_number" : None,
                            "make" : None,
                            "model" : None,
                            "asset_tag" : None,
                            "system_type" : None,
                            "architecture" : None,
                            "hostname" : None,
                            "platform" : None,
			    "platform_version" : None,
                            "physical_memory" : None,
                            "cpu_type" : None,
                            "cpu_cores" : None,
                            "ssd" : None,
                            "hdd" : None
                        }
        if export_map is None:
            export_map = {
                            "uuid" : "source",
                            "event_uuid" : None,
                            "hardware_uuid" : None,
                            "serial_number" : None,
                            "make" : None,
                            "model" : None,
                            "asset_tag" : None,
                            "system_type" : None,
                            "architecture" : None,
                            "hostname" : None,
                            "platform" : None,
			    "platform_version" : None,
                            "date" : "datestamp",
                            "physical_memory" : None,
                            "cpu_type" : None,
                            "cpu_cores" : None,
                            "ssd" : None,
                            "hdd" : None,
                        }     
        
        if rds_export_map is None:
            rds_export_map = {
                    "uuid" : "hardware_uuid",
                    "serial_number" : None,
                    "make" : None,
                    "model" : None,
                    "asset_tag" : None,
                    "system_type" : None,
                    "architecture" : None,
                    "last_seen" : "submit_datestamp",
                    "mac_address" : None,
                    "physical_memory" : None,
                    "cpu_type" : None,
                    "cpu_cores" : None,
                    "ssd" : None,
                    "hdd" : None,
                    }
        
        
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                        export_map=export_map,
                                        rds_export_map=rds_export_map,
                                        redshift_export_map=redshift_export_map)

class DiskInformationUpdateEvent(BaseModel):
    """
    Class representing system's disk information update event.

    :Example:

        >>> event = Event(type="UpdateDiskInformation",subject_area="ACME")
        >>> event.payload["hdd_sizes"]        = "274877906944,137438953472"
        >>> event.payload["ssd_sizes"]        = "1099511627776"
        >>> event.payload["total_disk_sizes"] = "68719476736,137438953472"
        >>> event.source = "564D6931-CFAE-8346-969D-261706A20897"
        >>>
        >>> model = DiskInformationUpdateEvent(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "date": "2017-05-07 21:00:30",
             "source_uuid": "564D6931-CFAE-8346-969D-261706A20896",
             "event_uuid": "105d4d36-eb8c-4aea-bac4-43fbe7a414d9"
        }
        >>>
        
        ## Fields hdd_sizes, ssd_sizes and total_disk_sizes are processed by custom publisher UpdateDiskInformationPublisher to populate drive_type and size_bytes.
        ## In above example hdd_sizes, ssd_sizes and total_disk_sizes will processed and results in 5 rows of disk_information table by custom publisher UpdateDiskInformationPublisher.
        ## Row 1: [source_uuid: 564D6931-CFAE-8346-969D-261706A20896] [drive_type: 0] [size_bytes: 274877906944]
        ## Row 2: [source_uuid: 564D6931-CFAE-8346-969D-261706A20896] [drive_type: 0] [size_bytes: 137438953472] 
        ## Row 3: [source_uuid: 564D6931-CFAE-8346-969D-261706A20896] [drive_type: 1] [size_bytes: 1099511627776]
        ## Row 4: [source_uuid: 564D6931-CFAE-8346-969D-261706A20896] [drive_type: 2] [size_bytes: 68719476736]
        ## Row 5: [source_uuid: 564D6931-CFAE-8346-969D-261706A20896] [drive_type: 2] [size_bytes: 137438953472]


    """

    _hdd_sizes        = []    #: Sizes of all hdd's(Hard Disk Drives) on system.
    _ssd_sizes        = []    #: Sizes of all ssd's(Solid State Drives) on system.
    _total_disk_sizes = []    #: Sizes of all unknown disk types which cannot be classified as hdd or ssd
                              #: Also in case of windows 7 when could not determine type of disk it will be saved via 
                              #: total_disk_sizes with drive type type 2

    @property
    def hdd_sizes(self):
        return self._hdd_sizes

    @hdd_sizes.setter
    def hdd_sizes(self,value):
        if isinstance(value,basestring):
            self._hdd_sizes = value.split(",")
        else:
            self._hdd_sizes = value

    @property
    def ssd_sizes(self):
        return self._ssd_sizes

    @ssd_sizes.setter
    def ssd_sizes(self,value):
        if isinstance(value,basestring):
            self._ssd_sizes = value.split(",")
        else:
            self._ssd_sizes = value

    @property
    def total_disk_sizes(self):
        return self._total_disk_sizes

    @total_disk_sizes.setter
    def total_disk_sizes(self,value):
        if isinstance(value,basestring):
            self._total_disk_sizes = value.split(",")
        else:
            self._total_disk_sizes = value

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

        self.disk_size        = None    #: Size of disk on system in bytes.
        self.drive_type       = None    #: Type of disk drive example- hdd(Hard Disk Drive), ssd(Solid Disk Drive) etc.
        self.hdd_sizes        = None    #: List of sizes of all hdds in system.
        self.ssd_sizes        = None    #: List of sizes of all ssds in system.
        self.total_disk_sizes = None    #: List of sizes of unknown disk types, which cannot be classified as hdd or ssd.
        self.source_uuid      = None    #: Device instance uuid of system which contains disk.


        if payload_map is None:
            payload_map = {
                                "hdd_sizes"        : "hdd_sizes",
                                "ssd_sizes"        : "ssd_sizes",
                                "total_disk_sizes" : "total_disk_sizes",
                                "source_uuid"      : "source",
                                "event_uuid"       : "event_uuid"
        }

        if export_map is None:
            export_map = {
                                "drive_type"  : "drive_type",
                                "size_bytes"  : "disk_size",
                                "date"        : "<type=datetime>",
                                "source_uuid" : "source",
                                "event_uuid" : None,
                        }

        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                        export_map=export_map)

class UpdateDiskInformationPublisher(BasePublisher):
    """
    Class used to orchestrate publishing information to RDS, S3, and Redshift
    This class extends our :py:class:`Basepublisher` class to provide the
    ability to de-collate a single event with multiple disk sizes
    (via model.hdd_sizes, model.ssd_sizes, total_disk_sizes), and creates a separate record for each.
    """

    name = "UpdateDiskInformationPublisher"

    def commit_to_rds(self,model=None,table=None,
                                        *args,**kwargs):
        """
        Method which will commit the provided event to RDS. If multiple
        disk sizes are specified in our model via hdd_sizes, ssd_sizes 
        and total_disk_sizes(for unknown types) we will commit a row for each disk size.

        """

        hdd_list        = model.hdd_sizes
        ssd_list        = model.ssd_sizes
        total_size_list = model.total_disk_sizes

        hdd_did_publish        = False
        ssd_did_publish        = False
        total_size_did_publish = False

        #Before adding new data removing old disk enteries from db
        BasePublisher.remove_rds_entry(self,model,key_name="source_uuid",table=table,batch_deletion=True)

        if hdd_list:
            for disk in hdd_list:
                hdd_did_publish = True
                model.disk_size = disk
                model.drive_type = HDD_TYPE
                BasePublisher.commit_to_rds(self,model,table,*args,**kwargs)

        if ssd_list:
            for disk in ssd_list:
                ssd_did_publish = True
                model.disk_size = disk
                model.drive_type = SSD_TYPE
                BasePublisher.commit_to_rds(self,model,table,*args,**kwargs)

        if total_size_list:
            for disk in total_size_list:
                total_size_did_publish = True
                model.disk_size = disk
                model.drive_type = TYPE_UNKNOWN
                BasePublisher.commit_to_rds(self,model,table,*args,**kwargs)

        if not hdd_did_publish and not ssd_did_publish and not total_size_did_publish:
            BasePublisher.commit_to_rds(self,model,table,*args,**kwargs)

    def commit_to_s3(self,model=None,models=None,s3key_prefix=None,
                                                        process_callbacks=None,
                                                        *args,**kwargs):
        """
        Method which will commit the provided event to S3 to facilitate delivery
        to Redshift. If multiple disk sizes are specified in our model via
        hdd_sizes, ssd_sizes and total_disk_sizes(for unknown types), we will 
        commit a row for each disk size.

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

            hdd_list        = my_model.hdd_sizes
            ssd_list        = my_model.ssd_sizes
            total_size_list = my_model.total_disk_sizes

            hdd_did_publish        = False
            ssd_did_publish        = False
            total_size_did_publish = False
            

            if hdd_list:
                for disk in hdd_list:
                    hdd_did_publish = True
                    new_model = my_model.__class__()
                    new_model.event = my_model.event
                    new_model.key_map["type"] = None
                    new_model.load_data(data=my_model.to_dict())
                    new_model.disk_size = disk
                    new_model.drive_type = HDD_TYPE
                    new_model.hdd_sizes = []
                    new_model.process_callback = my_model.process_callback
                    my_models.append(new_model)

            if ssd_list:
                for disk in ssd_list:
                    ssd_did_publish = True
                    new_model = my_model.__class__()
                    new_model.event = my_model.event
                    new_model.key_map["type"] = None
                    new_model.load_data(data=my_model.to_dict())
                    new_model.disk_size = disk
                    new_model.drive_type = SSD_TYPE
                    new_model.ssd_sizes = []
                    new_model.process_callback = my_model.process_callback
                    my_models.append(new_model)

            if total_size_list:
                for disk in total_size_list:
                    total_size_did_publish = True
                    new_model = my_model.__class__()
                    new_model.event = my_model.event
                    new_model.key_map["type"] = None
                    new_model.load_data(data=my_model.to_dict())
                    new_model.disk_size = disk
                    new_model.drive_type = TYPE_UNKNOWN 
                    new_model.total_disk_sizes = []
                    new_model.process_callback = my_model.process_callback
                    my_models.append(new_model)

            if not hdd_did_publish and not ssd_did_publish and not total_size_did_publish:
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

class ManagementReport(BaseModel):
    """
    Class representing a management report for a device.
    
    :Example:

        >>> event = Event(type="ManagementReport",subject_area="ACME")
        >>> event.uuid = "E-UUID-XXX-XXX-YYY"
        >>> event.source = "S-UUID-XXX-XXX-XXX"
        >>> event.payload["management_status"] = "FullyManaged"
        >>> event.payload["acme_version"] = "1.2.0"
        >>>  
        >>> model = ManagementReport(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "uuid": "S-UUID-XXX-XXX-XXX", 
             "management_status": "FullyManaged", 
             "acme_version": "1.2.0", 
             "event_uuid": "E-UUID-XXX-XXX-YYY"
        }
        >>> 

    
    """
    
    management_status = None        #: Our management status
    acme_version = None             #: The running version of ACME on this system.
    
    def __init__(self,event=None,data=None,payload_map=None,export_map=None,
                                                    rds_export_map=None,
                                                    redshift_export_map=None):
        """
        Our constructor.
        
        :param event: Our event to process.
        :type event: `:py:class:Event`
        :param payload_map: Key=>Property map for data imports
        :type payload_map: dict(string,string)
        :param export_map: Key=>Property map for data exports
        :type export_map: dict(string,string)
        :param rds_export_map: RDS specific export mappings
        :type rds_export_map: dict(string,string)
        :param redshift_export_map: RDS specific export mappings
        :type redshift_export_map: dict(string,string)
        """
        
        self.type = "ManagementReport"
        
        if payload_map is None:
            payload_map = {
                            "management_status": None,
                            "acme_version" : None,
                        }
        if export_map is None:
            export_map = {
                            "source_uuid" : "source",
                            "event_uuid" : None,
                            "date" : "datestamp",
                            "management_status": None,
                            "acme_version" : None,
                        }     
        
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                        export_map=export_map,
                                        rds_export_map=rds_export_map,
                                        redshift_export_map=redshift_export_map)

class HeartBeatPublisher(BasePublisher):
    """
    Class used to orchestrate publishing information to RDS, S3, and Redshift
    """
    
    name = "HeartBeatPublisher"
        
    can_target_rds = True
    can_target_s3 = False 
    
    buffer_threshold = datetime.timedelta(minutes=30)  #: If our device latency is greater
                                                    #: than this threshold, then we will
                                                    #: buffer the request, rather
                                                    #: than submit it immediately.
                                                    
    buffer_flush_threshold = datetime.timedelta(minutes=5)   #: If we have a buffer, we will flush it
                                                             #: to the database after recovering to 
                                                             #: this threshold
    
    heartbeat_buffer = {}       #: Dictionary, keyed by device source, that contains
                                #: the most recent heartbeat for a given device.
    
    
    def process_model(self,model=None,targets=None):
        """
        Method which will process our heartbeat event, if this heartbeat is 
        currently latent, we will buffer the request and wait for more 
        heartbeat events from the device prior to updating device entries in RDS.
        This prevents excessive IO caused by devices which are offline
        for prolonged periods of time.
        """
        
        logger = logging.getLogger("{}:process_model()".format(self.name))
        
        now = datetime.datetime.utcnow()
        
        if model.date > now - self.buffer_threshold:
            BasePublisher.process_model(self,model=model,targets=targets)
            if model.source in self.heartbeat_buffer.keys():
                del(self.heartbeat_buffer[model.source])
        else:
            self.heartbeat_buffer[model.source] = model
            
            try:
                if model.process_callback is not None:
                    options = {"process_date" : now,
                                "process_time" : datetime.timedelta(seconds=0),
                                "status" : "Buffered"}
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
        
        
        buffer_depth = len(self.heartbeat_buffer)
        if model.date > now - self.buffer_flush_threshold and buffer_depth:
            logger.info("Flushing buffered heartbeats for {} devices.".format(buffer_depth))
            for buffered_model in self.heartbeat_buffer.values():
                buffered_model.process_callback = None
                del(self.heartbeat_buffer[buffered_model.source])
                BasePublisher.process_model(self,model=buffered_model,
                                                            targets=targets)

class SystemReportEventPublisher(BasePublisher):
    """
    Class used to orchestrate publishing information to RDS, S3, and Redshift
    """
    
    name = "SystemReportEventPublisher"
        
    can_target_rds = True
    can_target_s3 = False 
    
    def commit_to_rds(self,model=None,table=None,export_map=None):
        """
        Method which will commit the provided event to RDS.
        In this case, we simply update the 'username' value on the 
        'device_instance' table in RDS.
        
        :param model: The event to commit
        :type model: :py:class:`SystemReport` object
        :param table: The name of the table to publish
        :type table: str
        :param device_table: The name of the device table to publish
        :type device_table: str
        
        """
        
        BasePublisher.commit_to_rds(self,model=model,table=table,export_map=export_map)
        return
        
        hardware_device_map = [ ]
        
        
        if model is None:
            model = self.queued_model
        
        if model is None:
            raise AttributeError("Model is not specified, cannot commit to RDS")
        
        if table is None:
            raise AttributeError("rds_table is not specified, cannot commit to RDS")
      
        logger = logging.getLogger("commit_to_rds()")
        
        logger.debug("Commiting '%s' to RDS table:%s for source:'%s'"
                                            % (model.type,table,model.source))
        
        rds = self.rds()
        
        try:
            last_seen = model.event.submit_date.strftime(pykarl.core.DATE_FORMAT)
        except (AttributeError) as exp:
            last_seen = datetime.datetime.utcnow().strftime(pykarl.core.DATE_FORMAT) 
        
        ## Attempt to update our hardware record
        query = ("UPDATE %s SET mac_address = $1, make = $2, model = $3, last_seen = $4, harware_uuid = $5 WHERE uuid = $6" % table)
        values = (model.hostname,model.platform,model.hardware_uuid,last_seen,model.hardware_uuid,model.source)
        
        logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
        result = int(rds.query(query,values))
        
        if result > 0:
            logger.info("Successfully updated record for event:%s (%s) in RDS." % (model.event_uuid,model.type))
            
        else:
            try:
                device_id = rds.query("INSERT INTO %s (uuid, hostname, platform, first_seen, last_seen, hardware_uuid) "
                        "VALUES($1,$2,$3,$4,$5,$6) RETURNING instance_id" % table,
                                                        (model.source,
                                                        model.hostname,
                                                        model.platform,
                                                        model.datestamp,
                                                        last_seen,
                                                        model.hardware_uuid))
            except pg.ProgrammingError as exp:
                logger.warning("Failed to update device record: {e}".format(e=exp))
        
        
        ## Attempt to update our instance record
        query = ("UPDATE %s SET hostname = $1, platform = $2, hardware_uuid = $3, last_seen = $4, harware_uuid = $5 WHERE uuid = $6" % table)
        values = (model.hostname,model.platform,model.hardware_uuid,last_seen,model.hardware_uuid,model.source)
        
        logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
        result = int(rds.query(query,values))
        
        if result > 0:
            logger.info("Successfully updated record for event:%s (%s) in RDS." % (model.event_uuid,model.type))
            
        else:
            try:
                device_id = rds.query("INSERT INTO %s(uuid, hostname, platform, first_seen, last_seen, hardware_uuid) "
                        "VALUES($1,$2,$3,$4,$5,$6) RETURNING instance_id" % table,
                                                        (model.source,
                                                        model.hostname,
                                                        model.platform,
                                                        model.datestamp,
                                                        last_seen,
                                                        model.hardware_uuid))
            except pg.ProgrammingError as exp:
                logger.warning("Failed to update device record: {e}".format(e=exp))

class LocalPasswordRotationEvent(BaseModel):
    """
    Class representing a local account password rotation event.
    
    :Example:

        >>> event = Event(type="LocalPasswordRotation",subject_area="ACME")
        >>> event.source = "UUID-XXX-XXX-XXX"
        >>> event.payload["status"] = "Success"
        >>> event.payload["password_length"] = 16
        >>> event.payload["secure_blob"] = "aefbadubjca1......"
        >>> event.payload["account_name"] = "Administrator"
        >>> event.payload["rotation_date"] = 1431384914
        >>>  
        >>> model = LocalPasswordRotationEvent(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "source_uuid": "UUID-XXX-XXX-XXX", 
             "status": "Success",
             "password_length" : 16,
             "secure_blob" : "aefbadubjca1......",
             "event_uuid": "11bdc456-f10a-499d-bff3-3ceaa539d64a"
        }
        >>> 
    
    """
    
    account_name = None     #: Name of the account rotated
    status = None           #: The status of our rotation attempt
    password_length = None  #: Length of the password generated
    secure_blob  = None     #: Represents our encrypted secret
    rotation_date = None    #: Variable representing The date we last rotated
    error_string = None     #: Error string (if applicable)
    
    def __init__(self,event=None,data=None,payload_map=None,export_map=None,
                                                    rds_export_map=None,
                                                    redshift_export_map=None):
        """
        Our constructor.
        
        :param event: Our event to process.
        :type event: `:py:class:Event`
        :param payload_map: Key=>Property map for data imports
        :type payload_map: dict(string,string)
        :param export_map: Key=>Property map for data exports
        :type export_map: dict(string,string)
        :param rds_export_map: RDS specific export mappings
        :type rds_export_map: dict(string,string)
        :param redshift_export_map: Redshift specific export mappings
        :type redshift_export_map: dict(string,string)
        """
        
        self.secure_blob = None
        self.password_length = None
        self.status = None
        self.error_string = None
        self.rotation_date = None
        self.account_name = None
        
        if payload_map is None:
            payload_map = {
                            "status" : None,
                            "password_length" : None,
                            "account_name" : None,
                            "secure_blob" : None,
                            "error_string" : None,
                            "rotation_date" : "rotation_datestamp",                            
                        }

        if export_map is None:
            export_map = {
                            "source_uuid" : "source",
                            "event_uuid" : None,
                            "status" : None,
                            "password_length" : None,
                            "account_name" : None,
                            "secure_blob" : None,
                            "error_string" : None,
                            "date" : "rotation_datestamp",
                        }
        
        if rds_export_map is None:
            rds_export_map = {
                            "source_uuid" : "source",
                            "password_length" : None,
                            "account_name" : None,
                            "event_uuid" : None,
                            "secure_blob" : None,
                            "date" : "rotation_datestamp"            
                        }
        
        BaseModel.__init__(self,event=event,data=data,payload_map=payload_map,
                                                export_map=export_map,
                                                rds_export_map=rds_export_map)
                                                        
    
    @property
    def rotation_datestamp(self):
        """
        Property which returns a datestamp formatted for
        SQL use.
        """
        
        if self.rotation_date:
            return self.rotation_date.strftime(pykarl.core.DATE_FORMAT)
        else:
            return None
        
    @rotation_datestamp.setter
    def rotation_datestamp(self,value):
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
                    logger.warning("Could not import rotation_datestamp value:{v} for model:{m}".format(v=value,m=self.__class__.__name__)) 

        self.rotation_date = the_date

class LocalPasswordRotationEventPublisher(BasePublisher):
    """
    Class used to orchestrate publishing information to RDS, S3, and Redshift
    """
    
    name = "LocalPasswordRotationEventPublisher"
        
    can_target_rds = True
    can_target_s3 = True 
    can_target_dynamo = True
    
    def commit_to_rds(self,model=None,table=None,key_name="source_uuid",
                                                            export_map=None):
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
                
        ## Only commit our model to RDS if it was a successfull rotation
        if (model.status == "Success"):
            BasePublisher.commit_to_rds(self,model=model,key_name=key_name,
                                            table=table,export_map=export_map)
        elif (model.status == "Confirm"):
            BasePublisher.commit_to_rds(self,model=model,key_name=key_name,
                                            table=table,export_map=export_map)
        else:
            logger = logging.getLogger(__name__)
            logger.warning("Encountered unknown PasswordRotation status:{}, skipping...".format(model.status))

    def commit_to_dynamo(self,model=None,table=None):

        """
        Method which will commit the provided event into DynamoDB Table with an event Timestamp

        :param model: The event to commit
        :type model: :py:class:LocalPasswordRotationEvent
        :param table: The name of the table to publish to
        :type table: string

        """

        if not model:
            raise Exception("model is not set!")

        if not table:
            raise Exception("dynamo table is not set!")

        item = model.to_dict(model.export_map)

        try:
            #create a datestamp to submit
            item["datestamp"] = datetime.datetime.utcnow().isoformat()

            #remove sensative information from dynamo - these fields are not required for metrics
            if 'secure_blob' in item: del item['secure_blob']
            if 'password_length' in item: del item['password_length']

            dynamo = self.ddb_resource()
            table = dynamo.Table(table)
            table.put_item(Item=item)
        except Exception as ex:
            detail = "Table: {}, Item: {}".format(table, item)
            s = "commit_to_dynamo() FAILED. Detail: {}. Exception: {}".format(detail, ex)
            raise Exception(s)

class PluginLoadEvent(BaseModel):
    """
    Class representing a network change event.

    :Example:

        >>> event = Event(type="pluginloadevent",subject_area="ACME")
        >>> event.source = "UUID-XXX-XXX-XXX"
        >>> event.payload["file"] = "myfile.dll"
        >>> event.payload["version"] = "1.7.8"
        >>> event.payload["hash"] = "HASH#########"
        >>> event.payload["loaduuid"] = "UUID-XXX-XXX-XXX"
        >>> event.payload["error"] = False
        >>> event.payload["domain"] = "ServiceDomain"
        >>> event.payload["agents"] = "MyAgent1, MyAgent2"
        >>> event.payload["num_agents"] = 2
        >>> event.payload["num_failures"] = 0
        >>> event.payload["loadtime"] = 140
        >>> event.date = datetime.datetime.strptime("2014-02-01","%Y-%m-%d")
        >>>  
        >>> model = acme_module.PluginLoadEvent(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
           "source_uuid": "UUID-XXX-XXX-XXX", 
           "domain": "ServiceDomain", 
           "load_uuid": "UUID-XXX-XXX-XXX", 
           "hash": "HASH#########", 
           "num_failures": 0, 
           "loadtime": 140, 
           "agent": "MyAgent1", 
           "version": "1.7.8", 
           "file": "myfile.dll", 
           "date": "2014-02-01 00:00:00",
           "event_uuid": "0cfd37b0-304f-4cc4-b2b0-727ae6726b73"
        }
        {
           "source_uuid": "UUID-XXX-XXX-XXX", 
           "domain": "ServiceDomain", 
           "load_uuid": "UUID-XXX-XXX-XXX", 
           "hash": "HASH#########", 
           "num_failures": 0, 
           "loadtime": 140, 
           "agent": "MyAgent2", 
           "version": "1.7.8", 
           "file": "myfile.dll", 
           "date": "2014-02-01 00:00:00",
           "event_uuid": "0cfd37b0-304f-4cc4-b2b0-727ae6726b73"
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
        
        self.agents = []
        self.domain = ""
        self.hash = ""
        self.type = "ACMEPluginLoadEvent"
        self.filename = ""
        self.version = ""
        self.num_failures = 0
        self.loadtime = 0
        self.load_uuid = None
        self.was_error = None
        
        if payload_map is None:
            payload_map = {
                            "domain":None,
                            "file":"filename",
                            "hash":None,
                            "loadtime":None,
                            "version":None,
                            "num_failures":None,
                            "num_agents" : None,
                            "loaduuid" : "load_uuid",
                            "error": "was_error",
                        }
        if export_map is None:
            export_map = {
                            "source_uuid" : "source",
                            "event_uuid" : None,
                            "load_uuid" : None,
                            "domain":None,
                            "file":"filename",
                            "hash":None,
                            "loadtime":None,
                            "version":None,
                            "num_failures":None,
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
            self.agents = event.payload["agents"].split(", ")
        except KeyError:
            pass
    
    def to_event(self):
        """
        Method which will output a KARL Event based on our object.
        Relies heavily on :py:func:`BaseModel.to_event`

        """

        event = BaseModel.to_event(self)
    
        event.payload["agents"] = ", ".join(self.agents)
    
        return event
    
    def export_as_csv(self,delimiter="|"):
        """
        Method to export our record as a delimited text record.
        uuid|domain|agent|file|hash|loadtime|version|num_failures|date
        """
        
        csv = None
        
        for agent in self.agents:
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
                elif key == "agent":
                    prop_value = agent
                
             
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
        
        :returns str: JSON string representing our PluginLoadEvent
        """
        
        logger = logging.getLogger("export_as_redshift_json()")
        
        my_string = ""
        
        if export_map is None:
            export_map = self.export_map
        
        for agent in self.agents:
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
            data["agent"] = agent
            
            my_string = "%s\n%s" % (my_string,json.dumps(data,indent=3))
            
        return my_string
                
class PluginLoadEventPublisher(BasePublisher):
    """
    Class used to orchestrate client data reported from ACME involving 
    plug-in details loaded by ACME and publishing to RDS, S3, and Redshift
    """
    
    can_target_rds = True  #: Flag denoting whether our class interacts with RDS, 
                        #: if true, :py:func:`commit_to_rds` will be called.
    can_target_s3 = True   #: Flag denoting whether our class interacts with S3
                        #: if true, :py:func:`can_target_s3` will be called.
    
    def commit_to_rds(self,model=None,table="acme_plugin",
                                        device_table="device_instance"):
        """
        Method which will commit the provided event to RDS.
        
        :param model: The event to commit
        :type model: :py:class:`PluginLoadEvent` object
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
        
        logger.debug("Commiting 'acmepluginload' to RDS table:%s for source:'%s'"
                                                    % (table,model.source))
        
        rds = self.rds()
        
        """ 05/07/15: beauhunt@ This code has been implemented in base model.
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
        
        ## First, purge any load events for this domain that don't match our current load_uuid
        query = "DELETE FROM %s WHERE source_uuid = $1 AND domain = $2 AND load_uuid != $3" % table
        values = (model.source,model.domain,model.load_uuid)
        
        logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
        result = rds.query(query,values)
        
        ## Insert a row into our DB for each agent
        if model.agents:
            for agent in model.agents:
                query = ("INSERT INTO %s (agent, domain, file, hash, loadtime, version, num_failures,date,"
                                " source_uuid, event_uuid, load_uuid) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)" % table)
                values = (agent,model.domain,model.filename,
                                model.hash,model.loadtime,model.version,
                                model.num_failures,model.datestamp,model.source,
                                model.event_uuid,
                                model.load_uuid)
                
                logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
                rds.query(query,values)
        else:
            agent = "Unknown"
            query = ("INSERT INTO %s (agent, domain, file, hash, loadtime, version, num_failures,date,"
                                " source_uuid, event_uuid, load_uuid) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)" % table)
            
            values = (agent,model.domain,model.filename,
                                model.hash,model.loadtime,model.version,
                                model.num_failures,model.datestamp,model.source,
                                model.event_uuid,
                                model.load_uuid)
            
            logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
            rds.query(query,values)

class RemediationEvent(BaseModel):
    """ Class representing remediation event.

    :Example:

        >>> event = Event(type="RemediationEvent",subject_area="ACME")
        >>> event.source= "S-UUID-XXX-XXX-XXX"
        >>> event.payload["action_identifier"] = "I-UUID-XXX-XXX-YYY"
        >>> event.payload["hostname"] = "testhost"
        >>> event.payload["username"] = "testuser"
        >>> event.payload["platform"] = "windows"
        >>> event.payload["ACME_version"] = "1.4.9"
        >>> event.payload["module_name"] = "Third Party"
        >>> event.payload["module_compliance_status"] = "2"
        >>> event.payload["remediation_action"] = "1"
        >>> event.payload["is_forced"] = "true"
        >>> event.payload["date_time"] = datetime.datetime.strptime("2014-02-01","%Y-%m-%d")
        >>>
        >>> model = RemediationEvent(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "type": "RemediationEvent",
             "action_identifier": "I-UUID-XXX-XXX-YYY",
             "source_uuid": "S-UUID-XXX-XXX-XXX",
             "hostname":"testhost",
             "username": "testuser",
             "platform": "windows",
             "ACME_version":"1.4.9",
             "module_name":"Third Party"
             "module_compliance_status":"1",
             "remediation_action":"1",
             "is_forced":"true",
             "date_time": "2014-02-01 00:00:00"
        }
        >>>
    """

    def __init__(self, event=None, data=None, payload_map=None, export_map=None):

        self.type = "RemediationEvent"

        if payload_map is None:
            payload_map = {
                "action_identifier": None,
                "hostname":None,
                "username": None,
                "platform": None,
                "ACME_version": None,
                "module_name":None,
                "module_compliance_status": None,
                "remediation_action":None,
                "is_forced":None,
                "date_time": None,
            }
        if export_map is None:
            export_map = {
                "action_identifier": None,
                "source_uuid": "source",
                "hostname":None,
                "username": None,
                "platform": None,
                "ACME_version": None,
                "module_name":None,
                "module_compliance_status": None,
                "remediation_action":None,
                "is_forced": None,
                "date_time": None,
            }

        BaseModel.__init__(self, event=event, data=data, payload_map=payload_map,
                           export_map=export_map)

class RemediationEventPublisher(BasePublisher):

    """
    Class used to orchestrate client data reported from ACME publishing to RDS, S3, and Redshift
    """
    name = "RemediationEventPublisher"
    logger = logging.getLogger("RemediationEventPublisher")
    logger.info("{} is runnning...".format(name))
    
    can_target_rds = True  #: Flag denoting whether our class interacts with RDS,
                           #: if true, :py:func:`commit_to_rds` will be called.
    can_target_s3 = True  #: Flag denoting whether our class interacts with S3
                          #: if true, :py:func:`can_target_s3` will be called.

class MaintenanceWindowEvaluationEvent(BaseModel):
    """ Class representing Maintenance Window evaluation event.

    :Example:

        >>> event = Event(type="MaintenanceWindowEvaluationEvent",subject_area="ACME")
        >>> event.uuid = "E-UUID-XXX-XXX-YYY"
        >>> event.source= "S-UUID-XXX-XXX-XXX"
        >>> event.payload["module_name"] = "third party"
        >>> event.payload["platform"] = "windows"
        >>> event.payload["eval_in_mw"] = "true"
        >>> event.payload["date_time"] = datetime.datetime.strptime("2014-02-01","%Y-%m-%d")
        >>>
        >>> model = MaintenanceWindowEvaluationEvent(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "type": "MaintenanceWindowEvaluationEvent",
             "source_uuid": "S-UUID-XXX-XXX-XXX",
             "event_uuid": "11bdc456-f10a-499d-bff3-3ceaa539d64a"
             "module_name": "third party"
             "platform": "windows",
             "eval_in_mw":"true",
             "date_time": "2014-02-01 00:00:00"
        }
        >>>
    """

    def __init__(self, event=None, data=None, payload_map=None, export_map=None):
        self.type = "MaintenanceWindowEvaluationEvent"

        if payload_map is None:
            payload_map = {
                "module_name": None,
                "platform": None,
                "eval_in_mw":None,
                "date_time": None,
            }
        if export_map is None:
            export_map = {
                "event_uuid": None,
                "source_uuid": "source",
                "module_name": None,
                "platform": None,
                "eval_in_mw": None,
                "date_time": None,
            }

        BaseModel.__init__(self, event=event, data=data, payload_map=payload_map,
                           export_map=export_map)

class MaintenanceWindowRemediationEvent(BaseModel):
    """ Class representing Maintenance Window remediation event.

    :Example:

        >>> event = Event(type="MaintenanceWindowRemediationEvent",subject_area="ACME")
        >>> event.uuid = "E-UUID-XXX-XXX-YYY"
        >>> event.source= "S-UUID-XXX-XXX-XXX"
        >>> event.payload["module_name"] = "third party"
        >>> event.payload["platform"] = "windows"
        >>> event.payload["rem_in_mw"] = "true"
        >>> event.payload["date_time"] = datetime.datetime.strptime("2014-02-01","%Y-%m-%d")
        >>>
        >>> model = MaintenanceWindowRemediationEvent(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "type": "MaintenanceWindowRemediationEvent",
             "event_uuid": "11bdc456-f10a-499d-bff3-3ceaa539d64a"
             "source_uuid": "S-UUID-XXX-XXX-XXX",
             "module_name": "third party",
             "platform": "windows",
             "rem_in_mw":"true",
             "date_time": "2014-02-01 00:00:00"
        }
        >>>
    """

    def __init__(self, event=None, data=None, payload_map=None, export_map=None):

        self.type = "MaintenanceWindowRemediationEvent"

        if payload_map is None:
            payload_map = {
                "module_name": None,
                "platform": None,
                "rem_in_mw":None,
                "date_time": None,
            }
        if export_map is None:
            export_map = {
                "event_uuid":None,
                "source_uuid": "source",
                "module_name": None,
                "platform": None,
                "rem_in_mw": None,
                "date_time": None,
            }
            
        BaseModel.__init__(self, event=event, data=data, payload_map=payload_map,
                           export_map=export_map)
    
class MaintenanceWindowUINotificationEvent(BaseModel):
    """ Class representing MaintenanceWindowUINotificationEvent.

    :Example:

        >>> event = Event(type="MaintenanceWindowUINotificationEvent",subject_area="ACME")
        >>> event.source= "S-UUID-XXX-XXX-XXX"
        >>> event.date = datetime.datetime.strptime("2014-02-01","%Y-%m-%d")
        >>> event.payload["platform"] = "windows"
        >>> event.payload["status"] = "true"
        >>> model = MaintenanceWindowUINotificationEvent(event=event)
        >>> print model.export_as_redshift_json()
        {
            "source_uuid": "S-UUID-XXX-XXX-XXX",
            "type": "MaintenanceWindowUINotificationEvent",
            "date_time": "2014-02-01 00:00:00",
            "status": "true",
            "platform": "windows"
        }
    """

    def __init__(self, event=None, data=None, payload_map=None, export_map=None):


        self.type = "MaintenanceWindowUINotificationEvent"
        if payload_map is None:
            payload_map = {
                "platform": None,
                "mw_showed": None,
                "notification_showed": None
            }
        if export_map is None:
            export_map = {
                "source_uuid": "source",
                "date_time": "date",
                "platform": None,
                "mw_showed": None,
                "notification_showed": None
            }

        BaseModel.__init__(self, event=event, data=data, payload_map=payload_map,
                           export_map=export_map)

class MaintenanceWindowEvent(BaseModel):
    """ Class representing MaintenanceWindowEvent.

    :Example:

        >>> event = Event(type="MaintenanceWindowEvent",subject_area="ACME")
        >>> event.source= "S-UUID-XXX-XXX-XXX"
        >>> event.payload["is_mw_enabled"] = "true"
        >>> event.payload["mw_changed_date"] = datetime.datetime.strptime("2018-06-01","%Y-%m-%d")
        >>> event.payload["platform"] = "Windows"
        >>> model = MaintenanceWindowEvent(event=event)
        >>> print model.export_as_redshift_json()
        {
            "source_uuid": "S-UUID-XXX-XXX-XXX",
            "type": "MaintenanceWindowEvent",
            "mw_changed_date": "2019-06-01 00:00:00",
            "is_mw_enabled": "true",
            "platform": "Windows",
        }
    """

    def __init__(self, event=None, data=None, payload_map=None, export_map=None):


        self.type = "MaintenanceWindowEvent"

        if payload_map is None:
            payload_map = {
                "is_mw_enabled": None,
                "mw_changed_date": None,
                "platform": None,
            }
        if export_map is None:
            export_map = {
                "source_uuid": "source",
                "is_mw_enabled": None,
                "mw_changed_date": None,
                "platform": None,
            }
        BaseModel.__init__(self, event=event, data=data, payload_map=payload_map, export_map=export_map)

class IdleStatusEvent(BaseModel):
    """ Class representing idle status event.

    :Example:

        >>> event = Event(type="IdleStatusEvent",subject_area="ACME")
        >>> event.source= "S-UUID-XXX-XXX-XXX"
        >>> event.payload["idle_start_time"] = datetime.datetime.strptime("2014-02-01","%Y-%m-%d")
        >>>
        >>> model = IdleStatusEvent(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "source_uuid": "S-UUID-XXX-XXX-XXX",
             "idle_start_time": "2014-02-01 00:00:00",
        }
        >>>
    """
    def __init__(self, event=None, data=None, payload_map=None, export_map=None):

        self.type = "IdleStatusEvent"

        if payload_map is None:
            payload_map = {
                "idle_start_time": None,
            }
        if export_map is None:
            export_map = {
                "uuid": "source",
                "idle_start_time": None,
            }
        BaseModel.__init__(self, event=event, data=data, payload_map=payload_map,
                           export_map=export_map)
        
module_name = "acme"   #: The name of our module, used for filtering operations
                                
event_handler = BaseEventHandler(name=module_name)  #: Our modules event handler, this MUST be 
                                    #: defined if our module is intended to be
                                    #: called.
                                    
event_handler.subject_areas = ["ACME"]
event_handler.action_map = {"PluginLoadEvent" : {
                                            "obj_class":PluginLoadEvent,
                                            "pub_class": PluginLoadEventPublisher,
                                            "s3key_prefix" : "acme/karl_pluginload_event",
                                            "archive_table" : "acme_plugin",
                                            },
                            "SystemReport" : { 
                                            "obj_class" : SystemReport,
                                            "rds_table" : "hardware_device",
                                            "rds_key" : "uuid",
                                            "s3key_prefix" : "acme/karl_systemreport_event",
                                            "archive_table" : "system_report",
                                            "update_device" : True,
                                            },
                            "ReportOwner" : {
                                            "obj_class":OwnerReportEvent,
                                            "pub_class": OwnerEventPublisher,
                                            },
                            "HeartBeat"   : {
                                            "obj_class" : HeartBeat,
                                            "pub_class" : HeartBeatPublisher,
                                            "update_device" : True,
                                            "rds_action" : "none",
                                            },
                            "LocalPasswordRotation" : {
                                    "obj_class" : LocalPasswordRotationEvent,
                                    "pub_class" : LocalPasswordRotationEventPublisher,
                                    "dynamo_table" : "local_password_rotation_events",
                                    "rds_table" : "system_account_keyescrow",
                                    "s3key_prefix" : "acme/karl_localpasswordrotation_event",
                                    "archive_table" : "system_account_keyescrow",                            
                                },
                            "ManagementReport" : {
                                    "obj_class" : ManagementReport,
                                    "update_device" : True,
                                    "rds_action" : "update",
                                    "rds_key" : "source_uuid",
                                    "rds_table" : "management_status",
                                    "archive_table" : "management_status",
                                    "s3key_prefix" : "acme/karl_managementreport_event",
                                
                                },
                            "UpdateDiskInformation" : {
                                            "obj_class" : DiskInformationUpdateEvent,
                                            "pub_class" : UpdateDiskInformationPublisher,
                                            "rds_table" : "disk_information",
                                            "rds_action" : "insert",
                                            "rds_key" : "uuid",
                                            "s3key_prefix" : "acme/karl_diskupdate_event",
                                            "archive_table" : "disk_information",
                               },
                            "RemediationEvent" : {
                                "obj_class": RemediationEvent,
                                "pub_class": RemediationEventPublisher,
                                "rds_table": "remediation_event",
                                "rds_action": "insert",
                                "s3key_prefix": "acme/karl_remediation_event",
                                "archive_table": "remediation_event",
                            },
                            "MaintenanceWindowEvaluationEvent": {
                                "obj_class": MaintenanceWindowEvaluationEvent,
                                "rds_table": "maintenance_window",
                                "rds_action": "insert",
                                "rds_key": "uuid",
                                "s3key_prefix": "acme/karl_mw_evaluation_event",
                                "archive_table": "maintenance_window",
                            },
                            "MaintenanceWindowRemediationEvent": {
                                "obj_class": MaintenanceWindowRemediationEvent,
                                "rds_table": "maintenance_window",
                                "rds_action": "insert",
                                "rds_key": "uuid",
                                "s3key_prefix": "acme/karl_mw_remediation_event",
                                "archive_table": "maintenance_window",
                            },

                            "MaintenanceWindowUINotificationEvent": {
                                "obj_class": MaintenanceWindowUINotificationEvent,
                                "rds_table": "maintenance_window_notification_event",
                                "rds_action": "update",
                                "rds_key" : "source_uuid",
                                "s3key_prefix": "acme/maintenace_window_ui_notification",
                                "archive_table": "maintenance_window_notification_event",
                            },
                            "MaintenanceWindowEvent": {
                                "obj_class": MaintenanceWindowEvent,
                                "rds_table": "maintenance_window_notification_event",
                                "rds_action": "update",
                                "rds_key": "source_uuid",
                                "s3key_prefix": "acme/maintenace_window_ui_notification",
                                "archive_table": "maintenance_window_notification_event",
                            },
                            "IdleStatusEvent" : {
                                "obj_class": IdleStatusEvent,
                                "rds_table": "device_instance",
                                "rds_action": "update",
                                "rds_key" : "uuid",
                                "s3key_prefix": "acme/IdleStatusEvent",
                                "archive_table": "device_instance",
                            },
                }

event_handler.action_map["NewOwner"] = event_handler.action_map["ReportOwner"]
event_handler.action_map["SystemInfo"] = event_handler.action_map["SystemReport"]
event_handler.action_map["LocalPasswordEscrow"] = event_handler.action_map["LocalPasswordRotation"]


