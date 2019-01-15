"""
**network** - Provides custom facilities for processing and routing network events

.. module:: network
   :platform: RHEL5
   :synopsis: Module plugin that provides network data models.
   
.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

import datetime
import logging
import pykarl
import pykarl.core
from .base import BaseModel,BasePublisher,BaseEventHandler
from pykarl.core import DATE_FORMAT

class NetworkSiteChange(BaseModel):
    """
    Model Representing a Network site change.
    
    :Example:

        >>> event = Event(type="NetworkSiteChange",subject_area="Network")
        >>> event.uuid = "E-UUID-XXX-XXX-YYY"
        >>> event.source = "S-UUID-XXX-XXX-XXX"
        >>> event.payload["sitename"] = "MyNewSite"
        >>> event.date = datetime.datetime.strptime("2014-02-01","%Y-%m-%d")
        >>> 
        >>> model = NetworkSiteChange(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "source_uuid": "S-UUID-XXX-XXX-XXX", 
             "type": "NetworkSiteChange", 
             "event_uuid": "E-UUID-XXX-XXX-YYY", 
             "site": "MyNewSite", 
             "date": "2014-02-01 00:00:00"
        }
        >>>

    
    """
    
    site = None #: The name of the new site.
    
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
            payload_map = {"sitename" : "site"}
        if export_map is None:
            export_map = {"event_uuid" : None,
                            "source_uuid" : "source",
                            "type" : None,
                            "site" : None,
                            "date" : "datestamp",
                         }
        
        
        BaseModel.__init__(self,event=event,data=data,
                                            payload_map=payload_map,
                                            export_map=export_map)

class NetworkSiteChangePublisher(BasePublisher):
    """
    Class used to orchestrate publishing information to RDS, S3, and Redshift
    """
    
    name = "NetworkSiteChangePublisher"
        
    def commit_to_rds(self,model=None,table="device_instance",
                                        device_table="device_instance"):
        """
        Method which will commit the provided event to RDS.
        
        :param model: The event to commit
        :type model: :py:class:`NetworkChange` object
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
        
        logger.debug("Commiting '%s' to RDS table:%s for source:'%s'"
                                            % (model.type,table,model.source))
        
        rds = self.rds()
        
        ## Attempt to update our device record
        try:
            last_seen = model.event.submit_date.strftime(DATE_FORMAT)
        except (AttributeError) as exp:
            last_seen = datetime.datetime.utcnow().strftime(DATE_FORMAT) 
        
        query = "UPDATE %s SET site = $1, last_seen = $2 WHERE uuid = $3" % device_table
        values = (model.site,last_seen,model.source)
        
        logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
        result = int(rds.query(query,values).dictresult())
        
        if result == 0:
            device_id = rds.query("INSERT INTO %s (uuid, site, first_seen, last_seen) "
                        "VALUES($1,$2,$3,$4) RETURNING instance_id" % device_table,
                        (model.source,model.site,model.datestamp,last_seen))
                        
class NetworkSessionChange(BaseModel):
    """
    Class representing a network session change.
    
    :Example:
    
        >>> event = Event(type="NetworkSessionEnd",subject_area="Network")
        >>> event.uuid = "E-UUID-XXX-XXX-YYY"
        >>> event.source = "S-UUID-XXX-XXX-XXX"
        >>> event.payload["session_uuid"] = "SESSION-UUID-XXXXXX"
        >>> event.payload["ip_address"] = "127.0.0.1"
        >>> event.payload["state"] = 15
        >>> event.payload["start"] = 1423900800        
        >>> event.payload["end"] = 1423904400
        >>> 
        >>> model = NetworkSessionChange(event=event)
        >>>
        >>> print model.export_as_redshift_json()
        {
             "source_uuid": "S-UUID-XXX-XXX-XXX", 
             "uuid": "SESSION-UUID-XXXXXX", 
             "end_date": "2015-02-14 09:00:00", 
             "event_uuid": "E-UUID-XXX-XXX-YYY", 
             "ip_address": "127.0.0.1", 
             "state": 15, 
             "type": "NetworkSessionEnd", 
             "start_date": "2015-02-14 08:00:00"
        }
        >>>

    
    """

    uuid = None         #: The sessions unique ID
    ip_address = None   #: Primary IP address
    state = None        #: Bitwise mask representing NetworkState
                        #: (Explicit off flags are used by client side
                        #: targeting logic)
                        #: 
                        #: State                    Value
                        #: ===================      ======
                        #: NETWORK_STATUS_NONE      0
                        #: NETWORK_STATUS_ONLINE    1 << 1
                        #: NETWORK_STATUS_OFFLINE   1 << 2
                        #: NETWORK_STATUS_ONDOMAIN  1 << 3
                        #: NETWORK_STATUS_OFFDOMAIN 1 << 4
                        #: NETWORK_STATUS_ONVPN     1 << 5
                        #: NETWORK_STATUS_OFFVPN    1 << 6
    
    start_date = None   #: The start date of our session
    end_date = None     #: The end date of the session

    @property
    def start_datestamp(self):
        """
        Property which returns a datestamp formatted for
        SQL use.
        """
        
        if self.start_date:
            return self.start_date.strftime(DATE_FORMAT)
        else:
            return None
        
    @start_datestamp.setter
    def start_datestamp(self,value):
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
                    the_date = datetime.datetime.strptime(value,DATE_FORMAT)
                except ValueError as exp:
                    logger = logging.getLogger(self.__class__.__name__)
                    logger.warning("Could not import start_datestamp value:{v} for model:{m}".format(v=value,m=self.__class__.__name__)) 

        self.start_date = the_date
        
    @property
    def end_datestamp(self):
        """
        Property which returns a datestamp formatted for
        SQL use.
        """
        
        if self.end_date:
            return self.end_date.strftime(DATE_FORMAT)
        else:
            return None
        
    @end_datestamp.setter
    def end_datestamp(self,value):
        """
        Setter for our datestamp
        """
        the_date = None
        
        if isinstance(value,datetime.datetime):
            the_date = value
        elif value is not None:
            try:
                the_date = datetime.datetime.utcfromtimestamp(float(value))
            except (TypeError,ValueError) as exp:
                try:
                    the_date = datetime.datetime.strptime(value,DATE_FORMAT)
                except ValueError as exp:
                    logger = logging.getLogger(self.__class__.__name__)
                    logger.warning("Could not import end_datestamp value:{v} for model:{m}".format(v=value,m=self.__class__.__name__))
        self.end_date = the_date
        
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
        
        self.uuid = None
        self.ip_address = None
        self.state = None
        self.site = None
        self.start_date = None
        self.end_date = None
        
        if payload_map is None:
            payload_map = {
                            "session_uuid" : "uuid",
                            "ip_address" :None,
                            "state" : None,
                            "site" : None,
                            "start" : "start_datestamp",
                            "end" : "end_datestamp",
                        }
        if export_map is None:
            export_map = {
                            "uuid" : None,
                            "event_uuid" : None,
                            "source_uuid" : "source",
                            "type" : None,
                            "ip_address" :None,
                            "state" : None,
                            "start_date" : "start_datestamp",
                            "end_date" : "end_datestamp",
                        
                        }
        
        BaseModel.__init__(self,event=event,data=data,
                                            payload_map=payload_map,
                                            export_map=export_map)

class NetworkSessionChangePublisher(BasePublisher):
    """
    Class used to orchestrate publishing information to RDS, S3, and Redshift
    """
      
    name = "NetworkSessionChangePublisher"
    
    can_target_rds = True
    can_target_s3 = True
    
    def commit_to_rds(self,model=None,table="network_session",
                                        device_table="device_instance"):
        """
        Method which will commit the provided event to RDS.
        
        :param model: The event to commit
        :type model: :py:class:`NetworkChange` object
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
        
        logger.debug("Commiting '%s' to RDS table:%s for source:'%s'"
                                            % (model.type,table,model.source))
        
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
                logger.error("Failed to load device id for identifier:%s" % model.source)
                return
        else:
            device_id = rds.query("INSERT INTO %s (uuid,first_seen) "
                        "values($1,$2) RETURNING instance_id" % device_table,
                        (model.source,model.datestamp))
        """
        
        ## Update our network_session table
        if model.type == "NetworkSessionStart":
            
            map = { "uuid" : None,
                    "source_uuid" : "source",
                    "event_uuid" : None,
                    "type" : None,
                    "ip_address" : None,
                    "state" : None,
                    "start_date" : "start_datestamp",
                    }
            
            if not self.update_rds_entry(model=model,key_name="source_uuid",table=table,
                                                            key_map=map):
                self.create_rds_entry(model=model,table=table,key_map=map)
        
            """
            query = "UPDATE %s SET uuid = $1, source_uuid = $2, event_uuid = $3, type = $4,  WHERE source_uuid = $1" % table
            values = (model.source)3333
            
            logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
            result = rds.query(query,values)
            
            if not result or result == "0":
                query = ("INSERT INTO %s (uuid,source_uuid,event_uuid,type,ip_address,state,start_date)"
                                    " values($1,$2,$3,$4,$5,$6,$7)" % table)
                values = (model.uuid,model.source,model.event_uuid,model.type,
                                model.ip_address,model.state,model.start_datestamp)
                
                logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
                rds.query(query,values)
            """

class NetworkChange(BaseModel):
    """
    Class representing a network change event.
    This event has been supplanted by NetworkSessionChange
    """
    
    def __init__(self,event=None,data=None):
        """
        Our constructor.
        
        :param event: Our event to process.
        :type event: `:py:class:Event`
        """
        
        BaseModel.__init__(self,event,data)
        
        self.mac_address = None
        self.ip_address = None
        self.site = None
        self.hostname = None
        self.online = False
                
        self.payload_map = {
                            "MACAddress":"mac_address",
                            "IPAddress":"ip_address",
                            "hostname": None,
                            "Online":"online",
                        }
        self.export_map = {
                        "source_uuid" : "source",
                        "hostname" : None,
                        "mac_address" : None,
                        "ip_address" : None,
                        "online" : None,
                        "date" : "datestamp"
                        }
        
        if event is not None:
            self.load_event(event)
        if data is not None:
            self.load_data(data)
        
    def export_as_csv1(self,delimiter="|"):
        """
        Method to export our record as a delimited text record.
        uuid|hostname|mac_address|IPAddress|ADSite|Online|date
        """
        
        csv = None
        
        if self.mac_address:
            mac_address = self.mac_address.replace(":","")
        else:
            mac_address = ""
        
        if self.online is None:
            online = 0
        else:
            try:
                online = int(self.online)
            except (ValueError,TypeError):
                online = 0
    
        for value in (self.uuid,self.hostname,mac_address,
                                        self.IPAddress,self.ADSite,
                                        online,self.datestamp):
            if not csv:
                csv = value
            else:
                csv = "%s%s%s" % (csv,delimiter,value)
        
        return csv
    
    def load_data(self,data):
        """Method to load data from a record."""

        logger = logging.getLogger()
        
        payload_map = {"device_uuid":self.uuid,
                            "hostname":self.hostname,
                            "mac_address":self.mac_address,
                            "ipaddress":self.IPAddress,
                            "adsite":self.ADSite,
                        }
                        
        for key,attribute in payload_map.iteritems():
            try:
                attribute = data[key]
            except KeyError as exp:
                pass

        try:
            self.date = datetime.datetime.strptime(data["date"],DATE_FORMAT)
        except ValueError:
            logger.warning("Failed to load date: %s" % exp)
        except KeyError:
            pass

        try:
            if data["online"] == "t":
                self.online = True
            else:
                self.online = False
        except (ValueError,KeyError):
            pass
            
class NetworkChangePublisher(BasePublisher):
    """
    Publisher class for NetworkChange events. This has been deprecated
    in lieu of 'NetworkSessionChange' events
    """
    
    name = "NetworkChangePublisher"
    
    can_target_rds = True
    can_target_s3 = True
    
    def commit_to_rds(self,model=None,table="network_event",
                                            device_table="device_instance"):
        """
        Method which will commit the provided event to RDS.
        
        :param model: The event to commit
        :type model: :py:class:`NetworkChange` object
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
            raise AttributeError("table is not specified, cannot commit to RDS")
        
        if device_table is None:
            raise AttributeError("device_table is not specified, cannot commit to RDS")
        
        logger = logging.getLogger("commit_to_rds()")
        
        logger.debug("Commiting '%s' to RDS table:%s for source:'%s'"
                                            % (model.type,table,model.source))
        
        rds = self.rds()
        nc = model
        
        ## Attempt to update our device record
        try:
            last_seen = model.event.submit_date.strftime(pykarl.core.DATE_FORMAT)
        except (AttributeError) as exp:
            last_seen = datetime.datetime.utcnow().strftime(pykarl.core.DATE_FORMAT) 
            
        query = "UPDATE %s SET last_seen = $1 WHERE uuid = $2" % device_table
        values = (last_seen,model.source)
        
        logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
        result = rds.query(query,values)
        
        if result == 0:
            instance_id = rds.query("INSERT INTO %s (uuid, first_seen, last_seen) "
                        "VALUES($1,$2,$3) RETURNING instance_id" % device_table,
                        (model.source,model.datestamp,last_seen))
        
        ## Update our logininfo table
        query = ("UPDATE %s SET hostname = $1, "
                            "mac_address = $2,ip_address = $3, site = $4, online = $5, date = $6 WHERE source_uuid = $7" % table)
        values = (nc.hostname,nc.mac_address.replace(":",""),nc.IPAddress,nc.ADSite,int(nc.online),nc.datestamp,nc.source)

        logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
        result = int(rds.query(query,values))
        
        if not result or result == "0":
            query = ("INSERT INTO %s(hostname, mac_address, ip_address, site, online, date,"
                            " source_uuid) VALUES($1,$2,$3,$4,$5,$6,$7)")
            values = (nc.hostname,nc.mac_address.replace(":",""),nc.IPAddress,nc.ADSite,int(nc.online),nc.datestamp,nc.source)
            
            logger.debug("Running Query:\"\"\"{}\"\"\", {}".format(query,values))
            rds.query(query,values)

NETWORK_STATUS_NONE = 0                 #: Constant denoting unknown network access levels
NETWORK_STATUS_ONLINE = 1 << 1          #: Constant denoting network access
NETWORK_STATUS_OFFLINE = 1 << 2         #: Constant denoting no network access
NETWORK_STATUS_ONDOMAIN = 1 << 3        #: Constant denoting domain intranet access
NETWORK_STATUS_OFFDOMAIN = 1 << 4       #: Constant denoting no domain intranet access
NETWORK_STATUS_ONVPN = 1 << 5           #: Constant denoting active VPN connection
NETWORK_STATUS_OFFVPN = 1 << 6          #: Constant denoting no active VPN connection

def string_from_network_status(status):
    """
    Method which returns a string denoting current network access
    
    :param int status: The current network status bitmask
    
    :returns str: String denoting network status, multiple flags will be comma 
            delimited
    
    #: Output       Value
    #: ========     ======
    #: None         NETWORK_STATUS_NONE
    #: Online       NETWORK_STATUS_ONLINE
    #: Offline      NETWORK_STATUS_OFFLINE
    #: OnDomain     NETWORK_STATUS_ONDOMAIN
    #: OffDomain    NETWORK_STATUS_OFFDOMAIN
    #: OnVPN        NETWORK_STATUS_ONVPN
    #: OffVPN       NETWORK_STATUS_OFFVPN
    
    """
    
    status_string = None
    
    if status == NETWORK_STATUS_NONE:
        status_string = "None"
        
    if (status & NETWORK_STATUS_ONLINE) != NETWORK_STATUS_NONE:
        if status_string is None:
            status_string = "Online"
        else:
            status_string += ", Online"
        
    if (status & NETWORK_STATUS_OFFLINE) != NETWORK_STATUS_NONE:
        if status_string is None:
            status_string = "Offline"
        else:
            status_string += ", Offline"
        
    if (status & NETWORK_STATUS_ONDOMAIN) != NETWORK_STATUS_NONE:
        if status_string is None:
            status_string = "OnDomain"
        else:
            status_string += ", OnDomain"
        
    if (status & NETWORK_STATUS_OFFDOMAIN) != NETWORK_STATUS_NONE:
        if status_string is None:
            status_string = "OffDomain"
        else:
            status_string += ", OffDomain"
        
    if (status & NETWORK_STATUS_ONVPN) != NETWORK_STATUS_NONE:
        if status_string is None:
            status_string = "OnVPN"
        else:
            status_string += ", OnVPN"
        
    if (status & NETWORK_STATUS_OFFVPN) != NETWORK_STATUS_NONE:
        if status_string is None:
            status_string = "OffVPN"
        else:
            status_string += ", OffVPN"
        
    return status_string

module_name = "network" #: The name of our module, used for filtering operations

event_handler = BaseEventHandler(name=module_name)
event_handler.subject_areas = ["Network"];
event_handler.action_map = {
                    "NetworkChange" : { "obj_class" : NetworkChange,
                        "pub_class" : NetworkChangePublisher,
                        "s3key_prefix" : "network/karl_networkinterfacechange_",
                        "archive_table" : "networkchange"
                    },
                    "NetworkSessionStart" : {"obj_class" : NetworkSessionChange,
                        "pub_class": NetworkSessionChangePublisher,
                        "s3key_prefix" : "network/karl_networksessionchange_",
                        "archive_table" : "network_session"
                    },
                    "NetworkSessionEnd" : {"obj_class" : NetworkSessionChange,
                        "pub_class": NetworkSessionChangePublisher,
                        "s3key_prefix" : "network/karl_networksessionchange_",
                        "archive_table" : "network_session"
                    },
                    "NetworkSiteChange" : {"obj_class" : NetworkSiteChange,
                        "pub_class": NetworkSiteChangePublisher,
                    },
                }

