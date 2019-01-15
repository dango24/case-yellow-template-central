import copy
import datetime
import logging
import os
import re
import subprocess
import time
import acme.core
import acme.agent as agent
import pykarl.event
import systemprofile
from systemprofile.util_helper import get_bytes

__version__ = "1.2"

MGMT_STATUS_NONE = 0
MGMT_STATUS_ACMEMANAGED = 1
MGMT_STATUS_CORPMANAGED = 2

class HeartBeatAgent(agent.BaseAgent):
    """
    Agent which will periodically heartbeat to KARL while online.
    """
    
    
    def __init__(self,*args,**kwargs):
        
        self.identifier = "HeartBeatAgent"           #: This MUST be unique
        self.name = "HeartBeatAgent"                 #: This SHOULD be unique
        self.run_frequency = datetime.timedelta(minutes=30)
        self.run_frequency_skew = datetime.timedelta(minutes=5) #: Run skew is recommended for scheduled agents to distrubute load on dependent systems
        
        self.prerequisites = agent.AGENT_STATE_ONLINE
        self.triggers = agent.AGENT_TRIGGER_SCHEDULED
        
        self.priority = agent.AGENT_PRIORITY_LOW
        
        #: When subclassing, always init superclasses
        super(HeartBeatAgent,self).__init__(name=self.name,
                                            identifier=self.identifier,
                                            *args,**kwargs)
    
    def execute(self,trigger=None,data=None):
        """
        Method to send a KARL HeartBeat
        
        :param trigger: The trigger executing the action
        :type trigger: int: bitwise mask value. See AGENT_TRIGGER_*
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.info("{} Executing!".format(self.identifier))
        
        ## Send heartbeat to KARL
        dispatcher = pykarl.event.dispatcher
        if dispatcher.is_configured():
            evt = pykarl.event.Event(type="HeartBeat",subject_area="ACME")            
            dispatcher.dispatch(evt)
        
        ## Cleanup
        self.last_execution_status = agent.AGENT_EXECUTION_STATUS_SUCCESS 
        self.last_execution = datetime.datetime.utcnow()
        
        logger.info("{} Finished Executing!".format(self.identifier))




class OwnershipAgent(agent.BaseAgent):
    """
    Agent which will report the device owner, provided it has changed
    from previous settings. 
    """
    
    last_owner = None           #: The last-known owner of this system.
    last_report_date = None     #: The last date the owner was reported
    report_frequency = None     #: The frequency in which we'll report the owner 
                                #: (even if no change is detected)
    
    def __init__(self,*args,**kwargs):
        
        
        self.identifier = "OwnershipAgent"
        self.name = "OwnershipAgent"
        self.last_owner = None
        self.last_report_date = None
        
        self.run_frequency = datetime.timedelta(days=1)
        self.run_frequency_skew = datetime.timedelta(seconds=5)
        
        self.report_frequency = datetime.timedelta(days=30)
        
        self.triggers = agent.AGENT_TRIGGER_SCHEDULED
        
        self.priority = agent.AGENT_PRIORITY_LOW
        
        super(OwnershipAgent,self).__init__(name=self.name,
                                            identifier=self.identifier,
                                            *args,**kwargs)
        
        self.key_map["last_owner"] = None
        self.key_map["last_report_date"] = "<type=datetime>"
        self.state_keys.append("last_owner")

    def execute(self,trigger=None,data=None):
        """
        Method to verify current owner, and report up if known owner has 
        changed.
        
        :param trigger: The trigger executing the action
        :type trigger: int: bitwise mask value. See AGENT_TRIGGER_*
        """
        logger = logging.getLogger(self.logger_name)
        
        now = datetime.datetime.utcnow()
        
        logger.info("{} Executing!".format(self.identifier))
        
        ## Send ownership data to KARL, provided it has changed
        dispatcher = pykarl.event.dispatcher
        if dispatcher.is_configured():
            
            profile = systemprofile.profiler
        
            payload = {}
            owner = profile.owner()
            payload["owner"] = owner
            
            should_report = False
            report_type = "ReportOwner"
            
            if not self.last_owner == owner:
                logger.info("New device owner found:{}, reporting to KARL (old:{})".format(
                                                        owner,self.last_owner))
                report_type = "NewOwner"
                should_report = True
                
            elif (self.last_report_date 
                        and self.last_report_date + self.report_frequency < now):
                logger.info("Reporting system owner.")
                report_type = "ReportOwner"
                should_report = True
                
            if should_report:
                evt = pykarl.event.Event(type=report_type,subject_area="ACME",
                                                            payload=payload)            
                dispatcher.dispatch(evt)
                self.last_owner = owner
                self.last_report_date = datetime.datetime.utcnow()
            
            self.last_execution_status = agent.AGENT_EXECUTION_STATUS_SUCCESS
        
        else:
            ## Here if we KARL isn't configured
            logger.error("No KARL dispatcher is configured, will not report owner!")
            self.last_execution_status = agent.AGENT_EXECUTION_STATUS_ERROR
        
        ## Cleanup
        self.last_execution = datetime.datetime.utcnow()
        
        logger.info("{} Finished Executing!".format(self.identifier))

class BasicSystemInfoAgent(agent.BaseAgent):
    """
    Scheduled agent to report basic system information.
    """
    
    def __init__(self,*args,**kwargs):
                
        self.identifier = "BasicSystemInfoAgent"
        self.name = "BasicSystemInfoAgent"
        
        self.run_frequency = datetime.timedelta(days=2)
        self.run_frequency_skew = datetime.timedelta(seconds=5)
        
        self.triggers = agent.AGENT_TRIGGER_SCHEDULED
        
        self.priority = agent.AGENT_PRIORITY_LOW
        
        super(BasicSystemInfoAgent,self).__init__(name=self.name,
                                            identifier=self.identifier,
                                            *args,**kwargs)

    def execute(self,trigger=None,data=None):
        """
        Our primary execution method. This method will be called by
        our scheduler or during events as registered by our triggers.
        
        :param trigger: The trigger executing the action
        :type trigger: int: bitwise mask value. See AGENT_TRIGGER_*
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.info("{} Executing!".format(self.identifier))
        
        ## Send heartbeat to KARL
        dispatcher = pykarl.event.dispatcher
        if dispatcher.is_configured():
            
            profile = systemprofile.profiler
        
            payload = {}
            payload["mac_address"] = profile.mac_address()
            payload["hardware_id"] = profile.hardware_identifier()
            payload["hostname"] = profile.hostname()
            payload["make"] = profile.hardware_make()
            payload["model"] = profile.hardware_model()
            payload["asset_tag"] = profile.asset_tag()
            payload["serial_number"] = profile.serial_number()
            payload["system_type"] = profile.system_type()
            payload["architecture"] = profile.architecture()
            payload["platform"] = systemprofile.current_platform()
            payload["platform_version"] = profile.system_version()
            payload["physical_memory"] = profile.physical_memory()
            payload["cpu_type"] = profile.cpu_type()
            payload["cpu_cores"] = profile.cpu_cores()
            disk_data = profile.load_disk_info()
            ssd_size = []
            hdd_size = []
            payload_disk_info = {}
            
            try:
                if payload["platform"] == "Ubuntu":
                    for data in disk_data:
                        size,suffix = data["size"][:-2], data["size"][-2:]
                        size_in_bytes = get_bytes(size, suffix)
                        if data["rota"] == "0":
                            payload["ssd"] = 1
                            ssd_size.append(size_in_bytes)
                        else:
                            payload["hdd"] = 1
                            hdd_size.append(size_in_bytes)
                elif payload["platform"] == "OS X" or payload["platform"] == "macOS":
                    for data in disk_data:
                        if data["file_system"] == "APFS":
                            ## Here for APFS volumes
                            if data["physical_drive"]["is_internal_disk"] == "yes":
                                size_in_bytes = data["size_in_bytes"]
                                if data["physical_drive"]["medium_type"] == "ssd":
                                    payload["ssd"] = 1
                                    ssd_size.append(size_in_bytes)
                                else:
                                    payload["hdd"] = 1
                                    hdd_size.append(size_in_bytes)
                        else:
                            ## Here for legacy CoreStorage volumes
                            for item in data["com.apple.corestorage.pv"]:
                                size_in_bytes = item["com.apple.corestorage.pv.size"]
                                if item["is_internal_disk"] == "yes":
                                    if item["medium_type"] == "ssd":
                                        payload["ssd"] = 1
                                        ssd_size.append(size_in_bytes)
                                    elif item["medium_type"] == "hdd":
                                        payload["hdd"] = 1
                                        hdd_size.append(size_in_bytes)
            except Exception as exp:
                logger.error("Failed to collect disk information: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):", exc_info=1)
                
            ## Send our SystemInfo event
            evt = pykarl.event.Event(type="SystemInfo",subject_area="ACME", payload=payload)            
            dispatcher.dispatch(evt)

            ## Send UpdateDiskInformation event if we collected relevant data. 
            if ssd_size or hdd_size:
                payload_disk_info["hardware_id"] = payload["hardware_id"]
                try:
                    if ssd_size:
                        payload_disk_info["ssd_sizes"] = ", ".join([str(x) 
                                                            for x in ssd_size])
                    if hdd_size:
                        payload_disk_info["hdd_sizes"] = ", ".join([str(x) 
                                                            for x in hdd_size])
                except:
                    pass
            
                evt_update_disk =  pykarl.event.Event(type="UpdateDiskInformation",subject_area="ACME", payload=payload_disk_info)
                dispatcher.dispatch(evt_update_disk)
        
        ## Cleanup
        self.last_execution_status = agent.AGENT_EXECUTION_STATUS_SUCCESS 
        self.last_execution = datetime.datetime.utcnow()
        
        logger.info("{} Finished Executing!".format(self.identifier))
       
class ManagementAgent(agent.BaseAgent):
    """
    Scheduled agent to report on system management status.
    """
    
    def __init__(self, key_map=None, state_keys=None, settings_keys=None,
                                                            *args,
                                                            **kwargs):
             
        self.last_submission_date = None   #: var which represents the last date that we submitted results
        
        self.submission_frequency = datetime.timedelta(hours=24)  #: var which represents how often we should submit data that hasn't changed.
        
        self.acme_version = None           #: var which represents our last know acme version
        self.management_status = None      #: var which represents our last known management status
                    
        self.management_flag_path = None    #: var which represents the location of our flag
        
        if acme.platform == "OS X" or acme.platform == "macOS":
            self.management_flag_path = "/Library/.AmznManaged"
        else:
            self.management_flag_path = "/etc/.adm-laptop-pass"            
        
        if key_map is None:
            key_map = {
                        "last_submission_date" : "<type=datetime>",
                        "submission_frequency" : "<type=timedelta>",
                        "acme_version" : None,
                        "management_status" : None,
                        "management_flag_path" : None,
            }
            key_map.update(agent.BaseAgent.key_map)
        
        if state_keys is None:
            state_keys = ["last_submission_date", "acme_version", "management_status"]
            state_keys.extend(agent.BaseAgent.state_keys)
        
        if settings_keys is None:
            settings_keys = ["submission_frequency", "management_flag_path"]
            settings_keys.extend(agent.BaseAgent.settings_keys)
        
        
        self.run_frequency = datetime.timedelta(hours=4)
        self.run_frequency_skew = datetime.timedelta(seconds=5)
        
        self.identifier = "ManagementAgent"
        self.name = "ManagementAgent"
        
        self.triggers = (agent.AGENT_TRIGGER_SCHEDULED|agent.AGENT_TRIGGER_PROCESSSTART)
        
        self.priority = agent.AGENT_PRIORITY_LOW
        
        super(ManagementAgent,self).__init__(name=self.name,
                                            identifier=self.identifier,
                                            key_map=key_map,
                                            state_keys=state_keys,
                                            settings_keys=settings_keys,
                                            *args,**kwargs)

    def execute(self,trigger=None,data=None):
        """
        Our primary execution method. This method will be called by
        our scheduler or during events as registered by our triggers.
        
        :param trigger: The trigger executing the action
        :type trigger: int: bitwise mask value. See AGENT_TRIGGER_*
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.info("{} Executing!".format(self.identifier))
        
        status_did_change = False
        
        ## Lookup acme version
        if acme.daemon.__version__ != self.acme_version:
            self.acme_version = acme.daemon.__version__
            status_did_change = True
            
        ## Lookup management status
        new_management_status = None
        if self.management_flag_path and os.path.exists(self.management_flag_path):
            new_management_status = MGMT_STATUS_CORPMANAGED
        else:
            new_management_status = MGMT_STATUS_ACMEMANAGED
            
        if self.management_status != new_management_status:
            self.management_status = new_management_status
            status_did_change = True
            
        should_report = False
        if status_did_change:
            should_report = True
        elif not self.last_submission_date:
            should_report = True            
        elif (datetime.datetime.utcnow() - self.submission_frequency 
                                                >= self.last_submission_date):
            should_report = True
        
        did_report = False
        if should_report:
            dispatcher = pykarl.event.dispatcher
            if dispatcher.is_configured():
                payload = {}
                payload["management_status"] = self.management_status
                payload["acme_version"] = self.acme_version
                
                evt = pykarl.event.Event(type="ManagementReport",
                                                        subject_area="ACME",
                                                        payload=payload)            
                dispatcher.dispatch(evt)
                did_report = True
        
        ## Cleanup
        if should_report and did_report:
            self.last_execution_status = agent.AGENT_EXECUTION_STATUS_SUCCESS
            self.last_submission_date = datetime.datetime.utcnow()
        elif should_report and not did_report:
            self.last_execution_status = agent.AGENT_EXECUTION_STATUS_ERROR
        else:
            self.last_execution_status = agent.AGENT_EXECUTION_STATUS_NONE
            
        self.last_execution = datetime.datetime.utcnow()
        
        logger.info("{} Finished Executing!".format(self.identifier))

class UserSessionAgent(agent.BaseAgent):
    """
    Agent which reports active user sessions 
    """
    
    def __init__(self,*args,**kwargs):
        
        self.identifier = "UserSessionAgent"           
        self.name = "UserSessionAgent"                 
        
        self.triggers = (agent.AGENT_TRIGGER_SESSIONUNLOCK |
                                    agent.AGENT_TRIGGER_SESSIONSTART)
        
        self.priority = agent.AGENT_PRIORITY_MEDIUM
        
        #: When subclassing, always init superclasses
        super(UserSessionAgent,self).__init__(name=self.name,
                                            identifier=self.identifier,
                                            *args,**kwargs)
    
    def execute(self,trigger=None,data=None):
        """
        Method to send a KARL HeartBeat
        
        :param trigger: The trigger executing the action
        :type trigger: int: bitwise mask value. See AGENT_TRIGGER_*
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.info("{} Executing!".format(self.identifier))
        
        event_type = None
        
        if trigger & agent.AGENT_TRIGGER_SESSIONUNLOCK:
            event_type = "AuthEvent"
        elif trigger & agent.AGENT_TRIGGER_SESSIONSTART:
            event_type = "LoginEvent"
        
        ## Send heartbeat to KARL
        dispatcher = pykarl.event.dispatcher
        if dispatcher.is_configured():
            event = pykarl.event.Event(type=event_type,subject_area="Auth")
            try:
                event.payload["Username"] = data["username"]
            except TypeError, KeyError:
                event.payload["Username"] = "Unknown"            
            dispatcher.dispatch(event)
        else:
            logger.error("No KARL dispatcher is configured, cannot report {}".format(event_type))
        
        ## Cleanup
        self.last_execution_status = agent.AGENT_EXECUTION_STATUS_SUCCESS 
        self.last_execution = datetime.datetime.utcnow()
        
        logger.info("{} Finished Executing!".format(self.identifier))        

class RebootMetricAgent(agent.BaseAgent):
    """
    Agent which will send reboot metric - last reboot and reboot duration to KARL whenever
    AGENT_TRIGGER_START flag is triggered.
    """
    
    def __init__(self,*args,**kwargs):
        
        self.identifier = "RebootMetricAgent"           
        self.name = "RebootMetricAgent"                
        
        self.triggers = agent.AGENT_TRIGGER_STARTUP
        
        self.priority = agent.AGENT_PRIORITY_MEDIUM
        
        #: When subclassing, always init superclasses
        super(RebootMetricAgent,self).__init__(name=self.name,
                                            identifier=self.identifier,
                                            *args,**kwargs)
    
    def execute(self,trigger=None,data=None):
        """
        Method to send a KARL reboot payload.
        
        :param trigger: The trigger executing the action
        :type trigger: int: bitwise mask value. See AGENT_TRIGGER_*
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.info("{} Executing!".format(self.identifier))
        
        profile = systemprofile.profiler
        ## getting time it took to boot the system and last/most recent bootup.
        last_boot_time = profile.system_start_date()
        boot_duration_delta =  datetime.datetime.utcnow() - last_boot_time
        boot_duration = boot_duration_delta.total_seconds()
            
        ## Send message to KARL
        dispatcher = pykarl.event.dispatcher
        if dispatcher.is_configured():
	    payload={}
	    epoch_date = datetime.datetime(1970,1,1)
            delta_last_boot_time = (last_boot_time - epoch_date).total_seconds()
	    payload["DateTime"] = delta_last_boot_time
	    payload["BootDuration"] = boot_duration
            evt = pykarl.event.Event(type="RebootEvent",subject_area="System",
                                                            payload=payload)
            dispatcher.dispatch(evt)
        
        ## Cleanup
        self.last_execution_status = agent.AGENT_EXECUTION_STATUS_SUCCESS
        self.last_execution = datetime.datetime.utcnow()
        
        logger.info("{} Finished Executing!".format(self.identifier))

