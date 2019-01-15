"""
.. package:: acme.daemon
    :synopsis: Package containing classes used by ACME for daemonized
        executables, including system daemon (ACMEd) and persistant user agent.
        (ACMEAgent).
    :platform: OSX, Ubuntu

.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>

"""


#MARK: Imports
import argparse
import datetime
import logging
import logging.handlers
import os
import signal
import sys
import threading
import time
import uuid
import json

import acme
import acme.compliance as compliance
import acme.crypto as crypto
import acme.ipc as ipc
import acme.network as network
import acme.registration as registration
import acme.systemevents as systemevents
import acme.core
import acme.configuration as configuration
import acme.configuration.compliance
import acme.configuration.configfile
import acme.configuration.ststoken
import acme.usher_configuration
import acme.usher_configuration.usher as usher_config
import acme.quarantine as quarantine
import acme.systemevents.proxy
import acme.agent
import acme.claims as claims
from acme.utils import validate_runfile, dt_parse
import random
import pykarl.event
from pykarl.event import Event, EventEngine
import systemprofile
import systemprofile.directoryservice
import constant
import acme.aea as aea
import acme.usher
from acme.usher import UsherController
from __builtin__ import True
from shutil import copyfile

__version__ = "1.5.3"

if acme.platform == "OS X" or acme.platform == "macOS":
    from Foundation import NSRunLoop
    DEFAULT_SYSTEM_DIR="/usr/local/amazon/var/acme"
    DEFAULT_USER_DIR=os.path.expanduser("~/Library/Application Support/ACME")
    DEFAULT_MANIFEST_DIR = '/Library/Preferences'
elif acme.platform == "Ubuntu":
    DEFAULT_SYSTEM_DIR="/usr/local/amazon/var/acme"
    DEFAULT_USER_DIR=os.path.expanduser("~/.acme")
elif acme.platform == "RedHat":
    DEFAULT_SYSTEM_DIR="/usr/local/amazon/var/acme"
    DEFAULT_USER_DIR=os.path.expanduser("~/.acme")
else:
    raise UnsupportedPlatformError("Platform:{} is not supported by acme.daemon".format(acme.platform))

systemprofile.profiler.file_dir = DEFAULT_SYSTEM_DIR

#MARK: Module defaults
DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_BOTO_LOGLEVEL = logging.WARNING

DEFAULT_DAEMON_PORT = 9216
DEFAULT_AGENT_PORT = 9217


LOGGING_FORMAT = "%(asctime)s [%(name)s] <%(levelname)s> %(message)s"
LOGGING_FORMAT_DETAILED = "%(asctime)s [PID:%(process)d %(name)s-%(filename)s:%(lineno)d] <%(levelname)s> %(message)s"
LOG_FILE_ROTATION_SIZE = 100000000  #: Rotate our log file every 100MB
LOG_FILE_NUM_RETAIN = 3 #: Retain 3 copies

NSRUNLOOP_DURATION = datetime.timedelta(seconds=2)

STATUS_IDLE = 0
STATUS_AGENT_EXECUTING = 1 << 1
STATUS_EVALUATING = 1 << 2
STATUS_REMEDIATING = 1 << 3
STATUS_FATAL_ERROR = 1 << 4
STATUS_UNKNOWN = 1 << 5

STARTUP_THRESHOLD = datetime.timedelta(seconds=300)
LOGIN_THRESHOLD = datetime.timedelta(seconds=120)
ROUTINE_TIMER_INTERVAL = datetime.timedelta(minutes=5)

class ACMEControls(acme.core.ConfigurableObject):
    """
    Our ACME Controls class that controls ACME features.
    By default, they are all set to true and can be over-ridden through a manifest
    """
    key_map = {
                "usher_enabled": "<type=bool>;",
                "usher_watcher_enabled": "<type=bool>;",
                "karl_registrar_enabled": "<type=bool>;",
                "compliance_enabled": "<type=bool>;"
                }
    settings_keys = key_map.keys()
    
    def __init__(self, key_map=None, settings_keys=None, *args, **kwargs):
        self.usher_enabled = False
        self.usher_watcher_enabled = False
        self.karl_registrar_enabled = True
        self.compliance_enabled = True
        
        if key_map is None:
            key_map = {}
            key_map.update(ACMEControls.key_map)
        if settings_keys is None:
            settings_keys = ACMEControls.settings_keys[:]
            
        acme.core.ConfigurableObject.__init__(self, key_map=key_map,
                                                settings_keys=settings_keys,
                                                *args, **kwargs)

#MARK: -
#MARK: Classes
class ACMEd(ipc.Server):
    """
    Our primary controller class for our server component. This is the
    portion of ACME that runs in the root context
    """
    
    #MARK: Properties
    base_dir = DEFAULT_SYSTEM_DIR
    
    maintenance_timer = None
    
    karl_enabled = True         #: Variable that determines whether karl will be loaded.
    karl_event_engine = None
    
    agents_enabled = True       #: Variable that determines whether AgentController will be loaded.
    agent_controller = None     #: Our agent controller object
    
    event_handler = None        #: Our event handler
    
    _manifest_dir = None    #: Backing var for manifest_dir property
    _installers_dir = None    #: Backing var for installers_dir property
    _state_dir = None       #: Backing var for state_dir property
    _routes_dir = None      #: Backing var for routes_dir property
    _installers_state_dir = None  #: Backing var for _installers_state_dir property
    
    default_route = None    #: Default route to route events to. Should defined in init
    
    receiving_proxy = None      #: Our daemon EventProxy, used to proxy
                                #: forwarded events from ACME clients.
    
    forwarding_proxy = None     #: Our client EventProxy, used to forward local
                                #: events to running clients.
    
    last_group_cache = None        #: Date that we last cached groups for system owner
    last_group_cache_attempt = None #: Date that we last tried to cache groups
    
    group_cache_frequency = datetime.timedelta(minutes=5) #: How frequently we should refresh groups for the system owner.
    group_cache_max_frequency = datetime.timedelta(seconds=30) #: The max frequency that we will attempt to cache groups in.
    
    use_karl_registrar = True  #: Value which controls whether we utilize a registrar for active system registration.
    

    usher_codesign_verify = True   #: Value which controls whether we verify the installers before installing.
    dynamic_acme_controls = True    #: Value that enables dynamic acme controls.    
    acme_controls = None                #: Value that contains configurations for controlling features in ACME.
    
    @property
    def manifest_dir(self):
        """
        Property which designates in which directory our configuration manifests
        reside.
        """
        
        if self._manifest_dir:
            return self._manifest_dir
        elif self.base_dir:
            return os.path.join(self.base_dir,"manifests")
    
    @manifest_dir.setter
    def manifest_dir(self,value):
        self._manifest_dir = value
    
    @property
    def installers_dir(self):
        """
        Property which designates in which directory our configuration manifests
        reside.
        """
        if self._installers_dir:
            return self._installers_dir
        elif self.base_dir:
            return os.path.join(self.base_dir,"installers")

    @installers_dir.setter
    def installers_dir(self,value):
        self._installers_dir = value

    @property
    def state_dir(self):
        """
        Property which designates in which directory our runtime state
        is saved to.
        """
        if self._state_dir:
            return self._state_dir
        elif self.base_dir:
            return os.path.join(self.base_dir,"state")
    
    @state_dir.setter
    def state_dir(self,value):
        self._state_dir = value
    
    @property
    def installers_state_dir(self):
        """
        Property which designates in which directory our configuration manifests
        reside.
        """
        if self._installers_state_dir:
            return self._installers_state_dir
        elif self.base_dir:
            return os.path.join(self.state_dir,"installers")

    @installers_state_dir.setter
    def installers_state_dir(self,value):
        self._installers_state_dir = value
        
    @property
    def routes_dir(self):
        """
        Property which designates in which directory our route files.
        reside.
        """
        if self._routes_dir:
            return self._routes_dir
        elif self.base_dir:
            return os.path.join(self.base_dir,"routes")
    
    @routes_dir.setter
    def routes_dir(self,value):
        self._routes_dir = value
    
    @property
    def run_directory(self):
        """
        Returns our run file directory
        """
        if self._run_directory:
            return self._run_directory
        elif self.base_dir:
            return os.path.join(self.base_dir,"run")
    
    #MARK: Constructors & Loading Methods
    def __init__(self,base_dir=None,*args,**kwargs):
        """
        Constructor.
        """
        self.acme_controls = ACMEControls()
        self.modules = {}
        self.karl_event_engine = None
        self.agent_controller = acme.agent.AgentController()
        self.compliance_controller = compliance.ComplianceController()
        self.identity = None
        self.claims = claims.Claims(
                            compliance_controller=self.compliance_controller)
        self.aea = aea.AeaModule()
        self.event_handler = systemevents.system_handler
        self.registrant = registration.Applicant() # Registration module that handles registration process
        self.configuration_controller = None
        self.quarantine_handler = quarantine.QuarantineController()

        self.registration_check_frequency = datetime.timedelta(minutes=60)       #: How often we will verify registration status when healthy
        self.registration_check_skew = datetime.timedelta(minutes=15)            #: Our skew for registration checks
        self.registration_retry_frequency = datetime.timedelta(seconds=30)       #: The period we will wait after our first failure
        self.registration_retry_max_frequency = datetime.timedelta(hours=1)      #: The maximum time we will wait before attempting retrying registration
        self.registration_timer = None
        
        self.usher_controller = UsherController()
        self.usher_config_controller = None
        self.usher_health_check_frequency = datetime.timedelta(minutes=5)
        self.usher_health_check_skew = datetime.timedelta(seconds=5)
        
        self.jwt_generated = None
        self.default_route = None

        if base_dir:
            self.base_dir = base_dir

        if not self.logger_name:
            self.logger_name = "ACMEd"

        self.receiving_proxy = acme.systemevents.proxy.DaemonEventProxy()
        self.forwarding_proxy = acme.systemevents.proxy.ClientEventProxy()
        
        self.registration_thread = None
        self.cli_register_status = None
        self.cli_register_status_msg = None
        
        self.evaluation_thread = None
        self.remediation_thread = None
        
        super(ACMEd,self).__init__(*args,**kwargs)

    def setup(self):
        """
        Method to perform basic configuration and register signal handlers
        """

        logger = logging.getLogger(self.logger_name)

        ## Set our default locale
        os.environ["LC_TIME"] = "C.UTF-8"

        ## Make sure our base directory exists
        if self.base_dir and not os.path.exists(self.base_dir):
            os.mkdir(self.base_dir,0755)

        if self.state_dir and not os.path.exists(self.state_dir):
            os.mkdir(self.state_dir,0755)

        if self.manifest_dir and not os.path.exists(self.manifest_dir):
            os.mkdir(self.manifest_dir,0755)
        
        
        if self.installers_dir and not os.path.exists(self.installers_dir):
            os.mkdir(self.installers_dir, 0755)
        
        if self.installers_state_dir and not os.path.exists(self.installers_state_dir):
            os.mkdir(self.installers_state_dir, 0755)
        
        installers_state_config_file = os.path.join(self.installers_state_dir, "InstallerConfig.json")
        
        if os.path.exists(installers_state_config_file):
            self.copy_config_files(installers_state_config_file)
            
        #Loading acme_controls
        acme_controls = self.load_acme_controls()
        if acme_controls:
            self.acme_controls = acme_controls

        logger.debug("Setting up:\n\tBasedir:'{}'\n\tStatedir:'{}'\n\tManifestdir:'{}'\n\tRoutesdir:'{}'".format(
                                                self.base_dir,
                                                self.state_dir,
                                                self.manifest_dir,
                                                self.routes_dir))

        try:
            self.load_systemprofiler()
        except Exception as exp:
            logger.error("Failed to load system profiler: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        self.load_signal_handlers()
        
        self.load_state()
        
        self.load_network()
        
        if self.acme_controls.karl_registrar_enabled:
            try:
                self.load_registration_data()
            except Exception as exp:
                logger.error("Failed to load registration data :{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
            try:
                self.load_identity()
            except Exception as exp:
                logger.error("Failed to load system identity; {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        if self.karl_enabled:
            self.load_karl()
            
        if self.agents_enabled:
            self.load_agent_controller()
        
        if self.acme_controls.compliance_enabled:
            self.load_compliance_controller()
        
        ## Load our configuration system
        if self.acme_controls.karl_registrar_enabled:
            if (self.registrant and self.registrant.is_registered() 
                            and self.identity and self.identity.is_signed()):
                try:
                    self.load_configuration_controller()
                except Exception as exp:
                    logger.error("Failed to start configuration controller! Error:{}".format(
                                            exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        self._register_configuration_controller_handlers()
        
        if self.acme_controls.usher_enabled:
            self.load_usher_controller()
        
        self.register_proxy_forwarders()
    
    def copy_config_files(self, installer_config_file):
        """
        Method to copy the files based on the contents of the config file.
        InstallerConfig.json file's content can be as below
            [
                  {"file_name":"usher_routes.json",
                  "src_location": "config/usher_routes.json",
                  "dest_location": "/usr/local/amazon/var/acme/watcher/routes/usher_routes.json",
                  "overwrite": 1
                  }
            ]
        """
        logger = logging.getLogger(self.logger_name)
        data = None
        with open(installer_config_file) as f:
            data = json.load(f)
        
        #obj=json.load(data)
        path_to_config_file = os.path.dirname(installer_config_file)
        
        for i in data:
            dest_dir = os.path.dirname(i["dest_location"])
            dest_dir_exists = os.path.isdir(dest_dir)
            
            if not dest_dir_exists:
                os.makedirs(dest_dir,mode=0755)
                
            dest_file_exists = os.path.isfile(i["dest_location"])
            src_file = os.path.join(path_to_config_file, i["src_location"])
            src_file_exists = os.path.isfile(src_file)
            
            if src_file_exists:
                if dest_file_exists:
                    if i["overwrite"]:
                        copyfile(src_file, i["dest_location"])
                else:
                    copyfile(src_file, i["dest_location"])
            else:
                logger.error("Found the installer config (InstallerConfig.json) file but the expected config file:{} is missing.".format(i["src_location"]))

    def reload(self):
        """
        Method which will cause our system to reload all configuration files.
        """
        logger = logging.getLogger(self.logger_name)

        logger.info("Reloading configuration...")
        
                
        if self.karl_event_engine:
            try:
                system_identifier = systemprofile.profiler.system_identifier()
                if system_identifier:
                    self.karl_event_engine.default_source = system_identifier
                else:
                    logger.error("Could not establish a system identifier for KARL messaging, communication will be broken!")

            except Exception as exp:
                logger.error("Failed to establish a system identifier for KARL messaging, communication will be broken! Error:{}".format(exp))
            
            self.karl_event_engine.identity = self.identity
            self.karl_event_engine.load_default_routes_map(self.routes_dir)
            self.karl_event_engine.load_routes_map(self.routes_dir)
            self.karl_event_engine.reload()

        self.update_network()

        self.agent_controller.reload()
        self.compliance_controller.reload()
        
        try:
            self.load_systemprofiler(reload=True)
        except Exception as exp:
            logger.error("Failed to load system profiler: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        self.reload_acme_controls()
        '''
        for module in self.modules:
            module.reload()
        '''
    def get_system_identifier(self):
        """
        Method to get system identifier
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("Getting system identifier...")
        try:
            system_identifier = systemprofile.profiler.system_identifier()
        except Exception as exp:
            logger.error("Failed to load system profiler: {}".format(exp))
            system_identifier = None
        return system_identifier
    
    def compliance_evaluate(self, identifier=None):
        """
        Method to run compliance evaluation
        :param string identifier: identifier of compliance module

        """
        logger = logging.getLogger(self.logger_name)

        if self.compliance_controller:
            cc = self.compliance_controller
            module_list = []
            
            if not identifier:
                for cmodule in cc.modules.values():
                    module_list.append(cmodule) 
            else:
                module_list.append(cc.modules.get(identifier))

            for cmodule in module_list:
                if cmodule is not None:
                    if cmodule.status == acme.compliance.ModuleStatus.IDLE:
                        cmodule.evaluate()
                    elif cmodule.status == acme.compliance.ModuleStatus.EVALUATING:
                        logger.info("Compliance module: {} is already evaluating.".format(cmodule.identifier))
                    else:
                        logger.error("Compliance module: {} is in state: {}. Not evaluating.".format(cmodule.identifier, acme.compliance.ModuleStatus.to_string(cmodule.status)))

    def compliance_remediate(self, identifier=None):
        """
        Method to run compliance remediation
        :param string identifier: identifier of compliance module
        
        """
        logger = logging.getLogger(self.logger_name)

        if self.compliance_controller:
            cc = self.compliance_controller
            module_list = []
            
            if not identifier:
                for cmodule in cc.modules.values():
                    module_list.append(cmodule) 
            else:
                module_list.append(cc.modules.get(identifier))

            for cmodule in module_list:
                if cmodule is not None:  
                    if cmodule.status == acme.compliance.ModuleStatus.IDLE:
                        cmodule.remediate()
                    elif cmodule.status == acme.compliance.ModuleStatus.REMEDIATING:
                        logger.info("Compliance module: {} is already remediating.".format(cmodule.identifier))
                    else:
                        logger.error("Compliance module: {} is in state: {}. Not remediating.".format(cmodule.identifier, acme.compliance.ModuleStatus.to_string(cmodule.status)))
    
    def compliance_status(self, include_history=True):
        """
        Method to return our compliance data
        """
        
        data = {}
        if self.compliance_controller:
            cc = self.compliance_controller
            data = {"running" : cc.should_run,
                        "execution_thread_count" : cc.cmodule_executor_count(),
                        "max_execution_thread_count": cc.maxnum_executors,
                        "status": cc.status(),
                        "compliance_status": cc.get_device_status(),
                        "compliance_deadline": None,
                        "isolation_deadline": None,
                        "plugin_path" : cc.plugin_path,
                        "queue_length" : None,
                        "state_dirpath" : cc.state_dirpath,
                        "manifest_dirpath" : cc.manifest_dirpath,
                        "modules" : {},
                    }
            
            deadline = cc.compliance_deadline()
            if deadline:
                data["compliance_deadline"] =  "{}".format(deadline)
            
            deadline = cc.isolation_deadline()
            if deadline:
                data["isolation_deadline"] = "{}".format(deadline)
            
            if cc.execution_queue:
                data["queue_length"] = len(cc.module_queue_data)

            for module in cc.modules.values():
                data["modules"][module.identifier] = module.to_dict()
                if not include_history:
                    data["modules"][module.identifier]["evaluation_history"] = []
                    data["modules"][module.identifier]["remediation_history"] = []
        
        return data
            
    def get_current_user(self):
        """
        Method to get the current user
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("Getting current user...")
        try:
            current_user = systemprofile.profiler.current_user()
        except Exception as exp:
            logger.error("Failed to load current user: {}".format(exp))
            current_user = None
        return current_user


    def load_signal_handlers(self):
        """
        Method to setup our POSIX signal handlers.
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("Registering signal handlers...")

        signal.signal(signal.SIGTERM,self.handle_signal)
        signal.signal(signal.SIGCONT,self.handle_signal)
        signal.signal(signal.SIGHUP,self.handle_signal)
        signal.signal(signal.SIGUSR1,self.handle_signal)
        signal.signal(signal.SIGUSR2,self.handle_signal)
        signal.signal(signal.SIGINT,self.handle_signal)

    def load_systemprofiler(self, reload=None):
        """
        Method which bootstraps our systemprofiler caches.
        """

        logger = logging.getLogger(self.logger_name)

        systemprofile.profiler.base_dir = self.base_dir

        ds_group_cache_file = os.path.join(self.state_dir,
                                                        "group_cache.data")

        dsp = systemprofile.directoryservice.profiler

        if not reload:
            if os.path.exists(ds_group_cache_file):
                try:
                    logger.debug("Loading group cache from file:'{}'".format(
                                                        ds_group_cache_file))
                    dsp.group_cache.load_from_file(ds_group_cache_file)
                except Exception as exp:
                    logger.warning("Failed to load group cache from file:'{}'. Error:{}".format(
                                                        ds_group_cache_file,exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        else:
            dsp.expire_group_cache()
            self.last_group_cache = None

    def load_network(self):
        """
        Method which sets up initial network state and configures
        network change listeners.
        """

        logger = logging.getLogger(self.logger_name)

        logger.debug("Loading network...")

        if not network.state.site_info:
            site = network.NetworkSiteInfo()
        else:
            site = network.state.site_info

        ## Todo: move this to a config file
        site.temporary_site_filter = ".*Datacenter.*"
        network.site_info = site

        network_state_file = os.path.join(self.base_dir,"state",
                                                                "network.data")
        if os.path.exists(network_state_file):
            try:
                logger.log(5, "Loading network state from file:'{}'...".format(
                                                        network_state_file))
                network.state.load_from_file(network_state_file)
            except Exception as exp:
                logger.warning("Failed to load network state from file:'{}'. Error:{}".format(
                                                    network_state_file,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        try:
            logger.log(5, "Updating network state...")
            network.state.update()
        except Exception as exp:
            logger.warning("Failed to update network state! Error:{}".format(
                                                    exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        self.event_handler.register_handler("NetworkDidChange",
                                                        self.update_network)
        self.event_handler.register_handler("NetworkSessionDidChange",
                                            self.network_session_did_change)
    def update_network(self):
        """
        Method which updates our network state from file.
        """

        network_state_file = os.path.join(self.state_dir,"network.data")
        network.state.update()
        network.state.save_to_file(filepath=network_state_file)
    
    def update_claims_status(self,token_state):
        """
        Method to report claims status to KARL
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("Reporting claims status to karl")
        now = datetime.datetime.utcnow()
        self.claims.last_token_generation_attempt = now.strftime(acme.DATE_FORMAT)
        self.claims.token_state = token_state
        self.claims.event_type = 'ClaimsUpdateEvent'
        payload = self.claims.to_dict(output_null=False)
        event = Event(type='ClaimsUpdateEvent',subject_area='Claims',payload=payload)
        pykarl.event.dispatcher.dispatch(event)

    def network_session_did_change(self,new_session=None,old_session=None,
                                                            *args,**kwargs):
        """
        Method to report network session changes to KARL
        """

        key_map = {
            "session_uuid" : "session_guid",
            "state" : None,
            "ip_address" : None,
            "start" : "<type=datetime,format=epoch>",
            "end" : "<type=datetime,format=epoch>"
        }

        logger = logging.getLogger(self.logger_name)

        logger.debug("NetworkSession Change detected!")

        if old_session and old_session.session_end:
            payload = old_session.to_dict(key_map=key_map,output_null=False)
            event = Event(type="NetworkSessionEnd",subject_area="Network",
                                                        payload=payload)
            pykarl.event.dispatcher.dispatch(event)


        if new_session:
            payload = new_session.to_dict(key_map=key_map,output_null=False)
            event = Event(type="NetworkSessionStart",subject_area="Network",
                                                        payload=payload)
            pykarl.event.dispatcher.dispatch(event)

    def load_usher_controller(self, acme_controls=None):
        """
        Method to load Usher Controller
        """
        if not acme_controls:
            acme_controls = self.acme_controls
        logger = logging.getLogger(self.logger_name)
        logger.info("Loading UsherController...")
        try:
            self.usher_controller.identity=self.identity
            self.usher_controller.registrant=self.registrant
            self.usher_controller.karl_event_engine=self.karl_event_engine
            self.usher_controller.health_check_frequency=self.usher_health_check_frequency
            self.usher_controller.health_check_skew=self.usher_health_check_skew
            self.usher_controller.health_check_enabled=acme_controls.usher_enabled
            self.usher_controller.usher_load_path = os.path.join(self.base_dir, "installers")
            self.usher_controller.verify_codesign_enabled =  self.usher_codesign_verify
            
            if not os.path.exists(self.usher_controller.usher_load_path):
                os.makedirs(self.usher_controller.usher_load_path, mode = 0755)
        except Exception as exp:
            logger.error("Failed to load UsherController. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
    
    def start_usher_controller(self):
        """
        Method to start Usher Controller
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("Starting UsherController...")
        try:
            if self.usher_controller:
                self.usher_controller.start()
        except Exception as exp:
            logger.error("Failed to start usher_controller. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1) 
            
    def stop_usher_controller(self):
        """
        Method to start Usher Controller
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("Stopping UsherController...")
        try:
            if self.usher_controller:
                self.usher_controller.stop()
        except Exception as exp:
            logger.error("Failed to stop usher_controller. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
    
    def reload_usher_controller(self):
        """
        Method to reload Usher Controller
        """
        logger = logging.getLogger(self.logger_name)
        logger.info("Reloading UsherController...")
        try:
            if self.acme_controls.usher_enabled and self.usher_controller:
                self.usher_controller.karl_event_engine = self.karl_event_engine
                self.usher_controller.reload()
        except Exception as exp:
            logger.error("Failed to reload usher_controller. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
    
    def load_agent_controller(self):
        """
        Method to load our AgentController.
        """

        logger = logging.getLogger(self.logger_name)
        logger.info("Loading AgentController...")

        ac = self.agent_controller

        if self.manifest_dir:
            ac.manifest_dirpath = os.path.join(self.manifest_dir,"service_agents")

        if self.state_dir:
            ac.state_dirpath = os.path.join(self.state_dir,"service_agents")

        if self.base_dir:
            ac.plugin_path = os.path.join(self.base_dir,"service_agents")

        try:
            ac.load()
        except Exception as exp:
            logger.error("Failed to load AgentController. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

    def load_compliance_controller(self):
        """
        Method to load our ComplianceController.
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.info("Loading ComplianceController...")
        
        cc = self.compliance_controller
        
        if self.manifest_dir:
            cc.manifest_dirpath = os.path.join(self.manifest_dir,"compliance_modules")
        
        if self.state_dir:
            cc.state_dirpath = os.path.join(self.state_dir,"compliance_modules")
                    
        cc.plugin_path = os.path.join(self.base_dir, "compliance_modules")
        
    def load_registration_data(self):
        """
        Method used to validate and/or establish a new system identifier with
        the KARL registrar service.
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.info("Loading registration data...")
        registration_data_file = os.path.join(self.manifest_dir,
                                                "registration_data.json")
        if os.path.exists(registration_data_file): 
            logger.debug("Loading registration data from file:'{}'".format(
                                                registration_data_file))
            self.registrant.load_from_file(registration_data_file)
    
    def load_identity(self):
        """
        Method used to load our identity.
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.info("Loading system identity...")
        
        system_identifier = systemprofile.profiler.system_identifier()
        
        if system_identifier is None:
            raise crypto.IdentityNotFoundError("No system identifier is established!")
        
        identity = crypto.Identity(common_name=system_identifier)
        identity.identity_directory = os.path.join(self.base_dir, "identity")
        identity.load()
        
        self.identity = identity
    
    def register_system(self, token=None, auth_type=None):
        """
        Method which is used to register our system with KARL.
        """

        logger = logging.getLogger(self.logger_name)
        
        is_new_uuid = False
        current_uuid = systemprofile.profiler.system_identifier()
        if current_uuid is None:
            current_uuid = "{}".format(uuid.uuid4())
            is_new_uuid = True
        
        if auth_type is None:
            auth_type = registration.AuthType.KERBEROS_SYSTEM
        
        if not self.manifest_dir:
            raise pykarl.core.ConfigurationError("No manifest directory specified!")
        else:
            config_path = os.path.join(self.manifest_dir,"registration.json")
        
        self.registrant.uuid = current_uuid
        self.registrant.load_from_file(config_path)
        
        logger.log(25, "Registering system with registrar ('{}')...".format(
                                        self.registrant.registrar_address))
        
        new_uuid = current_uuid
        with self.registrant.credential_session(auth_type):
            try:
                self.registrant.negotiate(token = token)
                new_uuid = str(self.registrant.uuid)
                logger.info("Successfully negotiated UUID:'{}' with registrar...".format(self.registrant.uuid))
            except acme.registration.RegistrationUUIDReset as exp:
                logger.info(exp)
                new_uuid = str(exp.new_uuid)
                is_new_uuid = True
        
        ## Always create new key material during registration
        identity = crypto.Identity(common_name=new_uuid)
        identity.generate()
        identity.save()
        
        csr_data = identity.create_csr(output=crypto.Type.PEM)
        reg_data = self.registrant.register(csr_data=csr_data)
        registration_filepath = os.path.join(self.manifest_dir,"registration_data.json")
        self.registrant.load_dict(reg_data)
        self.registrant.save_to_file(filepath = registration_filepath,
                                                            data=reg_data)
        identity.process_csr(cert=reg_data["certificate"])
        self.identity = identity
        identity.save()
        
        if is_new_uuid:
            logger.debug("Saving new system identifier: '{}'".format(new_uuid))
            systemprofile.profiler.set_system_identifier(new_uuid)
        
        
        ## Reload KARL
        self.load_karl()
        
        ## Todo: save config data
        
        logger.log(25, "System successfully registered with ID: '{}'!".format(
                                                        self.registrant.uuid))
        logger.debug("Registration data:\n{}".format(
                                            json.dumps(reg_data, indent=4)))
        
        ## Trigger SystemDidRegister event asyncronously
        if self.event_handler:
            handler = self.event_handler
        else:
            handler = systemevents.system_handler
        try:
            t = threading.Thread(target=handler.system_did_register)
            t.start()
        except Exception as exp:
            logger.error("Failed to trigger SystemDidRegister event! Error:{}".format(
                                                                exp.message))

    def renew_registration(self):
        """
        Method which is used to renew our system with KARL.
        """

        logger = logging.getLogger(self.logger_name)
        logger.info("Renewing system with registrar...")
        
        is_new_uuid = False
        current_uuid = systemprofile.profiler.system_identifier()
        if current_uuid is None:
            current_uuid = "{}".format(uuid.uuid4())
            is_new_uuid = True
        
        if not self.manifest_dir:
            raise pykarl.core.ConfigurationError("No manifest directory specified!")
        else:
            config_path = os.path.join(self.manifest_dir,"registration.json")
        
        self.registrant.uuid = current_uuid
        self.registrant.load_from_file(config_path)
        
        new_uuid = current_uuid
        
        csr_data = self.identity.create_csr(output=crypto.Type.PEM)
        reg_data = self.registrant.renew(identity=self.identity, csr_data=csr_data)
        registration_filepath = os.path.join(self.manifest_dir,"registration_data.json")
        self.registrant.renewal_date = None
        self.registrant.load_dict(reg_data)
        self.registrant.save_to_file(filepath = registration_filepath,
                                                            data=reg_data)
        self.identity.process_csr(cert=reg_data["certificate"])
        self.identity.save()
        
        if is_new_uuid:
            logger.debug("Saving new system identifier: '{}'".format(new_uuid))
            systemprofile.profiler.set_system_identifier(new_uuid)
        
        
        ## Reload KARL
        self.load_karl()
        
        ## Todo: save config data
        
        logger.info("System Renewal complete.")
        logger.log(5, "Renewal data:\n{}".format(
                                            json.dumps(reg_data, indent=4)))
        
        ## Trigger SystemDidRegister event asyncronously
        if self.event_handler:
            handler = self.event_handler
        else:
            handler = systemevents.system_handler
        try:
            t = threading.Thread(target=handler.system_did_register)
            t.start()
        except Exception as exp:
            logger.error("Failed to trigger SystemDidRegister event! Error:{}".format(
                                                                exp.message))
            
    def register_system_using_cli(self, token):
        """
         Method which is used to register our system with KARL through CLI. This method is a wrapper around register_system().
         param str token: Token generated from Midway portal, passed through CLI.
        """

        logger = logging.getLogger(self.logger_name)
        logger.info("Registering system using token...")
        
        try:
            
            self.register_system(token, auth_type=registration.AuthType.NONE)
            self.cli_register_status = True            
        except registration.RegistrationError as registration_exp:
            self.cli_register_status = False
            self.cli_register_status_msg = registration_exp.message
            
        except Exception as exp:
            self.cli_register_status = False
            self.cli_register_status_msg = exp.message
    
    def check_registration(self):
        """
        Method to verify whether or not our system is registered.
        
        :returns: (bool) True if we are registered
        :returns: (bool) True if we are need renewal
        
        :raises: :py:class:`RegistrationError` If the system is not registered
        """
        
        needs_registration, needs_renewal = False, False
        message = None
        if not self.registrant:
            message = "System has no registration data!"
            needs_registration = True
        elif self.registrant:
            try:
                if not self.registrant.is_registered():
                    message = "System is not registered!"
                    needs_registration = True
            except Exception as exp:
                message = "Could not verify registration status: {}".format(
                                                        exp.message)
                raise acme.registration.RegistrationError(message), None, sys.exc_info()[2]
        
        if needs_registration:
            raise acme.registration.RegistrationError(message)
        
        if not self.identity: 
            message = "System has no loaded identity!"         
            needs_registration = True
        elif self.identity:
            try:
                if not self.identity.is_signed():
                    needs_registration = True
                    message = "Loaded identity is self-signed!"
            except Exception as exp:
                message = "Could not verify identity status: {}".format(
                                                        exp.message)
                raise acme.registration.RegistrationError(message), None, sys.exc_info()[2]
        
        if needs_registration:
            raise acme.registration.RegistrationError(message)
        
        renew_datetime = self.registrant.get_renewal_datetime()
        if not self.registrant.renewal_date:
            reg_data = self.registrant.to_dict()
            reg_data["renewal_date"] = str(renew_datetime)
            registration_filepath = os.path.join(self.manifest_dir,"registration_data.json")
            self.registrant.save_to_file(registration_filepath, reg_data)
            self.load_registration_data()
        
        needs_renewal = datetime.datetime.utcnow() > renew_datetime
        
        if needs_renewal:
            message = "Certificate is about to expire, needs renewal!"
            raise acme.registration.RegistrationRenewalError(message)
        
        return needs_registration, needs_renewal
        
    def registration_handler(self, *args, **kwargs):
        """
        Method which is responsible for verifying we are registered and
        renewing registration as necessary.
        """
        
        logger = logging.getLogger(__name__)
        
        logger.debug("Checking registration status...")
        needs_registration = False
        try:
            needs_registration, needs_renewal = self.check_registration()
        except acme.registration.RegistrationRenewalError as exp:
            needs_renewal = True
            logger.warning("Registration renewal check failed: {}".format(exp.message))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        except Exception as exp:
            needs_registration = True
            logger.warning("Registration check failed: {}".format(exp.message))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        

        ## Todo: Re-registration checks go here
        if needs_registration:
            self.register_system()
            return
        
        if needs_renewal:
            self.renew_registration()
            
    def post_registration_handler(self, *args, **kwargs):
        """
        Method which performs post-registration activities, such as
        ensuring our configuration controller is properly configured and
        running.
        """
        
        logger = logging.getLogger(__name__)
        try:
            if self.configuration_controller:
                self.configuration_controller.stop()
        except Exception as exp:
            logger.warning("Failed to stop configuration controller after registration! Error:{}".format(
                                            exp.message))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

            
        try:
            self.load_configuration_controller()
            self.configuration_controller.start()
            if self.acme_controls.usher_enabled:
                self.usher_config_controller.start()
        except Exception as exp:
            logger.error("Failed to start configuration controller after registration! Error:{}".format(
                                            exp.message))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        # Report registration to Usher
        try:
            if self.acme_controls.usher_enabled and self.usher_controller:
                self.usher_controller.report_health_status()
        except Exception as exp:
            logger.error("Failed to start send health report change after registration! Error:{}".format(
                                            exp.message))
    
    def load_karl(self):
        """
        Method to load KARL.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.info("Loading KARL...")
        
        ## Setup our event engine
        if not self.karl_event_engine:
            self.karl_event_engine = EventEngine()
        
        ee = self.karl_event_engine
        
        if not self.manifest_dir:
            raise pykarl.core.ConfigurationError("No manifest directory specified!")
            
        ee.action_stream_map_filepath = os.path.join(self.manifest_dir,
                                            "karl_streams.config")
        
        ee.queue_file_path = os.path.join(self.state_dir,"karl_queue.data")
        
        ee.karl.kinesis_region = "us-west-2" ## Todo: may be a good idea to move this to the streams file path (or creds file)
        ee.karl.firehose_region = "us-west-2"
        ee.load_default_routes_map(self.routes_dir)
        ee.load_routes_map(self.routes_dir)
        ## Ensure we have an identifier, if not create one and fire an event
        system_identifier = systemprofile.profiler.system_identifier()
        new_system = None
        if not system_identifier:
            new_system = True
            
            if not self.acme_controls.karl_registrar_enabled:
                system_identifier = str(uuid.uuid4())
                systemprofile.profiler.set_system_identifier(system_identifier)
            else:
                logger.error("KARL could not establish a system identifier, messaging will be broken!")
        
        ee.default_source = system_identifier
        ee.identity = self.identity
        ee.load()
        
        ## Register our event handlers
        self.event_handler.register_handler("NetworkDidChange",
                                                        ee.network_changed)

        ## Setup our dispatcher
        delegates = pykarl.event.dispatcher.delegates
        if not self.karl_event_engine.commit_event in delegates:
            delegates.append(self.karl_event_engine.commit_event)

        if new_system:
            system_identifier = str(uuid.uuid4())
            systemprofile.profiler.set_system_identifier(system_identifier)

            event = Event(type="SystemRegInfo",subject_area="ACME")
            event.payload["mac_address"] = systemprofile.profiler.mac_address()
            event.payload["hostname"] = systemprofile.profiler.hostname()
            event.payload["hardware_id"] = systemprofile.profiler.hardware_identifier()
            event.payload["platform"] = acme.platform

            pykarl.event.dispatcher.dispatch(event)
    
    def load_acme_controls(self):
        """
        Reads the ACME controls dynamically from file and returns the controls
        """
        if not self.dynamic_acme_controls:
            return
        acme_controls = ACMEControls()
        logger = logging.getLogger(self.logger_name)
        file_name = "acme.json"
        acme_controls_path = os.path.join(self.manifest_dir, file_name)
        logger.info("Loading ACME controls from {}".format(acme_controls_path))
        try:
            acme_controls.load_settings(filepath=acme_controls_path)
        except Exception as exp:
            logger.error("Failed to load acme_controls due to error :{}".format(exp))
        return acme_controls
    
    def reload_acme_controls(self):
        """
        Loads the ACME controls dynamically from file and reloads the system
        """
        logger = logging.getLogger(self.logger_name)
        acme_controls = self.load_acme_controls()
        if acme_controls is not None:
            #Processing change in controls.
            try:
                if not acme_controls.usher_enabled == self.acme_controls.usher_enabled:
                    logger.info("Controls changed: Processing acme controller usher_enabled")
                    if not acme_controls.usher_enabled:
                        self.stop_usher_controller()
                        self.stop_usher_configuration_controller()
                    else:
                        self.load_usher_controller(acme_controls)
                        self.start_usher_controller()
                        self.load_usher_configuration_controller()
                        self.start_usher_configuration_controller()
            except Exception as exp:
                logger.error("Unable to process acme_controls usher_enabled:{0} with error:{1}".format(acme_controls.usher_enabled,exp))
            
            try:
                if not acme_controls.usher_watcher_enabled ==self.acme_controls.usher_watcher_enabled:
                    logger.info("Controls changed: Processing acme controller usher_watcher_enabled")
                    if not acme_controls.usher_watcher_enabled:
                        self.usher_controller.disable_watcher()
                    else:
                        self.usher_controller.enable_watcher()
            except Exception as exp:
                logger.error("Unable to process acme_controls usher_watcher_enabled:{0} with error:{1}".format(acme_controls.usher_enabled,exp))
            
            try:
                if not acme_controls.compliance_enabled == self.acme_controls.compliance_enabled:
                    logger.info("Controls changed: Processing acme controller compliance_enabled")
                    if not acme_controls.compliance_enabled:
                        self.compliance_controller.stop()
                    else:
                        self.load_compliance_controller()
                        self.compliance_controller.start()
            except Exception as exp:
                logger.error("Unable to process acme_controls compliance_enabled:{0} with error:{1}".format(acme_controls.usher_enabled,exp))

            self.acme_controls = acme_controls
            
    def load_state(self):
        """
        Method to load our state
        """

        logger = logging.getLogger(self.logger_name)
        logger.debug("Loading State...")

        if not self.state_dir:
            return

        network_state_file = os.path.join(self.state_dir,"network.data")
        if os.path.exists(network_state_file):
            try:
                network.state.load_from_file(network_state_file)
            except Exception as exp:
                logger.warning("Failed to load state from file:{}. Error:{}".format(network_state_file,exp),exc_info=1)

            ## Todo: load state for all our compliance modules and agents
            pass

    def load_modules(self):
        """
        Method which instantiates our modules.
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("Loading Modules...")

        self.modules["Updates"] = compliance.ComplianceModule(name="Updates")
        self.modules["Updates"].status = "NonCompliant"

        self.modules["Crypto"] = compliance.ComplianceModule(name="Crypto")
        self.modules["Crypto"].status = "Compliant"

    def save_state(self):
        """
        Method to save our state
        """

        logger = logging.getLogger(self.logger_name)
        logger.debug("Saving State...")

        if self.state_dir and not os.path.exists(self.state_dir):
            os.mkdir(self.state_dir)

        ds_group_cache_file = os.path.join(self.state_dir,
                                                        "group_cache.data")
        try:
            systemprofile.directoryservice.profiler.group_cache.save_to_file(ds_group_cache_file)
        except Exception as exp:
            logger.warning("Failed to save DirectoryService state to file:'{}'. Error:{}".format(
                                                ds_group_cache_file,
                                                exp))

        network_state_file = os.path.join(self.state_dir,"network.data")
        try:
            network.state.save_to_file(network_state_file)
        except Exception as exp:
            logger.warning("Failed to load state from file:{}. Error:{}".format(network_state_file,exp),exc_info=1)
        ## Todo: save state for our compliance modules

        ## Todo: save state for our agents

        ## Save KARL state
        if self.karl_enabled and self.karl_event_engine:
            self.karl_event_engine.save()

    #MARK: Service control methods
    def start(self):
        """
        Initiate our service workers.
        """

        logger = logging.getLogger(self.logger_name)
        logger.log(25,"Starting {} IPC Service...".format(self.__class__.__name__))
        
        if self.acme_controls.karl_registrar_enabled:
            if self.registrant.is_registered() and self.identity:
                try:
                    self.start_configuration_controller()
                except Exception as exp:
                    logger.error("Failed to start configuration controller: {}".format(exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                
                #If ACME is not registered, Watcher will try to remediate at first and then install baseline version of ACME.
                if self.acme_controls.usher_enabled:
                    try:
                        self.start_usher_configuration_controller()
                    except Exception as exp:
                        logger.error("Failed to start usher configuration controller: {}".format(exp))
                        logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
            ## Start our registration handler
            self.start_registration_handler()
        
        if self.karl_enabled and self.karl_event_engine:
            self.start_karl_engine()
        
        if self.agents_enabled:
            self.start_agent_controller()
        
        if self.acme_controls.compliance_enabled:
            self.start_compliance_controller()
        
        if self.acme_controls.usher_enabled:
            self.start_usher_controller()
        
        super(ACMEd,self).start()
        
        ## kicking off maintenance routine and compliance check for initial acme start
        self.run_routine_checks()
        self.start_routine_checks()
        self.start_event_listener()
        
        self.startup_finished()
        
        #time.sleep(30)
        #self.stop()
        
    def load_configuration_controller(self):
        """
        Method that will load our configuration controller settings and 
        then will start our controller
        
        :raises: :py:class:`acme.configuration.ConfigurationError` If necessary assets are not provided.
        """
        
        logger = logging.getLogger(__name__)
        logger.info("Loading Configuration Controller...")
        
        ## Make sure we're registered
        if not self.registrant:
            raise acme.configuration.ConfigurationError("Cannot load configuration controller: no registration controller is configured!")
        elif not self.registrant.is_registered():
            raise acme.configuration.ConfigurationError("Cannot load configuration controller: system is not registered!")
        
        if not self.identity:
            raise acme.configuration.ConfigurationError("Cannot load configuration controller: no identity is available!")
        elif not self.identity.is_signed():
            raise acme.configuration.ConfigurationError("Cannot load configuration controller: loaded identity is self-signed!")
        
        ## Create our runtime directories
        config_state_dir = os.path.join(self.state_dir, "config")
        if not os.path.exists(config_state_dir):
            try:
                os.makedirs(config_state_dir, mode=0755)
            except Exception as exp:
                raise acme.configuration.ConfigurationError("Could not create state dir at path:'{}'. {}".format(
                                        config_state_dir, 
                                        exp.message)), None, sys.exc_info()[2]
        
        config_manifest_dir = os.path.join(self.manifest_dir, "config")
        if not os.path.exists(config_manifest_dir):
            try:
                os.makedirs(config_manifest_dir, mode=0755)
            except Exception as exp:
                raise acme.configuration.ConfigurationError("Could not create manifest dir at path:'{}'. {}".format(
                                        config_manifest_dir, 
                                        exp.message)), None, sys.exc_info()[2]    
        
        ## Init our primary controller
        config_controller = acme.configuration.ConfigurationController(
                                        identity=self.identity,
                                        registrant=self.registrant)
        self.registrant.checkin_lock = config_controller.checkin_lock
        self.configuration_controller = config_controller
        
        ## Setup STS token config fetching
        name = "STSTokenConfigModule"
        settings_filepath = os.path.join(config_manifest_dir, "{}.json".format(name))
        state_filepath = os.path.join(config_state_dir, "{}.json".format(name))
        
        sts = configuration.ststoken.STSTokenConfigModule(name=name,
                                        controller=config_controller,
                                        karl_engine=self.karl_event_engine,
                                        routes_dir=self.routes_dir,
                                        state_filepath=state_filepath,
                                        settings_filepath=settings_filepath,
                                        state_dir=self.state_dir,
                                        manifest_dir=self.manifest_dir
                                    )
        config_controller.register_module(sts)
        
        ## Setup Module fetching
        name = "ComplianceConfigModule"
        settings_filepath = os.path.join(config_manifest_dir, "{}.json".format(name))
        state_filepath = os.path.join(config_state_dir, "{}.json".format(name))
        load_path = os.path.join(self.base_dir, "compliance_modules")
        staging_path = os.path.join(self.state_dir, "compliance_modules", 
                                                                "staging")
        
        cm = configuration.compliance.ComplianceConfigModule(name = name,
                controller=config_controller,
                compliance_controller=self.compliance_controller,
                settings_filepath=settings_filepath,
                load_path=load_path,
                staging_path=staging_path)
                
        config_controller.register_module(cm) 
        ## Load modules
        try:
            cm.load_compliance_modules()
        except Exception as exp:
            logger.warning("Failed to load compliance modules from disk. {}".format(
                                                        exp.message))
        config_controller.register_module(cm)        
        

        if self.acme_controls.usher_enabled:
            self.load_usher_configuration_controller()
        
        ## Setup ConfigFile fetching
        name = "FileConfigModule"
        
        settings_filepath = os.path.join(config_manifest_dir, "{}.json".format(name))
        state_filepath = os.path.join(config_state_dir, "{}.json".format(name))
        
        cf = configuration.configfile.ConfigurationFileConfigModule(name=name,
                                        state_filepath=state_filepath,
                                        settings_filepath=settings_filepath,
                                        state_dir=self.state_dir,
                                        manifest_dir=self.manifest_dir
                                    )
        config_controller.register_module(cf)
    
    def start_configuration_controller(self):
        """
        Method to start our configuration controller.
        
        :raises: :py:class:`acme.configuration.ConfigurationError` If necessary assets are not provided.
        
        """
        if not self.configuration_controller:
            raise acme.configuration.ConfigurationError("Cannot start configuration controller: no  controller is configured!")
        self.configuration_controller.start()
        
    def load_usher_configuration_controller(self):
        """
        Method to load our configuration controller.
        
        :raises: :py:class:`acme.configuration.ConfigurationError` If necessary assets are not provided.
        
        """
        
        usher_config_controller = acme.usher_configuration.UsherConfigurationController(
                                        identity=self.identity,
                                        registrant=self.registrant)
        self.usher_config_controller = usher_config_controller
    
        ## Setup Installer fetching
        name = "UsherConfigModule"
        config_installers_dir = os.path.join(self.manifest_dir, "config")
        usher_config_state_dir = os.path.join(self.state_dir, "installers")
        settings_filepath = os.path.join(config_installers_dir, "{}.json".format(name))
        state_filepath = os.path.join(usher_config_state_dir, "{}.json".format(name))
        load_path = os.path.join(self.base_dir, "installers")
        staging_path = os.path.join(self.state_dir, "installers", 
                                                            "staging")
    
        um = usher_config.UsherConfigModule(name = name,
            controller=usher_config_controller,state_filepath=state_filepath,
            usher_controller=self.usher_controller,
            settings_filepath=settings_filepath,
            load_path=load_path,
            staging_path=staging_path, verify_codesign_enabled = self.usher_codesign_verify)
            
        self.usher_config_controller.register_module(um) 
        
    def start_usher_configuration_controller(self):
        """
        Method to start our usher configuration controller.
        
        :raises: :py:class:`acme.configuration.ConfigurationError` If necessary assets are not provided.
        
        """
        
        if not self.usher_config_controller:
            raise acme.usher_configuration.ConfigurationError("Cannot start usher configuration controller: no  controller is configured!")
        self.usher_config_controller.start()
        
    def stop_configuration_controller(self):
        """
        Method to stop our configuration controller
        """
        
        if self.configuration_controller:
            self.configuration_controller.stop()
    
    def stop_usher_configuration_controller(self):
        """
        Method to stop our usher configuration controller
        """
        if self.usher_config_controller:
            self.usher_config_controller.stop()
            
    def startup_finished(self):
        """
        Method called when startup finishes. Currently unused.
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.log(25, "Daemon finished startup!")
        
    def stop(self):
        """
        Method to stop our service workers.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.log(25,"Stopping {}...".format(self.__class__.__name__))
        
        if self.configuration_controller:
            try:
                self.stop_configuration_controller()
            except Exception as exp:
                logger.error("Failed to stop configuration controller: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        if self.usher_config_controller:
            try:
                self.stop_usher_configuration_controller()
            except Exception as exp:
                logger.error("Failed to stop configuration controller: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        if self.karl_event_engine:
            try:
                pykarl.event.dispatcher.delegates.remove(self.karl_event_engine.commit_event)
            except ValueError:
                pass
            
            self.stop_karl_engine()
        
        if self.registration_timer:
            try:
                self.stop_registration_handler()
            except Exception as exp:
                logger.error("Failed to stop registration handler: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        if self.agent_controller:
            try:
                self.stop_agent_controller()
            except Exception as exp:
                logger.error("Failed to stop AgentController: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        if self.compliance_controller:
            try:
                self.stop_compliance_controller()
            except Exception as exp:
                logger.error("Failed to stop compliance controller: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        if self.usher_controller:
            try:
                self.stop_usher_controller()
            except Exception as exp:
                logger.error("Failed to stop usher controller: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                         
        super(ACMEd,self).stop()
        
        if self.maintenance_timer:
            self.maintenance_timer.cancel()
        
        self.stop_event_listener()
    
    def start_routine_checks(self):
        if not self.maintenance_timer:
            self.maintenance_timer = acme.RecurringTimer(
                            frequency=ROUTINE_TIMER_INTERVAL.total_seconds(),
                            handler=self.run_routine_checks)
            self.maintenance_timer.start()
    
    def start_registration_handler(self):
        """
        Method which will start our registration handler, which 
        is responsible for maintaining active healthy registration with
        KARL.
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("Starting registration handler...")
        
        ## Perform a syncronous registration check and attempt
        was_error = False
        try:
            self.registration_handler()
        except Exception as exp:
            was_error = True
        
        ## Attempt to tear down our handler in case one already exists
        self.stop_registration_handler()
        
        timer = acme.core.RecurringTimer(
                                name="RegistrationTimer",
                                frequency=self.registration_check_frequency,
                                handler=self.registration_handler
                                )
        timer.retry_frequency = self.registration_retry_frequency
        timer.max_retry_frequency = self.registration_retry_max_frequency
        timer.skew = self.registration_check_skew
        
        timer.start()
        
        self.registration_timer = timer
        
        ## If our syncronous execution failed, iterate our fail count and
        ## reset our timer
        if was_error:
            timer._num_consecutive_failures = 1
            timer.reset(self.registration_retry_frequency)
            
    def stop_registration_handler(self):
        """
        Method to stop our registration handler.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        ## Setup our timer
        try:
            if self.registration_timer:
                self.registration_timer.cancel()
        except Exception as exp:
            logger.warning("Failed to shutdown existing registration handler; {}".format(
                                                    exp.message))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
    
    def start_event_listener(self):
        """
        Method which will setup and start our event listeners,
        which hook into OS facilities to monitor a variety of events.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        handler = self.event_handler
        
        try:
            handler.register_subsystems()
        except Exception as exp:
            logger.critical("An error occurred when registering subsystems: {}".format(exp.message))
            logger.log(5,"Failure stack trace (handled cleanly):", exc_info=1)
        
        try:
            handler.start_listener()
        except Exception as exp:
            logger.critical("An error occurred when attempting to start our event listener: {}".format(exp.message))
            logger.log(5,"Failure stack trace (handled cleanly):", exc_info=1)

    def stop_event_listener(self):
        """
        Method which will stop our event listeners
        """
        logger = logging.getLogger(self.logger_name)
        handler = self.event_handler
        
        try:
            handler.stop_listener()
        except Exception as exp:
            logger.critical("An error occurred when attempting to stop our event listener: {}".format(exp.message))
            logger.log(5,"Failure stack trace (handled cleanly):", exc_info=1)
        
        try:
            handler.unregister_subsystems()
        except Exception as exp:
            logger.critical("An error occurred when unregistering subsystems: {}".format(exp.message))
            logger.log(5,"Failure stack trace (handled cleanly):", exc_info=1)
        
    def start_agent_controller(self):
        """
        Method to start our agent controller.
        """

        logger = logging.getLogger(self.logger_name)

        ac = self.agent_controller

        try:
            self._register_agent_controller_handlers()
        except Exception as exp:
            logger.error("Failed to start AgentController. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

            ## Todo: report error to KARL?

        try:
            ac.start()
        except Exception as exp:
            logger.error("Failed to start AgentController. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

            ## Todo: report error to KARL?

    def start_compliance_controller(self):
        """
        Method to start our agent controller.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        cc = self.compliance_controller
        try:
            cc.start()
        except Exception as exp:
            logger.error("Failed to start compliance_controller. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1) 

    def stop_agent_controller(self):
        """
        Method to start our agent controller.
        """
        logger = logging.getLogger(self.logger_name)

        ac = self.agent_controller

        try:
            ac.stop()
        except Exception as exp:
            logger.error("Failed to stop AgentController. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        try:
            self._unregister_agent_controller_handlers()
        except Exception as exp:
            logger.error("Failed to unregister AgentController handlers. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

    def stop_compliance_controller(self):
        """
        Method to start our compliance controller.
        """
        logger = logging.getLogger(self.logger_name)

        cc = self.compliance_controller

        try:
            cc.stop()
        except Exception as exp:
            logger.error("Failed to stop compliance controller. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

#         try:
#             self._unregister_agent_controller_handlers()
#         except Exception as exp:
#             logger.error("Failed to unregister compliance controller handlers. Error:{}".format(exp))
#             logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
    def _register_configuration_controller_handlers(self):
        """
        Method responsible for registering configuration controller handlers
        """
        self.event_handler.register_handler("SystemDidRegister",
                                                self.post_registration_handler)
    
    def _register_agent_controller_handlers(self):
        """
        Method responsible for registering all agent controller handlers.
        You should not call this directly, it is invoked by
        :py:func:`start_agent_controller`
        """

        logger = logging.getLogger(self.logger_name)

        ac = self.agent_controller

        self.event_handler.register_handler("UserSessionUnlocked",ac.session_unlock_handler)
        self.event_handler.register_handler("UserSessionLocked",ac.session_lock_handler)
        self.event_handler.register_handler("UserDidLogin",ac.user_login_handler)
        self.event_handler.register_handler("UserWillLogout",ac.user_logout_handler)
        self.event_handler.register_handler("SystemResumed",ac.system_resume_handler)
        self.event_handler.register_handler("SystemWillSuspend",ac.system_suspend_handler)
        self.event_handler.register_handler("SystemDidStartup",ac.startup_handler)
        self.event_handler.register_handler("ProcessDidStart",ac.process_start_handler)
        self.event_handler.register_handler("SystemWillShutdown",ac.shutdown_handler)
        self.event_handler.register_handler("NetworkDidChange",ac.network_change_handler)
        self.event_handler.register_handler("DidConnectToInternet",ac.internet_connect_handler)
        self.event_handler.register_handler("DidLeaveInternet",ac.internet_disconnect_handler)
        self.event_handler.register_handler("DidConnectToIntranet",ac.intranet_connect_handler)
        self.event_handler.register_handler("DidLeaveIntranet",ac.intranet_disconnect_handler)

    def _unregister_agent_controller_handlers(self):
        """
        Method responsible for registering all agent controller handlers.
        You should not call this directly, it is invoked by
        :py:func:`stop_agent_controller`
        """

        logger = logging.getLogger(self.logger_name)

        ac = self.agent_controller

        self.event_handler.unregister_handler("UserSessionUnlocked",ac.session_unlock_handler)
        self.event_handler.unregister_handler("UserSessionLocked",ac.session_lock_handler)
        self.event_handler.unregister_handler("UserDidLogin",ac.user_login_handler)
        self.event_handler.unregister_handler("UserWillLogout",ac.user_logout_handler)
        self.event_handler.unregister_handler("SystemResumed",ac.system_resume_handler)
        self.event_handler.unregister_handler("SystemWillSuspend",ac.system_suspend_handler)
        self.event_handler.unregister_handler("SystemDidStartup",ac.startup_handler)
        self.event_handler.unregister_handler("ProcessDidStart",ac.process_start_handler)
        self.event_handler.unregister_handler("SystemWillShutdown",ac.shutdown_handler)
        self.event_handler.unregister_handler("NetworkDidChange",ac.network_change_handler)
        self.event_handler.unregister_handler("DidConnectToInternet",ac.internet_connect_handler)
        self.event_handler.unregister_handler("DidLeaveInternet",ac.internet_disconnect_handler)
        self.event_handler.unregister_handler("DidConnectToIntranet",ac.intranet_connect_handler)
        self.event_handler.unregister_handler("DidLeaveIntranet",ac.intranet_disconnect_handler)

    def start_karl_engine(self):
        """
        Method to configure and start our event engine.
        """

        logger = logging.getLogger(self.logger_name)

        if self.karl_event_engine:
            ee = self.karl_event_engine
            ee.start()
            self.event_handler.register_handler("NetworkDidChange",
                                                            ee.network_changed)
        else:
            logger.error("Cannot start KARL engine: no engine configured (run load_karl())")

    def register_proxy_forwarders(self):
        """
        Method which registers event handlers
        """

        fproxy = self.forwarding_proxy

        logger = logging.getLogger(self.logger_name)
        try:
            if fproxy:
                fproxy.register_proxy_forwarders(event_handler=self.event_handler)
            else:
                logger.log(5,"Will not register proxy forwarders: receiving_proxy not set!")
        except Exception as exp:
            logger.error("Failed to register proxy forwarders. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)


    def stop_karl_engine(self):
        """
        Method to stop our event engine.
        """

        ee = self.karl_event_engine

        if ee:
            ee.stop()
            try:
                self.event_handler.unregister_handler("NetworkDidChange",
                                                            ee.network_changed)
            except:
                pass

    #MARK: Interogation methods
    def activity_status(self):
        """
        Method to return our current status data.
        """
        status = STATUS_IDLE
        compliance_status = STATUS_UNKNOWN
        
        ## Check Compliance status
        if self.compliance_controller:
            cc = self.compliance_controller
            compliance_status = cc.get_device_status()
            status |= cc.status()
       ## Check Agent Controller Status
        if self.agents_enabled and self.agent_controller:
            agent_status = self.agent_controller.status()
            if agent_status & (acme.agent.AGENT_STATUS_EXECUTING
                                            |acme.agent.AGENT_STATUS_QUEUED):
                status |= STATUS_AGENT_EXECUTING
        
        data = {"acme_status": status,
                "compliance_status": compliance_status
                }
        
        return data
    
    def module_with_name(self,name):
        """
        Returns the module with the provided name.
        """

        my_module = None

        for module_name in self.modules.keys():
            if name.lower() == module_name.lower():
                try:
                    my_module = self.modules[module_name]
                except KeyError:
                    pass
        if not my_module:
            raise compliance.ModuleNotFoundError("Module:{} is not registered!".format(name))

        return my_module

    #MARK: Processing methods
    def run_routine_checks(self):
        """
        Maintenance method called on a recurring basis.
        """
        
        now = datetime.datetime.utcnow()
        logger = logging.getLogger(self.logger_name)
        
        logger.log(2,"Running routine checks...")
        if self.shutdown:
            logger.log(2, 'Software is shutting down...')
            return
       
        if (self.last_group_cache_attempt and (self.last_group_cache_attempt
                    + self.group_cache_max_frequency) > now):
        
            ## Here if we've hit our max frequency, throttle calls
            pass
        elif (not self.last_group_cache or (self.last_group_cache
                                    + self.group_cache_frequency) <= now):
            self.last_group_cache_attempt = now
            try:
                if self.try_cache_groups():
                    self.last_group_cache = now
            except Exception as exp:
                logger.warning("Failed to cache groups for owner... {}".format(exp))
            
    
    def try_cache_groups(self):
        """
        Method to cache groups for our owner. This method is only effective
        if we have an established owner, and if we are on the corporate
        network.

        :returns: (bool) - True if groups were cached.

        """

        logger = logging.getLogger(self.logger_name)

        sp = systemprofile.profiler
        owner = sp.owner()

        if not owner:
            return False

        network_mask = network.state.state

        if not network_mask or not network_mask & network.NETWORK_STATE_ONDOMAIN:
            return False

        logger.debug("Caching groups for user: {}".format(owner))
        sp.directoryservice.cache_groups_for_user(username=owner)

        return True

    def handle_signal(self,signum,frame):
        """
        Method to handle POSIX Signals sent to notify us of
        termination. (i.e. when calling kill `pid` from shell)
        """

        logger = logging.getLogger(self.logger_name)
        if signum == signal.SIGTERM:
            logger.log(25,"Recieved SIGTERM, shutting down...")
            self.stop()
        elif signum == signal.SIGHUP:
            logger.log(25,"Recieved SIGHUP, reloading...")
            ## Attempt to re-establish default log level.
            try:
                logger.info("Setting default logging verbosity level...")
                reconfigure_loggers(verbosity=cli.verbosity)
            except Exception:
                pass
            self.reload()
        elif signum == signal.SIGINT:
            logger.log(25,"Recieved SIGINT, shutting down...")
            self.stop()
        elif signum == signal.SIGCONT:
            logger.log(25,"Recieved SIGCONT, shutting down...")
            self.stop()
        elif signum == signal.SIGUSR1:
            logger.info("Recieved SIGUSR1 setting verbose logging (v:4)")
            try:
                reconfigure_loggers(verbosity=4)
            except Exception:
                pass
        elif signum == signal.SIGUSR2:
            logger.info("Recieved SIGUSR2 setting verbose logging (v:6)")
            try:
                reconfigure_loggers(verbosity=6)
            except Exception:
                pass
        else:
            logger.warning("Recieved unknown signal:{}, ignoring...".format(signum))

    def process_request(self,request):
        """
        Method which will process an Request object from a client. This
        executes syncronously, and will typically be called off the main thread
        or primary server_thread to prevent blocking.

        :param request: The request to process
        :type request: ipc.Request

        :returns: ipc.Response
        """

        logger = logging.getLogger(__name__)

        response = ipc.Response(request=request)
        if request.action.lower() == "Shutdown".lower():
            logger.log(25,"Recieved shutdown command, shutting down...")
            timer = threading.Timer(.5,self.stop)
            timer.start()
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "Reload".lower():
            logger.log(25,"Recieved reload command, reloading configuration files...")
            try:
                self.reload()
                response.status_code = ipc.StatusCode.SUCCESS
            except Exception as exp:
                msg = "Failed to reload configurations: {}".format(exp)
                response.status_code = ipc.StatusCode.ERROR
                response.status = msg
                logger.error(msg)
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        elif request.action.lower() == "ComplianceEvaluate".lower():
            logger.log(25,"Recieved compliance evaluate command, running compliance evaluation...")
            self.evaluation_thread = None

            try:
                identifier = request.options.get("identifier")

                logger.info("Starting the compliance evaluation thread")
                self.evaluation_thread = threading.Thread(target = self.compliance_evaluate, kwargs={'identifier':identifier})
                self.evaluation_thread.daemon = True
                self.evaluation_thread.start()
                response.status = "Success"
                response.status_code = ipc.StatusCode.STATUS_PROCESS_RUNNING
                

            except Exception as exp:
                msg = "Failed to evaluate compliance: {}".format(exp)
                response.status_code = ipc.StatusCode.ERROR
                response.status = msg
                logger.error(msg)
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)


        elif request.action.lower() == "getComplianceEvaluationStatus".lower():
            if self.evaluation_thread:
                if self.evaluation_thread.is_alive():
                    response.status = "Running"
                    response.status_code = ipc.StatusCode.STATUS_PROCESS_RUNNING
                else:
                    response.status = "Compliance evaluation completed."
                    response.status_code = ipc.StatusCode.SUCCESS
            
            else:
                response.status = "Failed to start the thread"
                response.status_code = ipc.StatusCode.ERROR
            
        elif request.action.lower() == "ComplianceRemediate".lower():
            logger.log(25,"Recieved compliance remediate command, running compliance remediation...")
            self.remediation_thread = None

            try:
                identifier = request.options.get("identifier")

                logger.info("Starting the compliance remediation thread")
                self.remediation_thread = threading.Thread(target = self.compliance_remediate, kwargs={'identifier':identifier})
                self.remediation_thread.daemon = True
                self.remediation_thread.start()
                response.status = "Success"
                response.status_code = ipc.StatusCode.STATUS_PROCESS_RUNNING
                

            except Exception as exp:
                msg = "Failed to remediate compliance: {}".format(exp)
                response.status_code = ipc.StatusCode.ERROR
                response.status = msg
                logger.error(msg)
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)


        elif request.action.lower() == "getComplianceRemediationStatus".lower():
            if self.remediation_thread:
                if self.remediation_thread.is_alive():
                    response.status = "Running"
                    response.status_code = ipc.StatusCode.STATUS_PROCESS_RUNNING
                else:
                    response.status = "Compliance remediation completed."
                    response.status_code = ipc.StatusCode.SUCCESS
            
            else:
                response.status = "Failed to start the thread"
                response.status_code = ipc.StatusCode.ERROR
        elif request.action.lower() == "GetComplianceStatus".lower():
            logger.log(25,"Received ComplianceStatus command, listing compliance module data...")
            try:
                no_history = request.options.get("no-history")
                if no_history:
                    response.data = self.compliance_status(include_history=False)
                else:
                    response.data = self.compliance_status(include_history=True)
                response.status_code = ipc.StatusCode.SUCCESS
            except Exception as exp:
                msg = "Failed to get compliance module data: {}".format(exp)
                response.status_code = ipc.StatusCode.ERROR
                response.status = msg
                logger.error(msg)
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                
        elif request.action.lower() == "GetVersion".lower():
            response.data = {
                                "ACMELib": __version__,
                                "KARLLib": pykarl.core.__version__,
                                "ACMECoreLib": acme.core.__version__,
                                }
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "GetStatus".lower():
            response.data = self.activity_status()

            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "GetACMEReachable".lower():
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "GetIsRegistered".lower():
            try:
                registration_data_file = os.path.join(self.manifest_dir,
                                                                 "registration_data.json")
                response.data = self.registrant.is_registered(registration_data_file)
                response.status = "Success"
                response.status_code = ipc.StatusCode.SUCCESS
            except Exception as exp:
                response.status_code = ipc.StatusCode.ERROR
                response.status = "Failed to check if the device is registered: {}".format(exp)
                logger.error(response.status)
                logger.log(5,"Failure stack trace (handled cleanly):", exc_info=1)

        elif request.action.lower() == "GetJWT".lower():
            try:
                
                if not self.identity:
                    raise Exception("No identity is currently established!")
                if 'duration' in request.options:
                    duration = request.options["duration"]
                    posture_token = self.claims.create_posture_token(duration)
                else:
                    posture_token = self.claims.create_posture_token()
                jwt = self.identity.get_jwt(data=posture_token, b64encode = False)
                response.data = {
                                    "jwt" : jwt,
                                }
                response.status = "Success"
                response.status_code = ipc.StatusCode.SUCCESS
                if self.jwt_generated == False or self.jwt_generated is None:
                    self.jwt_generated = True
                    self.update_claims_status(self.claims.TOKEN_STATE_AVAILABLE)
            except Exception as exp:
                response.status_code = ipc.StatusCode.ERROR
                if self.jwt_generated == True or self.jwt_generated is None:
                    self.jwt_generated = False
                    self.update_claims_status(self.claims.TOKEN_STATE_UNAVAILABLE)
                response.status = "Failed to generate JWT: {}".format(exp)
                logger.error(response.status)
                logger.log(5,"Failure stack trace (handled cleanly):", exc_info=1)
        elif request.action.lower() == "GetAEAConfig".lower():
            try:
                logger.debug("Fetching AEA configuration (cli request)")
                config_data = None
                try:
                    config_module_name = "FileConfigModule"
                    config_file_name = "config"
                    config_module = self.configuration_controller.modules[
                                                            config_module_name]
                    config_file = config_module.files[config_file_name]
                    config_data = config_file.content()
                except Exception as exp:    
                    logger.error("Failed to fetch AEA configuration; {}".format(
                                                            exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly):", 
                                                            exc_info=1)
                    
                response.data = {
                                    "aea_config" : config_data
                                }
                response.status = "Success"
                response.status_code = ipc.StatusCode.SUCCESS
            except Exception as exp:
                response.status_code = ipc.StatusCode.ERROR
                response.status = "Failed to get AEA configuration (cli request): {}".format(exp)
                logger.error(response.status)
                logger.log(5,"Failure stack trace (handled cleanly):", exc_info=1)
        elif request.action.lower() == "GetSystemID".lower():
            try:
                response.data = self.get_system_identifier()
                response.status = "Success"
                response.status_code = ipc.StatusCode.SUCCESS
            except Exception as exp:
                response.data = ""
                response.status = "Failed to get sys id: {}".format(exp)
                response.status_code = ipc.StatusCode.ERROR

        elif request.action.lower() == "GetCurrentUser".lower():
            try:
                response.data = self.get_current_user()
                response.status = "Success"
                response.status_code = ipc.StatusCode.SUCCESS
            except Exception as exp:
                response.data = ""
                response.status = "Failed to get current user id: {}".format(exp)
                response.status_code = ipc.StatusCode.ERROR

        elif request.action.lower() == "ProxyEvent".lower():
            try:
                event = systemevents.proxy.ProxiedEvent(
                                    dict_data=request.options["event_data"])
                self.receiving_proxy.handle_proxied_event(event)
            except Exception as exp:
                logger.error("Failed to process proxied event:'{}'".format(event.key))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

            response.data = self.activity_status()

            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "GetNetworkStatus".lower():
            try:
                response.data = network.state.to_dict()
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
                return response
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "GetGroupCache".lower():
            try:
                response.data = systemprofile.directoryservice.profiler.group_cache.to_dict()
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
                return response
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "ResetFirefoxExtensionsScope".lower():
            try:
                self.aea.reset_firefox_extensions_scope()
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
                return response
            response.data = ""
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "EnableFirefoxExtensionsScope".lower():
            try:
                self.aea.enable_firefox_extensions_scope()
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
                return response
            response.data = ""
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "DisableChromeAutoInstall".lower():
            try:
                self.aea.disable_chrome_autoinstall()
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
                return response
            response.data = ""
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "EnableChromeAutoInstall".lower():
            try:
                self.aea.enable_chrome_autoinstall()
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
                return response
            response.data = ""
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS
        
	elif request.action.lower() == "DisableChromiumAutoInstall".lower():
            try:
                self.aea.disable_chromium_autoinstall()
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
                return response
            response.data = ""
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "EnableChromiumAutoInstall".lower():
            try:
                self.aea.enable_chromium_autoinstall()
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
                return response
            response.data = ""
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "GetQuarantineResponse".lower():
            try:
                logger.info("Connecting to quarantine server: {}.".format(request.options["server_uri"]))
                quarantine_controller_response = self.quarantine_handler.fetch_qc_data(options=request.options, identity=self.identity)
                response.data = quarantine_controller_response
            except Exception as exp:
                logger.error("Failed to connect to quarantine server: {}. Error is {}.".format(request.options["server_uri"],exp))
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
                return response
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS
            
        elif request.action.lower() == "GetKARLStatus".lower():
            try:
                data = {}
                if self.karl_event_engine:
                    key_map = {"access_key_id" : None,
                            "state" : None,
                            "source_id" : "default_source",
                            "online" : "is_online",
                            "has_network_access" : None,
                            "has_credentials" : None,
                            "num_failed_commits" : None,
                            "last_failed_commit" : "last_failed_commit_datestamp",
                            "queue_length" : None,
                            }
                    data = self.karl_event_engine.settings_to_dict(key_map=key_map)
                elif pykarl.event.dispatcher.is_configured():
                    data["state"] = pykarl.event.ENGINE_STATE_DISPATCH
                else:
                    data["state"] = pykarl.event.ENGINE_STATE_UNCONFIGURED

                data["enabled"] = self.karl_enabled

                response.data = data
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR

                logger.error("Failed to process action:GetKARLStatus! Error:{}".format(exp))


                return response
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS
            
        elif request.action.lower() == "GetACMEHealthInfo".lower():
            try:
                result_data = {}
                data = {}
                ee_healthy = False
                if self.karl_event_engine:
                    key_map = {"access_key_id" : None,
                            "state" : None,
                            "source_id" : "default_source",
                            "online" : "is_online",
                            "has_network_access" : None,
                            "has_credentials" : None,
                            "num_failed_commits" : None,
                            "last_failed_commit" : "last_failed_commit_datestamp",
                            "queue_length" : None,
                            }
                    data = self.karl_event_engine.settings_to_dict(key_map=key_map)
                    ee_healthy = data["num_failed_commits"] == 0
                    
                result_data["ee_healthy"] = ee_healthy
                registration_data_file = os.path.join(self.manifest_dir, "registration_data.json")
                result_data["is_registered"] = self.registrant.is_registered(registration_data_file)
                result_data["acme_version"] = __version__
                response.data = result_data
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
                logger.error("Failed to process action:GetACMEHealthInfo! Error:{}".format(exp))
                return response
            
            response.status = "Success"
            response.status_code = ipc.StatusCode.SUCCESS

        elif request.action.lower() == "GetAgentStatus".lower():
            try:
                data = {}
                if self.agent_controller:
                    ac = self.agent_controller
                    data = {"running" : ac.should_run,
                                "execution_thread_count" : ac.agent_executor_count(),
                                "max_execution_thread_count": ac.maxnum_agent_executors,
                                "plugin_path" : ac.plugin_path,
                                "queue_length" : None,
                                "state_dirpath" : ac.state_dirpath,
                                "manifest_dirpath" : ac.manifest_dirpath,
                                "agents" : [],
                            }

                    if ac.execution_queue:
                        data["queue_length"] = len(ac.agent_queue_data)

                    for agent in ac.agents.values():
                        key_map = agent.key_map_for_keys(["name",
                                                    "last_execution",
                                                    "last_execution_status"])
                        data["agents"].append(agent.to_dict())

                response.data = data
                response.status = "Success"
                response.status_code = ipc.StatusCode.SUCCESS
            except Exception as exp:
                logger.error("An error occurred retrieving AgentStatus: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR

        elif request.action.lower() == "CommitKARLEvent".lower():

            event = None
            try:
                event = Event(json_data=request.options["event_data"])
                self.karl_event_engine.commit_event(event)
                response.status = "Event Committed"
                response.status_code = ipc.StatusCode.SUCCESS

            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR

        elif constant.ACTION_MODULE_STATUS.lower() == request.action.lower():
            try:
                with self.compliance_controller.load_lock:
                    cmodule = self.compliance_controller.get_cmodule(request.options["identifier"])
                    if cmodule:
                        response.data = cmodule.to_json()
                    response.status = "Success"
                    response.status_code = ipc.StatusCode.SUCCESS                 
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
           
        elif request.action.lower() == constant.ACTION_RELOAD_MODULES.lower():
            identifier = None
            try:
                logger.info("Reloading compliance modules. (cli request)")
                cm_name = "ComplianceConfigModule"
                cm = self.configuration_controller.modules[cm_name]
                
                cm.timer.reset(.1)
                
                response.status = "Success"
                response.status_code = ipc.StatusCode.SUCCESS
            except KeyError as exp:
                if exp[0] == "ComplianceConfigModule":
                    response.status = "{} is not loaded!".format(cm_name)
                    response.status_code = ipc.StatusCode.SUBSYSTEM_UNSET
                logger.error("Failed to load compliance module (cli request). Error:{}".format(
                                                    response.status))
                logger.log(5,"Failure stack trace (handled cleanly)", 
                                                                exc_info=1)
                
            except Exception as exp:
                response.status = exp.message
                response.status_code = ipc.StatusCode.ERROR
                logger.error("Failed to load compliance module (cli request). Error:{}".format(
                                                    response.status))
                logger.log(5,"Failure stack trace (handled cleanly)", 
                                                                exc_info=1)
        elif request.action.lower() == "getRegistrationStatus".lower():
            if self.registration_thread:
                if self.registration_thread.is_alive():
                    response.status = "Running"
                    response.status_code = ipc.StatusCode.STATUS_PROCESS_RUNNING
                else:
                    if self.cli_register_status is True:
                        response.status = "Successfully registered with UUID:{}".format(self.get_system_identifier())
                        response.status_code = ipc.StatusCode.SUCCESS
                    elif self.cli_register_status is False:
                        response.status = self.cli_register_status_msg
                        response.status_code = ipc.StatusCode.ERROR
                    
            else:
                response.status = "Failed to start the thread"
                response.status_code = ipc.StatusCode.ERROR
                  
        elif request.action.lower() == "RegisterWithToken".lower():
            logger.info("Registration started with token:{}".format(request.options.get("token")))
            try:
                token = request.options.get("token")
                force = request.options.get("force")
                registration_data_file = os.path.join(self.manifest_dir,
                                                                 "registration_data.json")
                #Need this, in case the user deletes the certificate manually and it was registered through CLI previously. More reliable than cli_registration_status.
                registration_status = self.registrant.is_registered(registration_data_file)
                if force:
                    # Resetting the values, so the future calls would try to register fresh.
                    self.cli_register_status = False
                    self.registration_thread = None 
                    registration_status = False
                    
                if token:
                    if self.registration_thread is None:
                        if registration_status:
                            response.status = "System already registered with UUID:{}".format(self.get_system_identifier())
                            response.status_code = ipc.STATUS_REGISTERED_ALREADY
                        else:
                            logger.info("Starting the registration thread to register to KARL with token: {}".format(token))
                            self.registration_thread = threading.Thread(target = self.register_system_using_cli, kwargs={'token':token})
                            self.registration_thread.daemon = True
                            self.registration_thread.start()
                            response.status = "Success"
                            response.status_code = ipc.StatusCode.SUCCESS
                    else:
                        if self.registration_thread.is_alive():
                            response.status = "Running"
                            response.status_code = ipc.StatusCode.STATUS_PROCESS_RUNNING
                        else:
                            # If thread has finished executing then fetch the registration results.
                            if self.cli_register_status:
                                response.status = "System already registered with UUID:{}".format(self.get_system_identifier())
                                response.status_code = ipc.StatusCode.STATUS_REGISTERED_ALREADY
                            else:
                                # Registration has failed in the past, will start new thread to register
                                logger.info("Starting the registration thread to register to KARL with token: {}".format(token))
                                self.registration_thread = threading.Thread(target = self.register_system_using_cli, kwargs={'token':token})
                                self.registration_thread.daemon = True
                                self.registration_thread.start()
                                
                                response.status = "Running"    
                                response.status_code = ipc.StatusCode.STATUS_PROCESS_RUNNING              
                else:
                    response.status = "Please provide the token as --register --token '<token>'"
                    response.status_code = ipc.StatusCode.ERROR
            except Exception as exp:
                response.status = exp
                response.status_code = ipc.StatusCode.ERROR
        return response
    
    

#MARK: -
class ACMEAgent(ACMEd):
    """
    Our primary controller class for our ACME agent which runs in the
    user context.
    """

    system_base_dir = None      #: Base directory of our ACME daemon
    dynamic_acme_controls = False   #: Value that controls if ACME controls should be loaded dynamically.
    
    @property
    def run_directory(self):
        """
        Returns our run file directory
        """
        if self._run_directory:
            return self._run_directory
        elif self.base_dir:
            return os.path.join(self.system_base_dir,"run")

    #MARK: Constructors & Loading Methods
    def __init__(self,*args,**kwargs):

        self.logger_name = "ACMEAgent"

        self.system_base_dir = "/usr/local/amazon/var/acme"
        
        super(ACMEAgent,self).__init__(*args,**kwargs)
        if self.acme_controls:
            #Disable system level features/functions.
            self.acme_controls.karl_registrar_enabled = False
            self.acme_controls.compliance_enabled = False
            self.acme_controls.usher_enabled = False
            self.acme_controls.usher_watcher_enabled = False
        
        self.karl_event_engine = None

        self.event_handler = systemevents.session_handler

        self.receiving_proxy = acme.systemevents.proxy.ClientEventProxy()
        self.forwarding_proxy = acme.systemevents.proxy.DaemonEventProxy()

        self.group_cache_frequency = datetime.timedelta(minutes=5)

    def configure_karl_dispatcher(self):
        """
        Method which configures our agent for use.
        """

        ## Configure our KARL dispatcher.
        pykarl.event.dispatcher.delegates.append(self.dispatch_event)

    def dispatch_event(self,event):
        """
        Method to dispatch to our ACME server over IPC.
        """
        with ipc.Client(run_directory=self.run_directory) as c:
            r = ipc.Request(action="CommitKARLEvent")
            r.options["event_data"] = event.to_json(base64encode=True)
            re = c.submit_request(r)

    def startup_finished(self):
        """
        Method called when startup finishes.
        """

        logger = logging.getLogger(self.logger_name)

        lname = os.environ["USER"]

        logger.log(25, "Session daemon finished startup for user: '{}'".format(
                                                                    lname))
        now = datetime.datetime.utcnow()

        ## Determine if we just logged in
        login_date = systemprofile.profiler.last_login_for_user(lname)
        if login_date and login_date < now and login_date + LOGIN_THRESHOLD >= now:
            logger.log(5,"User: {} has recently logged in (last:{} UTC)".format(
                                                            lname,
                                                            login_date))
            self.event_handler.user_did_login(username=lname)
        else:
            logger.log(5,"User: {} has not recently logged in (last:'{}' now:'{}')".format(
                                                            lname,
                                                            login_date,
                                                            now))

    def load_systemprofiler(self, reload=None):
        """
        Method which bootstraps our systemprofiler caches.
        """

        logger = logging.getLogger(self.logger_name)

        systemprofile.profiler.base_dir = self.base_dir

        dsp = systemprofile.directoryservice.profiler
        dsp.enable_group_lookup = False

        try:
            if self.try_cache_groups():
                now = datetime.datetime.utcnow()
                self.last_group_cache = now
                self.last_group_cache_attempt = now
        except Exception as exp:
            logger.error("Failed to cache groups from daemon... {}".format(exp))

    def try_cache_groups(self):
        """
        Method to cache groups for our owner. This method is only effective
        if we have an established owner, and if we are on the corporate
        network.

        :returns: (bool) - True if groups were cached.

        """

        logger = logging.getLogger(self.logger_name)

        dsp = systemprofile.directoryservice.profiler

        logger.info("Caching User/Group data...")
        with ipc.Client(run_directory=self.run_directory) as c:
            request = ipc.Request(action="GetGroupCache")
            response = c.submit_request(request)
            dsp.group_cache.load_dict(response.data)

        return True

    def load_network(self):
        """
        Method which sets up initial network state and configures
        network change listeners.
        """
        logger = logging.getLogger(self.logger_name)

        if not network.state.site_info:
            site = network.NetworkSiteInfo()
        else:
            site = network.state.site_info

        ## Todo: move this to a config file
        site.temporary_site_filter = ".*Datacenter.*"
        network.site_info = site

        systemevents.DELAYED_NETWORK_TIMER_INTERVAL = datetime.timedelta(seconds=5)

        network_state_file = os.path.join(self.system_base_dir,"state",
                                                                "network.data")
        if os.path.exists(network_state_file):
            try:
                network.state.load_from_file(network_state_file)
            except Exception as exp:
                logger.warning("Failed to load network state from file:'{}'. Error:{}".format(
                                                    network_state_file,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

    def load_compliance_controller(self):
        """
        Method to load our ComplianceController.
        
        .. note: 
            Our user agent does not currently need to concern itself with 
            compliance, so this is a no-op at this level (this may someday
            change).
        """
        
        pass
    
    def load_registration_data(self):
        """
        Method to bootstrap registration. 
        
        .. note: 
            Our user agent does not need to concern itself with registration,
            so this is a no-op at this level. 
        """
        
        pass
    
    def load_agent_controller(self):
        """
        Method to load our AgentController.
        """

        logger = logging.getLogger(self.logger_name)
        logger.info("Loading AgentController...")

        ac = self.agent_controller

        if self.manifest_dir:
            ac.manifest_dirpath = os.path.join(self.manifest_dir,"session_agents")

        if self.state_dir:
            ac.state_dirpath = os.path.join(self.state_dir,"session_agents")

        if self.base_dir:
            ac.plugin_path = os.path.join(self.system_base_dir,"session_agents")

        try:
            ac.load()
        except Exception as exp:
            logger.error("Failed to load AgentController. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

    def load_karl(self):
        """
        Method to load KARL.
        """

        logger = logging.getLogger(self.logger_name)

        logger.info("Loading KARL...")

        self.configure_karl_dispatcher()

    def load_state(self):
        """
        Method to load our state
        """

        logger = logging.getLogger(self.logger_name)
        logger.debug("Loading State...")

        if not self.state_dir:
            return

    def update_network(self):
        """
        Method which updates our network state from file.
        """
        logger = logging.getLogger(self.logger_name)

        network_state_file = os.path.join(self.system_base_dir,"state","network.data")
        if os.path.exists(network_state_file):
            try:
                network.state.update_from_file(filepath=network_state_file)
            except Exception as exp:
                logger.warning("Failed to load state from file:{}. Error:{}".format(network_state_file,exp),exc_info=1)

    def save_state(self):
        """
        Method to save our state
        """

        logger = logging.getLogger(self.logger_name)
        logger.debug("Saving State...")

        if self.state_dir and not os.path.exists(self.state_dir):
            os.mkdir(self.state_dir)


        ## Todo: save state for our compliance modules

        ## Todo: save state for our agents

        pass

    def start_karl_engine(self):
        """
        Method to override standard KARL event engine setup (ACMEAgent proxies
        all KARL events over IPC to ACMEd).
        """
        pass

#MARK: -
class ACMEdCLI(object):
    """
    Our primary CLI controller class. Responsible for parsing arguments and
    executing accordingly.
    """

    version = ".0b"
    acme_server = None      #: Our :py:class:`ACMEd` instance
    verbosity = 0           #: Our configured verbosity level
    arguments = None        #: list(str) of arguments passed via CLI
    logger_name = "ACMEdCLI"

    run_as_agent = False    #: Flag which determines whether we run as agent

    def __init__(self, arguments=None):

        if arguments is not None:
            self.arguments = arguments

    def setup_parser(self,parser=None):
        """
        Method which is used to setup our cli argument parser and
        define our parser arguments.

        :param parser: Our parser object to load, if none is specified we will
                        use self.parser
        :type parser: :py:class:`argparse.parser` object.

        :returns: :py:class:`argparse.parser` object.
        """

        if parser is None:
            parser = argparse.ArgumentParser(prog="acmed")

        cmd_group = parser.add_argument_group("ACME Daemon Options")
        cmd_group.add_argument("--base-dir",
                        help=("Specify our base directory. "))
        cmd_group.add_argument("--manifest-dir",
                        help=("Specify our manifest directory used for settings."))
        cmd_group.add_argument("--state-dir",
                        help=("Specify our manifest directory used for state persistence."))
        cmd_group.add_argument("--port",
                        type=int,
                        help=("The port to listen on. (default 9216)"))
        cmd_group.add_argument("--session",action="store_true",
                        help=("Execute in user session mode."))
        cmd_group.add_argument("-v","--verbose",action="count",
                        help=("Increase our level of output detail."))
        cmd_group.add_argument("-f","--log-file",
                        help=("File used for logging."))

        parser.add_argument_group(cmd_group)

        return parser

    def configure_from_args(self,args):
        """
        Method to configure settings based on the provided args.

        :param args: ArgumentParser object containing parsed data.
        :type args:

        """

        try:
            if args.session:
                self.acme_server = ACMEAgent()
            else:
                self.acme_server = ACMEd()
        except:
            self.acme_server = ACMEd()

        try:
            if args.base_dir:
                self.acme_server.base_dir = args.basedir
            else:
                self.acme_server.base_dir = DEFAULT_SYSTEM_DIR
        except:
            pass

        try:
            if args.port:
                self.acme_server.port = args.port
            else:
                start_port = DEFAULT_DAEMON_PORT
                if args.session:
                    start_port = DEFAULT_AGENT_PORT

                new_port = self.get_new_port(start_port=start_port)
                if new_port:
                    self.acme_server.port = new_port
        except Exception as exp:
            raise Exception("Configure From Args Failed, {}".format(exp))


        try:
            if args.manifest_dir:
                self.acme_server.manifest_dir = args.manifest_dir
        except:
            pass

        try:
            if args.state_dir:
                self.acme_server.state_dir = args.state_dir
        except:
            pass
        
        try:
            if args.routes_dir:
                self.acme_server.routes_dir = args.routes_dir
        except:
            pass


        try:
            if args.session:

                self.run_as_agent = True

                if not args.base_dir:
                    self.acme_server.base_dir = DEFAULT_USER_DIR

                if not args.manifest_dir:
                    self.acme_server.manifest_dir = os.path.join(DEFAULT_SYSTEM_DIR,"manifests")
        except:
            pass


        try:
            if args.verbosity:
                self.verbosity = args.verbosity
        except:
            pass

    def get_new_port(self,start_port=None,max_num_ports=None):
        """
        Method which returns our port to use.
        """

        port = None

        if not start_port:
            start_port = 9216

        if not max_num_ports:
            max_num_ports = 20

        run_path = os.path.join(self.acme_server.base_dir,"run")

        allowed_ports = range(start_port,start_port+max_num_ports)
        used_ports = []

        try:
            client = ipc.Client()
            rundata = client.load_runfile_details(run_path)
            for client_type, type_entries in rundata.iteritems():
                try:
                    for entry in type_entries:
                        if validate_runfile(entry):
                            used_ports.append(entry["port"])
                except:
                    pass
        except:
            pass

        available_ports = filter(lambda x: x not in used_ports, allowed_ports)

        if available_ports:
            port = available_ports[0]
        else:
            raise Exception("No Available Ports Left.")

        return port

    def parse_args(self,arguments=None,args=None):
        """
        Method to parse provided arguments and configure as appropriately.
        This method calls argparse.parse_args() and as such will exit
        the program and display help output if invalid arguments are passed.

        :param list arguments: List(str) of arguments passed.
        :param args:

        :returns: :py:class:`argparse.Namespace` object
        """

        if arguments is None:
            arguments = self.arguments

        parser = self.setup_parser()

        args = parser.parse_args(arguments)

        return args

    def run(self, arguments=None):
        """
        Our primary execution point.
        """
        
        if arguments is None:
            arguments = self.arguments
        
        args = self.parse_args(arguments=arguments)
        sp = systemprofile.profiler
        
        ## Configure logging
        try:
            verbosity = args.verbose
        except:
            verbosity = None
        try:
            log_file = args.log_file
        except:
            log_file = None
        try:
            configure_logging(verbosity=verbosity,
                                log_file=log_file)
        except Exception as exp:
            sys.stderr.write("Error! Could not setup file logging: {}".format(exp))
            return 10
        
        logger = logging.getLogger(self.logger_name)
        try:
            self.configure_from_args(args=args)
        except Exception as exp:
            logger.critical("A fatal error occurred during startup: {}".format(exp))
            logger.debug("Failure stack trace (handled cleanly):",exc_info=1)
            
            return 1
        
        logger.log(25, "Starting up...")
        
        acme_version = __version__
        acme_core_version = acme.core.__version__
        acme_systemprofile_version = systemprofile.__version__
        platform = "Unknown"
        sys_version = "Unknown"
        
        hostname = "Unknown"
        try:
            platform = sp.platform()
        except Exception as exp:
            logger.warning("Failed to determine system platform:  {}".format(
                                                    exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        try:
            sys_version = sp.system_version()
        except Exception as exp:
            logger.warning("Failed to determine system version:  {}".format(
                                                    exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        try:
            hostname = sp.hostname()
        except Exception as exp:
            logger.warning("Failed to determine system hostname:  {}".format(
                                                    exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        
        logger.info("Runtime Info:\n\tHostname: {}\n\tACME Version: {}\n\tACME Core Version: {}\n\tACME SystemProfile Version: {}\n\tPlatform: {}\n\tOS Version: {}".format(
                                            hostname,
                                            acme_version,
                                            acme_core_version,
                                            acme_systemprofile_version,
                                            platform,
                                            sys_version
                                            ))
        
        self.acme_server.setup()
        #self.acme_server.load_modules()
        self.acme_server.start()
        
        while self.acme_server.server_thread.is_alive():
            try:
                if acme.platform == "OS X" or acme.platform == "macOS":
                    future = datetime.datetime.now() + datetime.timedelta(seconds=NSRUNLOOP_DURATION.total_seconds())
                    NSRunLoop.currentRunLoop().runUntilDate_(future)
                    time.sleep(.5)
                else:
                    time.sleep(1)
            
            except KeyboardInterrupt:
                ## Note: we should never get here as we're catching the
                ## signal via acmed.load_signal_handlers()
                logger.warning("Recieved Keyboard Interrupt, shutting down...")
                self.acme_server.stop()
                break
        
        self.cleanup()
    
    def cleanup(self):
        """
        Method used to save ourselves prior to termination.
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.info("Cleaning Up...")
        self.acme_server.save_state()
        
        logger.log(25,"Shutdown complete...")
        
        return 0

#MARK: Module functions
def get_logging_config(verbosity=None):
    """
    Method which returns a key=>value dictionary with the
    following:

    Key             Description               Example
    ==========      ========================= ===========================
    log_format      Format string for logging [%(levelname)s] %(message)s
    log_level       Log level to setup (int)  10  (logging.DEBUG)
    boto_log_level  Log level for boto        20  (logging.INFO)
    """

    ## Establish defaults
    log_level = DEFAULT_LOG_LEVEL
    boto_log_level = DEFAULT_BOTO_LOGLEVEL
    requests_log_level = DEFAULT_LOG_LEVEL
    log_format = LOGGING_FORMAT

    ## See if verbosity was specified
    if verbosity is not None:
        if verbosity > 6:
            log_format = LOGGING_FORMAT_DETAILED
            log_level = logging.NOTSET
            boto_log_level = logging.NOTSET
            requests_log_level = logging.NOTSET
        if verbosity >= 5:
            log_format = LOGGING_FORMAT_DETAILED
            if log_level > 2:
                log_level = 2
            if boto_log_level > logging.DEBUG:
                boto_log_level = logging.DEBUG
            if requests_log_level > 5:
                requests_log_level = 5
        if verbosity >= 4:
            log_format = LOGGING_FORMAT_DETAILED
            if log_level > 5:
                log_level = 5
            if boto_log_level > logging.INFO:
                boto_log_level = logging.INFO
            if requests_log_level > logging.DEBUG:
                requests_log_level = logging.DEBUG
        elif verbosity >= 3:
            if log_level > 9:
                log_level = 9
            if boto_log_level > logging.INFO:
                boto_log_level = logging.INFO
            if requests_log_level > logging.INFO:
                requests_log_level = logging.INFO
        elif verbosity >= 2:
            if log_level > logging.DEBUG:
                log_level = logging.DEBUG
        elif verbosity >= 1:
            if log_level > logging.INFO:
                log_level = logging.INFO

    results = { "log_format" : log_format,
                "log_level" : log_level,
                "boto_log_level" : boto_log_level,
                "requests_log_level" : requests_log_level
                }
    return results

def configure_logging(verbosity=None,log_file=None):
    """
    Method which will configure our logging behavior based on the
    passed arguments. If env is ommited we will consult os.environ.

    :param env: Dictionary to consult to determine our logging params. If
        not provided we will source from os.environ
    :type args: argparse.Namespace object
    """

    ## Read our config
    config = get_logging_config(verbosity=verbosity)

    log_level = config["log_level"]
    boto_log_level = config["boto_log_level"]
    requests_log_level = config["requests_log_level"]
    log_format = config["log_format"]

    ## Setup our boto logger
    boto_logger = logging.getLogger("boto")
    boto_logger.setLevel(boto_log_level)

    ## Setup our requests logger
    requests_logger = logging.getLogger("requests")
    requests_logger.setLevel(requests_log_level)

    requests_logger = logging.getLogger("requests_kerberos")
    requests_logger.setLevel(requests_log_level)

    ## Add our custom log levels
    logging.addLevelName(25,"IMPORTANT")
    logging.addLevelName(15,"DETAILED")
    logging.addLevelName(9,"DEBUG2")
    logging.addLevelName(5,"API")
    logging.addLevelName(2,"API2")
    logging.addLevelName(1,"API3")

    ## Setup our root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    if log_file:
        clean_log_file = os.path.expanduser(log_file)

        if not os.path.exists(os.path.dirname(clean_log_file)):
            os.makedirs(os.path.dirname(clean_log_file))

        fh = logging.handlers.RotatingFileHandler(clean_log_file,
                                maxBytes=LOG_FILE_ROTATION_SIZE,
                                backupCount=LOG_FILE_NUM_RETAIN)
        fh.setFormatter(logging.Formatter(log_format))
        logger.addHandler(fh)
    else:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter(log_format))
        logger.addHandler(sh)


def reconfigure_loggers(verbosity=None):
    """
    Method to reconfigure all registered loggers.
    """

    ## Read our config
    config = get_logging_config(verbosity=verbosity)

    log_level = config["log_level"]
    boto_log_level = config["boto_log_level"]
    log_format = config["log_format"]

    ## Setup our boto logger
    boto_logger = logging.getLogger("boto")
    boto_logger.setLevel(boto_log_level)
    for handler in boto_logger.handlers:
        handler.setFormatter(logging.Formatter(log_format))

    ## Setup our root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter(log_format))

    ## Setup all other
    for key,logger in logging.Logger.manager.loggerDict.iteritems():
        try:
            logger.setLevel(log_level)
            for handler in logger.handlers:
                handler.setFormatter(logging.Formatter(log_format))
        except Exception:
            pass

#MARK: Module vars
cli = ACMEdCLI()

