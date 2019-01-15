"""
.. module:: acme.agent
    :synopsis: Module containing classes used by the ACME Agent system
    :platform: RHEL, OSX, Ubuntu
    
.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>
    

>>> import random
>>> import mock
    
"""

#MARK: Imports
import copy
import datetime
import imp
import inspect
import math
import logging
import os
import random
import time
import threading
import Queue
import uuid

import multiprocessing


import acme
import acme.core
import acme.plugin as plugin
import acme.network as network

from cPickle import PicklingError

import json

## Attempt to load pykarl library
try:
    import pykarl.event
except ImportError:
    pass

## Attempt to use Python3 QueueHandler class
import logging.handlers
try:
    QueueHandler = logging.handlers.QueueHandler
except:
    QueueHandler = acme.LogQueueHandler

#MARK: -
#MARK: Constants
RESPONSE_QUEUE_FETCH_TIMEOUT = datetime.timedelta(seconds=.5)
LOGGER_QUEUE_FETCH_TIMEOUT = datetime.timedelta(seconds=.5)
KARL_QUEUE_FETCH_TIMEOUT = datetime.timedelta(seconds=.5)
EXECUTOR_QUEUE_FETCH_TIMEOUT = datetime.timedelta(seconds=.5)
EXECUTOR_EXECUTION_SLA = datetime.timedelta(seconds=15)     ## Threshold which specifies how long an agent can remain in a queued state before we spin up a new executor thread
EXECUTOR_LOOP_WAIT_TIME = datetime.timedelta(seconds=.5)

EXECUTOR_IDLE_TTL = datetime.timedelta(minutes=1)
EXECUTOR_SHUTDOWN_WAIT_TIME = datetime.timedelta(seconds=1)
CONTROLLER_LOOP_WAIT_TIME = datetime.timedelta(seconds=.5)

#MARK: -
#MARK: Globals
AGENT_PRIORITY_NONE = 0
AGENT_PRIORITY_LOW = 1
AGENT_PRIORITY_MEDIUM = 2
AGENT_PRIORITY_HIGH = 3
AGENT_PRIORITY_CRITICAL = 4

AGENT_STATUS_IDLE = 0
AGENT_STATUS_EXECUTING = 1 << 0
AGENT_STATUS_QUEUED = 1 << 1

AGENT_STATE_NONE = 0
AGENT_STATE_IDLEMACHINE = 1 << 0
AGENT_STATE_ONLINE = 1 << 1
AGENT_STATE_OFFLINE = 1 << 2
AGENT_STATE_ONDOMAIN = 1 << 3
AGENT_STATE_OFFDOMAIN = 1 << 4
AGENT_STATE_ONVPN = 1 << 5
AGENT_STATE_OFFVPN = 1 << 6

AGENT_TRIGGER_EVENTBASED = 1 << 1
AGENT_TRIGGER_STARTUP = 1 << 2
AGENT_TRIGGER_SHUTDOWN = 1 << 3
AGENT_TRIGGER_SESSIONSTART = 1 << 4
AGENT_TRIGGER_SESSIONEND = 1 << 5 
AGENT_TRIGGER_SESSIONLOCK = 1 << 6
AGENT_TRIGGER_SESSIONUNLOCK = 1 << 7
AGENT_TRIGGER_SCHEDULED = 1 << 8
AGENT_TRIGGER_NETWORKCHANGE = 1 << 9
AGENT_TRIGGER_SYSTEMSUSPEND = 1 << 10
AGENT_TRIGGER_SYSTEMRESUMED = 1 << 11
AGENT_TRIGGER_INTRANETCONNECT = 1 << 12
AGENT_TRIGGER_INTRANETDISCONNECT = 1 << 13
AGENT_TRIGGER_PUBLICNETWORKCONNECT = 1 << 14
AGENT_TRIGGER_PUBLICNETWORKDISCONNECT = 1 << 15
AGENT_TRIGGER_PROCESSSTART = 1 << 16

AGENT_EXECUTION_STATUS_NONE = 0
AGENT_EXECUTION_STATUS_SUCCESS = 1 << 0
AGENT_EXECUTION_STATUS_ERROR = 1 << 1
AGENT_EXECUTION_STATUS_FATAL = 1 << 2

AGENT_EXECUTOR_STATUS_NONE = 0
AGENT_EXECUTOR_STATUS_RUNNING = 1 << 1
AGENT_EXECUTOR_STATUS_EXECUTING = 1 << 2
AGENT_EXECUTOR_STATUS_STOPPING = 1 << 3

AGENT_EXECUTION_LIMITS_NONE = 0
AGENT_EXECUTION_LIMITS_RUNONCE = 1 << 0
AGENT_EXECUTION_LIMITS_SUCCEEDONCE = 1 << 1

AGENT_QUALIFICATION_QUALIFIED = 0
AGENT_QUALIFICATION_TRIGGERNOTQUALIFIED = 1 << 0
AGENT_QUALIFICATION_PRERQUISITESNOTMET = 1 << 1
AGENT_QUALIFICATION_SITENOTQUALIFIED = 1 << 2
AGENT_QUALIFICATION_PROBABILITYFAILED = 1 << 3
AGENT_QUALIFICATION_MAXFREQUENCYHIT = 1 << 4
AGENT_QUALIFICATION_EXECUTIONLIMITSREACHED = 1 << 5


#MARK: -
#MARK: Classes
class BaseAgent(acme.core.ConfigurableObject, acme.core.PersistentObject):
    """
    Class which represents our baseline interface used by any agent.
    """
    
    name_ = None        #: User friendly name for the agent (backing var)
    identifier = None   #: Unique identifier for the agent
    
    state_path = None      #: Filesystem path to our state file or directory
    state_filepath_ = None  #: Accessor backing variable.

    needs_state_dir = False  #: If true, the system will provision a dedicated 
                             #: folder for state storage for this plugin
    
    manifest_path = None        #: Filesystem path to our manifest file or directory
    needs_manifest_dir = False  #: If true, the system will provision a dedicated 
                                #: folder for manifest storage for this plugin
    
    status = AGENT_STATUS_IDLE  #: bitmask of our agents current status
    priority = AGENT_PRIORITY_NONE #: bitmask priority of our agent
    
    execution_limits = AGENT_EXECUTION_LIMITS_NONE #: Bitmask of our agent's execution limits
    
    execution_lock = None   #: Lock object invoked by agent executors to prevent
                            #: concurrent execution.
    
    run_frequency = None  #: Denotes the frequency in which this scheduled agent will run. This value will be ignored if the AGENT_TRIGGER_SCHEDULED flag is not set. Can be :py:class:`datetime.timedelta`
    
    run_frequency_skew = None #: Denotes the maximum skew that will be applied to scheduled executions. Can be :py:class:`datetime.timedelta`
    
    random_skew = None #: Denotes the last chosen skew in a random roll. This value will be within the threshold defined by RunFrequencySkew and will be reset with each execution.
    
    min_run_frequency = None #: Denotes the minimum frequency in which this scheduled agent will run. When an agent breaches the minimum alotted frequency, the scheduler will +1 it's priority (i.e. Low Priority becomes Medium Priority). If set, breaching this threshold will result in immediate execution of the agent, despite configured triggers (Prerequisites are still honored).
    
    max_run_frequency = None #: Denotes the maximum frequency in which this agent can run. A time value of &lt;&eq; indicates no max frequency will be inforced.
    
    last_execution = None #: :py:class:`datetime.datetime` value denoting our last execution time.
    
    last_execution_status = AGENT_EXECUTION_STATUS_NONE #: Bitmask value which represents the outcome of our last execution.
    
    prerequisites = AGENT_STATE_NONE #: Bitmask representation of prerequisites needed to trigger us.
    
    triggers = None #: Bitmask of triggers which will initiate execution checks.
    
    run_probability = None #: Number between 1 and 1000 that dictates the probability that this agent will trigger. For instance, if you specify a run Probability of 60, whenever this agent is a candidate to run, a random number between 1-1000 will be chosen. If that number is 60 or less, we will trigger. If it is 61 or greater, we will defer until next execution time. (overridden by MinRunFrequency)
    
    ad_site_includes = None #: A list of AD sites to be targeted. If this value is populated and the system is not a membor of a denoted site, it will not execute.
    
    ad_site_include_filter = None #: Regex pattern used for site include filtering. This is addative to ad_site_includes.
    
    ad_site_excludes = None #: List of AD sites that would be excluded from targeting. An empty list means that no sites will be explicitely excluded. In the event of a conflict between ad_site_include* and ad_site_exclude*, Excludes will always win.
    
    ad_site_exclude_filter = None #: Regex pattern used for site exclusion filtering. This is addative to ad_site_excludes. In the event of a conflict between s* and ad_site_exclude*, Excludes will always win.
        
    key_map = {"name" : None,
                    "identifier" : None,
                    "priority" : None,
                    "execution_limits" : None,
                    "run_frequency" : "<type=timedelta>;",
                    "run_frequency_skew" : "<type=timedelta>;",
                    "random_skew" : "<type=timedelta>;",
                    "min_run_frequency" : "<type=timedelta>;",
                    "max_run_frequency" : "<type=timedelta>;",
                    "last_execution" : "<type=datetime>;",
                    "last_execution_status" : None,
                    "prerequisites" : None,
                    "triggers" : None,
                    "run_probability" : None,
                    "ad_site_includes" : None,
                    "ad_site_include_filter" : None,
                    "ad_site_excludes" : None,
                    "ad_site_exclude_filter": None,
                    "status" : None,
                    }       #: Our default keymap, referenced by acme.SerializedObject
    
    state_keys = ["name","identifier","random_skew","last_execution",
                            "last_execution_status"]  #: Our default state keys 
        
    settings_keys = ["name","identifier","priority","run_frequency",
                        "run_frequency_skew","min_run_frequency",
                        "max_run_frequency", "prerequisites","triggers",
                        "run_probability","ad_site_includes",
                        "ad_site_include_filter","ad_site_excludes",
                        "ad_site_exclude_filter"]    #: Our default settings keys
    
    
    #MARK: Properties
    @property
    def name(self):
        """
        Property denoting our agent name
        """
        if self.name_: return self.name_
        
        return self.identifier
            
    @name.setter
    def name(self,value):
        """
        Setter accessor for name
        """
        self.name_ = value
        
    @property
    def logger_name(self):
        """
        Property which returns our logger name.
        """
        name = None
        if self.name:
            name = "Agent:{}".format(self.name)
        elif self.identifier:
            name = "Agent:{}".format(self.identifier)
        else:
            name = "Agent"
        return name
    
    @property
    def state_filepath(self):
        """
        Property to return our state filepath.
        """
        path = None
        
        if self.state_filepath_:
            path = self.state_filepath_
        elif self.needs_state_dir and self.state_path:
            path = os.path.join(self.state_path,"{}.json".format(self.identifier))
        elif self.state_path:
            path = self.state_path
        return path
    
    @state_filepath.setter
    def state_filepath(self, value):
        """
        Property to set our state filepath.
        """
        self.state_filepath_ = value
    
    @state_filepath.deleter
    def state_filepath(self, value):
        """
        Property to delete our state filepath.
        """
        del(self.state_filepath_)
    
    @property
    def settings_filepath(self):
        """
        Property to return our settings filepath.
        """
        path = None
        
        if self.settings_filepath_:
            path = self.state_filepath_
        elif self.needs_manifest_dir and self.manifest_path:
            path = os.path.join(self.manifest_path,"{}.json".format(self.identifier))
        elif self.manifest_path:
            path = self.manifest_path
        
        return path

    @property
    def settings_filepath(self):
        """
        Property to return our settings filepath.
        """
        path = None
        
        if self.settings_filepath_:
            path = self.state_filepath_
        elif self.needs_manifest_dir and self.manifest_path:
            path = os.path.join(self.manifest_path,"{}.json".format(self.identifier))
        elif self.manifest_path:
            path = self.manifest_path
        
        return path
        
    @settings_filepath.setter
    def settings_filepath(self, value):
        """
        Property to set our settings filepath.
        """
        self.settings_filepath_ = value
        
    @settings_filepath.deleter
    def settings_filepath(self):
        """
        Property to unset our settings filepath.
        """
        del(self.settings_filepath_)
    
    @property
    def manifest_filepath(self):
        """
        Property to return our manifest filepath.
        
        ..warning:
            This property has been replaced by settings_filepath, use that
            instead. (keeping here for compatability)
        
        """
        return self.settings_filepath    
    
    
    #MARK: Constructors
    def __init__(self, name=None, identifier=None, key_map=None, 
                                                    state_keys=None,
                                                    settings_keys=None,
                                                    use_lock=None,
                                                    *args, **kwargs):
        """
        Our Constructor.
        
        :param str name: The name of our Agent (should be unique)
        :param str identifier: The identifier of our agent (MUST be unique)
        :param dict key_map: Key mapping dictionary used for serialization
        :param list state_keys: List of keys that will be used to serialize object state
        :param list settings_keys: List of keys that will be used to serialize object settings
        :param bool use_lock: Setting to control whether we use a mutex to prevent concurrent execution across multiple executor threads. If set to False, we will allow concurrent execution
        
        """
        if name:
            self.name = name
        
        if identifier:
            self.identifier = identifier
        
        self.status = AGENT_STATUS_IDLE
        
        if use_lock:
            self.execution_lock = threading.RLock()
        
        if key_map is None:
            key_map = {}
            key_map.update(BaseAgent.key_map)
        
        if state_keys is None:
            state_keys = BaseAgent.state_keys[:]
        
        if settings_keys is None:
            settings_keys = BaseAgent.settings_keys[:]
        
        super(BaseAgent,self).__init__(key_map=key_map, state_keys=state_keys,
                                                settings_keys=settings_keys,
                                                *args,**kwargs)
    
    #MARK: Loading/Saving
    def load(self):
        """
        Method used to load all agent values. It will be called
        once per agent when loaded by the system. The agent
        should register any event handlers and perform any other 
        prerequisite actions.
        """
        logger = logging.getLogger(self.logger_name)
        
        try:
            self.load_state()
        except Exception as exp:
            logger.error("Agent:{} encountered an error when attempting to load previous state. Error:{}".format(self.identifier,exp))
            logger.log(9,"Failure stack trace (handled cleanly):", exc_info=1)
        
        try:
            self.load_settings()
        except Exception as exp:
            logger.error("Agent:'{}' encountered an error when attempting to load agent settings. Error:{}".format(self.identifier,exp))
            logger.log(9,"Failure stack trace (handled cleanly):", exc_info=1)
        
    def save(self,filepath=None):
        """
        Method used to save agent state. This will be called after every
        execution.
        """
        
        return self.save_state(filepath=filepath)
        
    def unload(self):
        """
        Method used to shutdown our agent. This is where an agent should
        deregister any event handlers and cleanup any other used system 
        resources. After executing this method, and agent should be 
        consuming no resources or have any open handles.
        """
        pass
        
    def load_settings(self,filepath=None):
        """
        Method to load our settings.
        """
                
        if not filepath and self.manifest_filepath:
            filepath = self.manifest_filepath
        
        acme.core.ConfigurableObject.load_settings(self, filepath=filepath)
                        
    def reset_skew(self):
        """
        Method that will re-roll our currently chozen random_skew,
        as seeded by run_frequency_skew.
        """
        logger = logging.getLogger(self.logger_name)
        
        skew = datetime.timedelta(seconds=0)
        
        if self.run_frequency_skew:
            try:
                max_seconds = abs(self.run_frequency_skew.total_seconds())
                rand_num = random.randint(0,max_seconds)
                rand_skew = rand_num - (max_seconds / 2)
                skew = datetime.timedelta(seconds=rand_skew)
            except Exception as exp:
                logger.warning("Failed to set skew: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly)",exc_info=1)
            
        self.random_skew = skew
        
    def execute(self,trigger=None,data=None):
        """
        Our primary execution method. This method will be called by
        our scheduler or during events as registered by our triggers.
        
        :param trigger: The trigger executing the action
        :type trigger: int: bitwise mask value. See AGENT_TRIGGER_*
        """
        logger = logging.getLogger(self.logger_name)
                
        logger.info("{} Executing from trigger:{} with data:{}".format(
                                                        self.identifier,
                                                        trigger,
                                                        data))
        
        ## Do something here
        time.sleep(.5)
        
        ## Report status
        self.last_execution_status = AGENT_EXECUTION_STATUS_NONE
        
        logger.info("{} Finished Executing!".format(self.identifier))
    
    def copy(self):
        """
        Method to return a shallow copy of our object.
        """
        
        data = self.to_dict()
        
        new_copy = self.__class__(dict_data=data, key_map=self.key_map,
                                            state_keys=self.state_keys,
                                            settings_keys=self.settings_keys)
          
        ## Copy items which are not accounted for in our Serialization process
        new_copy.state_path = self.state_path
        new_copy.manifest_path = self.manifest_path
        new_copy.needs_state_dir = self.needs_state_dir
        new_copy.needs_manifest_dir = self.needs_manifest_dir
        
        return new_copy
    
    def deepcopy(self):
        """
        Method to provide a deep copy of our object.
        """
        
        json_data = self.to_json()
        
        new_copy = self.__class__(json_data=json_data, key_map=self.key_map,
                                            state_keys=self.state_keys,
                                            settings_keys=self.settings_keys)
        
        ## Copy items which are not accounted for in our Serialization process                                            
        new_copy.state_path = self.state_path
        new_copy.manifest_path = self.manifest_path
        new_copy.needs_state_dir = self.needs_state_dir
        new_copy.needs_manifest_dir = self.needs_manifest_dir
        
        return new_copy

    
    
#MARK: -
class AgentController(object):
    """
    Primary controller class which provides asyncronous scheduling and 
    triggering of agents.
    """
    
    agents = {}                #: Var which represents loaded agents, keyed by identifier
    plugin_path = None         #: Directory which we load our agents from.
    
    agent_qualifier = None     #: Qualifier object
    
    maxnum_agent_executors = 5    #: Maximum number of execution agents
    
    execution_queue = None  #: :py:class:`multiprocessing.Queue` object to 
                            #: monitor for new execution requests
                            
    response_queue = None   #: :py:class:`multiprocessing.Queue` object used 
                            #: to proxy agent execution status updates.
                            
    karl_queue = None       #: :py:class:`multiprocessing.Queue` object used to 
                            #: proxy KARL events
    
    logger_queue = None     #: logger queue to be used for logging by agents 
                            #: (as logging module is not multi-process)
    
    use_multiprocessing_queues = False #: Boolean flag to set whether our AgentExecutors
                                   #: configure queue mechanisms (logger, 
                                   #: karl, etc) to facilitate multi-process
                                   #: executions.
                                       
    execution_threads = []        #: Array of executor thread objects
    
    agent_queue_data = {}         #: Dictionary of queued requests, keyed by agent identifier+trigger
    
    agent_queue_lock = None       #: Locking mechanism to control modifications to agent_queue
    
    agent_requeue_threshold = None     #: Timedelta object representing how frequently we will re-queue already queued agents  
                
    state_dirpath = None       #: Directory containing state files
    manifest_dirpath = None    #: Directory
    
    logger_name = "AgentController"  #: Name of our logger in debug mode
    
    should_run = None           #: Semaphore flag used to stop 
    process_thread = None       #: Our processing thread.
    
    _domain = None              #: Backing var for our domain property
    
    @property
    def domain(self):
        """
        Property which represents our Agent domain 
        
        Domain
        ==========
        ServiceAgents
        SessionAgents
        UntrustedAgents
        
        """
        
        domain = None
        
        if self._domain:
            domain = self._domain
        elif self.plugin_path:
            dirname = os.path.basename(self.plugin_path)
            if dirname == "service_agents":
                domain = "ServiceAgents"
            elif dirname == "session_agents":
                domain = "SessionAgents"
            elif dirname == "untrusted_agents":
                domain = "UntrustedAgents"
        
        return domain

    @domain.setter
    def domain(self,value):
        self._domain == value
    
    
    def __init__(self,plugin_path=None,state_dirpath=None,
                                                    manifest_dirpath=None,
                                                    domain=None,
                                                    *args,**kwargs):
        """
        :param string plugin_path: The path to our plugins directory
        :param string state_dirpath: The path to our state directory (must have read+write access)
        :param string manifest_dirpath: The path to our manifest directory (must have read access)
        :param string domain: The domain our controller represents
        
        """
        
        self.plugin_path = plugin_path
        self.state_dirpath = state_dirpath
        self.manifest_dirpath = manifest_dirpath
        
        self.agent_queue_data = {}
        self.agent_queue_lock = threading.RLock()
        self.agent_requeue_threshold = datetime.timedelta(minutes=10)
        
        self.domain = domain
        
        self.qualifier = AgentQualifier()
        
    def load(self):
        """
        Method to load our controller agents.
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.log(9,"AgentController loading. Directories - State:{} Manifest:{} Plugin:{}".format(self.state_dirpath,
                                    self.manifest_dirpath,
                                    self.plugin_path))
        
        if self.state_dirpath:
            if not os.path.exists(self.state_dirpath):
                logger.info("State directory:{} does not exist, creating!".format(
                                                    self.state_dirpath))
                os.mkdir(self.state_dirpath,0755)
        
        if self.manifest_dirpath:
            if not os.path.exists(self.manifest_dirpath):
                logger.info("Manifest directory:{} does not exist, creating!".format(
                                                    self.manifest_dirpath))
                os.mkdir(self.manifest_dirpath,0755)
        
        if self.plugin_path:
            self.load_agents(path=self.plugin_path)
        

    def reload(self):
        """
        Method to reload settings.
        """
        logger = logging .getLogger(self.logger_name)
        
        logger.debug("Reloading AgentController...")
        
        for agent in self.agents.values():
            agent.load_settings()
        

    def load_agent(self,agent):
        """
        Method to load the provided agent.
        
        :param agent: The agent to load
        :type agent: :py:class:`BaseAgent`
        
        """
        if self.state_dirpath:
            if agent.needs_state_dir:
                agent.state_path = os.path.join(self.state_dirpath,
                                                            agent.identifier)
                if not os.path.exists(agent.state_path):
                    os.mkdir(agent.state_path,0755)
            else:
                agent.state_path = os.path.join(self.state_dirpath,"{}.json".format(agent.identifier))
            
        if self.manifest_dirpath:
            if agent.needs_manifest_dir:
                agent.manifest_path = os.path.join(self.manifest_dirpath,
                                                            agent.identifier)
                if not os.path.exists(agent.manifest_path):
                    os.mkdir(agent.manifest_path,0755)
            else:
                agent.manifest_path = os.path.join(self.manifest_dirpath,
                                        "{}.json".format(agent.identifier))
        agent.load()
        
        existing_agent = self.agents.pop(agent.identifier,None)
                
        if existing_agent:            
            key_map = agent.key_map_for_keys(agent.state_keys)
            key_map["status"] = None
            
            agent.load_dict(existing_agent.to_dict(key_map=key_map),
                                                            key_map=key_map)
                    
        self.agents[agent.identifier] = agent
        
    def load_agents(self, path=None, send_karl_events=True):
        """
        Method to load our agents from the specified path.
        
        :param string path: The path to load agents from. If ommitted we will
                        reference instance variable
                        
        :param bool send_karl_events: Whether we will send KARL events.
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if path is None:
            path = self.plugin_path
        
        if not path:
            logger.warning("Cannot load agents: no path defined.")
            return
        elif not os.path.isdir(path):
            logger.warning("Cannot load agents: no directory exists at:'{}'.".format(path))
            return
        
        loaded_agents = {}
        
        
        ## Moved to plugin system 8/27
        ##agents = self.agents_from_path(path)
        
        load_uuid = "{}".format(uuid.uuid4())
        
        try:
            pc = acme.plugin.PluginController(path=path,
                                                classes=[acme.agent.BaseAgent])
            pc.load()
            
            for plugin in pc.plugins:
                
                start_time = datetime.datetime.utcnow()
                
                agents = {}
                num_load_failures = 0
                                    
                for agent in plugin.get_targets().values():
                    try:
                        self.load_agent(agent)
                        
                        logger.info("Loaded agent:{} version:{}".format(
                                                            agent.identifier,
                                                            plugin.version()))
                        logger.log(5,"Agent info:{}".format(agent.to_json(
                                                        output_null=False)))
                        
                        agents[agent.identifier] = agent
                        loaded_agents[agent.identifier] = agent
                    except Exception as exp:
                        num_load_failures += 1
                        logger.error("Failed to load agent:'{}'. Error:{}".format(agent,exp))
                        logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                
                
                end_time = datetime.datetime.utcnow()
                
                load_time = end_time - start_time
                
                if plugin.load_time:
                    load_time += plugin.load_time
                
                if send_karl_events:
                    karl_payload = plugin.to_dict()
                    
                    karl_payload["load_uuid"] = load_uuid
                    karl_payload["loadtime"] = int(load_time.total_seconds() 
                                                                        * 1000)
                    
                    karl_payload["domain"] = self.domain
                    karl_payload["agents"] = ", ".join(agents.keys())
                    karl_payload["num_agents"] = len(agents)
                    
                    if plugin.load_failures:
                        karl_payload["error"] = True
                    else:
                        karl_payload["error"] = False
                    
                    karl_payload["num_failures"] = (len(plugin.load_failures) 
                                                        + num_load_failures)
                    
                    try:
                        event = pykarl.event.Event(type="PluginLoadEvent",
                                                    subject_area="ACME",
                                                    payload=karl_payload)
                        pykarl.event.dispatcher.dispatch(event)
                    except NameError:
                        logger.error("Cannot dispatch KARL event: KARL module not available!")
                    except Exception as exp:
                        logger.error("Failed to dispatch KARL event:{}... {}".format(
                                                                    event.type,
                                                                    exp))
                        logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                        
        except Exception as exp:
            logger.error("Failed to load agent PluginController! Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        
        logger.info("Loaded {} agents from path:'{}'".format(len(loaded_agents),
                                                                        path))
        agent_names = loaded_agents.keys()
        logger.debug("Loaded Agents: '{}'".format("', '".join(agent_names)))
        self.agents = loaded_agents
    
        
    def status(self):
        """
        Outputs the status of our controller.
        """
        
        status = AGENT_STATUS_IDLE
        
        for agent in self.agents.values():
            status |= agent.status
        
        return status
    
    def agent_executor_count(self):
        """
        Returns the number of spun up executors.
        
        """
        active_count = 0
        
        for executor in self.execution_threads:
            active_count += 1
        
        return active_count
        
    def agent_executor_idealcount(self):
        """
        Method which returns the number of executors that we should
        currently have executing agents concurrently.
        
        Todo: Revisit this
        """
        
        ideal_count = 0
        num_queued_agents = len(self.agent_queue_data)
        
        if not self.should_run:
            ideal_count = 0
        else:
            if (num_queued_agents > 0 
                        and num_queued_agents < self.maxnum_agent_executors):
                ideal_count = int(math.ceil(float(num_queued_agents) / 3.0))
            elif num_queued_agents:
                ideal_count = num_queued_agents
            
            if ideal_count < self.maxnum_agent_executors:
                ## If we have not breached our maximum number of executors:
                ## scale our number of executors up until our thresholds
                ## have recovered.
                num_overqueued = self.num_overqueued_agents()
                if num_overqueued > 0:
                    current_executor_count = self.agent_executor_count()
                    if current_executor_count == ideal_count:
                        ideal_count = current_executor_count + num_overqueued
                    elif current_executor_count > ideal_count:
                        ideal_count = current_executor_count
         
        ## We should never have more executors than agents
        if ideal_count > num_queued_agents:
            ideal_count = num_queued_agents
        
        ## Make sure we never breach our maxnum settings.
        if ideal_count > self.maxnum_agent_executors:
            ideal_count = self.maxnum_agent_executors
        
        return ideal_count
    
    def num_overqueued_agents(self):
        """
        Method which returns the number of queued agents which are currently
        past our allowed queue SLA.
        """
        
        now = datetime.datetime.utcnow()
        
        num_overqueued_agents = 0
        
        with self.agent_queue_lock:
            for queue_id, request in self.agent_queue_data.iteritems():
                if request.agent and request.agent.status == AGENT_STATUS_QUEUED:
                    if request.date + EXECUTOR_EXECUTION_SLA <= now:
                        num_overqueued_agents += 1
                    
        return num_overqueued_agents
    
    def next_executor_name(self):
        """
        Method to output the name of the next executor to spawn, in the 
        format 'Executor_X', where X is replaced by a numeric value,
        higher than any current running executors.
        """
        
        highest_index = 0
            
        for executor in self.execution_threads:
            if executor.name:
                try:
                    current_index = int(executor.name.replace("Executor_",""))
                except ValueError:
                    continue
                if highest_index < current_index:
                    highest_index = current_index
        
        next_index = highest_index + 1
        
        return "Executor_{}".format(next_index)
    
    def start_executor_thread(self,name=None):
        """
        Method to start a new executor thread.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if name:
            logger.log(9,"Spinning up new executor thread ({})...".format(name))
        else:
            logger.log(9,"Spinning up new executor thread...")
        
        try:
            ## Setup our queues. Note: only configure karl_queue or 
            ## logging_queue if we are using multiprocessing queues. 
            execution_queue = self.execution_queue
            response_queue = self.response_queue
            karl_queue = None
            logger_queue = None
            
            if self.use_multiprocessing_queues:
                logger_queue = self.logger_queue
                karl_queue = self.karl_queue
            
            et = AgentExecutor(name=name,
                                execution_queue=execution_queue,
                                response_queue=response_queue,
                                karl_queue=karl_queue,
                                logger_queue=logger_queue)
            
            logger.log(5,"AgentExecutor ({}) starting...".format(name))
            et.start()
            logger.log(5,"AgentExecutor ({}) started...".format(name))
            self.execution_threads.append(et)
            logger.log(5,"AgentExecutor ({}) added to execution_threads (len:{})...".format(name,len(self.execution_threads)))

        except Exception as exp:
            logger.error("Failed starting up new AgentExecutor: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
    
    def stop_executor_thread(self):
        """
        Method to stop a running executor thread.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        best_thread = None
        idle_threads = []
        candidate_threads = []        
        
        for executor in self.execution_threads:
            status = executor.status
            if status & AGENT_EXECUTOR_STATUS_RUNNING:
                if not status & AGENT_EXECUTOR_STATUS_STOPPING:
                    candidate_threads.append(executor)
                if not status & AGENT_EXECUTOR_STATUS_EXECUTING:
                    idle_threads.append(executor)
        
        try:
            best_thread = idle_threads[:-1][0]
        except IndexError:
            try:
                best_thread = candidate_threads[:-1][0]
            except IndexError:
                pass
                
        if best_thread:
            if best_thread.name:
                logger.log(9,"Stopping agent executor:{}".format(best_thread.name))
            else:
                logger.log(9,"Stopping agent executor thread:{}".format(best_thread))
            
            best_thread.should_run = False
        else:
            logger.log(2,"Cannot stop threads: no threads eligible!")
       
    def stop_executor_threads(self):
        """
        Method to stop all executor threads.
        """
        logger = logging.getLogger(self.logger_name)
        
        for executor in self.execution_threads:
            logger.log(9,"Stopping thread:{}".format(executor.name))
            executor.should_run = False
            
        time.sleep(EXECUTOR_SHUTDOWN_WAIT_TIME.total_seconds())
        
        for executor in self.execution_threads:
            if executor.is_alive():
                logger.info("Waiting for agent executors to quit...")
                time.sleep(EXECUTOR_SHUTDOWN_WAIT_TIME.total_seconds())
    
    def manage_execution_threads(self):
        """
        Method which is used to regulate how many active executor
        threads are running. 
        """
        
        logger = logging.getLogger(self.logger_name)
                
        if self.should_run:
        
            ## Clear out stale threads
            for executor in self.execution_threads[:]:
                if not executor.is_alive():
                    try:
                        logger.log(9,"AgentExecutor:{} is no longer alive, removing from pool...".format(executor.name))
                        self.execution_threads.remove(executor)
                    except Exception as exp:
                        logger.warning("Failed to remove executor:{} Error:{}".format(
                                                        executor,
                                                        exp))
                        logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
                        
            ideal_count = self.agent_executor_idealcount()
            
            count = self.agent_executor_count()
            
            logger.log(2,"Managing execution threads... Found {} listed, {} running agent executors, ideal count:{}...".format(
                                                                len(self.execution_threads),
                                                                count,
                                                                ideal_count))
            
            if count < ideal_count:
                logger.log(5,"Found {} running agent executors ({} queued agents), ideal count:{}, spinning up new processors...".format(
                                                                count,
                                                                len(self.agent_queue_data),
                                                                ideal_count
                                                                ))
                for i in xrange(ideal_count - count):
                    next_name = self.next_executor_name()
                    self.start_executor_thread(name=next_name)
            
            elif count > ideal_count:
                if ideal_count == 0:
                    ## If we are terminating them all, allow executor
                    ## TTL to expire the thread
                    ideal_count += 1
                else:
                    logger.log(5,"Found {} running agent executors, ideal count:{}, expiring processors...".format(
                                                                count,
                                                                ideal_count))
                for i in xrange(count - ideal_count):
                    self.stop_executor_thread()
        else:
            logger.debug(5,"System is set to shutdown, flagging all agent executors!")
            for executor in self.execution_threads:
                executor.should_run = False
                
    def execute_trigger(self,trigger,data=None):
        """
        Method to execute the provided trigger.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        trigger_names = string_for_enum_pattern(trigger,"AGENT_TRIGGER_")
        
        logger.log(5,"Executing trigger(s):'{}' data:{}".format(trigger_names,data))
        
        qualifier = self.qualifier
        
        current_state = self.qualifier.current_state_flags()
        
        for agent in self.agents.values():
            results = qualifier.run_qualifications(
                                                agent=agent,
                                                trigger=trigger,
                                                data=data,
                                                current_state=current_state)
            
            if results & AGENT_QUALIFICATION_TRIGGERNOTQUALIFIED:
                continue
                
            if results & AGENT_QUALIFICATION_EXECUTIONLIMITSREACHED:
                logger.debug("Agent:'{}' has hit execution limits, will not trigger...".format(
                                                            agent.identifier))
                continue
                
            if results & AGENT_QUALIFICATION_SITENOTQUALIFIED:
                logger.debug("Agent:'{}' failed network site restrictions, will not trigger...".format(
                                                            agent.identifier))
                continue
                
            if results & AGENT_QUALIFICATION_MAXFREQUENCYHIT:
                logger.debug("Agent:'{}' exceeds maximum execution frequency (last run:{}), will not trigger...".format(
                                                        agent.identifier,
                                                        agent.last_execution))
                continue
            
            if results & AGENT_QUALIFICATION_PRERQUISITESNOTMET:
                current_state_names = string_for_enum_pattern(current_state,
                                                            "AGENT_STATE_")
                req_state_names = string_for_enum_pattern(agent.prerequisites,
                                                            "AGENT_STATE_")
                logger.debug("Agent:'{}' failed to meet defined prerequisites, will not trigger... (Current:'{}' Required:'{}')".format(
                                        agent.identifier,
                                        current_state_names,
                                        req_state_names))
                continue
            
            if results & AGENT_QUALIFICATION_PROBABILITYFAILED:
                logger.debug("Agent:'{}' failed run_probability tests not met, will not trigger... (run_probability:'{}')".format(
                                        agent.identifier,
                                        agent.run_probability))
                continue
                
            request = AgentExecutionRequest(agent=agent, trigger=trigger,
                                                                    data=data)
            self.try_queue_request(request)
            
    def try_queue_request(self, request):
        """
        Method which will attempt to queue the provided request. This method 
        will log errors but will not surface any exceptions to the caller.
        
        :returns: True if queued, False on error, and None if no-op
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        did_queue = None
        
        agent = request.agent
        
        if not agent:
            logger.error("Agent execution request does not referenece an agent, cannot queue!")
            return
        
        if not self.execution_queue:
            logger.warning("Request:{} has attempted to queue but no execution queue exists...".format(request.queue_id()))
            return False
        
        try:
            with self.agent_queue_lock:
                requeue = False
                queue_id = request.queue_id()
                if queue_id in self.agent_queue_data:
                    e_request = self.agent_queue_data[queue_id]
                    requeue_time = e_request.date + self.agent_requeue_threshold
                    if datetime.datetime.utcnow() >= requeue_time:
                        logger.warning("Request:{} is already queued but has hit requeue threshold. Re-queueing agent...".format(
                                                                    queue_id))
                        requeue = True
                
                if requeue or not queue_id in self.agent_queue_data:
                    self.agent_queue_data[queue_id] = request
                    
                    ## Queue a copy of our request and agent to ensure
                    ## proper context isolation
                    self.execution_queue.put(request.copy())                        
                    did_queue = True
                    agent.status = AGENT_STATUS_QUEUED
                    logger.log(5,"Added agent:{} to execution queue...".format(agent.identifier))
                else:
                    logger.debug("Agent:'{}' is already queued, will not execute...".format(
                                                    agent.identifier))
        except Exception as exp:
            agent.status = AGENT_STATUS_IDLE
            agent.last_execution_status = AGENT_EXECUTION_STATUS_FATAL
            logger.error("Failed to queue agent: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",
                                                    exc_info=1)
            did_queue = False
        
        return did_queue
    
    def start(self):
        """
        Starts our run loop.
        """
        
        if not self.use_multiprocessing_queues:
            self.execution_queue = Queue.Queue()
            self.response_queue = Queue.Queue()
            self.karl_queue = Queue.Queue()
            self.logger_queue = Queue.Queue()    
        else: 
            self.execution_queue = multiprocessing.Queue()
            self.response_queue = multiprocessing.Queue()
            self.karl_queue = multiprocessing.Queue()
            self.logger_queue = multiprocessing.Queue()  
        
        logger = logging.getLogger(self.logger_name)
        
        logger.info("Starting {}...".format(self.logger_name))
        
        self.should_run = True
        
        self.process_thread = threading.Thread(target=self.loop,
                                        name="{}Thread".format(self.logger_name))
        self.process_thread.daemon = True
        self.process_thread.start()
    
    def stop(self):
        """
        Shutdown our run loop.
        """
    
        logger = logging.getLogger(self.logger_name)
        logger.info("Stopping {}...".format(self.logger_name))
        
        self.should_run = False
        self.stop_executor_threads()
        
        logger.info("AgentController successfully stopped...")
    
    def loop(self):
        """
        Our primary run thread.
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.log(9,"Initiating main run loop...")
        
        ## Primary Run Loop
        while self.should_run:
            try:
                self.manage_execution_threads()
            except Exception as exp:
                logger.error("Failed to manipulate execution threads:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
            try:
                self.trigger_scheduled_agents()
            except Exception as exp:
                logger.error("Failed to process scheduled agents:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
            try:
                self.process_logging_queue()
            except Exception as exp:
                logger.error("An error occurred while processing logger entries: {}".format(
                                                                        exp))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
            
            try:
                self.process_execution_responses()
            except Exception as exp:
                logger.error("An error occurred while processing execution responses: {}".format(
                                                                        exp))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
            
            try:
                self.proxy_karl_events()
            except Exception as exp:
                logger.error("An error occurred while proxying KARL events: {}".format(
                                                                        exp))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)

            
            time.sleep(CONTROLLER_LOOP_WAIT_TIME.total_seconds())
        
        logger.log(9,"Main run loop finished executing...")
    
    def trigger_scheduled_agents(self,date=None):
        """
        Method to trigger scheduled agents
        
        :param date: The date to use for evaluation
        :type data: :py:class:`datetime.datetime`
        """
        
        logger = logging.getLogger(self.logger_name)
               
        logger.log(2,"Triggering scheduled agents...")
                  
        if date is None:
            date = datetime.datetime.utcnow()
              
        qualifier = AgentQualifier()
        
        ## Check to see if agents should be triggered
        for agent in self.agents.values():
            try:
                if agent.status != AGENT_STATUS_IDLE:
                    continue
                
                if qualifier.agent_qualifies_for_trigger(agent=agent,
                                        trigger=AGENT_TRIGGER_SCHEDULED):
                    if (qualifier.run_qualifications(agent=agent,date=date) 
                                        == AGENT_QUALIFICATION_QUALIFIED):
                        if qualifier.agent_qualifies_for_run_frequency_with_skew(agent):
                            request = AgentExecutionRequest(
                                            agent=agent,
                                            trigger=AGENT_TRIGGER_SCHEDULED)
                            self.try_queue_request(request)                         
                            
            except Exception as exp:
                logger.error("Failed to qualify agent:'{}'. Error:{}".format(
                                                    agent.identifier,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                            exc_info=1)
    
    def process_logging_queue(self,max_entries=25):
        """
        Method to proxy Queued logging messages
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.log(2,"Processing logging queue...")
        
        if not self.logger_queue:
            logger.error("No logging queue is configured, cannot proxy log entries!")
            return
        
        entry_num = 1
        while entry_num <= max_entries:
            record = None
            try:
                record = self.logger_queue.get(
                            timeout=LOGGER_QUEUE_FETCH_TIMEOUT.total_seconds())
                if record:
                    proxy_logger = logging.getLogger(record.name)
                    proxy_logger.handle(record)
                    entry_num += 1
                self.logger_queue.task_done()
            except Queue.Empty:
                break
    
    def proxy_karl_events(self,max_events=25):
        """
        Method to proxy KARL events to our main engine.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.log(2,"Processing KARL events...")
        
        ## Check to see if we have any responses
        if not self.karl_queue:
            logger.error("No KARL queue is configured, cannot proxy events!")
            return
        
        event_num = 1
        while event_num <= max_events:
            event = None
            try:
                event = self.karl_queue.get(
                    timeout=KARL_QUEUE_FETCH_TIMEOUT.total_seconds())
                event_num += 1 
            except Queue.Empty:
                break
            
            if event:
                logger.log(5,"Recieved KARL event:{} for proxying.".format(event.type))
                
                try:
                    pykarl.event.dispatcher.dispatch(event)
                except NameError:
                    logger.error("Cannot Proxy KARL event: pykarl module not available!")
                except Exception as exp:
                    logger.error("Failed to proxy KARL event:{}!".format(event.type))
                
            self.karl_queue.task_done()
            
            if event_num == max_events:
                logger.warning("Processed maximum KARL events this pass ({}), deferring...".format(
                                                            event_num))
        
    def process_execution_responses(self,max_responses=25):
        """
        Method to process execution responses.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.log(2,"Processing execution responses...")

        ## Check to see if we have any responses
        if not self.response_queue:
            logger.error("No response queue is configured, cannot report status for agent:{}".format(agent.identifier))
            return
            
        response_num = 1
        while response_num <= max_responses:
            response = None
            try:
                response = self.response_queue.get(
                    timeout=RESPONSE_QUEUE_FETCH_TIMEOUT.total_seconds())
                response_num += 1 
            except Queue.Empty:
                break
            
            if response:
                qid = response.request_queue_id
                
                logger.log(5,"Recieved execution response for request:{}".format(
                                                                qid))
                
                with self.agent_queue_lock:
                    r_agent = response.agent
                    my_agent = None
                    
                    if qid in self.agent_queue_data:                                
                        my_agent = self.agent_queue_data[qid].agent
                    else:
                        logger.debug("Execution response was unqueued: {}".format(
                                                        qid))
                        if self.agent_queue_data:
                            logger.log(5, "Queued requests: \n\t{},".format(
                                    ",\n\t".join(self.agent_queue_data.keys())))
                        try:
                            my_agent = self.agents[r_agent.identifier]
                        except KeyError:
                            ## We log this below in a general catch-all
                            pass
                    
                    if my_agent:
                        try:
                        
                            key_map = my_agent.key_map_for_keys(my_agent.state_keys)
                            key_map["status"] = None
                            my_agent.load_dict(data=r_agent.to_dict(key_map=key_map),
                                                        key_map=key_map)
                        

                            ## If the agent is idle, remove it from our queue
                            if my_agent.status == AGENT_STATUS_IDLE:
                                logger.log(5,"Request:'{}' has finished executing, removing from execution queue...".format(qid))
                                self.agent_queue_data.pop(qid,None)
                            else:
                                logger.log(5,"Request:'{}' reported status change. Status:{}".format(
                                                            qid, my_agent.status))
                        except Exception as exp:
                            logger.error("An error occurred processing execution response: {}".format(
                                                                            exp))
                            logger.log(5,"Failure stack trace (handled cleanly):",
                                                            exc_info=1)
                    else:
                        logger.error("Recieved execution response from unknown agent:{} known agents:{}".format(
                                            r_agent.identifier,
                                            ", ".join(self.agents.keys())))
                        
            self.response_queue.task_done()
                
            if response_num >= max_responses:
                logger.warning("Processed maximum execution responses this pass ({}), deferring...".format(
                                                            response_num))
    
    def session_lock_handler(self,username=None,*args,**kwargs):
        """
        Handler to process session_unlock events
        """
        
        data = {"username" : username }
        
        self.execute_trigger(AGENT_TRIGGER_SESSIONLOCK,data=data)

    
    def session_unlock_handler(self,username=None,*args,**kwargs):
        """
        Handler to process session_unlock events
        """
        
        data = {"username" : username }
        
        self.execute_trigger(AGENT_TRIGGER_SESSIONUNLOCK,data=data)
    

    def user_login_handler(self,username=None,*args,**kwargs):
        """
        Handler to process user login events
        """
        
        data = {"username" : username }

        self.execute_trigger(AGENT_TRIGGER_SESSIONSTART,data=data)
    
    def user_logout_handler(self,username=None,*args,**kwargs):
        """
        Handler to process user logout events
        """
        
        data = {"username" : username }
        
        self.execute_trigger(AGENT_TRIGGER_SESSIONEND,data=data)
    
    def process_start_handler(self, *args, **kwargs):
        """
        Handler to process start events
        """
        
        self.execute_trigger(AGENT_TRIGGER_PROCESSSTART)
        
    
    def startup_handler(self,*args,**kwargs):
        """
        Handler to process startup events
        """
        
        self.execute_trigger(AGENT_TRIGGER_STARTUP)
    
    def shutdown_handler(self,*args,**kwargs):
        """
        Handler to process startup events
        """
        
        self.execute_trigger(AGENT_TRIGGER_SHUTDOWN)
    
    def system_suspend_handler(self,*args,**kwargs):
        """
        Handler to process system sleep events
        """
        
        self.execute_trigger(AGENT_TRIGGER_SYSTEMSUSPEND)

    def system_resume_handler(self,*args,**kwargs):
        """
        Handler to process system awake-from-sleep events
        """
        
        self.execute_trigger(AGENT_TRIGGER_SYSTEMRESUMED)
    
    def network_change_handler(self,*args,**kwargs):
        """
        Handler to process system network-change events
        """
        
        self.execute_trigger(AGENT_TRIGGER_NETWORKCHANGE)
    
    def intranet_connect_handler(self,*args,**kwargs):
        """
        Handler to process Intranet connection events
        """
        
        self.execute_trigger(AGENT_TRIGGER_INTRANETCONNECT)

    def intranet_disconnect_handler(self,*args,**kwargs):
        """
        Handler to process Intranet connection events
        """
        
        self.execute_trigger(AGENT_TRIGGER_INTRANETDISCONNECT)
    
    def internet_connect_handler(self,*args,**kwargs):
        """
        Handler to process internet connection events
        """
        
        self.execute_trigger(AGENT_TRIGGER_PUBLICNETWORKCONNECT)
    
    def internet_disconnect_handler(self,*args,**kwargs):
        """
        Handler to process internet disconnection events
        """
        
        self.execute_trigger(AGENT_TRIGGER_PUBLICNETWORKDISCONNECT)

#MARK: -
class AgentQualifier(object):
    """
    Class to qualify agents for execution.
    """
    
    site_info = None
    
    def run_qualifications(self,agent,trigger=None,date=None,data=None,
                                                    current_state=None):
        """
        Method to execute all qualifications tests against our agent and return
        the results.
        
        :param agent: The agent to test
        :type agent: :py:class:`BaseAgent`
        :param int trigger: BitMask denoting the trigger to test for
        :param date: The execution date (UTC) to test against 
                    (defaults to datetime.datetime.utcnow())
        :type date: :py:class:`datetime.datetime`
        
        :param data: Additional data to consult during checks
        :type data: Dictionary of arbitrary data
        :param current_state: Optional state, as returned from 
                :py:func:`current_state_flags()` that can be provided to 
                prevent extraneous lookups. 
        :type current_state: int
        
        :returns: (int) BitMask representing qualification results. A result
                of AGENT_QUALIFICATION_QUALIFIED indicates all tests have 
                passed
        
        """
        
        results = AGENT_QUALIFICATION_QUALIFIED
        
        if current_state is None:
            current_state = self.current_state_flags()
        
        if not date:
            date = datetime.datetime.utcnow()
        
        if trigger is not None:
            if not self.agent_qualifies_for_trigger(agent=agent,trigger=trigger):
                results |= AGENT_QUALIFICATION_TRIGGERNOTQUALIFIED
        
        ## Check for state reqs
        if not self.agent_qualifies_for_state(agent=agent,state=current_state):
            results |= AGENT_QUALIFICATION_PRERQUISITESNOTMET
        
        ## Check for AD site
        if not self.agent_qualifies_for_ad_sitedata(agent=agent):
            results |= AGENT_QUALIFICATION_SITENOTQUALIFIED
        
        ## Check for probability
        if not self.agent_qualifies_for_probabality_check(agent=agent):
            results |= AGENT_QUALIFICATION_PROBABILITYFAILED
        
        ## Check for max frequency
        if self.agent_exceeds_max_frequency(agent,date):
            results |= AGENT_QUALIFICATION_MAXFREQUENCYHIT
        
        ## Check execution limits
        if self.agent_exceeds_execution_limits(agent):
            results |= AGENT_QUALIFICATION_EXECUTIONLIMITSREACHED
        
        return results
        
    def current_state_flags(self):
        """
        Method to return our current status as a bitmask.
        """
        
        state = AGENT_STATE_NONE
        
        session = network.state.active_network_session
        
        if not session:
            session = network.NetworkSession()
            session.load()
        
        network_state = session.state
        
        if network_state & network.NETWORK_STATE_ONLINE:
            state |= AGENT_STATE_ONLINE
        elif network_state & network.NETWORK_STATE_OFFLINE:
            state |= AGENT_STATE_OFFLINE
            
        if network_state & network.NETWORK_STATE_ONDOMAIN:
            state |= AGENT_STATE_ONDOMAIN
        elif network_state & network.NETWORK_STATE_OFFDOMAIN:
            state |= AGENT_STATE_OFFDOMAIN
            
        if network_state & network.NETWORK_STATE_ONVPN:
            state |= AGENT_STATE_ONVPN
        elif network_state & network.NETWORK_STATE_OFFVPN:
            state |= AGENT_STATE_OFFVPN
            
        return state
    
    def agent_qualifies_for_probabality_check(self,agent):
        """
        Method which runs a probability check against our agent. If our agent 
        has a run_probability defined that is between 0 and 1000, we will 
        roll the dice and see if we should run. We will run if our randomly 
        generated number is less than or equal to the provided agent's 
        defined run probability.
        
        :Example:
            >>> agent.run_probability = 1000
            >>> qualifier.agent_qualifies_for_probability_check(agent)
            True
            >>>
            >>> agent.run_probability = 0
            >>> qualifier.agent_qualifies_for_probability_check(agent)
            True
            >>>
            >>> agent.run_probability = 500  ## (50% success rate)
            >>> random.randint = mock.Mock(return_value=499) ## Force random roll to 499
            >>> qualifier.agent_qualifies_for_probability_check(agent)
            False
            >>>
            >>> random.randint = mock.Mock(return_value=500)
            >>> qualifier.agent_qualifies_for_probability_check(agent)
            True
            """
        qualifies = True
        
        if (agent.run_probability and agent.run_probability > 0 
                                            and agent.run_probability < 1000):
            dice_roll = random.randint(1,1000)
            if agent.run_probability > dice_roll:
                qualifies = False
        
        return qualifies
    
    def agent_qualifies_for_ad_sitedata(self,agent,site_info=None):
        """
        Method to seef if an agent qualifies to run given the current
        network site and any defined site limitations.
        """
        
        qualifies = False
        
        if site_info is None and self.site_info is not None:
            site_info = self.site_info
        elif site_info is None:
            site_info = network.state.site_info
        
        ## Check for inclusions
        if agent.ad_site_includes or agent.ad_site_include_filter:
            if agent.ad_site_includes:
                for network_site in agent.ad_site_includes:
                    if site_info and site_info.qualifies_for_site(network_site):
                        qualifies = True
                        break
            if (agent.ad_site_include_filter 
                                    and site_info.qualifies_for_site_filter(
                                                agent.ad_site_include_filter)):
                qualifies = True
        else:
            qualifies = True
        
        ## Check for exclusions
        if qualifies and agent.ad_site_excludes or agent.ad_site_exclude_filter:
            exclude_qualifies = False
            
            if agent.ad_site_excludes:
                for network_site in agent.ad_site_excludes:
                    if site_info and site_info.qualifies_for_site(network_site):
                        exclude_qualifies = True
                        break
            
            if (agent.ad_site_exclude_filter 
                                    and site_info 
                                    and site_info.qualifies_for_site_filter(
                                                agent.ad_site_exclude_filter)):
                exclude_qualifies = True
            
            if exclude_qualifies:
                qualifies = False
        
        return qualifies
        
    def agent_qualifies_for_state(self,agent,state):
        """
        Method which determines whether the provided agent qualifies against
        the provided state mask.
        
        :param agent: The Agent to evaluate
        :type agent: :py:class:`BaseAgent`
        :param int state: The state mask to use.
        """
        results = True
        
        if (state & agent.prerequisites) != agent.prerequisites:
            results = False
        
        return results
                
    def agent_qualifies_for_run_frequency_with_skew(self,agent,date=None):
        """
        Returns whether our agent qualifies for our run frequency, accounting
        for skew.
        """
        result = False
        
        if date is None:
            date = datetime.datetime.utcnow()
            
        if agent.run_frequency_skew:
            if not agent.random_skew:
                agent.reset_skew()
                
            date = date + agent.random_skew
        
        return self.agent_qualifies_for_run_frequency(agent=agent,date=date)   
        
    def agent_qualifies_for_run_frequency(self,agent,date=None):
        """
        Returns whether our agent qualifies for our run frequency.
        """
        result = False
        
        if date is None:
            date = datetime.datetime.utcnow()
        
        last_run = agent.last_execution
        
        if agent.run_frequency and last_run:
            if date >= last_run + agent.run_frequency:
                result = True
        elif agent.run_frequency:
            result = True
        
        return result
    
    def agent_exceeds_min_frequency(self,agent,date=None):
        """
        Returns whether our agent exceeds min frequency.
        """
        result = False
        
        if date is None:
            date = datetime.datetime.utcnow()
        
        last_run = agent.last_execution
        min_frequency = agent.min_run_frequency
        
        if min_frequency and last_run:
            if now >= last_run + min_frequency:
                result = True
        elif min_frequency and not last_run:
            result = True
        
        return result
            
    def agent_exceeds_max_frequency(self,agent,date=None):
        """
        Returns whether our agent exceeds max frequency.
        """
        result = False
        
        if date is None:
            date = datetime.datetime.utcnow()
        
        last_run = agent.last_execution
        max_frequency = agent.max_run_frequency
        
        if max_frequency and last_run:
            if now <= last_run + max_frequency:
                result = True
        
        return result
                    
    def agent_exceeds_execution_limits(self,agent):
        """
        Returns whether our agent has exceeded it's execution limits.
        """
        result = False
        
        if agent.execution_limits is not None:
            if agent.execution_limits & AGENT_EXECUTION_LIMITS_RUNONCE:
                if agent.last_execution:
                    result = True
            elif agent.execution_limits & AGENT_EXECUTION_LIMITS_SUCCEEDONCE:
                if agent.last_execution_status == AGENT_EXECUTION_STATUS_SUCCESS:
                    result = True
        return result
    
    def agent_qualifies_for_trigger(self,agent,trigger):
        """
        Method which returns whether or not our agent is registered
        for the provided trigger.
        """
        
        result = False
        if agent.triggers and (agent.triggers & trigger) == trigger:
            result = True
        
        return result
        
    
    
#MARK: -
class AgentExecutor(object):
    """
    Class which monitors our agent event queue and provides execution 
    capabilities.
    """
    
    name = None             #: Identifier for our executor.
    
    _logger_name = None     #: Backing var to store an explicit logger name.
    
    
    execution_queue = None  #: :py:class:`multiprocessing.Queue` object to 
                            #: monitor for new execution requests
                            
    response_queue = None   #: :py:class:`multiprocessing.Queue` object used 
                            #: to proxy agent execution status updates.
                            
    karl_queue = None       #: :py:class:`multiprocessing.Queue` object used to 
                            #: proxy KARL events
                            
    logger_queue = None     #: logger queue to be used for logging by agents 
                            #: (as logging module is not multi-process)
                            
    should_run_mp = None    #: :py:class:`multiprocessing.Value` object 
                            #: which denotes whether we should continue to run
                                
    qualifier = None        #: :py:class:`AgentQualifier` instance used to qualify execution.
    is_executing = False    #: Denotes whether we are currently executing
    
    process = None          #: Our :py:class:`multiprocessing.Process` object
    
    status = None           #: Our executor status.
    
    last_activity = None    #: :py:class:`datetime.datetime` object representing
                            #: the last time we performed any activity.
    ttl = None              #: :py:class:`datetime.timedelta` object representing
                            #: how long we will sit idle before terminating.
    
    
    @property
    def logger_name(self):
        """
        Property which returns our logger name.
        """
        
        if self._logger_name:
            return self._logger_name
        elif self.name:
            return "AgentExecutor:{}".format(self.name)
        else:
            return "AgentExecutor"
    
    @logger_name.setter
    def logger_name(self,value):
        """
        Setter to set an explicit logger name.
        """
        
        self._logger_name = value
    
    @property
    def status(self):
        """
        Property which returns our status.
        """
        status = AGENT_EXECUTOR_STATUS_NONE
        
        if self.is_alive():
            status |= AGENT_EXECUTOR_STATUS_RUNNING
            if not self.should_run:
                status |= AGENT_EXECUTOR_STATUS_RUNNING
                
        if self.is_executing:
            status |= AGENT_EXECUTOR_STATUS_EXECUTING
        
        return status
    
    @property
    def should_run(self):
        """
        Property which puts a boolean wrapper around our 
        :py:class:`multiprocessing.Value` object in self.should_run_mp
        """
        
        should_run = False
        
        if self.should_run_mp:
            should_run = bool(self.should_run_mp.value)
        
        return should_run
        
    @should_run.setter
    def should_run(self,value):
        """
        Setter access for our should_run property
        """
        
        should_run = int(bool(value))
        
        self.should_run_mp.value = should_run
    
    def __init__(self,name=None,execution_queue=None,response_queue=None,
                                                            karl_queue=None,
                                                            logger_queue=None,
                                                            *args,**kwargs):
        """
        Constructor.
        
        :param str name: The name of our executor instance.
        :param execution_queue: The queue to monitor.
        :type execution_queue: :py:class:`multiprocessing.Queue`
        :param response_queue: The queue to use for posting updates.
        :type response_queue: :py:class:`multiprocessing.Queue`
        :param karl_queue: The queue to use for KARL event proxying. If provided,
            we will reconfigure global KARL event routing to be passed over
            the provided karl_queue instance. This is desirable behavior when executing 
            in a forked environment, such as provided by the :py:mod:`multiprocessing` 
            Python module. In a non-forked environment (such as provided by
            the :py:mod:`threading` Python module), this may lead to recursion
            issues. 
        :type karl_queue: :py:class:`multiprocessing.Queue`
        :param logger_queue: The queue to use for log output routing. If provided,
            we will reconfigure global log messaging to be passed over
            the provided logger_queue. This is desirable behavior when executing 
            in a forked environment, such as provided by the :py:mod:`multiprocessing` 
            Python module.

        :type logger_queue: :py:class:`multiprocessing.Queue`
        
        .. warning:
            Providing karl_queue and/or logger_queue objects will cause the
            executor instance to reconfigure global KARL and logging routing, 
            respectively. Provide these only if you are using a forked model,
            such as provided by :py:class:`multiprocessing.Process`. If 
            you provide these queues in a non-forked model, it will cause
            badniz (such as failed KARL event publication and
            queue recursion).         
        
        """
        
        if name is not None:
            self.name = name
        else:
            self.name = "Unnamed"
        
        if execution_queue is not None:
            self.execution_queue = execution_queue
            
        if response_queue is not None:
            self.response_queue = response_queue
        
        if karl_queue is not None:
            self.karl_queue = karl_queue
        
        if logger_queue is not None:
            self.logger_queue = logger_queue
        
        self.ttl = EXECUTOR_IDLE_TTL
        self.last_activity = datetime.datetime.utcnow()
        
        self.should_run_mp = multiprocessing.Value('i',0)
        
        super(AgentExecutor,self).__init__(*args,**kwargs)
        
    def is_alive(self):
        """
        Method to return whether we're alive.
        """
        is_alive = False
        
        if self.process and self.process.is_alive():
            is_alive = True
        
        return is_alive
    
    def start(self,*args,**kwargs):
        """
        Method to start our queue.
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.log(5, "{} is starting up...".format(self.logger_name))
        
        self.should_run = True
                
        d = {"name" : self.logger_name,
            "execution_queue" : self.execution_queue,
            "logger_queue" : self.logger_queue,
            "karl_queue" : self.karl_queue,
            "response_queue" : self.response_queue,
            "should_run" : self.should_run_mp}

        self.process = threading.Thread(target=self.run)
                
        #self.process = multiprocessing.Process(target=run_executor,kwargs=d)
        #self.process.daemon = True  #: Note: this setting may have agent implications, need to dig into this.
        self.process.start()
    
    def configure_karl(self,reset_delegates=False):
        """
        Method which will configure KARL globally to send messages via our
        KARL queue. This method is invoked by self.run if self.karl_queue is
        populated 
        
        :param bool reset_delegates: If true, we will reset all KARL delegates
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.debug("Configuring KARL dispatcher to utilize local queue...")
        try:
            if reset_delegates:
                pykarl.event.dispatcher.delegates = []
                    
            pykarl.event.dispatcher.delegates.append(self.proxy_karl_event)
        except NameError:
            logger.error("Cannot configure KARL: pykarl module not available!")
        except Exception as exp:
            logger.error("Failed to configure KARL:{}!".format(event.type))
    
    def configure_queue_logging(self,logger_queue=None):
        """
        Method which will configure logging output to use our logger_queue
        queue object. This method must be called if this object is to be used
        in a separate process (via multiprocessing.Process). This is necessary
        because the logging module does not support writing to the same 
        datastream/file across multiple processes. 
        See: https://docs.python.org/dev/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes 
        This method is invoked by self.run if self.logger_queue exists
        """
        
        if logger_queue is None:
            logger_queue = self.logger_queue
        
        qh = QueueHandler(queue=logger_queue)
                
        rl = logging.getLogger()
        
        loggers = [rl]
        
        try:
            loggers.extend(logging.Logger.manager.loggerDict.values())
        except Exception as exp:
            logger.debug("An error occurred looking up loggers:")
            pass
        
        for logger in loggers:
            try:
                current_handlers = logger.handlers[:]
                for handler in current_handlers:
                    try:
                        logger.removeHandler(handler)
                    except Exception as exp:
                        pass
            except Exception as exp:
                pass
        
        rl.addHandler(qh)
        
        logger = logging.getLogger(self.logger_name)
        logger.debug("Successfully configured queue-based logging...")
        
    def proxy_karl_event(self,event,karl_queue=None):
        """
        Method to dispatch events to KARL via a :py:class:`multiprocessing.Queue`
        object.
        """
        logger = logging.getLogger(self.logger_name)
        
        if not karl_queue:
            karl_queue = self.karl_queue
        
        if karl_queue:
            
            logger.log(9,"Proxying KARL event:{}".format(event.type))
            karl_queue.put(event)
        else:
            logger.warning("Cannot proxy KARL event, no queue configured!")
        
    def run(self):
        """
        Method which is executed when our thread is initialized. This
        will not typically be invoked directly but will be called asyncronously
        as a result of calling start().
        """
        
        ## If we have a logger queue, configure logs to send through that.
        if self.logger_queue:
            self.configure_queue_logging()
        
        logger = logging.getLogger(self.logger_name)
        
        logger.debug("{} is now running...".format(self.name))
        
        if not self.execution_queue:
            logger.warning("Executor started but no queue established!")
        
        ## If we have a karl queue, configure it for use. WARNING: This may
        ## cause recursion effects if this in ran in the primary runtime!
        if self.karl_queue:
            self.configure_karl(reset_delegates=True)
        
        while self.should_run:
            
            ## Check to see if our TTL has expired
            if self.ttl and self.last_activity:
                now = datetime.datetime.utcnow()
                if now > self.last_activity + self.ttl:
                    logger.log(9, "{} has gone idle, terminating...".format(self.name))
                    break
            
            ## If no queue is set, 
            if not self.execution_queue:
                logger.log(5,"No execution queue is established, waiting...")
                time.sleep(EXECUTOR_LOOP_WAIT_TIME.total_seconds())
                continue
            
            try:
                logger.log(2,"Checking for queued agents...")
                request = self.execution_queue.get(
                        timeout=EXECUTOR_QUEUE_FETCH_TIMEOUT.total_seconds())
                agent = request.agent
                logger.log(5,"Recieved execution request for agent:'{}'".format(
                                                            agent.identifier))
            except Queue.Empty:
                logger.log(2,"Execution queue is empty, waiting...")
                time.sleep(EXECUTOR_LOOP_WAIT_TIME.total_seconds())
                continue
                
            
            """ 
            Note: Do we want to qualify here? We already do it prior to queuing the 
            agent, though there is possibility of state change in between
            these two points, especially if we are throttling agent execution
            due to heavy load. Need to update code below...
            
            ## If we have a qualifier, make sure we qualify.
            if self.qualifier:
                try:
                    self.qualifier.qualify(request.agent,trigger=request.trigger)
                except AgentQualificationFailedError as exp:
                    logger.warning(exp.message)
                    continue
            """
            
            self.last_activity = datetime.datetime.utcnow()
                        
            try:
                self.is_executing = True
                
                trigger_name = string_for_enum_pattern(request.trigger, 
                                                            "AGENT_TRIGGER_")
                
                logger.log(15,"Agent:'{}' is executing (trigger:{})...".format(
                                                        agent.identifier,
                                                        trigger_name))
                agent.status = AGENT_STATUS_EXECUTING
                
                if self.response_queue:
                    try:
                        response = AgentExecutionResponse(
                                        request_uuid=request.uuid,
                                        request_queue_id=request.queue_id())
                        ## Submit a copy so that external actor cannot 
                        ## modify our internal state
                        response.agent = agent.deepcopy()
                        response.status = agent.status
                        logger.log(2,"Submitting response to queue...")
                        self.response_queue.put(response)
                    except Exception as exp:
                        logger.error("Failed to submit Agent response: {}".format(exp))
                        logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
                
                ## If our agent has an execution lock, utilize it.
                if agent.execution_lock:
                    with agent.execution_lock:
                        agent.execute(trigger=request.trigger,data=request.data)
                else:
                    agent.execute(trigger=request.trigger,data=request.data)
            
            except Exception as exp:
                agent.last_execution_status = AGENT_EXECUTION_STATUS_FATAL
                logger.error("Agent:'{}' failed execution with error:{}".format(
                                                    agent.identifier,
                                                    exp.message))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
            
            
            try:
                agent.status = AGENT_STATUS_IDLE
                agent.last_execution = datetime.datetime.utcnow()
                
                agent.reset_skew()
                agent.save()
                
            except Exception as exp:
                logger.error("Agent:'{}' failed post-execution tasks with error:{}".format(
                                                    agent.identifier,
                                                    exp.message))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
            
            logger.log(15,"Agent:'{}' finished executing...".format(
                                                        agent.identifier))
            if self.response_queue:
                try:
                    response = AgentExecutionResponse(
                                        request_uuid=request.uuid,
                                        request_queue_id=request.queue_id())
                    ## Submit a copy so that external actor cannot 
                    ## modify our internal state
                    response.agent = agent.deepcopy()
                    response.status = agent.status
                    self.response_queue.put(response)
                    logger.log(5,"Execution result submitted to queue (status:{})...".format(response.status))

                except Exception as exp:
                    logger.error("Failed to submit Agent response: {}".format(exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",
                                                            exc_info=1)
            else:
                logger.warning("No response queue is configured! Cannot report executor results to main process!")
            
            self.execution_queue.task_done()
            
            self.is_executing = False
            self.last_activity = datetime.datetime.utcnow()
            
        logger.log(9, "{} finished running...".format(self.logger_name))

#MARK: -
class AgentExecutionRequest(object):
    """
    Class which represents an execution request.
    """
    
    uuid = None         #: Our request UUID
    agent = None        #: The agent to execute
    trigger = None      #: The trigger effecting execution
    data = None         #: Additional context
    date = None         #: The date of the request
    
    def __init__(self, agent, trigger=None, data=None, date=None):
        """
        :param agent: The agent to execute
        :type agent: :py:class:`acme.agent.BaseAgent` descendent
        :param int trigger: The execution trigger, using a constant from 
                agent.AGENT_TRIGGER_*
        :param data: Key=>Value data relevant to the execution context. All
                    data represented must be Pickleable
        :type data: Dictionary
        :param date: The date of the request
        :type date: :py:class:`datetime.datetime`
        
        """
        
        self.uuid = uuid.uuid4()
        self.agent = agent
        self.trigger = trigger
        self.data = data
        if date is not None:
            self.date = date
        else:
            self.date = datetime.datetime.utcnow()
            
    
    def queue_id(self):
        """
        Method which returns a queue identifier for this request, which is 
        a concatonation of the agent identifier and trigger (if available)
        """
        
        trigger_id = string_for_enum_pattern(bitmask=self.trigger, 
                                                enum_pattern="AGENT_TRIGGER_",
                                                delim="|")
        
        if trigger_id:
            qid = "{}.{}".format(self.agent.identifier, trigger_id)
        else:
            qid = "{}".format(self.agent.identifier)
        
        return qid
    
    def copy(self):
        """
        Method which returns a new AgentExecutionRequest instance with
        isolated, but equal, data
        """
        
        new_instance = AgentExecutionRequest(agent=self.agent.deepcopy())
        new_instance.uuid = self.uuid
        new_instance.trigger = self.trigger
        new_instance.date = self.date
        
        if self.data is not None:
            new_instance.data = copy.deepcopy(self.data)

        return new_instance

#MARK: -
class AgentExecutionResponse(object):
    """
    Class which represents an execution result.
    """
    
    request_uuid = None         #: Our originating :py:class:`AgentExecutionRequest` UUID
    request_queue_id = None     #: Our originating requests queue_id
    status = None               #: The new status
    agent = None                #: The representing agent
    date = None                 #: The date of the response
    
    def __init__(self, agent=None, status=None, request_uuid=None, 
                                                    request_queue_id=None,
                                                    date=None):
        """
        :param agent: The agent to execute
        :type agent: :py:class:`acme.agent.BaseAgent` descendent
        :param int status: The current agent, using a constant from 
                agent.AGENT_STATUS_*
        :param str request_uuid: The UUID of the originating request
        :param str request_queue_id: The request_queue_id of the originating request
        :param date: The date of the request
        :type date: :py:class:`datetime.datetime`
        """

        self.agent = agent        
        self.request_uuid = request_uuid
        self.request_queue_id = request_queue_id
        self.status = status
        
        if date is not None:
            self.date = date
        else:
            self.date = datetime.datetime.utcnow()
    

#MARK: - Exceptions
class AgentQualificationFailedError(Exception):
    """
    Exception calss 
    """
    pass

#MARK: - Functions
def string_for_enum_pattern(bitmask, enum_pattern=None, delim=None):
    """
    Method to return a string representation of a global variable.
    
    .. example:
        
        >>> ENUM_VAL_1 = 1 << 0
        >>> ENUM_VAL_2 = 1 << 1
        >>> ENUM_VAL_3 = 1 << 2
        
        >>> string_for_enum_pattern(5, "ENUM_VAL")
        'ENUM_VAL_1, ENUM_VAL_3'
        
        >>> string_for_enum_pattern(5, "ENUM_VAL", ":")
        'ENUM_VAL_1:ENUM_VAL_3'
    """
    
    if delim is None:
        delim = ", "
    
    string_values = []
    module_vars = globals()
    
    for key in module_vars.keys():
        if enum_pattern:
            if not key.startswith(enum_pattern):
                continue
        try:
            if module_vars[key] == bitmask:
                string_values.append(key)
            elif module_vars[key] & bitmask:
                string_values.append(key)
        except Exception:
            pass
    
    if string_values:
        return delim.join(string_values)
        

    
