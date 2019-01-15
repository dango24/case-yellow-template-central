"""
.. package:: acme.compliance
    :synopsis: Module containing classes used by acme for compliance evaluation.
    :platform: OSX, Ubuntu

.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import acme
import acme.core

import copy
import datetime
import logging
import os
import random
import sys
import threading
import time

import acme.plugin as plugin
import acme.configuration as configuration
import acme.configuration.configfile

import time
import uuid
import acme.agent as agent_util
import Queue
import multiprocessing
import math
import acme.network as network
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
   
#MARK: Globals
CMODULE_EXECUTOR_STATUS_NONE = 0
CMODULE_EXECUTOR_STATUS_RUNNING = 1 << 1
CMODULE_EXECUTOR_STATUS_EXECUTING = 1 << 2
CMODULE_EXECUTOR_STATUS_STOPPING = 1 << 3

CMODULE_QUALIFICATION_QUALIFIED = 0
CMODULE_QUALIFICATION_TRIGGERNOTQUALIFIED = 1 << 0
CMODULE_QUALIFICATION_PRERQUISITESNOTMET = 1 << 1
CMODULE_QUALIFICATION_SITENOTQUALIFIED = 1 << 2
CMODULE_QUALIFICATION_PROBABILITYFAILED = 1 << 3
CMODULE_QUALIFICATION_MAXFREQUENCYHIT = 1 << 4
CMODULE_QUALIFICATION_EXECUTIONLIMITSREACHED = 1 << 5

#MARK: Constants
RESPONSE_QUEUE_FETCH_TIMEOUT = datetime.timedelta(seconds=.5)
LOGGER_QUEUE_FETCH_TIMEOUT = datetime.timedelta(seconds=.5)
KARL_QUEUE_FETCH_TIMEOUT = datetime.timedelta(seconds=.5)
EXECUTOR_QUEUE_FETCH_TIMEOUT = datetime.timedelta(seconds=.5)
EXECUTOR_EXECUTION_SLA = datetime.timedelta(seconds=15)     ## Threshold which specifies how long a module can remain in a queued state before we spin up a new executor thread
EXECUTOR_LOOP_WAIT_TIME = datetime.timedelta(seconds=.5)

EXECUTOR_IDLE_TTL = datetime.timedelta(minutes=1)
EXECUTOR_SHUTDOWN_WAIT_TIME = datetime.timedelta(seconds=1)
CONTROLLER_LOOP_WAIT_TIME = datetime.timedelta(seconds=5) 


#MARK: -
#MARK: Classes
class ComplianceStatus(acme.core.Enum):
    """
    Enum which represents compliance status
    """
    UNKNOWN = 0
    EXEMPT = 1 << 1
    COMPLIANT = 1 << 2
    ERROR = 1 << 3
    INGRACETIME = 1 << 4
    NONCOMPLIANT = 1 << 5
    ISOLATIONCANDIDATE = 1 << 6
    ISOLATED = 1 << 7

class ExecutionPrerequisites(acme.core.Enum):
    """
    Enum which represents Execution prerequisits flags
    """
    
    NONE = 0
    IDLEMACHINE = 1 << 0
    ONLINE = 1 << 1
    OFFLINE = 1 << 2
    ONDOMAIN = 1 << 3
    OFFDOMAIN = 1 << 4
    ONVPN = 1 << 5
    OFFVPN = 1 << 6


class ModuleStatus(acme.core.Enum):
    """
    Enum which represents various operational states of an compliance module
    """
    
    IDLE = 0
    QUEUED = 1
    EXECUTING = 1 << 1
    EVALUATING = 1 << 1 | 1 << 2
    REMEDIATING = 1 << 1 | 1 << 3

class ExecutionStatus(acme.core.Enum):
    """
    Enum which represents possible execution results
    """
    
    NONE = 0
    SUCCESS = 1 << 0
    ERROR = 1 << 1
    FATAL = 1 << 1 | 1 << 2

class ExecutionTrigger(acme.core.Enum):
    """
    Enum which represents possible evaluation triggers
    """
    
    SCHEDULED = 1 << 0
    MANUAL = 1 << 1

class EvaluationResult(acme.core.SerializedObject):
    """
    Class representing the results of an evaluation
    
    :param status: The status of our evaluation
    :type status: :py:class:`ComplianceStatus`
    :param status_codes: A list of status codes
    :type status_codes: list<string>
    :param start_date: Datestamp denoting when the evaluation started
    :type start_date: :py:class:`datetime.datetime`
    :param end_date: Datestamp denoting when the evaluation finished
    :type end_date: :py:class:`datetime.datetime`
    """
    
    key_map = {
                "compliance_status": None,
                "execution_status": None,
                "status_codes": None,
                "version": None,
                "support_files": "<getter=get_support_files,setter=load_support_files>;",
                "first_failure_date": "<type=datetime, format=%Y-%m-%dT%H:%M:%S.%f>",
                "compliance_deadline": "<type=datetime, format=%Y-%m-%dT%H:%M:%S.%f>",
                "isolation_deadline": "<type=datetime, format=%Y-%m-%dT%H:%M:%S.%f>",
                "start_date": "<type=datetime, format=%Y-%m-%dT%H:%M:%S.%f>",
                "end_date": "<type=datetime, format=%Y-%m-%dT%H:%M:%S.%f>"
                }
    
    def __init__(self, compliance_status=ComplianceStatus.UNKNOWN,
                                        execution_status=ExecutionStatus.NONE,
                                        status_codes=None, 
                                        version=None,
                                        first_failure_date=None,
                                        compliance_deadline=None,
                                        isolation_deadline=None,
                                        start_date=None,
                                        end_date=None,
                                        *args,
                                        **kwargs):
                                                        
        self.execution_status = execution_status
        self.compliance_status = compliance_status
        self.support_files = {}
        self.status_codes = status_codes
        
        self.first_failure_date = first_failure_date
        self.compliance_deadline = compliance_deadline
        self.isolation_deadline = isolation_deadline
        
        self.version = version
        
        if start_date:
            self.start_date = start_date
        else:
            self.start_date = datetime.datetime.utcnow()
            
        if end_date:
            self.end_date = end_date
        else:
            self.end_date = datetime.datetime.utcnow()
            
        super(EvaluationResult, self).__init__(*args, **kwargs)
        
    def __eq__(self, other):
        """
        Equality comparison.
        """
        
        result = True
        
        vars = ["execution_status", 
                        "compliance_status",
                        "status_codes",
                        "support_files",
                        "first_failure_date",
                        "compliance_deadline",
                        "isolation_deadline",
                        "start_date",
                        "end_date"]
        
        for var in vars:
            if getattr(self, var) != getattr(other, var):
                result = False
        
        return result
        
    def __ne__(self, other):
        """
        Inequality comparison.
        """
        
        return not self.__eq__(other)
        
    def get_support_files(self):
        """
        Method to serialize our loaded support files.
        """
        result = {}
        for key, file in self.support_files.iteritems():
            result[key] = file.to_dict()
        
        return result
        
    def load_support_files(self, files):
        """
        Method to serialize our loaded support files.
        """
        result = {}
        for key, file_data in files.iteritems():
            result[key] = SupportFile(dict_data=file_data)
        
        self.support_files = result        

class RemediationResult(acme.core.SerializedObject):
    """
    Class representing the results of a remediation
    
    :param execution_status: The status of our remediation task
    :type execution_status: :py:class:`ExecutionStatus`
    :param status_codes: A list of status codes
    :type status_codes: list<string>
    :param start_date: Datestamp denoting when the remediation started
    :type start_date: :py:class:`datetime.datetime`
    :param end_date: Datestamp denoting when the remediation finished
    :type end_date: :py:class:`datetime.datetime`
    """
    
    key_map = {
                "execution_status": None,
                "status_codes": None,
                "start_date": "<type=datetime, format=%Y-%m-%dT%H:%M:%S.%f>",
                "end_date": "<type=datetime, format=%Y-%m-%dT%H:%M:%S.%f>",
                "data" : "",
                }
    
    def __init__(self, execution_status=ExecutionStatus.NONE,
                                                    status_codes=None, 
                                                    start_date=None,
                                                    end_date=None,
                                                    data=None,
                                                    *args,
                                                    **kwargs):
                                                        
        self.execution_status = execution_status
        self.status_codes = status_codes
                
        if start_date:
            self.start_date = start_date
        else:
            self.start_date = datetime.datetime.utcnow()
            
        if end_date:
            self.end_date = end_date
        else:
            self.end_date = datetime.datetime.utcnow()
            
        self.data = data
        
        super(RemediationResult, self).__init__(*args, **kwargs)
    
    def __eq__(self, other):
        """
        Equality comparison.
        """
        
        result = True
        
        vars = ["execution_status", 
                        "status_codes",
                        "start_date",
                        "end_date",
                        "data"]
        
        for var in vars:
            if getattr(self, var) != getattr(other, var):
                result = False
        
        return result
        
    def __ne__(self, other):
        """
        Inequality comparison.
        """
        
        return not self.__eq__(other)

class SupportFile(acme.core.SerializedObject):
    """
    Class which represents a support file used for evaluation.
    
    :param name: Friendly name of our support file
    :type name: string
    :param filepath: The absolute path of our support file
    :type filepath: string
    :param hash: The hash of our file
    :type hash: string
    :param hash_algo: The hash algorithm for our file
    :type hash_algo: string
    :param load_hash: If true we will calculate the hash for the file on instantiation
    :type load_hash: string
    
    """
    
    key_map = {
                    "name": None,
                    "filepath": None,
                    "hash": None,
                    "hash_algo": None
                }
    
    def __init__(self, name=None, filepath=None, hash=None, 
                                                    load_hash=False,
                                                    hash_algo="sha256",
                                                    *args, **kwargs):
        
        self._name = name
        self.filepath = filepath
        self.hash = hash
        self.hash_algo = hash_algo
                
        super(SupportFile, self).__init__(*args, **kwargs)
    
        if load_hash and os.path.exists(self.filepath):
            self.update_hash()
            
    @property
    def name(self):
        name = None
        if self._name:
            name = self._name
        elif self.filepath:
            basename = os.path.splitext(os.path.basename(self.filepath))[0]
            if basename:
                name = basename
        return name
    
    @name.setter
    def name(self, value):
        self._name = value
    
    @name.deleter
    def name(self):
        self._name = None
    
    def exists(self):
        """
        Method to return whether or not the specified file exists
        on disk.
        
        :returns: (bool) Whether or not this file, and it's signature
                    exists on disk.
        
        :raises: ValueError if filepath is not configured
        """
        
        exists = False
        if not self.filepath:
            raise ValueError("filepath is not defined!")
        
        exists = os.path.isfile(self.filepath)
        
        return exists 
    
    def update_hash(self, hash_algo=None):
        """
        Method to update our stored hash.
        """
        
        self.hash = self.get_hash(hash_algo=hash_algo)
        
    def get_hash(self, hash_algo=None):
        """
        Method to return a hash our file on disk
        
        :param hash_algo: The hashing algorithm to use
        :type hash_algo: :py:class:`hashlib.HASH` object (default 'hashlib.sha256')
        :type hash_algo: string "sha256"
        
        :returns: string - hash 
        
        :raises: IOError on standard filesystem errors
        :raises: Exception on misc error
        """
        
        if hash_algo is None:
            hash_algo = self.hash_algo
        
        return acme.crypto.file_hash(self.filepath, hash=hash_algo)

    def __eq__(self, other):
        """
        Equality comparison.
        """
        
        result = True
        
        vars = ["name", 
                        "filepath",
                        "hash",
                        "hash_algo",
                        ]
        
        for var in vars:
            if getattr(self, var) != getattr(other, var):
                result = False
        
        return result

class BaseModule(acme.core.ConfigurableObject, acme.core.PersistentObject):
    """
    Class which represents our baseline interface used by any module.
    """
    name_ = None        #: User friendly name for the module (backing var)
    identifier = None   #: Unique identifier for the module
    
    version = None      #: Loaded module version
    
    state_path = None      #: Filesystem path to our state file or directory
    state_filepath_ = None  #: Accessor backing variable.
    
    needs_state_dir = False  #: If true, the system will provision a dedicated 
                             #: folder for state storage for this plugin
    
    manifest_path = None        #: Filesystem path to our manifest file or directory
    needs_manifest_dir = False  #: If true, the system will provision a dedicated 
                                #: folder for manifest storage for this plugin
    
    status = ModuleStatus.IDLE  #: bitmask of our modules current status
    
    ## Compliance Vars
    enforce_isolation = True    #: Bool var denoting whether we enforce Isolation

    exempt_flag = None          #: Bool value denoting whether or not we are exempt (validate only mode). If set in addition to exempt_until, exempt_until will take precidence
    
    exempt_until = None         #: :py:class:`datetime.datetime` value denoting a sunset date for exemption
         
    can_remediate = None        #: Bool var denoting whether our module has remediation capabilities
    
    auto_remediate = None       #: Bool var denoting whether our module supports automated remediation
              
    gracetime = datetime.timedelta(days=4)   #: :py:class:`datetime.timedelta` value denoting our configured gracetime.
    
    isolation_gracetime = datetime.timedelta(days=2)  #: :py:class:`datetime.timedelta` value denoting our configured isolation gracetime.
        
    first_failure_date = None   #: :py:class:`datetime.datetime` value denoting our first failed evaluation

    last_evaluation_result = None   #: The last stored evaluation result for this system.
    
    evaluation_history = None #: List of previous evaluation results
    
    evaluation_history_max_records = 10 #: integer defining how many records we persist
    
    last_remediaton_result = None #: Outcome of our last remediation
    
    remediation_history = None #: List of previous remediation results
    
    remediation_history_max_records = None #: integer defining how many records we persist
    
    last_compliance_status = None   #: Our last-known compliance status
    
    execution_lock = None   #: Lock object invoked by module executors to prevent
                            #: concurrent execution.
    
    evaluation_interval = None  #: Denotes the frequency in which this scheduled module will run.  Can be :py:class:`datetime.timedelta`

    min_evaluation_interval = None #: If a module has not evaluated within this time period, it should be be considered non-compliant
                
    retry_evaluation_interval = None #: Denotes the frequency in which this module will run under failure conditions (used for retry of failed executions)
    
    evaluation_skew = None #: Denotes the maximum skew that will be applied to scheduled executions. Expects :py:class:`datetime.timedelta`
    
    evaluation_skew_ = None #: Runtime var that holds our current skew roll
    
    remediation_interval = None  #: Denotes the frequency in which this scheduled module will run. This value will be ignored if the can_remediate or auto_remediate flags are not set. Can be :py:class:`datetime.timedelta`
    
    retry_remediation_interval = None #: Denotes the maximum frequency in which this module can run (used for retry of failed executions)
    
    remediation_skew = None #: Denotes the maximum skew that will be applied to scheduled remediations. Expects :py:class:`datetime.timedelta`
    
    remediation_skew_ = None #: Runtime var that holds our current skew roll
    
    last_known_compliant = None  #: :py:class:`datetime.datetime` value denoting the date we were last compliant
    
    last_known_noncompliant = None  #: :py:class:`datetime.datetime` value denoting the date we were last non compliant
    
    prerequisites = ExecutionPrerequisites.NONE #: Bitmask representation of prerequisites needed to trigger us.
    
    triggers = None #: Bitmask of triggers which will initiate execution checks.
    
    key_map = {"name" : None,
                    "identifier": None,
                    "version": None,
                    "priority": None,
                    "state_path": None,
                    "state_filepath_": None,
                    "needs_state_dir": None,
                    "manifest_path": None,
                    "needs_manifest_dir": None,
                    "status" : None,
                    "enforce_isolation": None,
                    "exempt_flag": None,
                    "exempt_until": "<type=datetime>",
                    "can_remediate": None,
                    "auto_remediate": None,
                    "gracetime": "<type=timedelta>",
                    "isolation_gracetime": "<type=timedelta>",
                    "first_failure_date": "<type=datetime>",
                    "last_evaluation_result" : "last_evaluation_result_dict",
                    "last_compliance_status": None,
                    "evaluation_interval" : "<type=timedelta>;",
                    "min_evaluation_interval" : "<type=timedelta>;",
                    "retry_evaluation_interval" : "<type=timedelta>;",
                    "evaluation_skew" : "<type=timedelta>;",
                    "evaluation_history": "evaluation_history_list",
                    "remediation_interval" : "<type=timedelta>;",
                    "retry_remediation_interval" : "<type=timedelta>;",
                    "remediation_skew" : "<type=timedelta>;",
                    "support_files": "<getter=get_support_files,setter=load_support_files>;",
                    "last_remediation_result" : "last_remediation_result_dict",
                    "remediation_history": "remediation_history_list",
                    "last_known_compliant": "<type=datetime, format=%Y-%m-%dT%H:%M:%S.%f>",
                    "last_known_noncompliant": "<type=datetime, format=%Y-%m-%dT%H:%M:%S.%f>",                    
                    "prerequisites" : None,
                    "triggers" : None,
                }       #: Our default keymap, referenced by acme.SerializedObject
    
    state_keys = [          
                            "name",
                            "identifier", 
                            "first_failure_date",
                            "last_evaluation_result",
                            "last_remediation_result",
                            "last_compliance_status",
                            "last_known_compliant",
                            "last_known_noncompliant",
                            "support_files",
                        ]  #: Our default state keys 
        
    settings_keys = ["name",
                            "identifier",
                            "priority", 
                            "state_path",
                            "needs_state_dir",
                            "enforce_isolation",
                            "exempt_flag",
                            "exempt_until",
                            "auto_remediate",
                            "gracetime",
                            "isolation_gracetime",
                            "evaluation_interval",
                            "min_evaluation_interval",
                            "retry_evaluation_interval",
                            "evaluation_skew",
                            "remediation_interval",
                            "retry_remediation_interval",
                            "remediation_skew",
                            "prerequisites",
                            "triggers",
                            "auto_remediate"]    #: Our default settings keys
    
    #MARK: Properties
    @property
    def name(self):
        """
        Property denoting our module name (if not assigned we will return 
        our identifier)
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
            name = "ComplianceModule:{}".format(self.name)
        elif self.identifier:
            name = "ComplianceModule:{}".format(self.identifier)
        else:
            name = "ComplianceModule"
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
            path = self.settings_filepath_
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
    def last_evaluation_result_dict(self):
        """
        Property for getting evaluation results in dict form (used
        for serialization).
        """
        
        result = {}
        
        if self.last_evaluation_result:
            result = self.last_evaluation_result.to_dict()
        
        return result
    
    @last_evaluation_result_dict.setter
    def last_evaluation_result_dict(self, value):
        """
        Property for setting evaluation results in dict form (used
        for serialization).
        """
        
        if value is None:
            self.last_evaluation_result = value
        else:
            er = EvaluationResult()
            er.load_dict(value)
            self.last_evaluation_result = er
    
    @property
    def last_remediation_result_dict(self):
        """
        Property for getting remediation results in dict form (used
        for serialization).
        """
        
        result = {}
        
        if self.last_remediation_result:
            result = self.last_remediation_result.to_dict()
        
        return result
    
    @last_remediation_result_dict.setter
    def last_remediation_result_dict(self, value):
        """
        Property for setting remediation results in dict form (used
        for serialization).
        """
        
        if value is None:
            self.last_remediation_result = value
        else:
            rr = RemediationResult()
            rr.load_dict(value)
            self.last_remediation_result = rr
    
    @property
    def manifest_filepath(self):
        """
        Property to return our manifest filepath.
        
        ..warning:
            This property has been replaced by settings_filepath, use that
            instead. (keeping here for compatability)
        
        """
        return self.settings_filepath
    
    @property
    def evaluation_history_list(self):
        """
        Property for getting evaluation history results in list form (used
        for serialization).
        """
        
        result = []
        
        if self.evaluation_history:
            for entry in self.evaluation_history:
                result.append(entry.to_dict())
                
        return result
    
    @evaluation_history_list.setter
    def evaluation_history_list(self, value):
        """
        Property for setting evaluation history results in list form (used
        for serialization).
        """
        
        my_list = []
        if value is None:
            self.evaluation_history = value
        else:
            for entry in value:
                er = EvaluationResult(dict_data=entry)
                my_list.append(er)
        
        self.evaluation_history = my_list
    
    @property
    def remediation_history_list(self):
        """
        Property for getting remediation history results in list form (used
        for serialization).
        """
        
        result = []
        
        if self.remediation_history:
            for entry in self.remediation_history:
                result.append(entry.to_dict())
                
        return result
    
    @remediation_history_list.setter
    def remediation_history_list(self, value):
        """
        Property for setting remediation history results in list form (used
        for serialization).
        """
        
        my_list = []
        if value is None:
            self.remediation_history = value
        else:
            for entry in value:
                rr = RemediationResult(dict_data=entry)
                my_list.append(rr)
        
        self.remediation_history = my_list
    
    def get_support_files(self):
        """
        Method to serialize our loaded support files.
        """
        result = {}
        for key, file in self.support_files.iteritems():
            result[key] = file.to_dict()
        
        return result
        
    def load_support_files(self, files):
        """
        Method to serialize our loaded support files.
        """
        result = {}
        for key, file_data in files.iteritems():
            result[key] = SupportFile(dict_data=file_data)
        
        self.support_files = result
    
    #MARK: Constructors
    def __init__(self, name=None, identifier=None, key_map=None, 
                                                    state_keys=None,
                                                    settings_keys=None,
                                                    use_lock=None,
                                                    *args, **kwargs):
        """
        Our Constructor.
        
        :param str name: The name of our module (should be unique)
        :param str identifier: The identifier of our module (MUST be unique)
        :param dict key_map: Key mapping dictionary used for serialization
        :param list state_keys: List of keys that will be used to serialize object state
        :param list settings_keys: List of keys that will be used to serialize object settings
        :param bool use_lock: Setting to control whether we use a mutex to prevent concurrent execution across multiple executor threads. If set to False, we will allow concurrent execution
        
        """
        
        if name:
            self.name = name
        
        if identifier:
            self.identifier = identifier
        
        self.status = ModuleStatus.IDLE
        
        if use_lock:
            self.execution_lock = threading.RLock()
        
        if key_map is None:
            key_map = {}
            key_map.update(BaseModule.key_map)
        
        if state_keys is None:
            state_keys = BaseModule.state_keys[:]
        
        if settings_keys is None:
            settings_keys = BaseModule.settings_keys[:]
                
        self.support_files = {}
        
        now = datetime.datetime.utcnow()
        self.last_evaluation_result = EvaluationResult(
                                                first_failure_date=now,
                                                compliance_deadline=now,
                                                isolation_deadline=now)
        
        self.last_remediation_result = RemediationResult()
        
        self.compliance_change_callbacks = None
                
        super(BaseModule, self).__init__(key_map=key_map,
                                                settings_keys=settings_keys,
                                                state_keys=state_keys)
            
    #MARK: Loading/Saving
    def load(self):
        """
        Method used to load all module values. It will be called
        once per module when loaded by the system. The module
        should register any event handlers and perform any other 
        prerequisite actions.
        """
        logger = logging.getLogger(self.logger_name)
        
        try:
            self.load_state()
        except Exception as exp:
            logger.error("Module:{} encountered an error when attempting to load previous state. Error:{}".format(self.identifier,exp))
            logger.log(9,"Failure stack trace (handled cleanly):", exc_info=1)
        
        try:
            self.load_settings()
        except Exception as exp:
            logger.error("Module:'{}' encountered an error when attempting to load module settings. Error:{}".format(self.identifier,exp))
            logger.log(9,"Failure stack trace (handled cleanly):", exc_info=1)
        
        try:
            self.register_support_files()
        except Exception as exp:
            logger.error("Module:'{}' encountered an error when attempting to register support files. Error:{}".format(
                                                        self.identifier,
                                                        exp))
            logger.log(9,"Failure stack trace (handled cleanly):", exc_info=1)
    
    def save(self,filepath=None):
        """
        Method used to save module state. This will be called after every
        execution.
        """
        
        return self.save_state(filepath=filepath)
        
    def unload(self):
        """
        Method used to shutdown our compliance module. This is where an module
        should deregister any event handlers and cleanup any other used system 
        resources. After executing this method, any module should be 
        consuming no resources or have any open handles.
        """
        pass
        
    def load_settings(self,filepath=None):
        """
        Method to load our settings.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if not filepath and self.manifest_filepath:
            filepath = self.manifest_filepath
         
        if filepath and os.path.isfile(filepath):
            logger.debug("Loading settings for compliance module:{} from file:{}".format(
                                                self.identifier,
                                                filepath))
            key_map = self.key_map_for_keys(self.settings_keys)
            self.load_from_file(key_map=key_map,filepath=filepath,
                                                    overwrite_null=True)
            ## Create support file reference
            try:
                file = SupportFile(name=self.identifier, 
                                                    filepath=filepath,
                                                    load_hash=True)
                self._support_files[self.identifier] = file
            except Exception as exp:
                logger.warning("Failed to create SupportFile reference for file:'{}'".format(
                                                            filepath))
     
    def copy(self):
        """
        Method to return a shallow copy of our object.
        """
        
        data = self.to_dict()
        
        new_copy = self.__class__(key_map=self.key_map,
                                            state_keys=self.state_keys,
                                            settings_keys=self.settings_keys)
        new_copy.load_dict(data)
        
        ## Copy items which are not accounted for in our Serialization process
        new_copy.state_filepath_ = self.state_filepath_
        new_copy.evaluation_skew_ = self.evaluation_skew_
        new_copy.remediation_skew_ = self.remediation_skew_        
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
        
        new_copy = self.__class__(key_map=self.key_map,
                                            state_keys=self.state_keys,
                                            settings_keys=self.settings_keys)
        
        new_copy.load_from_json(json_data=json_data)
        
        ## Copy items which are not accounted for in our Serialization process                                            
        new_copy.state_filepath_ = self.state_filepath_
        new_copy.evaluation_skew_ = self.evaluation_skew_
        new_copy.remediation_skew_ = self.remediation_skew_        
        new_copy.state_path = self.state_path
        new_copy.manifest_path = self.manifest_path
        new_copy.needs_state_dir = self.needs_state_dir
        new_copy.needs_manifest_dir = self.needs_manifest_dir
        
        return new_copy
    
    #MARK: Evaluation Methods
    def evaluate(self, trigger=None, *args, **kwargs):
        """
        High level method used to invoke an evaluation an subsequent follow-up
        activity.
        
        :param trigger: The trigger used to invoke the evaluation
        :type trigger: :py:class:`ExecutionTrigger` bitwise mask value.
        
        :returns: :py:class:`EvaluationResult`
        """
        
        logger = logging.getLogger(self.logger_name)
                
        logger.info("Beginning evaluation for compliance module:'{}'".format(
                                                            self.identifier))
        
        try:
            start_date = datetime.datetime.utcnow()
            self.status = ModuleStatus.EVALUATING
            result = self.evaluate_(trigger=trigger, *args, **kwargs)
        except Exception as exp:
            end_date = datetime.datetime.utcnow()
            logger.error("Compliance evaluation failed for module:'{}'. Error:{}".format(
                                                self.identifier,
                                                exp))
            logger.log(9,"Failure stack trace (handled cleanly):", exc_info=1)
            
            result = EvaluationResult(compliance_status=ComplianceStatus.ERROR,
                                        execution_status=ExecutionStatus.FATAL,
                                        start_date=start_date,
                                        end_date=end_date)
        
        self.status = ModuleStatus.IDLE

        if self.version:
            result.version = self.version
        
        ## Add our SupportFiles to our result
        keys = self.support_files.keys()
        for key in keys:
            try:
                file = self.support_files[key].deepcopy()
                if not file.hash and file.exists():
                    file.update_hash()
                    result.support_files[key] = file
                else:
                    result.support_files[key] = file
            except Exception as exp:
                logger.error("Failed to process evaluation support file: '{}'. Error:{}".format(
                                                        key,
                                                        exp.message))
        status = result.compliance_status

        if status & ComplianceStatus.COMPLIANT:
            self.last_known_compliant = datetime.datetime.utcnow()
            if self.first_failure_date:
                self.first_failure_date = None
            
        elif (status == ComplianceStatus.UNKNOWN or (
                                    status & ComplianceStatus.NONCOMPLIANT 
                                    | status & ComplianceStatus.ERROR)):
            self.last_known_noncompliant = result.end_date
            if not self.first_failure_date:
                self.first_failure_date = result.end_date
        
        #: Update our history
        self.last_evaluation_result = result
        self.archive_evaluation_result(result)
        
        logger.info("Finished evaluation for compliance module:'{}' ({})".format(
                                                self.identifier,
                                                ComplianceStatus.to_string(
                                                    result.compliance_status)))
        
        #: Get our status to trigger change notification callbacks
        self.last_compliance_status = self.compliance_status()
        
        return result
    
    def evaluate_(self, trigger=None, *args, **kwargs):
        """
        Our low-level evaluation method. This method is responsible for
        performing our evaluation and returning the results of that evaluation.
        This method should not be invoked directly. 
        
        :param trigger: The trigger used to invoke the evaluation
        :type trigger: :py:class:`ExecutionTrigger` bitwise mask value.
        
        :returns: :py:class:`EvaluationResult`
        """
        
        ## Note: Custom compliance modules should override this method
        
        result = EvaluationResult()
        
        ## Note: Do something here
        time.sleep(10)
        
        result.compliance_status = ComplianceStatus.COMPLIANT
        result.execution_status = ExecutionStatus.SUCCESS
        result.end_date = datetime.datetime.utcnow()
        
        return result
    
    def current_evaluation_interval(self):
        """
        Method to return our current evaluation interval, accounting for
        retry scenarios and randomized skews

        """
        
        interval = None
        
        if self.last_evaluation_result.execution_status & ExecutionStatus.ERROR:
            if self.retry_evaluation_interval:
                interval = self.retry_evaluation_interval
            else:
                interval = self.evaluation_interval
        else:
            interval = self.evaluation_interval
        
        if interval is not None and self.evaluation_skew:
            if not self.evaluation_skew_:
                self.reset_evaluation_skew()
            
            interval += self.evaluation_skew_
        
        return interval
    
    def is_evaluation_time(self):
        """
        Method which returns whether it is time for us to perform
        an evaluation. To qualify, we must be configured for the "SCHEDULED"
        trigger, we must be idle, and our last evaluation must be passed
        our threshold. 
        """
        
        result = False
        
        now = datetime.datetime.utcnow()
        
        if self.triggers is None or not self.triggers & ExecutionTrigger.SCHEDULED:
            result = False
        elif self.status != ModuleStatus.IDLE:
            result = False
        else:
            if (self.last_evaluation_result.execution_status 
                                                    != ExecutionStatus.NONE):
                if self.last_evaluation_result.version != self.version:
                    result = True
                else:
                    interval = self.current_evaluation_interval()
                    if interval is None:
                        interval = datetime.timedelta(seconds=0)
                    
                    result = (self.last_evaluation_result.end_date 
                                                        + interval <= now)
            else:
                result = True
        
        return result
    
    def archive_evaluation_result(self, result):
        """
        Method to archive the provided evaluation result
        
        :param result: The evaluation result to archive
        :type result: :py:class:`EvaluationResult`
        """
        
        history = []
        
        if self.evaluation_history is not None:
            history = self.evaluation_history
        elif len(history) > self.evaluation_history_max_records:
            history = history[self.evaluation_history_max_records * -1:]
        
        history.append(result)
        
        self.evaluation_history = history    
    
    def reset_evaluation_skew(self):
        """
        Method that will re-roll our currently chozen random_skew,
        as seeded by evaluation_skew.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        skew = datetime.timedelta(seconds=0)
        
        if self.evaluation_skew:
            try:
                max_seconds = abs(self.evaluation_skew.total_seconds())
                rand_num = random.randint(0,max_seconds)
                rand_skew = rand_num - (max_seconds / 2)
                skew = datetime.timedelta(seconds=rand_skew)
            except Exception as exp:
                logger.warning("Failed to set evaluation skew: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly)",exc_info=1)
            
        self.evaluation_skew_ = skew
    
    #MARK: Remediation methods
    def remediate(self, trigger=None, *args, **kwargs):
        """
        High level method used to invoke an remediation an subsequent follow-up
        activity.
        
        :param trigger: The trigger used to invoke the remediation
        :type trigger: :py:class:`ExecutionTrigger` bitwise mask value.
        
        :returns: :py:class:`RemediationResult`
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.info("Beginning remediation for compliance module:'{}'".format(
                                                            self.identifier))
        
        try:
            start_date = datetime.datetime.utcnow()
            result = self.remediate_(trigger=trigger, *args, **kwargs)

            logger.info("Finished remediation for compliance module:'{}' ({})".format(
                                                self.identifier,
                                                ExecutionStatus.to_string(
                                                    result.execution_status)))
            
        except Exception as exp:
            end_date = datetime.datetime.utcnow()
            logger.error("Compliance remediation failed for module:'{}'. Error:{}".format(
                                                self.identifier,
                                                exp))
            logger.log(9,"Failure stack trace (handled cleanly):", exc_info=1)
            
            result = RemediationResult(execution_status=ExecutionStatus.FATAL,
                                        start_date=start_date,
                                        end_date=end_date)
        
        #: Update our history
        self.last_remediation_result = result
        self.archive_remediation_result(result)
        
        return result
    
    def remediate_(self, trigger=None, *args, **kwargs):
        """
        Our low-level remediaton method. This method is responsible for
        performing our evaluation and returning the results of that evaluation.
        This method should not be invoked directly. 
        
        :param trigger: The trigger used to invoke the remediaton
        :type trigger: :py:class:`ExecutionTrigger` bitwise mask value.
        
        :returns: :py:class:`EvaluationResult`
        """
        
        ## Note: Custom compliance modules should override this method
        
        result = RemediationResult()
        
        ## Note: Do something here
        time.sleep(1)
        
        result.execution_status = ExecutionStatus.SUCCESS
        result.end_date = datetime.datetime.utcnow()
        
        return result
        
    def current_remediation_interval(self):
        """
        Method to return our current remediation interval, accounting for
        retry scenarios and randomized skews
        """
        
        interval = None

        if self.last_remediation_result.execution_status & ExecutionStatus.ERROR:
            if self.retry_remediation_interval:
                interval = self.retry_remediation_interval
            else:
                interval = self.remediation_interval
        else:
            interval = self.remediation_interval

        if interval is not None and self.remediation_skew:
            if not self.remediation_skew_:
                self.reset_remediation_skew()
            
            if self.remediation_skew_:
                interval += self.remediation_skew_
        
        return interval
    
    def is_remediation_time(self):
        """
        Method which returns whether it is time for us to perform
        an remediation. To qualify, we must be configured to auto remediate, 
        we must be idle, and our last remediation must be passed
        our threshold. 
        """
        
        result = False
        
        now = datetime.datetime.utcnow()
        comp_status = self.compliance_status()
        
        if not self.can_remediate or not self.auto_remediate:
            result = False
        elif self.status != ModuleStatus.IDLE:
            result = False
        elif comp_status & ComplianceStatus.NONCOMPLIANT:
            if (self.last_remediation_result.execution_status 
                                                    != ExecutionStatus.NONE):
                interval = self.current_remediation_interval()
                if interval is not None:
                    result = self.last_remediation_result.end_date + interval <= now
            else:
                result = True
            
        return result
    
    def archive_remediation_result(self, result):
        """
        Method to archive the provided remediation result
        
        :param result: The evaluation result to archive
        :type result: :py:class:`RemediationResult`
        """
        
        history = []
        
        if self.remediation_history is not None:
            history = self.remediation_history
        elif (self.remediation_history_max_records 
                    and len(history) > self.remediation_history_max_records):
            history = history[self.remediation_history_max_records * -1:]
        
        history.append(result)
        
        self.remediation_history = history
    
    def reset_remediation_skew(self):
        """
        Method that will re-roll our currently chozen random_skew,
        as seeded by remediation_skew.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        skew = datetime.timedelta(seconds=0)
        
        if self.remediation_skew:
            try:
                max_seconds = abs(self.remediation_skew.total_seconds())
                rand_num = random.randint(0,max_seconds)
                rand_skew = rand_num - (max_seconds / 2)
                skew = datetime.timedelta(seconds=rand_skew)
            except Exception as exp:
                logger.warning("Failed to set remediation skew: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly)",exc_info=1)
            
        self.remediation_skew_ = skew
    
    #MARK: Interogation methods
    def is_compliant(self):
        """
        Method to return whethor or not this module is currently compliant
        (by virtue of full compliance, gracetime, or exemption). 
        """
        
        is_compliant = False
        
        status = self.compliance_status()
        
        if status & ComplianceStatus.COMPLIANT:
            is_compliant = True
        elif status & ComplianceStatus.EXEMPT:
            is_compliant = True
        elif status & ComplianceStatus.INGRACETIME:
            is_compliant = True
        
        return is_compliant
    
    def has_compliance_issue(self):
        """
        Method indicating whether we have failed our compliance test, 
        regardless of exemptions or gracetime.
        """
        
        result = False
        
        status = self.compliance_status()
        
        if (status & (ComplianceStatus.NONCOMPLIANT
                                                | ComplianceStatus.ERROR)
                        or status == ComplianceStatus.UNKNOWN):
            result = True
        
        return result
        
    def is_exempt(self):
        """
        Method which determines whether our module is actively exempt.
        When we are exempt, the module will track compliance status, but
        will not enforce it.
        """
        
        result = False
        
        if self.exempt_until and self.exempt_until >= datetime.datetime.utcnow():
            result = True
        elif not self.exempt_until and self.exempt_flag:
            result = True
        
        return result
    
    def is_isolation_candidate(self):
        """
        Method to return whether or not we're an isolation candidate.
        """
        
        result = False
        
        now = datetime.datetime.utcnow()
        isolation_deadline = self.isolation_deadline()
        
        if isolation_deadline and isolation_deadline <= now:
            result = True
        
        return result
    
    def compliance_status(self):
        """
        Method to return our module's current compliance status. This 
        method will trigger change notifications as we change state.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        result = ComplianceStatus.UNKNOWN
        
        compliance_deadline = self.compliance_deadline()
        isolation_deadline = self.isolation_deadline()
        is_exempt = self.is_exempt()
        now = datetime.datetime.utcnow()
        
        eval_status = self.last_evaluation_result.compliance_status
        
        if eval_status & ComplianceStatus.COMPLIANT:
            result |= ComplianceStatus.COMPLIANT
        elif eval_status & ComplianceStatus.ERROR:
            result |= ComplianceStatus.NONCOMPLIANT|ComplianceStatus.ERROR
        elif eval_status == ComplianceStatus.UNKNOWN:
            result |= ComplianceStatus.NONCOMPLIANT
        elif eval_status & ComplianceStatus.NONCOMPLIANT:
            result |= ComplianceStatus.NONCOMPLIANT
            
        if result & ComplianceStatus.NONCOMPLIANT:
            if compliance_deadline and now < compliance_deadline:
                result |= ComplianceStatus.INGRACETIME
            elif self.is_isolation_candidate():
                result |= ComplianceStatus.ISOLATIONCANDIDATE
        
        if is_exempt:
            result |= ComplianceStatus.EXEMPT
        
        ## If our compliance status has changed, send a notification
        if self.last_compliance_status != result:
            logger.debug("Compliance status did change for module: '{}'\n\tNew:{} ({})\n\tOld:{} ({})".format(    
                                    self.identifier,
                                    result,
                                    ComplianceStatus.to_string(result),
                                    self.last_compliance_status,
                                    ComplianceStatus.to_string(self.last_compliance_status))
                            )
            self.compliance_status_did_change(new_status=result, 
                                        old_status=self.last_compliance_status) 
            self.last_compliance_status = result
        
        return result
    
    def compliance_deadline(self):
        """
        Method to return our compliance deadline
        
        :returns None: If there is no active deadline
        :returns :py:class:`datetime.datetime`: Deadline
        
        """
                
        deadline = None
        
        last_status = self.last_evaluation_result.compliance_status
        
        is_non_compliant = (last_status & (ComplianceStatus.NONCOMPLIANT| 
                                                        ComplianceStatus.ERROR)
                            or last_status == ComplianceStatus.UNKNOWN)
                                            
        is_exempt = self.is_exempt()
        
        if is_non_compliant:
            if self.last_evaluation_result.compliance_deadline:
                deadline = self.last_evaluation_result.compliance_deadline
            elif self.last_evaluation_result.first_failure_date:
                deadline = self.last_evaluation_result.first_failure_date + self.gracetime
            else:
                deadline = self.first_failure_date + self.gracetime
            
            if is_exempt and not self.exempt_until:
                deadline = None
            elif self.exempt_until and self.exempt_until > deadline:
                deadline = self.exempt_until
        
        return deadline
    
    def isolation_deadline(self):
        """
        Method to return our compliance deadline
        
        :returns None: If there is no active deadline
        :returns :py:class:`datetime.datetime`: Deadline
        
        """
           
        deadline = None
        
        if self.enforce_isolation:
            last_status = self.last_evaluation_result.compliance_status
            
            is_non_compliant = (last_status & ComplianceStatus.NONCOMPLIANT 
                                            == ComplianceStatus.NONCOMPLIANT
                            or last_status == ComplianceStatus.UNKNOWN)
                                                
            if is_non_compliant:
                if self.last_evaluation_result.isolation_deadline:
                    deadline = self.last_evaluation_result.isolation_deadline
                else:
                    compliance_deadline = self.compliance_deadline()
                    if compliance_deadline:
                        deadline = compliance_deadline + self.isolation_gracetime
                        
        return deadline
    
    def register_support_files(self):
        """
        Method which will is used to register support files which are 
        relevant to our module's evaluation results.
        
        .. note:
            Third party modules that rely on extrenal configuration,
            support, or manifest files that would otherwise affect results 
            of their evaluation should override this method and add config
            entries for each of their files. When overriding, you should
            always call super(..., self).support_files() 
        
        .. note:
            We will probably eventually expand our configuration bundle
            format to include support for these and provide Richter 
            integration.
            
        """
        
        ## Example code:
        ## self.support_files["custom_support_file"] = SupportFile(
        ##                            name="custom_support_file",
        ##                            filepath="/tmp/custom_support_file.json")
        
        return
    
    #MARK: Change registration
    def register_compliance_change_callback(self, callback):
        """
        Method to register a callback method which will be invoked 
        when compliance status changes occur.
        """
        
        if self.compliance_change_callbacks is None:
            self.compliance_change_callbacks = [callback]
        elif callback not in self.compliance_change_callbacks:
            self.compliance_change_callbacks.append(callback)
        
    def deregister_compliance_change_callback(self, callback):
        """
        Method to deregister a callback method which has been previously
        registered via register_compliance_change_callback()
        """
        
        callbacks = self.compliance_change_callbacks
        
        if callbacks is not None and callback in callbacks:
            callbacks.remove(callback)
    
    def compliance_status_did_change(self, new_status, old_status):
        """
        Callback method invoked whenever the status of our compliance module 
        changes.
        """
        
        logger = logging.getLogger(self.logger_name)
                
        logger.debug("Compliance status change detected for module:'{}'. New status:'{}', Old status:'{}'".format(
                        self.identifier, 
                        ComplianceStatus.to_string(new_status),
                        ComplianceStatus.to_string(old_status),
        ))
        
        if self.compliance_change_callbacks:
            for callback in self.compliance_change_callbacks:
                try:
                    callback(new_status=new_status, old_status=old_status, 
                                                                module=self)
                except Exception as exp:
                    logger.error("Failed to execute callback:'{}'. Error:{}".format(
                                        callback,
                                        exp))
                    logger.log(9,"Failure stack trace (handled cleanly):", exc_info=1)

class ComplianceModule(acme.SerializedObject):
    """Class which represents a compliance module in the system."""
    
    quarantine_to_aea_maps = { "Crypto" : "crypto",
                                "OSPatch" : "firstparty",
                                "3PPatch" : "thirdparty",
                                "Malware" : "malware",
                                "Infection" : "infection",
                                "Firewall" : "firewall",
                                "Management" : "management",
                                }

    aea_to_quarantine_maps = { "crypto" : "Crypto",
                                "firstparty" : "OSPatch",
                                "thirdparty" : "3PPatch",
                                "malware" : "Malware",
                                "infection" : "Infection",
                                "firewall" : "Firewall",
                                "management" : "Management",
                                }
    
    def __init__(self,name=None):
        
        key_map = { "name" : None,
                    "status" : None,
                    "last_evaluation" : "last_eval_datestamp",
                    "failed_on" : "failed_datestamp",
                    "gracetime" : "gracetime_ts",
                    "isolation_gracetime" : "isolation_gracetime_ts",
                }
                    
        acme.SerializedObject.__init__(self,key_map=key_map)
        
        self.name = name
        self.status = "AOK"
        self.last_evaluation = datetime.datetime.utcnow()
        self.failed_date = datetime.datetime.utcnow()
        self.gracetime = datetime.timedelta(days=4)
        self.isolation_gracetime = datetime.timedelta(days=3)
        
      
    @property
    def failed_datestamp(self):
        """
        Property which returns a datestamp formatted for
        SQL use.
        """
        
        if self.failed_date:
            return self.failed_date.strftime(acme.DATE_FORMAT)
        else:
            return None
        
    @failed_datestamp.setter
    def failed_datestamp(self,value):
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
                    the_date = datetime.datetime.strptime(value,acme.DATE_FORMAT)
                except ValueError as exp:
                    logger = logging.getLogger(self.__class__.__name__)
                    logger.warning("Could not import failed_datestamp value:{v} for model:{m}".format(
                                                v=value,
                                                m=self.__class__.__name__)) 

        self.failed_date = the_date
    
    @property
    def last_eval_datestamp(self):
        """
        Property which returns a datestamp formatted for
        SQL use.
        """
        
        if self.last_evaluation:
            return self.last_evaluation.strftime(acme.DATE_FORMAT)
        else:
            return None
        
    @last_eval_datestamp.setter
    def last_eval_datestamp(self,value):
        """
        Setter for our datestamp
        """
        the_date = None
        
        if isinstance(value,datetime.datetime):
            the_date = value
        elif value is not None:
            try:
                the_date = datetime.datetime.utcfromtimestamp(float(value))
            except (ValueError,TypeError) as exp:
                try:
                    the_date = datetime.datetime.strptime(value,acme.DATE_FORMAT)
                except ValueError as exp:
                    logger = logging.getLogger(self.__class__.__name__)
                    logger.warning("Could not import last_evaluation value:{v} for model:{m}".format(
                                        v=value,
                                        m=self.__class__.__name__)) 

        self.last_evaluation = the_date
                
    @property
    def gracetime_ts(self):
        """
        Property which returns our gracetime, in seconds.
        """
        
        if self.gracetime:
            return self.gracetime.total_seconds()
        else:
            return None
        
    @gracetime_ts.setter
    def gracetime_ts(self,value):
        """
        Setter for our gracetime
        """
        the_delta = None
        
        if isinstance(value,datetime.timedelta):
            the_delta = value
        else:
            try:
                the_delta = datetime.timedelta(seconds=float(value))
            except (TypeError,ValueError) as exp:
                logger = logging.getLogger(self.__class__.__name__)
                logger.warning("Could not import gracetime_ts value:{v} for model:{m}".format(
                                            v=value,
                                            m=self.__class__.__name__)) 

        self.gracetime = the_delta
    
    @property
    def isolation_gracetime_ts(self):
        """
        Property which returns our isolation_gracetime, in seconds.
        """
        
        if self.isolation_gracetime:
            return self.isolation_gracetime.total_seconds()
        else:
            return None
        
    @isolation_gracetime_ts.setter
    def isolation_gracetime_ts(self,value):
        """
        Setter for our isolation_gracetime
        """
        the_delta = None
        
        if isinstance(value,datetime.timedelta):
            the_delta = value
        else:
            try:
                the_delta = datetime.timedelta(seconds=float(value))
            except (TypeError,ValueError) as exp:
                logger = logging.getLogger(self.__class__.__name__)
                logger.warning("Could not import gracetime_ts value:{v} for model:{m}".format(v=value,m=self.__class__.__name__)) 

        self.isolation_gracetime = the_delta
    
    def compliance_deadline(self):
        """
        Method which returns our compliance deadline.
        
        :returns: :py:class:`datetime.datetime` or None if no deadline exists.
        """
        
        if self.failed_date:
            return self.failed_date + self.gracetime
        else:
            return None
    
    def isolation_deadline(self):
        """
        Method which returns our isolation deadline.
        
        :returns: :py:class:`datetime.datetime` or None if no deadline exists.
        """
        
        compliance_deadline = self.compliance_deadline()
        
        if compliance_deadline:
            return compliance_deadline + self.isolation_gracetime
        else:
            return None

    def get_modules(self):
        """
        Method that will get available quarantine compliance modules.
        
        :returns: list of quarantine modules
        """
        modules = []
        
        return modules

    def get_status(self,module=None):
        """
        Method that will get individual or all compliance modules status in Quarantine.

        :param module: If defined, compliance module to query compliance status.
            Else get compliance status for all active modules and return status of worst offender.
        :type module: string
        :returns: Integer bitwise mask representing compliance state.
        """

        module_list = self.get_modules()
        status = 0
        highest_status = 0

        if module == None:
            for module in module_list:
                status = self.get_module_status(module)
                if (status > highest_status):
                    highest_status = status
        elif module in module_list:
            return self.get_module_status(module)

        return highest_status

    def map_module_status(self,status):
        """
        Method that will map compliance module status in Quarantine to
        bitwise mask representing compliance state.

        :param status: Compliance module status.
        :type status: integer
        :returns: Integer bitwise mask representing compliance state.
        """

        try:
            status = Compliance.compliance_status_map[status]
        except KeyError:
            status = ComplianceStatus.UNKNOWN

        return status

class ModuleNotFoundError(Exception):
    pass


#MARK: - Module logic
Compliance = ComplianceModule

def _configure_macos():
    """
    Method to configure our compliance package for use with macOS
    """
    
    import compliance_macos
    global Compliance
    
    Compliance = compliance_macos.ComplianceMacOS


def _configure_ubuntu():
    """
    Method to configure our compliance package for use with Ubuntu
    """
    
    import compliance_ubuntu
    global Compliance
    
    Compliance = compliance_ubuntu.ComplianceUbuntu


## OS Configuration
if acme.platform == "OS X" or acme.platform == "macOS":
    _configure_macos()
elif acme.platform == "Ubuntu":
    _configure_ubuntu()
    
class ComplianceController(object):
    """
    Primary controller class which provides asyncronous scheduling and 
    triggering of compliance.
    """
    
    modules = {}                #: Var which represents loaded compliance module, keyed by identifier
    plugin_path = None         #: Directory which we load our modules from.
    
    qualifier = None     #: Qualifier object
    
    maxnum_executors = 5    #: Maximum number of execution threads/proceses
    
    execution_queue = None  #: :py:class:`multiprocessing.Queue` object to 
                            #: monitor for new execution requests
                            
    response_queue = None   #: :py:class:`multiprocessing.Queue` object used 
                            #: to proxy cmodule execution status updates.
                            
    karl_queue = None       #: :py:class:`multiprocessing.Queue` object used to 
                            #: proxy KARL events
    
    logger_queue = None     #: logger queue to be used for logging by remote processes 
                            #: (as logging module is not multi-process)
    
    use_multiprocessing_queues = False #: Boolean flag to set whether our ComplianceModuleExecutors
                                    #: configure queue mechanisms (logger, 
                                    #: karl, etc) to facilitate multi-process
                                    #: executions.
                                       
    execution_threads = []        #: Array of executor thread objects
    
    module_queue_data = {}         #: Dictionary of queued requests, keyed by cmodule identifier+trigger
    
    queue_lock = None       #: Locking mechanism to control modifications to cmodule_queue
    
    requeue_threshold = None    #: Timedelta object representing how frequently 
                                #: we will re-queue already queued modules  
                
    state_dirpath = None       #: Directory containing state files
    manifest_dirpath = None    #: Directory
    
    logger_name = "ComplianceController"  #: Name of our logger in debug mode
    
    should_run = None           #: Semaphore flag used to stop 
    process_thread = None       #: Our processing thread.
    
    _domain = None              #: Backing var for our domain property

    @property
    def domain(self):
        """
        Property which represents our module domain 
        
        """
        
        domain = None
        
        if self._domain:
            domain = self._domain
        elif self.plugin_path:
            domain = "ComplianceController"
        
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
        
        self.qualifier = ComplianceModuleQualifier()
        
        self.module_queue_data = {}
        self.queue_lock = threading.RLock()
        self.requeue_threshold = datetime.timedelta(minutes=10)
        
        self.domain = domain
        self.load_lock = threading.RLock()
        self.last_compliance_status = None
                
    def load(self):
        """
        Method to load our controller modules.
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.log(9,"ComplianceController loading. Directories - State:{} Manifest:{} Plugin:{}".format(self.state_dirpath,
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
        
    def reload(self):
        """
        Method to reload settings.
        """
        logger = logging .getLogger(self.logger_name)
        
        logger.debug("Reloading ComplianceController...")
        
        for module in self.modules.values():
            module.load_settings()
        

    def load_compliance_module(self, module):
        """
        Method to load the provided cmodule.
        
        :param module: The compliance module to load
        :type module: :py:class:`BaseCompliance`
        
        """
        result = False
        if self.state_dirpath:
            if module.needs_state_dir:
                module.state_path = os.path.join(self.state_dirpath,
                                                            module.identifier)
                if not os.path.exists(module.state_path):
                    os.mkdir(module.state_path,0755)
            else:
                module.state_path = os.path.join(self.state_dirpath,"{}.json".format(module.identifier))
            
        if self.manifest_dirpath:
            if module.needs_manifest_dir:
                module.manifest_path = os.path.join(self.manifest_dirpath,
                                                            module.identifier)
                if not os.path.exists(module.manifest_path):
                    os.mkdir(module.manifest_path,0755)
            else:
                module.manifest_path = os.path.join(self.manifest_dirpath,
                                        "{}.json".format(module.identifier))
        module.load()
        
        existing_module = self.modules.pop(module.identifier,None)
                
        if existing_module:            
            key_map = module.key_map_for_keys(module.state_keys)
            key_map["status"] = None
            
            module.load_dict(existing_module.to_dict(key_map=key_map),
                                                            key_map=key_map)
        module.register_compliance_change_callback(self.publish_cmodule_status)
        
        self.modules[module.identifier] = module
        
        result = True
        
        return result
    
    def list_modules(self):
        return self.modules.values()
    
    def get_cmodule(self, identifier):
        '''
        Get cmodule by identifier
        :param identifier:
        '''
        return self.modules.get(identifier)
    
    def is_device_status_change(self):
        logger = logging.getLogger("is_device_status_change")
        new_status = self.get_device_status()
        last_status = self.last_compliance_status
        self.last_compliance_status = new_status
        if last_status != new_status:
            karl_payload = {}
            karl_payload["new_status"] = new_status
            karl_payload["old_status"] = last_status
            karl_payload["change_date"] = datetime.datetime.utcnow().strftime("%s")
            
            try:
                event = pykarl.event.Event(type="ComplianceDeviceStatusEvent",
                                            subject_area="Compliance",
                                            payload=karl_payload)
                logger.log(5, "Committed karl event {}".format(event.to_json()))
                pykarl.event.dispatcher.dispatch(event)
            except NameError:
                logger.error("Cannot dispatch KARL event: KARL module not available!")
            except Exception as exp:
                logger.error("Failed to dispatch KARL event:{}... {}".format(
                                                            event.type,
                                                            exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)         
    
    def get_device_status(self):
        status = ComplianceStatus.UNKNOWN
        for cmodule in self.list_modules():
            cs = cmodule.compliance_status()
            if status < cs:
                status = cs
        return status 
        
    def compliance_deadline(self):
        """
        Method to return the compliance deadline for our module.
        
        :returns: :py:class:`datetime.datetime` object
        :returns: None if no deadline is currently applied
        
        """
        
        deadline = None
        for cmodule in self.list_modules():
            module_deadline = cmodule.compliance_deadline()
            if module_deadline is None:
                continue
            if deadline is None:
                deadline = module_deadline
            elif deadline > module_deadline:
                deadline = module_deadline
            
        return deadline 
    
    def isolation_deadline(self):
        """
        Method to return the isolation deadline for our module.
        
        :returns: :py:class:`datetime.datetime` object
        :returns: None if no deadline is currently applied
        
        """
        
        deadline = None
        for cmodule in self.list_modules():
            module_deadline = cmodule.isolation_deadline()
            if module_deadline is None:
                continue
            if deadline is None:
                deadline = module_deadline
            elif deadline > module_deadline:
                deadline = module_deadline
            
        return deadline 
        
    
    def publish_cmodule_status(self, new_status=None, old_status=None, module=None):
        logger = logging.getLogger("publish_cmodule_status_to_karl")
        karl_payload = module.to_dict()
        karl_payload["new_status"] = new_status
        karl_payload["old_status"] = old_status
        
        try:
            event = pykarl.event.Event(type="ComplianceModuleStatusEvent",
                                        subject_area="Compliance",
                                        payload=karl_payload)
            logger.log(5, "Committed karl event {}".format(event.to_json()))
            pykarl.event.dispatcher.dispatch(event)
        except NameError:
            logger.error("Cannot dispatch KARL event: KARL module not available!")
        except Exception as exp:
            logger.error("Failed to dispatch KARL event:{}... {}".format(
                                                        event.type,
                                                        exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)        
            
    def unload_compliance_module_by_identifier(self, identifier, send_karl_events=True):
        logger = logging.getLogger(self.logger_name)
        pop_cmodule = None
        result = False
        if self.module_is_loaded(identifier):
            pop_cmodule = self.modules.pop(identifier, None)
            result = True
        else:
            logger.warn("Cmodule {} hasn't been loaded".format(identifier))
        if result:
            logger.info("Cmodule {} has been unloaded".format(pop_cmodule.identifier))   
            if send_karl_events:
                karl_payload = {}
                karl_payload["unload_uuid"] = "{}".format(uuid.uuid4())
                karl_payload["unload_date"] = datetime.datetime.utcnow().strftime("%s")
                karl_payload["domain"] = self.domain
                karl_payload["identifier"] = identifier

                try:
                    event = pykarl.event.Event(type="ComplianceModuleUnLoadEvent",
                                                subject_area="Compliance",
                                                payload=karl_payload)
                    logger.log(5, "Committed karl event {}".format(event.to_json()))
                    pykarl.event.dispatcher.dispatch(event)
                except NameError:
                    logger.error("Cannot dispatch KARL event: KARL module not available!")
                except Exception as exp:
                    logger.error("Failed to dispatch KARL event:{}... {}".format(
                                                                event.type,
                                                                exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        return result
    
    def load_modules(self, path=None, send_karl_events=True):
        '''
        Method will load all compliance modules in compliance_modules folder
        '''
        logger = logging.getLogger(self.logger_name)
        
        if not path:
            path = self.plugin_path
        
        logger.debug("Loading compliance modules from path: '{}'".format(path))
        try:
            pc = acme.plugin.PluginController(path=path,
                                                classes=[BaseModule])
            pc.load()
            for plugin in pc.plugins:
                try:
                    self.load_modules_from_plugin(plugin,
                                            send_karl_events=send_karl_events)
                except Exception as exp:
                    logger.error("Failed to load compliance module {}".format(exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        except Exception as exp:
            logger.error("Failed to load all compliance modules {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                 
    #WANR: we can't load by module's identifier because we have to load all modules to find right identifier
    def load_compliance_module_by_name(self, name, path=None, send_karl_events=True):
        """
        Method to load module by name.
        :param string name: name of module
        :param string path: The path to load modules from. If ommitted we will
                        reference instance variable
                        
        :param bool send_karl_events: Whether we will send KARL events.
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if path is None:
            path = self.plugin_path
        path = os.path.join(path, name)
        if not path:
            logger.warning("Cannot load cmodule {}: no path defined.".format(name))
            return
        elif os.path.isfile("{}.py".format(path)):
            path = "{}.py".format(path)
            logger.debug("Found module {} at {}".format(name, path))
        elif os.path.isdir(path) and os.path.isfile(os.path.join(path, "__init__.py")):
            logger.debug("Found module {} at dir {}".format(name, path))            
        else:
            logger.warning("Cannot load cmodule {}: no file exist at:'{}'.".format(name, path))
            return
        
        loaded_modules = {}
        try:
            plugin = acme.plugin.Plugin(path=path, target_classes=[BaseModule])
            plugin.load()
            loaded_modules = self.load_modules_from_plugin(plugin,
                                            send_karl_events=send_karl_events)
        except Exception as exp:
            logger.error("Failed to load cmodule ComplianceController! Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        
        logger.info("Loaded {} modules from path:'{}'".format(len(loaded_modules), path))
        logger.debug("Loaded modules: '{}'".format("', '".join(loaded_modules.keys())))
        return loaded_modules
    
    def load_modules_from_plugin(self, plugin, send_karl_events=True):
        '''
        load a loaded plugin type module, return loaded cmodule
        :param plugin:
        :param send_karl_events:
        '''
        logger = logging.getLogger(self.logger_name)
        num_load_failures = 0
        loaded_modules = {}
        start_time = datetime.datetime.utcnow()
        
        for cmodule in plugin.get_targets().values():
            try:
                self.load_compliance_module(cmodule)
                logger.info("Loaded compliance module:'{}' (module version:'{}' plugin version:'{}')".format(
                                                    cmodule.identifier,
                                                    cmodule.version,
                                                    plugin.version()))
                logger.log(5,"Compliance module info:{}".format(cmodule.to_json(
                                                output_null=False)))
                loaded_modules[cmodule.identifier] = cmodule
                
            except Exception as exp:
                num_load_failures += 1
                logger.error("Failed to load compliance module:'{}'. Error:{}".format(cmodule,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                
        load_time = datetime.datetime.utcnow() - start_time
        #capturing plugin identifier, version, file hash, and load date.
        if send_karl_events:
            karl_payload = plugin.to_dict()
            
            karl_payload["load_uuid"] = "{}".format(uuid.uuid4())
            karl_payload["load_time"] = int(load_time.total_seconds() * 1000)
            karl_payload["load_date"] = datetime.datetime.utcnow().strftime("%s")
            karl_payload["domain"] = self.domain
            karl_payload["modules"] = ", ".join(loaded_modules.keys())
            karl_payload["num_modules"] = len(loaded_modules)
            
            if plugin.load_failures:
                karl_payload["error"] = True
            else:
                karl_payload["error"] = False
            
            karl_payload["num_failures"] = (len(plugin.load_failures) 
                                                + num_load_failures)
            
            try:
                event = pykarl.event.Event(type="ComplianceModuleLoadEvent",
                                            subject_area="Compliance",
                                            payload=karl_payload)
                logger.log(5, "Committed karl event {}".format(event.to_json()))
                pykarl.event.dispatcher.dispatch(event)
            except NameError:
                logger.error("Cannot dispatch KARL event: KARL module not available!")
            except Exception as exp:
                logger.error("Failed to dispatch KARL event:{}... {}".format(
                                                            event.type,
                                                            exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        return loaded_modules
                            
    def module_is_loaded(self, identifier):
        '''
        find whether specific compliance module has been loaded
        :param identifier:
        '''
        return identifier in self.modules
    
    def status(self):
        """
        Outputs the status of our compliance controller.
        """
        
        status = ModuleStatus.IDLE
        
        for module in self.modules.values():
            status |= module.status
        
        return status
    
    def cmodule_executor_count(self):
        """
        Returns the number of spun up executors.
        
        """
        
        return len(self.execution_threads)
        
    def module_executor_idealcount(self):
        """
        Method which returns the number of executors that we should
        currently have executing modules concurrently.
        
        Todo: Revisit this
        """
        
        ideal_count = 0
        num_queued_cmodules = len(self.module_queue_data)
        
        if not self.should_run:
            ideal_count = 0
        else:
            if (num_queued_cmodules > 0 
                        and num_queued_cmodules < self.maxnum_executors):
                ideal_count = int(math.ceil(float(num_queued_cmodules) / 3.0))
            elif num_queued_cmodules:
                ideal_count = num_queued_cmodules
            
            if ideal_count < self.maxnum_executors:
                ## If we have not breached our maximum number of executors:
                ## scale our number of executors up until our thresholds
                ## have recovered.
                num_overqueued = self.num_overqueued_modules()
                if num_overqueued > 0:
                    current_executor_count = self.cmodule_executor_count()
                    if current_executor_count == ideal_count:
                        ideal_count = current_executor_count + num_overqueued
                    elif current_executor_count > ideal_count:
                        ideal_count = current_executor_count
         
        ## We should never have more executors than modules
        if ideal_count > num_queued_cmodules:
            ideal_count = num_queued_cmodules
        
        ## Make sure we never breach our maxnum settings.
        if ideal_count > self.maxnum_executors:
            ideal_count = self.maxnum_executors
        
        return ideal_count
    
    def num_overqueued_modules(self):
        """
        Method which returns the number of queued modules which are currently
        past our allowed queue SLA.
        """
        
        now = datetime.datetime.utcnow()
        
        num_overqueued_modules = 0
        
        with self.queue_lock:
            for queue_id, request in self.module_queue_data.iteritems():
                if request.module and request.module.status == ModuleStatus.QUEUED:
                    if request.date + EXECUTOR_EXECUTION_SLA <= now:
                        num_overqueued_modules += 1
                    
        return num_overqueued_modules
    
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
            
            et = ComplianceModuleExecutor(name=name,
                                execution_queue=execution_queue,
                                response_queue=response_queue,
                                karl_queue=karl_queue,
                                logger_queue=logger_queue)
            
            logger.log(5,"ComplianceModuleExecutor ({}) starting...".format(name))
            et.start()
            logger.log(5,"ComplianceModuleExecutor ({}) started...".format(name))
            self.execution_threads.append(et)
            logger.log(5,"ComplianceModuleExecutor ({}) added to execution_threads (len:{})...".format(name,len(self.execution_threads)))

        except Exception as exp:
            logger.error("Failed starting up new ComplianceModuleExecutor: {}".format(exp))
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
            if status & CMODULE_EXECUTOR_STATUS_RUNNING:
                if not status & CMODULE_EXECUTOR_STATUS_STOPPING:
                    candidate_threads.append(executor)
                if not status & CMODULE_EXECUTOR_STATUS_EXECUTING:
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
                logger.log(9,"Stopping cmodule executor:{}".format(best_thread.name))
            else:
                logger.log(9,"Stopping cmodule executor thread:{}".format(best_thread))
            
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
                logger.info("Waiting for cmodule executors to quit...")
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
                        logger.log(9,"ComplianceModuleExecutor:{} is no longer alive, removing from pool...".format(executor.name))
                        self.execution_threads.remove(executor)
                    except Exception as exp:
                        logger.warning("Failed to remove executor:{} Error:{}".format(
                                                        executor,
                                                        exp))
                        logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
                        
            ideal_count = self.module_executor_idealcount()
            
            count = self.cmodule_executor_count()
            
            logger.log(2,"Managing execution threads... Found {} listed, {} "
                        "running module executors, ideal count:{}...".format(
                                                len(self.execution_threads),
                                                count,
                                                ideal_count))
            
            if count < ideal_count:
                logger.log(5,"Found {} running module executors ({} queued "
                            "modules), ideal count:{}, spinning up new "
                            "processors...".format(count,
                                                len(self.module_queue_data),
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
                    logger.log(5,"Found {} running cmodule executors, ideal count:{}, expiring processors...".format(
                                                                count,
                                                                ideal_count))
                for i in xrange(count - ideal_count):
                    self.stop_executor_thread()
        else:
            logger.debug(5,"System is set to shutdown, flagging all cmodule executors!")
            for executor in self.execution_threads:
                executor.should_run = False
                
    def execute_trigger(self,trigger,data=None):
        """
        Method to execute the provided trigger.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        trigger_names = ExecutionTrigger.to_string(trigger)
                
        logger.log(5,"Executing trigger(s):'{}' data:{}".format(trigger_names,data))
        
        qualifier = self.qualifier
        
        current_state = self.qualifier.current_state_flags()
        
        for cmodule in self.modules.values():
            results = qualifier.run_qualifications(
                                                cmodule=cmodule,
                                                trigger=trigger,
                                                data=data,
                                                current_state=current_state)
            
            if results & CMODULE_QUALIFICATION_TRIGGERNOTQUALIFIED:
                continue
                
            if results & CMODULE_QUALIFICATION_EXECUTIONLIMITSREACHED:
                logger.debug("Compliance Module:'{}' has hit execution limits, will not trigger...".format(
                                                            cmodule.identifier))
                continue
                
            if results & CMODULE_QUALIFICATION_SITENOTQUALIFIED:
                logger.debug("Compliance Module:'{}' failed network site restrictions, will not trigger...".format(
                                                            cmodule.identifier))
                continue
                
            if results & CMODULE_QUALIFICATION_PRERQUISITESNOTMET:
                current_state_names = agent_util.string_for_enum_pattern(current_state,
                                                            "CMODULE_STATE_")
                req_state_names = agent_util.string_for_enum_pattern(cmodule.prerequisites,
                                                            "CMODULE_STATE_")
                logger.debug("Compliance Module:'{}' failed to meet defined prerequisites, will not trigger... (Current:'{}' Required:'{}')".format(
                                        cmodule.identifier,
                                        current_state_names,
                                        req_state_names))
                continue
            
            if results & CMODULE_QUALIFICATION_PROBABILITYFAILED:
                logger.debug("Compliance Module:'{}' failed run_probability tests not met, will not trigger... (run_probability:'{}')".format(
                                        cmodule.identifier,
                                        cmodule.run_probability))
                continue
                
            request = ComplianceModuleExecutionRequest(module=cmodule, trigger=trigger,
             
                                                                data=data)
            action = None
            if data and "action" in data and data["action"]:
                action = data["action"]
            else:
                action = "evaluation"
            
            if action:
                request = ComplianceModuleExecutionRequest(module=cmodule, 
                                                        trigger=trigger,
                                                        action=action,
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
        
        cmodule = request.module
        
        if not cmodule:
            logger.error("ComplianceModule execution request does not referenece an cmodule, cannot queue!")
            return
        
        if not self.execution_queue:
            logger.warning("Request:{} has attempted to queue but no execution queue exists...".format(request.queue_id()))
            return False
        
        try:
            with self.queue_lock:
                requeue = False
                queue_id = request.queue_id()
                if queue_id in self.module_queue_data:
                    e_request = self.module_queue_data[queue_id]
                    requeue_time = e_request.date + self.requeue_threshold
                    if datetime.datetime.utcnow() >= requeue_time:
                        logger.warning("Request:{} is already queued but has hit requeue threshold. Re-queueing agent...".format(
                                                                    queue_id))
                        requeue = True
                
                if requeue or not queue_id in self.module_queue_data:
                    self.module_queue_data[queue_id] = request
                    self.execution_queue.put(request.copy())                        
                    did_queue = True
                    cmodule.status = ModuleStatus.QUEUED
                    logger.log(5,"Added compliance module:'{}' to execution queue for {}...".format(cmodule.identifier, request.action))
                else:
                    #should 
                    logger.debug("Compliance module:'{}' is already queued, will not execute...".format(
                                                    cmodule.identifier))
        except Exception as exp:
            cmodule.status = ModuleStatus.IDLE
            logger.error("Failed to queue compliance module:'{}'".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",
                                                    exc_info=1)
            did_queue = False
        
        return did_queue
    
    def start(self):
        """
        Starts our run loop.
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.info("Starting {}...".format(self.logger_name))
        
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
        
        ## Make directories
        for dir in self.state_dirpath, self.manifest_dirpath, self.plugin_path:
            try:
                if not os.path.exists(dir):
                    os.makedirs(dir, mode=0755)
            except Exception as exp:
                logger.error("Failed to create suppport directory:'{}'. Error: {}".format(
                                                            dir,
                                                            exp.message))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                            exc_info=1)

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
        
        logger.info("{} successfully stopped...".format(self.logger_name))
    
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
                with self.load_lock:
                    self.trigger_scheduled_modules()
            except Exception as exp:
                logger.error("Failed to process scheduled modules:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
            try:
                self.process_logging_queue()
            except Exception as exp:
                logger.error("An error occurred while processing logger entries: {}".format(
                                                                        exp))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
            
            try:
                with self.load_lock:
                    self.process_execution_responses()
            except Exception as exp:
                logger.error("An error occurred while processing execution responses: {}".format(
                                                                        exp))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
            try:
                with self.load_lock:
                    self.is_device_status_change()
            except Exception as exp:
                logger.error("An error occurred while checking device compliance status: {}".format(
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
    
    def trigger_scheduled_modules(self,date=None):
        """
        Method to trigger scheduled modules
        
        :param date: The date to use for evaluation
        :type data: :py:class:`datetime.datetime`
        """
        
        logger = logging.getLogger(self.logger_name)
               
        logger.log(2,"Triggering scheduled modules...")
                  
        if date is None:
            date = datetime.datetime.utcnow()
              
        qualifier = self.qualifier
        
        ## Check to see if modules should be triggered
        for cmodule in self.modules.values():
            try:
                if cmodule.status != ModuleStatus.IDLE:
                    continue
                
                if not qualifier.cmodule_qualifies_for_trigger(cmodule=cmodule,
                                        trigger=ExecutionTrigger.SCHEDULED):
                    continue
                if not (qualifier.run_qualifications(cmodule=cmodule,date=date) 
                                        == CMODULE_QUALIFICATION_QUALIFIED):
                    continue
                
                action = None
                if cmodule.is_evaluation_time(): 
                    action = "evaluation"
                elif cmodule.is_remediation_time(): 
                    action = "remediation"
                
                if action:
                    trigger = None
                    #trigger = ExecutionTrigger.SCHEDULED
                    request = ComplianceModuleExecutionRequest(module=cmodule, 
                                                            trigger=trigger,
                                                            action=action)
                    self.try_queue_request(request)
            
            except Exception as exp:
                logger.error("Failed to qualify compliance module:'{}'. Error:{}".format(
                                                    cmodule.identifier,exp))
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
            logger.error("No response queue is configured, cannot report status")
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
                with self.queue_lock:
                    r_cmodule = response.module
                    my_cmodule = None
                    
                    if qid in self.module_queue_data:                                
                        my_cmodule = self.module_queue_data[qid].module
                    else:
                        logger.warning("Recieved execution response from "
                                            "unqueued request:{}".format(qid))
                        try:
                            my_cmodule = self.modules[r_cmodule.identifier]
                        except KeyError:
                            logger.error("Recieved execution response from "
                                    "unknown compliance module:{} known "
                                    "modules:{}".format(
                                        r_cmodule.identifier,
                                        ", ".join(
                                            self.module_queue_data.keys())))
                    
                    if my_cmodule:
                        try:
                            if my_cmodule.state_keys:
                                key_map = my_cmodule.key_map_for_keys(
                                                        my_cmodule.state_keys)
                            else:
                                key_map = my_cmodule.key_map
                            
                            key_map["status"] = None
                            my_cmodule.load_dict(key_map=key_map,
                                                data=r_cmodule.to_dict(
                                                            key_map=key_map))
                            
                            ## If the module is idle, remove it from our queue
                            if my_cmodule.status == ModuleStatus.IDLE:
                                logger.log(5,"Request:'{}' has finished "
                                        "executing, removing from execution "
                                        "queue...".format(qid))
                                self.module_queue_data.pop(qid, None)
                            else:
                                logger.log(5,"Request:'{}' reported status "
                                        "change. Status:{}".format(
                                                    qid, my_cmodule.status))
                        except Exception as exp:
                            logger.error("An error occurred processing "
                                    "execution response: {}".format(exp))
                            logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
                    else:
                        logger.error("Recieved execution response (status:'{}') "
                            "from unknown Compliance Module: '{}' (module was "
                            "unloaded?), ignoring response.".format(
                                                    response.module.status,
                                                    r_cmodule.identifier))
            self.response_queue.task_done()
            
            ## Log a message that we're done processing.
            if response_num >= max_responses:
                logger.warning("Processed maximum execution responses this "
                            "pass ({}), deferring...".format(response_num))

#MARK: -
class ComplianceModuleQualifier(object):
    """
    Class to qualify agents for execution.
    """
    
    site_info = None
    
    def run_qualifications(self,cmodule,trigger=None,date=None,data=None,
                                                    current_state=None):
        """
        Method to execute all qualifications tests against our cmodule and return
        the results.
        
        :param cmodule: The cmodule to test
        :type cmodule: :py:class:`BaseComplianceModule`
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
                of CMODULE_QUALIFICATION_QUALIFIED indicates all tests have 
                passed
        
        """
        
        results = CMODULE_QUALIFICATION_QUALIFIED
        
        if current_state is None:
            current_state = self.current_state_flags()
        
        if not date:
            date = datetime.datetime.utcnow()
        
        if trigger is not None:
            if not self.cmodule_qualifies_for_trigger(cmodule=cmodule,trigger=trigger):
                results |= CMODULE_QUALIFICATION_TRIGGERNOTQUALIFIED
        
        ## Check for state reqs
        if not self.cmodule_qualifies_for_state(cmodule=cmodule,state=current_state):
            results |= CMODULE_QUALIFICATION_PRERQUISITESNOTMET
        
        return results
        
    def current_state_flags(self):
        """
        Method to return our current status as a bitmask.
        """
        
        state = ExecutionPrerequisites.NONE
        
        session = network.state.active_network_session
        
        if not session:
            session = network.NetworkSession()
            session.load()
        
        network_state = session.state
        
        if network_state & network.NETWORK_STATE_ONLINE:
            state |= ExecutionPrerequisites.ONLINE
        elif network_state & network.NETWORK_STATE_OFFLINE:
            state |= ExecutionPrerequisites.OFFLINE
            
        if network_state & network.NETWORK_STATE_ONDOMAIN:
            state |= ExecutionPrerequisites.ONDOMAIN
        elif network_state & network.NETWORK_STATE_OFFDOMAIN:
            state |= ExecutionPrerequisites.OFFDOMAIN
            
        if network_state & network.NETWORK_STATE_ONVPN:
            state |= ExecutionPrerequisites.ONVPN
        elif network_state & network.NETWORK_STATE_OFFVPN:
            state |= ExecutionPrerequisites.OFFVPN
            
        return state
    

        
    def cmodule_qualifies_for_state(self,cmodule,state):
        """
        Method which determines whether the provided cmodule qualifies against
        the provided state mask.
        
        :param cmodule: The ComplianceModule to evaluate
        :type cmodule: :py:class:`BaseComplianceModule`
        :param int state: The state mask to use.
        """
        results = True
        
        if (state & cmodule.prerequisites) != cmodule.prerequisites:
            results = False
        
        return results  
    
    def cmodule_qualifies_for_trigger(self,cmodule,trigger):
        """
        Method which returns whether or not our cmodule is registered
        for the provided trigger.
        """
        
        result = False
        if cmodule.triggers and (cmodule.triggers & trigger) == trigger:
            result = True
        
        return result
    
#MARK: -
class ComplianceModuleExecutor(object):
    """
    Class which monitors our cmodule event queue and provides execution 
    capabilities.
    """
    
    name = None             #: Identifier for our executor.
    
    _logger_name = None     #: Backing var to store an explicit logger name.
    
    
    execution_queue = None  #: :py:class:`multiprocessing.Queue` object to 
                            #: monitor for new execution requests
                            
    response_queue = None   #: :py:class:`multiprocessing.Queue` object used 
                            #: to proxy cmodule execution status updates.
                            
    karl_queue = None       #: :py:class:`multiprocessing.Queue` object used to 
                            #: proxy KARL events
                            
    logger_queue = None     #: logger queue to be used for logging by modules 
                            #: (as logging module is not multi-process)
                            
    should_run_mp = None    #: :py:class:`multiprocessing.Value` object 
                            #: which denotes whether we should continue to run
                                
    qualifier = None        #: :py:class:`ComplianceModuleQualifier` instance used to qualify execution.
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
            return "ComplianceModuleExecutor:{}".format(self.name)
        else:
            return "ComplianceModuleExecutor"
    
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
        status = CMODULE_EXECUTOR_STATUS_NONE
        
        if self.is_alive():
            status |= CMODULE_EXECUTOR_STATUS_RUNNING
            if not self.should_run:
                status |= CMODULE_EXECUTOR_STATUS_RUNNING
                
        if self.is_executing:
            status |= CMODULE_EXECUTOR_STATUS_EXECUTING
        
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
        
        super(ComplianceModuleExecutor,self).__init__(*args,**kwargs)
        
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
        #self.process.daemon = True  #: Note: this setting may have cmodule implications, need to dig into this.
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
            logger.error("Failed to configure KARL:{}!".format(exp))
    
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
            rl.debug("An error occurred looking up loggers:")
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
            
            execution_status = ExecutionStatus.NONE
            try:
                logger.log(2,"Checking for queued compliance execution requests...")
                request = self.execution_queue.get(
                        timeout=EXECUTOR_QUEUE_FETCH_TIMEOUT.total_seconds())
                cmodule = request.module
                
                logger.log(5,"Recieved execution request for compliance module:'{}'".format(
                                                            cmodule.identifier))
            except Queue.Empty:
                logger.log(2,"Execution queue is empty, waiting...")
                time.sleep(EXECUTOR_LOOP_WAIT_TIME.total_seconds())
                continue
            
            self.last_activity = datetime.datetime.utcnow()
            try:
                self.is_executing = True
                
                trigger_name = ExecutionTrigger.to_string(request.trigger)
                
                logger.log(15,"Compliance Module:'{}' is executing (trigger:{})...".format(
                                                        cmodule.identifier,
                                                        trigger_name))
                if request.action == "evaluation":
                    cmodule.status = ModuleStatus.EVALUATING
                elif request.action == "remediation":
                    cmodule.status = ModuleStatus.REMEDIATING
                else:
                    logger.error("Unknown execution request action:'{}'".format(
                                                            request.action))
                    ## Set to Queued for now, we will release later
                    cmodule.status = ModuleStatus.QUEUED
                    execution_status = ExecutionStatus.FATAL
                
                if self.response_queue:
                    response = ComplianceModuleExecutionResponse(
                                        request_uuid=request.uuid,
                                        request_queue_id=request.queue_id(),
                                        status=execution_status)
                    try:
                        response.module = cmodule.deepcopy()
                    except TypeError:
                        logger.warning("Warning: module:'{}' (class:{}) failed "
                            "copy(). Did we recently reload? Reporting back "
                            "execution results with generic BaseModule "
                            "object.".format(cmodule.identifier,
                                                cmodule.__class__.__name__))
                        m = BaseModule(key_map=cmodule.key_map, 
                                                dict_data=cmodule.to_dict())
                        m.status = cmodule.status
                        response.module = m
                    
                    try:
                        logger.log(2,"Submitting response to queue...")
                        self.response_queue.put(response)
                    except Exception as exp:
                        logger.error("Failed to submit ComplianceModule "
                                "response (status:executing): {}".format(exp))
                        logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
                
                ## If our cmodule has an execution lock, utilize it.
                if cmodule.execution_lock:
                    with cmodule.execution_lock:
                        if request.action == "evaluation":
                            cmodule.evaluate(trigger=request.trigger,
                                                            data=request.data)
                            execution_status = ExecutionStatus.SUCCESS
                        elif request.action == "remediation":
                            cmodule.remediate(trigger=request.trigger,
                                                            data=request.data)
                            execution_status = ExecutionStatus.SUCCESS
                else:
                    if request.action == "evaluation":
                        cmodule.evaluate(trigger=request.trigger,
                                                            data=request.data)
                        execution_status = ExecutionStatus.SUCCESS
                    elif request.action == "remediation":
                        cmodule.remediate(trigger=request.trigger,
                                                            data=request.data)
                        execution_status = ExecutionStatus.SUCCESS            
            except Exception as exp:
                execution_status = ExecutionStatus.ERROR
                logger.error("Compliance Module:'{}' failed execution "
                            "with error:{}".format(cmodule.identifier,
                                                                exp.message))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
            
            try:
                cmodule.status = ModuleStatus.IDLE
                cmodule.last_execution = datetime.datetime.utcnow()
                #skew is be reset in current_evaluation_interval()
                #cmodule.reset_skew()
                cmodule.save()
                
            except Exception as exp:
                logger.error("Compliance Module:'{}' failed post-execution "
                                                "tasks with error:{}".format(
                                                    cmodule.identifier,
                                                    exp.message))
                logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
            
            logger.log(15,"Compliance Module:'{}' finished executing...".format(
                                                        cmodule.identifier))
            if self.response_queue:
                response = ComplianceModuleExecutionResponse(
                                        request_uuid=request.uuid,
                                        request_queue_id=request.queue_id(),
                                        status=execution_status)
                try:
                    response.module = cmodule.deepcopy()
                except TypeError:
                    logger.warning("Warning: module:'{}' (class:{}) failed "
                            "copy(). Did we recently reload? Reporting back "
                            "execution results with generic BaseModule "
                            "object.".format(cmodule.identifier,
                                                cmodule.__class__.__name__))
                    m = BaseModule(key_map=cmodule.key_map, 
                                                dict_data=cmodule.to_dict())
                    m.status = cmodule.status
                    response.module = m
                
                try:
                    self.response_queue.put(response)
                    logger.log(5,"Execution result submitted to queue (status:{})...".format(
                                                    response.module.status))

                except Exception as exp:
                    logger.error("Failed to submit ComplianceModule response (status:Idle): {}".format(exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",
                                                                exc_info=1)
            else:
                logger.warning("No response queue is configured! Cannot report "
                                        "executor results to main process!")
            
            self.execution_queue.task_done()
            
            self.is_executing = False
            self.last_activity = datetime.datetime.utcnow()
            
        logger.log(9, "{} finished running...".format(self.logger_name))

#MARK: -
class ComplianceModuleExecutionRequest(object):
    """
    Class which represents an execution request.
    """
    
    uuid = None         #: Our request UUID
    module = None       #: The compliance module to execute
    trigger = None      #: The trigger effecting execution
    action = None       #: Our request action.
    data = None         #: Additional context
    date = None         #: The date of the request
    
    def __init__(self, module, trigger=None, action=None, data=None, date=None):
        """
        :param module: The compliance module to execute
        :type module: :py:class:`acme.cmodule.BaseComplianceModule` descendent
        :param int trigger: The execution trigger, using a constant from 
                module.ExecutionTrigger
        :param string action: The Execution action (i.e. "evaluate", "remediate")
        :param data: Key=>Value data relevant to the execution context. All
                    data represented must be Pickleable
        :type data: Dictionary
        :param date: The date of the request
        :type date: :py:class:`datetime.datetime`
        
        """
        
        self.uuid = uuid.uuid4()
        self.module = module
        self.trigger = trigger
        self.action = action
        self.data = data
        if date is not None:
            self.date = date
        else:
            self.date = datetime.datetime.utcnow()
        
    def queue_id(self):
        """
        Method which returns a queue identifier for this request, which is 
        a concatonation of the module identifier and trigger (if available)
        """
        
        trigger_id = ExecutionTrigger.to_string(self.trigger)
        
        if trigger_id:
            qid = "{}.{}".format(self.module.identifier, trigger_id)
        else:
            qid = "{}".format(self.module.identifier)
        
        return qid
    
    def copy(self):
        """
        Method which returns a new ComplianceModuleExecutionRequest instance with
        isolated, but equivalent, data
        """
        
        new_instance = ComplianceModuleExecutionRequest(
                                            module=self.module.deepcopy(),
                                            trigger=self.trigger,
                                            action=self.action,
                                            date=self.date)
        new_instance.uuid = self.uuid
        
        if self.data is not None:
            new_instance.data = copy.deepcopy(self.data)
        
        return new_instance

#MARK: -
class ComplianceModuleExecutionResponse(object):
    """
    Class which represents an execution result.
    """
    
    request_uuid = None         #: Our originating :py:class:`ComplianceModuleExecutionRequest` UUID
    request_queue_id = None     #: Our originating requests queue_id
    status = None               #: The execution status of our reuest
    module = None               #: The representing compliance_module
    date = None                 #: The date of the response
    
    def __init__(self, module=None, status=None, request_uuid=None, request_queue_id=None,
                                                                date=None):
        """
        :param compliance_module: The compliance_module to execute
        :type compliance_module: :py:class:`acme.compliance_module.BaseComplianceModule` descendent
        :param status: Our current request execution status. 
        :type status: (int) :py:class:`ExecutionStatus` value
        :param str request_uuid: The UUID of the originating request
        :param str request_queue_id: The request_queue_id of the originating request
        :param date: The date of the request
        :type date: :py:class:`datetime.datetime`
        """

        self.module = module        
        self.request_uuid = request_uuid
        self.request_queue_id = request_queue_id
        
        if status is None:
            self.status = ExecutionStatus.NONE
        else:
            self.status = status
        
        if date is not None:
            self.date = date
        else:
            self.date = datetime.datetime.utcnow()

