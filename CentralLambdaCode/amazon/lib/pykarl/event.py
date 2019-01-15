"""
**event** - Provides data model for KARL events

.. module:: pykarl.event
   :platform: RHEL5
   :synopsis: Module which provides KARL event models and routines 
        
.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

.. testsetup:: *
    import datetime

"""

#MARK: Imports
import base64
import boto
import datetime
import json
import logging
import md5
import os
import multiprocessing
import Queue
import threading
import time
import uuid
import math
import copy
import jwt

from pykarl.core import KARL, DATE_FORMAT
import pykarl.event_router
import OpenSSL.crypto
from boto import kinesis

try:
    import systemprofile
except ImportError:
    pass

#MARK: Defaults
ENGINE_STATE_UNCONFIGURED = 0
ENGINE_STATE_STOPPED = 1 << 0
ENGINE_STATE_RUNNING = 1 << 1
ENGINE_STATE_STOPPING = 1 << 2
ENGINE_STATE_DISPATCH = 1 << 3

ENGINE_QUEUE_TIMEOUT = datetime.timedelta(seconds=1)
ENGINE_REFRESH_QUEUE_TIME = datetime.timedelta(seconds=.2)
ENGINE_REFRESH_EMPTY_QUEUE_TIME = datetime.timedelta(seconds=1)
ENGINE_NETWORK_CHANGE_DELAY = datetime.timedelta(seconds=15)

#MARK: -
#MARK: Classes
class Event(object):
    """Class which represents a new event.
    
    :Example:
    
        >>> event = Event(type="MyEvent",subject_area="ACME",source="XXXX-YYYY-XXXXXX-ZZZZ")
        >>> event.date = datetime.datetime.strptime("2015-02-14","%Y-%m-%d")
        >>> event.payload["MyKey"] = "MyValue"
        >>> print event.to_json()
        {
             "src": "XXXX-YYYY-XXXXXX-ZZZZ", 
             "uuid": "c96563ca-297b-4e10-9488-4f37653f91fa", 
             "submit_date": 1423880630, 
             "data": "{\"MyKey\": \"MyValue\"}", 
             "date": 1423900800, 
             "type": "MyEvent", 
             "subject": "ACME"
        }
        >>>      
    
    """
    
    uuid = None          #: Unique identifier for our event
    size = None          #: Size in bytes of our event (may have to call get_size())
    type = None          #: The type of event
    subject_area = None  #: Subject area for the event (determines routing)
    data_template = None #: Defines DB schema/type information as provided by the client (not currently developed)
    source = None        #: producing system's source identifier (guid)
    signature = None     #: Signature provided by the producer
    date = None          #: The date the event occurred
    execution_time = None  #: Timedelta object representing execution time for the event.
    submit_date = None   #: The date the producer submitted the event
    process_date = None  #: The date the event was processed
    process_time = None  #: The time spent processing the event
    payload = None       #: Key->value dictionary payload containing event data
    
    import_jwt = None       #: JWT import, if we were loaded by JWT
    validated = None    #: Whethor or not we have validated imported JWT
    
    max_record_size = 51000  #: The maximum size, in bytes, of our event record
    sequence_number = None #: Ephemeral value to store our event sequence number. 
    sequence_number = None  #: This is only populated when processed by the KCL
    
    def __init__(self,type=None,event_uuid=None,subject_area=None,payload=None,
                                                    source=None,
                                                    date=None,
                                                    submit_date=None,
                                                    sequence_number=None,
                                                    data_template=None,
                                                    json_data=None,
                                                    required_keys=None):
        """
        Constructor for our event.
        
        :param str type: The type of event
        :param str subject_area: Subject area for the event (determines routing)
        :param dict payload: dictionary of key=>value data pairs
        :param str source: The submitting system's source identifier (guid)
        :param str source: The submitting system's source identifier (guid)
        :param date: The date the event occurred
        :type date: :py:class:`datetime.datetime`
        :param submit_date: The date the producer submitted the event
        :param str sequence_number: Our event sequence number (populated when 
                    processed by the KCL
        :param data_template: Defines DB schema/type information as provided 
                        by the client (not currently developed)
        :type data_template: (dict) data structure
        :param str json_data: json string containing data to load in
        :param required_keys: If provided, we will raise a 
                    :py:class:`EventLoadError`: in the event that any of the
                    provided keys are absent from the provided :py:class:`json_data` 
                    parameter.
        :param type required_keys: List of strings 
        """
        
        self.type = type
        
        self.signature = None
        
        if event_uuid is not None:
            self.uuid = event_uuid
        else:
            self.uuid = "%s" % uuid.uuid4()
        
        self.subject_area = subject_area         
        self.data_template = data_template
        
        if payload is not None:
            self.payload = payload
        else:
            self.payload = {}
        
        self.source = source  
        
        if not date:
            self.date = datetime.datetime.utcnow()
        else:
            self.date = date
    
        if not submit_date:
            self.submit_date = self.date
        else:
            self.submit_date = submit_date
        
        if sequence_number:
            self.sequence_number = sequence_number
        
        if json_data is not None:
            self.load_from_json(json_data=json_data,required_keys=required_keys)
        
    def get_records(self,sign=False,max_record_size=None):
        """
        Method which outputs a list of records to be put to the
        kinesis queue. 
        
        :param bool sign: If true, we will sign each record.
        :param int max_record_size: The max size, in bytes of our record.
        
        """
        
        if max_record_size is None:
            max_record_size = self.max_record_size
        
        records = []
        
        base_dict = {}
        
        payload = base64.b64encode(json.dumps(self.payload))
        
        md_size = 0
        payload_size = len(payload.encode("ascii"))
        
        base_dict["uuid"] = self.uuid
        base_dict["src"] = self.source
        base_dict["type"] = self.type
        base_dict["subject"] = self.subject_area
        base_dict["template"] = self.data_template
        base_dict["date"] = self.date.strftime("%s")
        
        if self.execution_time:
            base_dict["runtime"] = self.execution_time.total_seconds()
            
        base_dict["submit_date"] = self.submit_date.strftime("%s")
        base_dict["data"] = ""
        
        md_size = len(json.dumps(base_dict).encode("ascii"))
        
        if sign:
            md_size += 44
        
        if (md_size + payload_size) < max_record_size:
            base_dict["data"] = payload
            if sign:
                self.sign_record(base_dict)
            
            records.append(base_dict)
        else:
            ## modify our md_size to account for our page counts and rid
            md_size += 90
            
            num_pages = math.ceil(payload_size /
                                            float(max_record_size - md_size))
            page_size = math.ceil(payload_size / float(num_pages))
            
            current_page = 1
            while current_page <= num_pages:
                my_dict = copy.deepcopy(base_dict)
                my_dict["current_page"] = current_page
                my_dict["ttl_pages"] = num_pages
                start_index = int((current_page - 1) * page_size)
                end_index = int(start_index+page_size)
                
                ## print "Slicing data for page:%s [%s:%s]" % (current_page,
                ##                                                start_index,
                ##                                                end_index)
                
                current_page += 1
                
                my_dict["data"] = payload[start_index:end_index]
                
                if sign:
                    self.sign_record(my_dict)
                
                records.append(my_dict)
        
        return records
        
    def sign_record(self,record):
        """
        Method to sign the provided record.
        """
        
        signing_string = "%s_%s_%s_%s" % (record["src"],
                                            record["type"],
                                            record["date"],
                                            record["data"])
        
        my_hash = md5.md5(signing_string).hexdigest()
        
        record["sig"] = my_hash
    
    def load_from_json(self,json_data,required_keys=None):
        """
        Method to load our event from JSON string.
        
        :param json_data: Import data to load
        :type json_data: str
        :type json_data: dict
        :param required_keys: If provided, we will raise a 
                    :py:class:`EventLoadError`: in the event that any of the
                    provided keys are absent from the provided json string.
        
        :raises :py:class:`EventLoadError`: If required_keys are not present.
        
        
        """
        
        ## If we're a string, deserialize
        if isinstance(json_data, basestring):
            data = json.loads(json_data)
        else:
            data = json_data
                
        found_keys = []
        
        try:
            key = "uuid"
            self.uuid = data[key]
            found_keys.append(key)
        except KeyError:
            pass
        
        try:
            key = "type"
            self.type = data[key]
            found_keys.append(key)
        except KeyError:
            pass
        
        try:
            key = "subject"
            self.subject_area = data[key]
            found_keys.append(key)
        except KeyError:
            pass
        
        try:
            key = "template"
            self.data_template = data[key]
            found_keys.append(key)
        except KeyError:
            pass
        
        try:
            key = "src"
            self.source = data[key]
            found_keys.append(key)
        except KeyError:
            pass
        
        try:
            key = "date"
            self.date = datetime.datetime.utcfromtimestamp(float(data[key]))
            found_keys.append(key)
        except (KeyError,TypeError):
            pass
        
        try:
            key = "submit_date"
            self.submit_date = datetime.datetime.utcfromtimestamp(float(data[key]))
            found_keys.append(key)
        except (KeyError,TypeError):
            pass
        
        key = "data"
        if isinstance(data[key],basestring):
            try:
                self.payload = json.loads(data[key])
                found_keys.append(key)
            except ValueError:
                try:
                    decoded_json = base64.b64decode(data[key])
                    self.payload = json.loads(decoded_json)
                    found_keys.append(key)
                except (TypeError,ValueError) as exp:
                    raise EventLoadError("Failed to load event payload from data! Error:{}".format(exp))
            
            except KeyError:
                pass
        else:
            self.payload = data[key]
        
        try:
            key = "sig"
            self.signature = data[key]
            found_keys.append(key)
        except KeyError:
            pass
        
        self.size = len(json_data)
        found_keys.append("size")
        
        if required_keys:
            missing_keys = []
            for required_key in required_keys:
                if not required_key in found_keys:
                    missing_keys.append(required_key)
            if len(missing_keys) > 0:
                raise EventLoadError("Required keys are missing: {}".format(", ".join(missing_keys)))
    
    def to_json(self, base64encode=False, raw_data=None):
        """
        Method that will output JSON string from our event.
        
        :param base64encode: If true, we will base64 encode the json string
        :type base64encode: boolean
        :param bool raw_data: If true, we will return our data dictionary 
                                without converting to JSON.
        
        :returns: json string. (base64 encoded if base64encode is true).
        
        """
        
        dict = {}
        
        dict["type"] = self.type
        if self.subject_area:
            dict["subject"] = self.subject_area
        
        if self.data_template:
            dict["template"] = self.data_template
        
        dict["uuid"] = self.uuid
        dict["src"] = self.source
        dict["date"] = int((self.date - datetime.datetime(1970,1,1)).total_seconds())
        dict["submit_date"] = int((self.submit_date - datetime.datetime(1970,1,1)).total_seconds())
        
        payload = None
        if base64encode:
            payload = base64.b64encode(json.dumps(self.payload))
        else:
            payload = json.dumps(self.payload)
        
        dict["data"] = payload
        if self.signature:
            dict["sig"] = self.signature
            
        if not raw_data:
            return json.dumps(dict,indent=5)
        else:
            return dict
    
    def to_jwt(self, key, algorithm="RS256", base64encode=False):
        """
        Method that will output jwt from our event.
        
        :param  key: Our private key to use for signing.
        :type key: (string) PEM-formatted private key
        :type key: :py:class:`OpenSSL.crypto.PKey`
        :param base64encode: If true, we will base64 encode the jwt
        :type base64encode: boolean
        :returns: jwt. (base64 encoded if base64encode is true).
        """
        
        ## If our public key isn't a string, attempt to dump it to PEM
        if isinstance(key, basestring):
            pemkey = key
        else:
            pemkey = OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM,
                                                                key)
        
        data = self.to_json(raw_data=True)
        jwt_data = jwt.encode(payload=data, key=pemkey, algorithm=algorithm)
        
        if not base64encode:
            return jwt_data
        else:
            return base64.b64encode(jwt_data)
        
    def load_from_jwt(self, jwt_data, public_key=None, required_keys=None,
                                            algorithm="RS256"):
        """
        Method to load our event from a provided JWT string. If a 
        public key is provided, we will attempt to validate the
        signature of the provided JWT, and will set self.validated to True.
        
        In the event that a public key is provided, the event will loaded with 
        the provided data whether or not the signature is verified, however,
        we will re-raise the encountered OpenSSL.crypto exception. We will
        also set self.validated to false. 
        
        :param jwt_data: str data to load
        :type jwt_data: str
        :param  public_key: Our public key to use for signature verification.
        :type public_key: (string) PEM-formatted
        :type public_key: :py:class:`OpenSSL.crypto.PKey`
                    
        :param required_keys: If provided, we will raise a 
                    :py:class:`EventLoadError`: in the event that any of the
                    provided keys are absent from the provided json string.
        
        :raises :py:class:`jwt.exceptions.DecodeError`: If invalid JWT is passed
        :raises :py:class:`TypeError`: If key is bad
        :raises :py:class:`ValueError`: If key is malformed string
        :raises :py:class:`jwt.exceptions.InvalidTokenError`: If signature validation fails
        :raises :py:class:`EventLoadError`: If required_keys are not present.
        """
        
        ## Load decoded data into our event prior to attempting validation
        data = jwt.decode(jwt_data, verify=False)
        
        self.import_jwt = jwt_data
        self.validated = False
        
        self.load_from_json(json_data=data, required_keys=required_keys)
        
        if public_key:
            ## If our public key isn't a string, attempt to dump it to PEM
            if isinstance(public_key, basestring):
                pemkey = public_key
            else:
                pemkey = OpenSSL.crypto.dump_publickey(OpenSSL.crypto.FILETYPE_PEM,
                                                                public_key)
            
            jwt.decode(jwt=jwt_data, key=pemkey, algorithm=algorithm)
            self.validated = True
    
    def export_as_csv(self,delimiter="|"):
        """Method to export our record as a delimited text record.
        type|src|size|process_time|date|process_date|submit_date
        
        Note: this is deprecated in lieu of json based imports
        
        """
        
        if self.date:
            datestamp = self.date.strftime(DATE_FORMAT)
        else:
            datestamp = datetime.datetime.utcnow().strftime(DATE_FORMAT)
        
        if self.process_date:
            process_datestamp = self.process_date.strftime(DATE_FORMAT)
        else:
            process_datestamp = datetime.datetime.utcnow().strftime(DATE_FORMAT)
        
        if self.process_time:
            process_time = self.process_time
        else:
            process_time = ""
        
        if self.submit_date:
            submit_date = self.submit_date.strftime(DATE_FORMAT)
        else:
            submit_date = datestamp
        
        csv = None
        for value in (self.type,self.source,self.size,self.process_time,
                                datestamp,process_datestamp,submit_date):
            if not csv:
                csv = value
            else:
                csv = "%s%s%s" % (csv,delimiter,value)
        
        return csv
        
    def get_size(self):
        """
        Method which returns the size of our payload in bytes.
        """
        
        data = self.to_json(base64encode=True)
        
        return len(data)

#MARK: -
class EventEngine(object):
    """
    Class which provides provisions for submitting KARL events,
    maintaning a local event queue, and provides a multi-threaded processing
    engine.
    """
    
    #MARK: Properties
    karl = None                     #: Our KARL object
    
    identity = None                 #: Our acme.crypto.Identity object, used for signing operations
    
    default_source = None           #: Our defaurt event source.
    
    kinesis_client = None           #: Our kinesis object
    reload_kinesis = None           #: Flag which, when set to true, causes us to reload our creds
    
    reload_router = None            #: Flag which, when set to true, causes us to reload our creds
    
    cred_file_path = None           #: Path to our AWS credential file
    credentials = None              #: AWS STS credentials
    kinesis_record_size = None      #: The max size, in bytes, of our Kinesis record. All
                                    #: events submitted through this engine instance will
                                    #: inherit this setting.
    
    
    default_route = None           #: Dictionary containing default route eg: {'default_sink':'kinesis','default_target':'standard.test'}
    queue_file_path = None          #: Filesystem path which stores our Kinesis queue
    file_op_in_progress = None      #: Semaphore to control concurrency.
    queue = None                    #: Our Kinesis event queue
    
    lock = None                     #: lock for threadsafe operations
    
    engine_thread = None            #: Our engine's thread
    should_run = None               #: Flag which denotes whether KARL should be running
    online = None                   #: Flag which denotes whether KARL is online. 'Online'
                                    #: means that we have network access, credentials, and 
                                    #: a loaded route map
    has_network_access = None       #: Flag which denotes whether we have network access
    
    network_change_delay = None     #: Timespan object representing how long we wait after a network change prior to running tests. (This allows time for domain auth to function)
    network_delay_timer = None      #: Timer object that is started in the event of a net change
    last_failed_submission = None   #: The date of our last failed submission
    num_failed_commits = 0       #: Counter which denotes how many consecutive commit failures we have had.
    
    failed_retry_interval = None    #: Timespan object denoting how long we wait to retry after a failure
    
    max_backoff_delay = None        #: Timespan object denoting our maximum retry interval in the event of consecutive failures.
    
    kick_queue = None               #: When set to true, we will restart queue processing
    settings_key_map =  None
    
    logger_name = None              #: Name used by class logger objects
    
    @property
    def has_credentials(self):
        """
        Property which denotes whether or not we have loaded credentials
        """
        if self.karl.kinesis_access_key_id and self.karl.kinesis_secret_key:
            return True
        else:
            return False
    
    @property
    def queue_length(self):
        """
        Property to return 
        """
        if self.queue:
            return self.queue.qsize()
        else:
            return 0
    
    @property
    def state(self):
        """
        Property to output our engine state.
        """
        return self.engine_state()
    
    @property
    def is_online(self):
        """
        Property to output our engine state.
        """
        return self.online()
                
    @property
    def last_failed_commit_datestamp(self):
        """
        Property which returns a datestamp representing our last failed commit
        """
        
        logger = logging.getLogger(self.logger_name)
        if self.last_failed_submission:
            try:
                return self.last_failed_submission.strftime(DATE_FORMAT)
            except Exception as exp:
                logger.warning("Failed to format last failed submission date. Error:{}".format(exp))
        
        return None
        
    @last_failed_commit_datestamp.setter
    def last_failed_commit_datestamp(self,value):
        """
        Setter accessor for our last_failed_commit date
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
                    logger.warning("Could not import failed_datestamp value:{v} for model:{m}".format(v=value,m=self.__class__.__name__)) 

        self.last_failed_submission = the_date
    
    #MARK: Loading Routines
    def __init__(self, karl=None, identity=None, settings_key_map=None):
        """
        Primary constructor.
        """
        
        if karl is None:
            self.karl = KARL()
        else:
            self.karl = karl
            
        self.identity = identity
        
        self.logger_name = "KARL.Engine"
      
        self.max_backoff_delay = datetime.timedelta(hours=1)
        self.failed_retry_interval = datetime.timedelta(minutes=1)
        self.kinesis_record_size = 900000
        self.network_change_delay = datetime.timedelta(seconds=15)
        
        self.queue = Queue.Queue()
        self.lock = threading.RLock()
        
        if settings_key_map is None:
            self.settings_key_map = { "default_source" : None,
                                        "debug" : None,
                                        "kinesis_record_size" : None,
                                        "network_change_delay" : None,
                                        "failed_retry_interval" : None,
                                        "max_backoff_delay" : None,
                                    }
        else:
            self.settings_key_map = settings_key_map
        
        self.route_mappings = {}                                #: Dictionary which has mappings of sink and target based on subject area and event type
        self.route_mappings["subject_areas"] = {}         #: Dictionary to map subject areas to routes
        self.route_mappings["event_types"]   = {}         #: Dictionary to map event types to routes  
        
    
    def __del__(self):
        """
        Method to ensure our engine thread is stopped when we are 
        deconstructed.
        """
        
        if self.engine_thread and self.engine_thread.is_alive():
            self.stop()
            time.sleep(1)
    
    def settings_load_dict(self,data,key_map=None):
        """
        Method to load settings data from the provided dictionary.
        
        :param dict data: Dictionary of key->values to load
        :param dict key_map: Dictionary of key->attribute mappings
                which represent local properties used in our output
        """
        
        if key_map is None:
            payload_map = self.key_map
        else:
            payload_map = key_map
        
        for key,attribute in payload_map.iteritems():
            if attribute is None:
                attribute = key
            try:
                setattr(self,attribute,data[key])
            except KeyError as exp:
                pass
    
    def settings_to_dict(self,key_map=None):
        """
        Method to export our record in key=>value dictionary form,
        as prescribed by our export_map
        
        :param dict key_map: Dictionary of key->attribute mappings
                which represent local properties used in our output
        """
        
        if key_map is None:
            key_map = self.settings_key_map
        
        my_dict = {}
        
        for key,property in key_map.iteritems():
            if property is None:
                property = key
            
            try:
                value = getattr(self,property)
            except (NameError,AttributeError):
                value = None
            
            if property.lower() == "access_key_id":
                try:
                    value = self.karl.kinesis_access_key_id
                except (NameError,AttributeError):
                    value = None
            
            my_dict[key] = value
                
        return my_dict
    
    def load(self):
        """
        Method which loads our state from disk, which includes
        all saved state as well as provisioned credentials.
        
        This method will not raise any exceptions, but it will log errors
        
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.log(15,"Loading KARL Config...")
        
        path = self.queue_file_path
        if path and os.path.isfile(path):
            try:
                self.load_queued_events(filepath=path)
            except Exception as exp:
                logger.warning("Failed to load queue file from path:'{}' Error:{}".format(
                                                        path,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
               
        path = self.cred_file_path
        creds = self.credentials
        if path:
            try:
                self.load_credentials(filepath=path)
            except Exception as exp:
                logger.error("Failed to load AWS data from path:'{}' Error:{}".format(
                                                        path,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        elif creds:
            try:
                self.load_credentials(credentials=creds)
            except Exception as exp:
                logger.error("Failed to load AWS data from creds:'{}' Error:{}".format(
                                                        str(creds),exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        else:
            logger.warning("Failed to load KARL, no AWS data file at path:'{}'".format(path))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        if not self.default_source:
            logger.warning("No default source specified!")
      
    def reload(self):
        """
        Method which reloads our settings.
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.log(15,"Reloading KARL config...")
        
        path = self.cred_file_path
        creds = self.credentials
        if path:
            try:
                self.load_credentials(filepath=path)
            except Exception as exp:
                logger.error("Failed to load AWS data from path:'{}' Error:{}".format(
                                                        path,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        elif creds:
            try:
                self.load_credentials(credentials=creds)
            except Exception as exp:
                logger.error("Failed to load AWS data from creds:'{}' Error:{}".format(
                                                        str(creds),exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        else:
            logger.warning("Failed to load KARL, no AWS data file at path:'{}'".format(path))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        
        self.reload_kinesis = True
        self.reload_router = True
        self.kick()
    
    def load_default_routes_map(self, routes_dir):
        """
        Loads the default routes map from the given routes directory
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("Loading default routes map...")
        default_route_path = os.path.join(routes_dir,"default_route.json")
        default_route = None
        try:
            with open(default_route_path,"r") as fh:
                json_data = fh.read()
                default_route = json.loads(json_data)
        except Exception as exp:
                logger.warning("Failed to load default route map from file:{}. Error:{}".format(default_route_path,exp),exc_info=1)
        if default_route:
            self.default_route = default_route
    
    def load_routes_map(self, routes_dir):
        """
        Loads routes from the given routes directory.
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("Loading routes map...")
        route_mappings = {}
        route_mappings["subject_areas"] = {}
        route_mappings["event_types"] = {}
        try:
            for filename in os.listdir(routes_dir):
                if filename.endswith(".json") and filename != "default_route.json": 
                    routes_file_path = os.path.join(routes_dir, filename)
                    # eg.: routes = {"subject_areas":{"watcher":{"sink":"kinesis", "target":"standard.test.thipperu"}}}
                    routes = self.load_routes_from_file(routes_file_path)
                    route_mappings = self.merge_routes(route_mappings, routes)
                elif not filename.endswith(".json"):
                    logger.warning("Found a file, '{}' with non-json extension in the routes directory ('{}'), file will be ignored...".format(filename, routes_dir))
        except Exception as exp:
            logger.error("Failed to load routes from routes directory, Error: {}".format(exp))
        self.route_mappings = route_mappings
    
    def merge_routes(self, route_mappings, routes):
        """
        Adds new route_mappings to the event_engine, overrides if same keys exist.
        """
        
        if "subject_areas" in routes:
            route_mappings["subject_areas"].update(routes["subject_areas"])
        
        if "event_types" in routes:
            route_mappings["event_types"].update(routes["event_types"])
        
        return route_mappings
    
    def load_routes_from_file(self, routes_file_path):
        """
        Loading routes, its in the format of '{sink':'kinesis', 'target':'standard.test',"transport_mech":"json"}
        """
        route_mappings = None
        with open(routes_file_path,"r") as fh:
            json_data = fh.read()
            route_mappings = json.loads(json_data)
        return route_mappings
    
    def configure_event_router(self):
        """
        Method that configures the event router with route_mappings, default_route and publisher_map
        """
        
        logger = logging.getLogger(self.logger_name)
        kinesis_client = None
        kinesis_publisher = None
        publisher_map = {}
        event_router = None
        try:
            kinesis_client = self.karl.kinesis()
            kinesis_publisher = pykarl.event_router.KinesisPublisher(
                                        kinesis_client=kinesis_client,
                                        identity=self.identity,
                                        default_source=self.default_source)
            firehose_client = self.karl.firehose()
            firehose_publisher = pykarl.event_router.FirehosePublisher(
                                        firehose_client=firehose_client,
                                        identity=self.identity,
                                        default_source=self.default_source)                                                                       
            
            publisher_map = {
                            "Kinesis": kinesis_publisher,
                            "Firehose": firehose_publisher
                            }
            
            event_router = pykarl.event_router.EventRouter(self.route_mappings, 
                                                self.default_route, 
                                                publisher_map)
        except Exception as e:
            logger.error("Failed to configure event router:{}".format(e))
            self.num_failed_commits+=1
            self.last_failed_submission = datetime.datetime.utcnow()
        
        return event_router
    
    def register_routes(self, route_mappings):
        """
        Adds new route_mappings to the event_engine, overrides if same keys exist.
        """
        
        if "subject_areas" in route_mappings:
            self.route_mappings["subject_areas"].update(route_mappings["subject_areas"])
        
        if "event_types" in route_mappings:
            self.route_mappings["event_types"].update(route_mappings["event_types"])
        
        return self.route_mappings
            
    def load_queued_events(self,filepath=None):
        """
        Method which loads our event queue from disk.
        
        :param str filepath: The file path to save to (defaults to self.queue_file_path)
        
        :raises ValueError: If no filepath is provided.
        :raises IOError, OSError: If there are IO related errors
        :raises FileOpInProgressError: If there is already an IO activity in progress
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if filepath is None:
            filepath = self.queue_file_path
        
        if filepath is None:
            raise ValueError("No filepath could be determined!")
        
        try:
            with self.lock:
                if self.file_op_in_progress:
                    raise FileOpInProgressError()
                else:
                    self.file_op_in_progress = True
                
                logger.debug("Loading event queue from file:{}".format(filepath))
                with open(filepath,"r") as fh:
                    json_data = fh.read()
                    data = json.loads(json_data)
                    
                    if "QueuedEvents" in data:
                        for event_entry in data["QueuedEvents"]:
                            try:
                                event_json = base64.b64decode(event_entry)
                                event = Event(json_data=event_json)
                                self.queue.put(event)
                            except Exception as exp:
                                logger.error("Failed to load event from data:{}".format(event_entry))
                                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                        logger.debug("Loaded {} events from queue.".format(
                                                len(data["QueuedEvents"])))
        except Exception as exp:
            logger.error("Cannot load queue state: {}".format(exp))
            raise
        finally:
            with self.lock:
                self.file_op_in_progress = False
    
    def save(self): 
        """
        Top-level method for initiating state saving.
        """
        
        return self.save_queued_events()
        
    def save_queued_events(self,filepath=None):
        """
        Method which saves our event queue to disk.
        
        :param str filepath: The file path to save to (defaults to self.queue_file_path)
        
        :raises ValueError: If no filepath is provided.
        :raises IOError, OSError: If there are IO related errors
        :raises FileOpInProgressError: If there is already an IO activity in progress
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if filepath is None:
            filepath = self.queue_file_path
            
        if filepath is None:
            raise ValueError("No filepath could be determined!")
        
        try:
            with self.lock:
                if self.file_op_in_progress:
                    raise FileOpInProgressError()
                else:
                    self.file_op_in_progress = True
                
                logger.debug("Saving event queue to file:{}".format(filepath))
                events = []
                while True:
                    event = None
                    try:
                        event = self.queue.get(block=False)
                        events.append(event)
                    except Queue.Empty:
                        break
            
            data = {"QueuedEvents":[]}
            for event in events:
                data["QueuedEvents"].append(base64.b64encode(event.to_json()))
            
            with open(filepath,"w") as fh:
                fh.write(json.dumps(data))
            
            logger.info("Saved {} KARL events to queue...".format(len(events)))
            
        except Exception as exp:
            logger.error("Cannot save queue state: {}".format(exp))
            raise
        finally:
            with self.lock:
                self.file_op_in_progress = False
    

    def load_credentials(self,filepath=None,credentials=None):
        """
        Method which loads our creds from disk and populates our local 
        KARL object.
        
        :param str filepath: The file path to load from 
                (defaults to self.cred_file_path)
        :param object credentials: STS credentials object with SecretAccessKey, SessionToken and AccessKeyId
        
        :raises ValueError: If no filepath is provided.
        :raises IOError, OSError: If there are IO related errors

        """
        
        logger = logging.getLogger(self.logger_name)
        
        if filepath is None:
            filepath = self.cred_file_path
        
        if credentials is None:
            credentials = self.credentials
        
        self.load_kinesis_credentials(filepath, credentials)
        self.load_firehose_credentials(filepath, credentials)
        
         
    def load_kinesis_credentials(self, filepath=None, credentials=None):
        '''
        Method that loads Kinesis credentials to our KARL object.
        :param str filepath: The file path to load from 
                (defaults to self.cred_file_path)
        :param object credentials: STS credentials object with SecretAccessKey, SessionToken and AccessKeyId
        
        :raises ValueError: If no filepath is provided.
        :raises IOError, OSError: If there are IO related errors
        '''
        logger = logging.getLogger(self.logger_name)
        if credentials:
            logger.debug("Loading Kinesis creds from credentials data")
            if(("SecretAccessKey" not in credentials) or ("SessionToken" not in credentials) or "AccessKeyId" not in credentials):
                raise ValueError("Credentials does not have either SecretAccessKey or SessionToken or AccessKeyId")
            self.karl.kinesis_access_key_id = credentials["AccessKeyId"]
            self.karl.kinesis_secret_key = credentials["SecretAccessKey"]
            self.karl.kinesis_session_token = credentials["SessionToken"]
            self.karl.use_temp_cred = True
        elif filepath:
            logger.debug("Loading AWS data from file: {0}".format(filepath))
            
            with open(filepath,"r") as fh:
                file_data = fh.read()
                json_string = base64.b64decode(file_data)
                
                data = json.loads(json_string)
                
                for key in data.keys():
                    if key.lower() == "accesskeyid":
                        self.karl.kinesis_access_key_id = data[key]
                    elif key.lower() == "secretkey":
                        self.karl.kinesis_secret_key = data[key]
                    elif key.lower() == "region":
                        self.karl.region = data[key]
        else:
            raise ValueError("No credentials input provided, cannot load credentials!")
    
    def load_firehose_credentials(self, filepath=None, credentials=None):
        """
        Method that loads Firehose credentials to our local KARL object.
        :param str filepath: The file path to load from 
                (defaults to self.cred_file_path)
        :param object credentials: STS credentials object with SecretAccessKey, SessionToken and AccessKeyId
        
        :raises ValueError: If no filepath is provided.
        :raises IOError, OSError: If there are IO related errors
        """
        logger = logging.getLogger(self.logger_name)
        if credentials:
            logger.debug("Loading Firehose creds from credentials data")
            if(("SecretAccessKey" not in credentials) or ("SessionToken" not in credentials) or "AccessKeyId" not in credentials):
                raise ValueError("Credentials does not have either SecretAccessKey or SessionToken or AccessKeyId")
            self.karl.firehose_access_key_id = credentials["AccessKeyId"]
            self.karl.firehose_secret_key = credentials["SecretAccessKey"]
            self.karl.firehose_session_token = credentials["SessionToken"]
            self.karl.use_temp_cred = True
        elif filepath:
            logger.debug("Loading AWS data from file: {0}".format(filepath))
            
            with open(filepath,"r") as fh:
                file_data = fh.read()
                json_string = base64.b64decode(file_data)
                
                data = json.loads(json_string)
                
                for key in data.keys():
                    if key.lower() == "accesskeyid":
                        self.karl.firehose_access_key_id = data[key]
                    elif key.lower() == "secretkey":
                        self.karl.firehose_secret_key = data[key]
                    elif key.lower() == "region":
                        self.karl.firehose_region = data[key]
        else:
            raise ValueError("No credentials input provided, cannot load credentials!")
    
    def creds_are_loaded(self, filepath=None, credentials=None):
        """
        Method which returns whether or not we have loaded credentials for 
        Kinesis.
        
        :returns bool: True if we have loaded creds.
        """
        
        return (self.karl.kinesis_access_key_id and self.karl.kinesis_secret_key)
    
    #MARK: Main Interface
    def start(self):
        """
        Method to start our processing engine on a dedicated thread.
        This method does not block.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        engine_thread = self.engine_thread
        
        if engine_thread and engine_thread.is_alive():
            logger.debug("start() called but we are already running... kicking queue.")
            self.kick()
            return
        
        ## Check for network access. If this check fails: default to true
        ## standard KARL fallback will handle this gracefully
        try:
            self.has_network_access = systemprofile.profiler.online() 
        except NameError:
            logger.error("An error occurred checking online status: systemprofile module is not avaliable (defaulting True)")
            self.has_network_access = True
        except Exception as exp:
            logger.error("An error occurred checking online status: {} (defaulting True)".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):", exc_info=1)
            self.has_network_access = True
        
        if self.online():
            logger.info("Starting KARL queue processing engine...")
        elif self.has_network_access:
            logger.info("Starting KARL queue processing engine (noconfig)...")
        else:
            logger.info("Starting KARL queue processing engine (offline)...")
        
        self.should_run = True;
        
        engine_thread = threading.Thread(target=self.run_engine,
                                                name="KARLEventEngineThread")
        engine_thread.daemon = True
        engine_thread.start()
        
        self.engine_thread = engine_thread
        
    def stop(self):
        """
        Method to stop our processing engine.
        """
        
        self.should_run = False
        
        logger = logging.getLogger(self.logger_name)
        logger.info("Stopping KARL queue processing engine...");
    
    def online(self):
        """
        Method which denotes whether we are online. Online means that we have 
        network access, credentials, and a loaded routing map.
        """
        
        return (self.has_network_access 
                        and self.creds_are_loaded() 
                        and self.default_route_is_loaded())
    
    def engine_state(self):
        """
        Returns the state of our current engine.
        """
        state = ENGINE_STATE_STOPPED
        
        if self.engine_thread and self.engine_thread.is_alive():
            if self.should_run:
                state = ENGINE_STATE_RUNNING
            else:
                state = ENGINE_STATE_STOPPING
            
        return state
    
    def kick(self,update_network=False):
        """
        Method to kick our karl queue. This causes our queue processor
        to retry submitting KARL events immediately (bipassing any
        network or time-delay falloffs).
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.debug("Kicking KARL Queue (KAPOW!)")
        
        if update_network:
            self.update_network_availability()
        
        self.kick_queue = True

    
    def commit_event(self,event):
        """ 
        Method to enqueue a new event for submission to KARL. This method
        runs asyncronously and is the primary way to submit new events for
        posting to kinesis.
                
        :param event: The event to post.
        :type event: :py:class:`Event`
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if not self.online():
            logger.debug("Committing event: {} to the queue (offline).".format(event.type))
        else:
            logger.debug("Committing event: {} to the queue.".format(event.type))
        
        if not event.source:
            event.source = self.default_source
        
        event.kinesis_record_size = self.kinesis_record_size
        
        self.queue.put(event)
    
    def run_engine(self):
        """ 
        Syncronous method to run our main queue processing engine. 
        This method is fully blocking, generally you will call Start()
        to initiate this method on a dedicated thread.
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.info("RUN ENGINE INITIATED")
        kinesis_client = None
        event_router = None
        while self.should_run:
            
            ## If we have no items in our queue, chill a bit.
            if self.queue_length <= 0:
                time.sleep(ENGINE_REFRESH_EMPTY_QUEUE_TIME.total_seconds())
            else:
                time.sleep(ENGINE_REFRESH_QUEUE_TIME.total_seconds())
            
            if self.is_queue_process_time():
                if self.num_failed_commits > 10:
                    ## Update our network status and rebuild our kinesis client 
                    ## if we've failed 10 times.
                    self.update_network_availability()
                    
                    try:
                        self.load_credentials()
                    except Exception as exp:
                        logger.error("Failed to load KARL credentials.; Error: {}".format(exp));
                        self.num_failed_commits += 1
                        self.last_failed_submission = datetime.datetime.utcnow()
                        continue
                        
                    try:
                        event_router = self.configure_event_router()
                    except Exception as exp:
                        logger.error("Could not connect to Kinesis; Error: {}".format(exp));
                        self.num_failed_commits += 1
                        self.last_failed_submission = datetime.datetime.utcnow()
                        continue
                
                elif event_router is None or self.reload_router:
                    try:
                        self.reload_router = False
                        event_router = self.configure_event_router()
                    except Exception as exp:
                        logger.error("Could not configure event router; Error: {}".format(exp));
                        self.num_failed_commits += 1
                        self.last_failed_submission = datetime.datetime.utcnow()
                        continue
                
                ## Fetch our first item from our queue and attempt to Post it.
                event = None
                
                with self.lock:
                    try:
                        event = self.queue.get(timeout=ENGINE_QUEUE_TIMEOUT.total_seconds())
                    except Queue.Empty as exp:
                        continue
                    
                    try:
                        event_router.route_event(event)
                        self.queue.task_done()
                        self.num_failed_commits = 0
                        self.last_failed_submission = None
                    except Exception as exp:
                        self.num_failed_commits += 1
                        self.last_failed_submission = datetime.datetime.utcnow()
                        logger.error("Failed to commit event:'{0}', Error: {1}".format(
                                                            event.type,exp))
                        logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                        self.queue.put(event)
                                                        
        logger.info("KARL Engine shutdown complete...")
        
    def is_queue_process_time(self):
        """
        Method which returns whether it's time to process a record
        in our queue. This will return no if we have had a recent failure
        and are implementing a backoff.
        
        :returns True: If we should process our queue.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        should_process = True
        
        for i in xrange(0,1):
            if not self.should_run:
                logger.log(1,"is_queue_process_time() should_run is False")
                should_process = False 
                break
            if not self.has_network_access:
                logger.log(1,"is_queue_process_time() has_network_access is False")
                should_process = False
                break
                
            if not self.online():
                logger.log(1,"is_queue_process_time() online() is False")
                should_process = False
                break
            
            if self.kick_queue:
                logger.log(1,"is_queue_process_time() kick_queue is True")
                should_process = True
                break
            
            if not self.num_failed_commits == 0:
                if self.last_failed_submission:
                    try:
                        check_interval = datetime.timedelta(seconds=(
                                    self.failed_retry_interval.total_seconds() 
                                                        * self.num_failed_commits))
                        if check_interval > self.max_backoff_delay:
                            check_interval = self.max_backoff_delay
                    
                    except OverflowError:
                        check_interval = self.max_backoff_delay
                    
                    next_check = self.last_failed_submission + check_interval
                    
                    if datetime.datetime.utcnow() < next_check:
                        logger.log(1,"is_queue_process_time() throttling checks until:{}".format(next_check))
                        should_process = False
                        break
                else:
                    should_process = True
        
        if self.kick_queue:
            self.kick_queue = False
            if should_process:
                logger.info("KARL queue was kicked!")
            elif should_process:
                logger.info("KARL queue was kicked but we are offline!")
                        
        
        ##elif self.num_failed_commits == 0:
        ##    should_process = True
            
        logger.log(1,"is_queue_process_time() returning: {}".format(should_process))
        
        return should_process
    
    #MARK: Network Handlers
    def network_changed(self,*args,**kwargs):
        """
        Delegate method used to handle network change events. This must
        currently be wired by a controlling entity (PyKARL does not have
        internal event monitoring hooks)
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("Network change notification received, updating network availability...")
        
        self.update_network_availability()
    
    def update_network_availability(self):  
        """
        Method to check network availability and set state accordingly.
        
        """        
        logger = logging.getLogger(self.logger_name)
        
        had_network = self.has_network_access
        has_network = None
        try:
            has_network = systemprofile.profiler.online()
        except Exception as exp:
            logger.warning("Failed to run network availability checks. Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        self.has_network_access = has_network
        
        if has_network and not had_network:
            logger.info("Network change detected: KARL now has network access.");
        elif not has_network and had_network:
            logger.info("Network change detected: KARL no longer has network access.");
        else:
            logger.debug("Reaffirm network status... Online: {0}".format(has_network))
        
        karl_online = self.online()
        
        if karl_online and not had_network:
            if self.should_run and self.num_failed_commits > 0:
                logger.debug("KARL is back online with queued items, kicking queue!");
                self.kick(update_network=False)
        elif not had_network and has_network and not karl_online:
            if not self.creds_are_loaded() and not self.default_route_is_loaded():
                logger.error("KARL now has network access but is not configured. Remaining offline...")
            elif not self.creds_are_loaded():
                logger.error("KARL now has network access but has no loaded credentials. Remaining offline.");
                
            elif not self.default_route_is_loaded():
                logger.error("KARL now has network access but has no loaded route mappings. Remaining offline.");
    
    def default_route_is_loaded(self):
        """ 
        Method which returns weather our route_mappings is loaded.
        
        :returns bool: True if we have a loaded map.
        """
        
        if self.default_route:
            return True
        else:
            return False

#MARK: -
class EventDispatcher(object):
    """
    Class which provides event dispatching capabilities for local code use.
    While events can be dispatched to an EventEngine instance directly, the 
    EventDispatcher is usable as a proxy object that separates event 
    submission and dispatching from engine configuration. This class will 
    typically be used globally in a singleton pattern.
    """
    
    delegates = []      #: A list of delegates to route to. (delegates must 
                        #: be a method which accepts an argument 'event', this
                        #: method should also accept **kwargs to provide for
                        #: future expansion.
    
    lock = None
    
    def __init__(self):
        """
        Constructor.
        """
        
        self.lock = threading.RLock()
    
    def is_configured(self):
        """
        Method which can be used to determine if our dispatcher is configured.
        At this time, this simply means that we have registered delegates,
        though this may change in the future. 
        """
        
        return len(self.delegates) > 0
        
    def dispatch(self,event):
        """
        Method which is invoked to commit an event to KARL.
        """
        
        logger = logging.getLogger("EventDispatcher")
        
        delegates = self.delegates[:]
        
        did_dispatch = False
        my_exp = None
        for delegate in delegates:
            try:
                delegate(event)
                did_dispatch = True
            except Exception as exp:
                logger.warning("Failed to commit event:{} to delegate:{}. Error:{}".format(
                                                            event.type,
                                                            delegate,
                                                            exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                my_exp = exp
        
        if not did_dispatch and my_exp:
            raise my_exp
#MARK: -    
#MARK: Exceptions
class FileOpInProgressError(Exception):
    pass

class EventTypeError(Exception):
    """
    Exception which is raised in the event that an operation does not support the provided EventType
    """
    pass
    
class EventLoadError(Exception):
    """
    Exception which is raised in the event that an :py:class:`Event` fails to load from the provided json data
    """
    pass

#MARK: -
#MARK: Module vars
dispatcher = EventDispatcher()

#:MARK Module functions
#MARK: Module functions
def string_from_enginestate(state,affirm_only=True):
    """
    Method which returns a string representing the current engine state.
    """
    state_string = None
    if isinstance(state,EventEngine):
        state_int = state.state
    else:
        state_int = int(state)
    
    if state_int == ENGINE_STATE_UNCONFIGURED:
        state_string = "Not Configured"
    elif state_int == ENGINE_STATE_STOPPED:
        state_string = "Stopped"
    elif state_int & ENGINE_STATE_STOPPING:
        state_string = "Stopping"
    elif state_int & ENGINE_STATE_RUNNING:
        state_string = "Running"
    elif state_int & ENGINE_STATE_DISPATCH:
        state_string = "DispatchOnly"
    else:
        state_string = "Unknown"
    
    return state_string


