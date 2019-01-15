"""
**core** - Module which provides most of the functionality provided by the :py:package:`pykarl` root package.

:platform: RHEL5
:synopsis: This is the root module that can be used for loading KARL 
        credentials and resource handles. Also includes classes which 

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

:Example:
    
        >>> import pykarl.core
        >>> 
        >>> ## Instantiate our KARL object
        >>> karl = pykarl.core.KARL()
        >>> 
        >>> ## Setup configuration settings for our "test" environment.
        >>> karl.setup(env="test")
        >>>
        >>> karl.rds_host
        'karl-test.czzziwhpg9wv.us-west-2.rds.amazonaws.com'
        >>>
        >>> ## Retrieve and use a KARL subsystem
        >>> rs = karl.redshift()
        >>> rs.query("SELECT uuid,type FROM event LIMIT 1").dictresult()
        [{'uuid': 'a51fa661-b3f2-11e4-b7ae-3c15c2de0480', 'type': 'TestEvent'}]
        >>>
        >>> rds = karl.rds()
        >>> rds.query("SELECT uuid,type FROM event LIMIT 1").dictresult()
        [{'type': 'SpamAgent', 'uuid': '5bfe107d-25eb-46e5-9ae9-69145af42c42'}]
        >>>

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import os
import json
import argparse
import datetime
import boto
import boto.kinesis

try:
    import boto3
except ImportError:
    boto3 = None


import logging

## Best effort imports
DB_AVAILABLE = False
APOLLO_AVAILABLE = False
ODIN_AVAILBLE = False
DEFAULT_ENV = "prod"
default_env_config = {}

try:
    import pg
    DB_AVAILABLE = True
except:
    pass

try:
    from bender import apollo
    APOLLO_AVAILABLE = True
except:
    pass

try:
    import pyodinhttp
    ODIN_AVAILBLE = True
except:
    pass


#MARK: Constants
DEFAULT_OPCONFIGNAME = "KARL"
DEFAULT_CACHE_USE_LIMITS = 1000

__version__ = "1.1"


class KARL(object):
    """
    Class which provides several convenience methods for loading connection settings,
    loading credentials from odin, and vending connection objects to various AWS 
    services. Any object which connects to KARL resources will be a descendent of 
    this class.
    """
    
    redshift_host = None        #: The hostname of our redshift instance
    redshift_port = None        #: The port used for redshift access
    redshift_name = None        #: The name of the redshift database
    redshift_username = None    #: The username used for redshift access
    redshift_password = None    #: The password used for redshift access
    
    rds_host = None             #: The hostname used for rds access
    rds_port = None             #: The port used for rds access
    rds_name = None             #: The name of the rds database
    rds_username = None         #: The username used for rds access
    rds_password = None         #: The password used for rds access
    
    s3_bucketname = None        #: The s3 bucket used for uploads/ingest
    s3_access_key_id = None     #: The AWS access key used for S3 access
    s3_secret_key = None        #: The AWS secret key used for S3 access
    
    kinesis_access_key_id = None #: The AWS access key used for Kinesis access
    kinesis_secret_key = None    #: The AWS secret key used for Kinesis access
    kinesis_region = None        #: The Region used for Kinesis access
    kinesis_session_token = None #: The AWS session token for sts credentials used for Kinesis access
    use_temp_cred = False        #: Flag denoting whether to use temporary sts cred
    
    firehose_access_key_id = None
    firehose_secret_key = None
    firehose_region = None
    firehose_session_token = None

    dynamo_access_key_id = None
    dynamo_secret_access_key = None
    dynamo_region = None

    use_cached_objects = True   #: If true, we will attempt to cache connection 
                                #: objects for rds, redshift, and s3bucket
    cached_rds = None           #: Our cached rds connection
    cached_rds_uses = 0         #: Number of time our cached connection has been vended.
    cached_rds_limit = DEFAULT_CACHE_USE_LIMITS #: Limit on number of times our cached connection can be vended.
    
    cached_redshift = None      #: Our cached redshift connection
    cached_redshift_uses = 0         #: Number of time our cached connection has been vended.
    cached_redshift_limit = DEFAULT_CACHE_USE_LIMITS #: Limit on number of times our cached connection can be vended.

    cached_dynamo = None
    cached_dynamo_uses = None
    cached_dynamo_limit = DEFAULT_CACHE_USE_LIMITS

    cached_s3_bucket = None     #: Our cached S3 bucket
    
    def __init__(self,karl=None):
        """
        Constructor method.
        
        :param karl: A KARL instance to load settings from
        :type karl: :py:class:`KARL`
        
        """
        self.use_cached_objects = True
        
        if karl is not None:
            self.load_karl_settings(karl)
    
    def s3(self):
        """
        Method which returns a s3 connection object.
        
        :raises ValueError: If any necessary parameters are not established.
        
        :returns: :py:class:`boto.s3` object
        
        """
        
        ## Sanity check
        if self.s3_access_key_id is None:
            raise ValueError("s3_access_key not set!")
        elif self.s3_secret_key is None:
            raise ValueError("s3_secret_key set!")
        
        ## Create our s3 object
        s3 = boto.connect_s3(self.s3_access_key_id,self.s3_secret_key)
        
        return s3
    
    def s3_bucket(self,use_cache=None):
        """
        Method which returns an s3 bucket object
        
        :param bool use_cache: If True, we will attempt to use and cache our 
                            s3 bucket object. Default behavior is based on 
                            :py:var:`use_cached_objects` ivar
        
        :raises ValueError: If any necessary parameters are not established.
        
        :returns: :py:class:`boto.s3` object
        """
        
        if use_cache is None:
            use_cache = self.use_cached_objects
        
        s3_bucket = None
        if self.cached_s3_bucket is not None and use_cache:
            s3_bucket = self.cached_s3_bucket
        else:
            s3 = self.s3()
            s3_bucket = s3.get_bucket(self.s3_bucketname)
            if not use_cache:
                self.cached_s3_bucket = s3_bucket
        
        return s3_bucket
    
    def redshift(self,use_cache=None):
        """
        Method which returns a DB connection object.
        
        :param bool use_cache: If True, we will attempt to use and cache our 
                            redshift connection object. Default behavior is 
                            based on :py:var:`use_cached_objects` ivar
        
        :raises ValueError: If any necessary parameters are not established.
        :raises SubSystemUnavailableError: If we do not have module support for this service.
        
        :returns: :py:class:`pg.DB` object
        
        """
        
        if not DB_AVAILABLE:
            raise SubSystemUnavailableError("Subsystem (pg) to support RedShift is not available!")
        
        if use_cache is None:
            use_cache = self.use_cached_objects
        
        ## Sanity check
        if self.redshift_name is None:
            raise ValueError("DB not set!")
        elif self.redshift_host is None:
            raise ValueError("DB Host not set!")
        elif self.redshift_host is None:
            raise ValueError("DB Host not set!")
        elif self.redshift_port is None:
            raise ValueError("No DB Port set!")
        elif self.redshift_password is None:
            raise ValueError("No Password Set!")
        
        ## Return our cached connection if we have one (this saves about 60ms 
        ## per transaction)
        db = None
        if self.cached_redshift is not None and use_cache:
            if self.cached_redshift_uses < self.cached_redshift_limit:
                ## If we have a cached redshift connection, see if it's still alive
                db = self.cached_redshift
                try:
                    t = db.get_tables()
                    self.cached_redshift_uses += 1
                except Exception:
                    db = None
                
        if db is None:
            ## Here if we don't have a cached connection
            try:
                port = int(self.redshift_port)
            except ValueError:
                raise ValueError("DB Port is not an integer!")
            
            ## Create our DB Handler
            pg.set_decimal(float)
            db = pg.DB(dbname=self.redshift_name,port=int(self.redshift_port),host=self.redshift_host,
                                                        user=self.redshift_username,
                                                        passwd=self.redshift_password)
            if use_cache:
                self.cached_redshift = db
                self.cached_redshift_uses = 1
        
        return db
    
    def rds(self,use_cache=None):
        """
        Method which returns a DB connection object to our RDS instance.
        
        :param bool use_cache: If True, we will attempt to use and cache our 
                            rds connection object. Default behavior is based on 
                            :py:var:`use_cached_objects` ivar
        
        :raises ValueError: If any necessary parameters are not established.
        :raises SubSystemUnavailableError: If we do not have module support for this service.

        
        :returns: :py:class:`pg.DB` object
        
        """
        
        if not DB_AVAILABLE:
            raise SubSystemUnavailableError("Subsystem (pg) to support RDS is not available!")
        
        if use_cache is None:
            use_cache = self.use_cached_objects
        
        ## Sanity check
        if self.rds_name is None:
            raise ValueError("DB not set!")
        elif self.rds_host is None:
            raise ValueError("DB Host not set!")
        elif self.rds_port is None:
            raise ValueError("No DB Port set!")
        elif self.rds_password is None:
            raise ValueError("No Password Set!")
        
        db = None
        if self.cached_rds is not None and use_cache:
            if self.cached_rds_uses < self.cached_rds_limit:
                ## If we have a cached rds connection, see if it's still alive
                db = self.cached_rds
                try:
                    t = db.get_tables()
                    self.cached_rds_uses += 1
                except Exception:
                    db = None
                
        if db is None:
            ## Here if we don't have a cached connection
            try:
                port = int(self.rds_port)
            except ValueError:
                raise ValueError("DB Port is not an integer!")
            
            ## Create our DB Handler
            pg.set_decimal(float)
            db = pg.DB(dbname=self.rds_name,port=int(self.rds_port),
                                                    host=self.rds_host,
                                                    user=self.rds_username,
                                                    passwd=self.rds_password)
            
            if use_cache:
                self.cached_rds = db
                self.cached_rds_uses = 1
            
        return db

    def kinesis(self,use_boto_auth=False):
        """
        Method which returns a kinesis connection object. This will return
        a boto3 Kinesis client if boto3 is available, otherwise it will
        fall back to boto2.
        
        :param bool use_boto_auth: If true, we will fall back to boto's 
                                internal auth routines.
        
        :raises :py:class:`ConnectionError`: If a proper connection object cannot be established
        :raises ValueEror: If required connection details are not set.
        
        :returns: :py:class:`boto.KinesisConnection` object if boto3 is unavailable,
                otherwise returns a boto3.client.Kinesis object.
        
        """
        
        conn = None
        auth = {}
        ## Sanity check
        if not use_boto_auth:
            if self.kinesis_access_key_id is None:
                raise ValueError("kinesis_access_key_id not set!")
            elif self.kinesis_secret_key is None:
                raise ValueError("kinesis_secret_key not set!")
            elif self.kinesis_region is None:
                raise ValueError("kinesis_region not set!")
        
            auth["aws_access_key_id"] = self.kinesis_access_key_id
            auth["aws_secret_access_key"] = self.kinesis_secret_key 
            if(self.use_temp_cred):
                if boto3 is None:
                    auth["security_token"] = self.kinesis_session_token
                else:
                    auth["aws_session_token"] = self.kinesis_session_token
        
        if boto3 is None:
            conn = boto.kinesis.connect_to_region(self.kinesis_region, **auth)
        else:
            conn = boto3.client("kinesis",region_name=self.kinesis_region,
                                                            **auth)           
        
        return conn
    
    def firehose(self, use_boto_auth = False):
        """
        :param bool use_boto_auth: If true, we will fall back to boto's 
                                internal auth routines.
        
        :raises :py:class:`ConnectionError`: If a proper connection object cannot be established
        :raises ValueEror: If required connection details are not set.
        
        :returns: :py:class: returns a boto3.client.Firehose object.
        """
        
        auth = {}
        if not use_boto_auth:
            if self.firehose_access_key_id is None:
                raise ValueError("firehose_access_key_id not set!")
            elif self.firehose_secret_key is None:
                raise ValueError("firehose_secret_key not set!")
            elif self.firehose_region is None:
                raise ValueError("firehose_region not set!")
            
            auth["aws_access_key_id"] = self.firehose_access_key_id
            auth["aws_secret_access_key"] = self.firehose_secret_key 
            if(self.use_temp_cred):
                auth["aws_session_token"] = self.firehose_session_token
        
        conn = boto3.client("firehose", region_name = self.firehose_region, **auth)
        return conn

    def ddb(self, use_boto_auth=False):
        """
        Method which returns a ddb client object.

        :param bool use_boto_auth: If true, we will fall back to boto's
                                internal auth routines.

        """

        conn = None

        ## Sanity check
        if not use_boto_auth:
            if self.dynamo_access_key_id is None:
                raise ValueError("dynamo_access_key_id not set!")
            elif self.dynamo_secret_access_key is None:
                raise ValueError("dynamo_secret_access_key not set!")
            elif self.dynamo_region is None:
                raise ValueError("dynamo_region not set!")

            auth = {"aws_access_key_id":     self.dynamo_access_key_id,
                    "aws_secret_access_key": self.dynamo_secret_access_key}
        else:
            auth = {}
        conn = boto3.client("dynamodb", region_name=self.dynamo_region, **auth)

        return conn

    def ddb_resource(self, use_boto_auth=False):
        """
         Method which returns a ddb resource object.

         :param bool use_boto_auth: If true, we will fall back to boto's
                                 internal auth routines.
        """

        conn = None

        ## Sanity check
        if not use_boto_auth:
            if self.dynamo_access_key_id is None:
                raise ValueError("dynamo_access_key_id not set!")
            elif self.dynamo_secret_access_key is None:
                raise ValueError("dynamo_secret_access_key not set!")
            elif self.dynamo_region is None:
                raise ValueError("dynamo_region not set!")

            auth = {"aws_access_key_id":     self.dynamo_access_key_id,
                    "aws_secret_access_key": self.dynamo_secret_access_key}
        else:
            auth = {}
        conn = boto3.resource("dynamodb", region_name=self.dynamo_region, **auth)
        
        return conn
    
    def sns(self, **kargs):
        """
         Method which returns a sns resource object.
        it will get credential from service side
        """
        sns = boto3.resource('sns', **kargs)
        return sns

    def load_karl_settings(self,karl,load_attributes=None):
        """
        Method to load settings from a separate karl instance. This is a simple
        instance wrapper around :py:func:`load_karl_settings`

        :param karl: The KARL instance to load settings from
        :type karl: :py:class:`KARL`
        :param load_attributes: List of property names to duplicate.
        :type load_attributes: (list) of strings
        """
        
        load_karl_settings(source=karl,target=self,load_attributes=None)
    
    def karl_settings_dict(self,karl=None):
        """
        Method to output our settings in dictionary format, this includes
        configuration details in addition to username and credentials.
        
        :param karl: KARL object to output
        
        :returns: Key=>Value dictionary
        
        """
        
        dict = {}
        
        if karl is None:
            karl = self
        
        load_attributes = ["redshift_host",
                           "redshift_port",
                            "redshift_name",
                            "redshift_username",
                            "redshift_password",
                            "rds_host",
                            "rds_port",
                            "rds_name",
                            "rds_username",
                            "rds_password",
                            "s3_bucketname",
                            "s3_access_key_id",
                            "s3_secret_key",
                            "kinesis_access_key_id",
                            "kinesis_secret_key",
                            "kinesis_region",
                            "dynamo_access_key_id",
                            "dynamo_secret_access_key",
                            "dynamo_region"]
        
        for attribute in load_attributes:
            their_value = None
            try:
                their_value = getattr(karl,attribute)
            except AttributeError:
                pass
                
            if their_value is not None:
                dict[attribute] = their_value

        return dict

    def setup(self,opconfig_name=DEFAULT_OPCONFIGNAME,**kwargs):
        
        """
        Method which will load values from the provided Apollo opconfig. In the
        event of an error (missing parameters, ODIN lookup failures), we will
        attempt to setup the object as much as we can.
        
        :param str opconfig_name: The OpConfig to load (default "KARL")
        :param str env: The environment to load ('Prod'|'Integ'|'Test')
        :param str redshift_host: The address of our database. If ommitted we will pull
                        from OpConfig
        :param str redshift_name: The name of our database. If ommitted we will pull
                        from OpConfig
        :param str redshift_port: The network port for our database. If ommitted we will 
                        pull from OpConfig
        :param str redshift_ms: The odin material set used to load RedShift credentials.
        :param str rds_host: The address of our database. If ommitted we will pull
                        from OpConfig
        :param str rds_name: The name of our database. If ommitted we will pull
                        from OpConfig
        :param str rds_port: The network port for our database. If ommitted we will 
                        pull from OpConfig
        :param str rds_ms: The odin material set used to load RDS credentials.
        :param str s3_bucketname: The s3 bucket name. If ommitted we will
                        pull from OpConfig
        :param str s3_ms: The odin material set used for s3 access. If ommitted we will
                        pull from OpConfig
        :param str kinesis_region: The kinesis region to use. If ommitted we will
                        pull from OpConfig
        :param str kinesis_ms: The odin material set used for kinesis access. If ommitted we will
                        pull from OpConfig
        :param opconfig: An OpConfig dictionary to use in lieu of looking up via
                        Apollo (overrides opconfig_name if provided)
        :type opconfig: dict of key=>value pairs
        
        :raises ValueError: If various configuration values could not be determined
        :raises bender.apollo_error.ApolloError: If the opconfig is not set on
            this environment.
        :raises CredentialRetrievalError: If we fail to load any credentials from odin
        
        """
        
        ## Destroy our cached connection objects
        self.cached_redshift = None
        self.cached_rds = None
        self.cached_s3_bucket = None
        self.cached_dynamo = None

        setup(scope=self,opconfig_name=opconfig_name,**kwargs)
                                            

class Benchmarker(object):
    """
    Class which provides benchmarking facilities.
    
    :Example:
    
        >>> from pykarl.tanium import TaniumCollector
        >>> import time
        >>>  
        >>> tc = TaniumCollector(arg_list=["--integ"])
        >>> tc.benchmark_start_timer("test")
        >>> tc.benchmark_counter("test","counter1",1)
        >>> tc.benchmark_counter("test","counter1",1)
        >>> tc.benchmark_counter("test","counter2",1)
        >>> tc.benchmark_counter("test","counter1",13)
        >>> time.sleep(1)
        >>> tc.benchmark_end_timer("test")
        >>> tc.benchmark_start_timer("test2")
        >>> time.sleep(3)
        >>> tc.benchmark_start_timer("test2","timer2")
        >>> time.sleep(2)
        >>> tc.benchmark_counter("test","counter1",1)
        >>> tc.benchmark_counter("test","counter1",1)
        >>> tc.benchmark_counter("test","counter2",1)
        >>> tc.benchmark_counter("test","counter1",13)
        >>> tc.benchmark_end_timer("test2")
        >>> tc.benchmark_end_timer("test2","timer2")
        >>> tc.benchmark_start_timer("test3")
        >>> tc.benchmark_start_timer("test3","timer2")
        >>> tc.benchmark_end_timer("test3")
        >>> tc.benchmark_end_timer("test3","timer2")
        
        ## Print our benchmarks
        >>> tc.print_benchmarks()
        Performance Details:
            test - 
                Duration:   1.00
                counter1:   30
                counter2:   2
            test3 - 
                Duration:   0.00
                timer.timer2:   0.00
            test2 - 
                Duration:   7.02
                timer.timer2:   2.01
    """
    
    logger = None
    benchmarks = {}
    args = None
    
    def __init__(self,args=None,logger=None):
        """
        Default Constructor 
        """
        
        self.args = args
        self.benchmarks = {}
        
        if logger is not None:
            self.logger = logger
        else:
            self.logger = logging.getLogger()
        
    def print_benchmarks(self,args=None):
        """
        Method which outputs our benchmark data to stdout.
        
        :param results: Our results as processed by process_tanium_results()
        :type results: (dict) key=>value dictionary
        :param args: Argument namespace as generated by argparse.parse()
        :type args: Namespace
        
        :returns: None
        
        """
        
        if args is None:
            args = self.args
        
        ## Specify a list of benchmark keys that we want to provide a summary for
        overview_keys = ["Runtime","TaniumProcessing","TaniumQuery","DBReport"]
        
        message = "Performance Overview:\n"
        for key in overview_keys:
            if key in self.benchmarks.keys():
                bm = self.benchmarks[key]
                message += "    %s:\t%.2f\n" % (key,bm["duration"])
        
        timers_reported = []
        message = "\nPerformance Details:\n"
        for key,bm in self.benchmarks.iteritems():
            
            if bm not in overview_keys:
                message += "    %s - \n" % key
                message += "        Duration:\t%.2f\n" % bm["duration"]
                for c_key,value in bm["counters"].iteritems():
                    message += "        %-10s\t%s\n" % ("%s:" % c_key,value)
                
                for t_key,t_value in bm["timers"].iteritems():
                    if t_key != key:
                        message += "        timer.%s\t%.2f\n" % ("%s:"
                                            % t_key,t_value["duration"])
        
        self.logger.info(message)
        
    def print_results(self,results,args=None):
        """
        Method which outputs our results to stdout. This should be overridden.
        
        :param results: Our results as processed by process_tanium_results()
        :type results: (dict) key=>value dictionary
        :param args: Argument namespace as generated by argparse.parse()
        :type args: Namespace
        
        :returns: None
        
        """
        
        if args is None:
            args = self.args
        
        raise Exception("print_results() not implemented! Script must override!")
        
    def _benchmark_template(self):
        """Methed which returns a dictionary which represents our 
        benchmark template."""
        
        template = {"duration" : 0,"timers" : {},"counters" : {}}
        return template
        
    def benchmark_start_timer(self,name,timer_name=None):
        """
         Method which starts a benchmark timer.
        
        :param str name: The name of the benchmark.
        :param str timer_name: The name of the timer.
        """
        
        if not name in self.benchmarks:
            self.benchmarks[name] = self._benchmark_template()
        
        if timer_name is None:
            timer_name = name
        
        self.logger.debug("%s started %s..." % (name,timer_name))
        
        self.benchmarks[name]["timers"][timer_name] = {
                                        "start_time" : datetime.datetime.utcnow(),
                                        "end_time" : None,
                                        "duration" : 0}

    def benchmark_end_timer(self,name,timer_name=None):
        """Method which ends a benchmark timer and computes
        it's duration.
        
        :param str name: The name of the benchmark
        :param str timer_name: The name of the timer.
        """
        
        if not timer_name:
            timer_name = name
        
        if name in self.benchmarks:
            for tname,timer in self.benchmarks[name]["timers"].iteritems():
                if tname == timer_name and timer["end_time"] is None:
                    timer["end_time"] = datetime.datetime.utcnow()
                    timer["duration"] = (timer["end_time"] -
                                        timer["start_time"]).total_seconds()
                    break
        
        self.logger.debug("%s finished %s..." % (name,timer_name))
        
        ## Update our durations
        duration = 0
        for tname,timer in self.benchmarks[name]["timers"].iteritems():
            duration += timer["duration"]
        
        self.benchmarks[name]["duration"] = duration
        
    def benchmark_counter(self,name,counter_name,increment=0,new_value=None):
        """Method which creates or increments a numeric counter for a benchmark.
        
        If the provided benchmark does not exist, it will be created
        
        :param str name: The name of the benchmark
        :param str counter_name: The name of the counter.
        :param int increment: The number to increment the current value
        :param int new_value: If specified, our benchmark counter will be this
        """
        
        if not name in self.benchmarks:
            self.benchmarks[name] = self._benchmark_template()
        
        if new_value is not None:
            self.benchmarks[name]["counters"][counter_name] = new_value
            return
        
        if not counter_name in self.benchmarks[name]["counters"].keys():
            self.benchmarks[name]["counters"][counter_name] = increment
        else:
            self.benchmarks[name]["counters"][counter_name] += increment

    def duration_for_timer(self,name,timer_name=None):
        """
        Method which returns the duration for the requested benchmark timer
                
        :param str name: The name of the benchmark
        :param str timer_name: The name of the timer.
        """
        
        if not timer_name:
            timer_name = name
        
        duration = None
        if name in self.benchmarks:
            for tname,timer in self.benchmarks[name]["timers"].iteritems():
                if tname == timer_name:
                    if "duration" in timer:
                        duration = timer["duration"]
                    elif "end_time" not in timer or timer["end_time"] is None:
                        end_time = datetime.datetime.utcnow()
                        duration = (end_time - timer["start_time"]).total_seconds()
                    break
        return duration
        
    def value_for_counter(self,name,counter_name):
        """
        Method which returns the value for the requested counter 
                
        :param str name: The name of the benchmark
        :param str counter_name: The name of the counter.
        
        :returns None: If no data is found
        :returns Number: Current value of counter
        """
        try:
            return self.benchmarks[name]["counters"][counter_name]
        except:
            return None
        

class KARLCollector(KARL,Benchmarker):
    """
    Class which provides several convenience methods for dealing with
    scripts that archive data to/from KARL.
    """
    
    env_config = None
    env_filepath = None
    
    parser = None
    args = None
    
    logger = None
    
    supports_test_env = True            #: Flag to denote if this collector supports a "test" stack
    supports_integ_env = True           #: Flag to denote if this collector supprots an "integ" stack
    supports_prod_env = True            #: Flag to denote if this collector supports a "prod" stack
    
    def __init__(self,env_config=None,
                            env_filepath=None,
                            arg_list=None,
                            s3key_prefix=None,
                            s3key_suffix=".txt",
                            karl=None):
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
        
        KARL.__init__(self,karl=karl)
        
        self.env_config = {}
        self.parser = None
        self.args = None
        self.logger = None
        
        self.s3key_prefix = s3key_prefix
        self.s3key_suffix = s3key_suffix
        
        if env_filepath is not None and os.path.exists(env_filepath):
            self.env_filepath = env_filepath
        
        if env_config is not None:
            self.merge_config(env_config)
        
        ## Setup our parser and parse our arguments
        self.setup_parser()
        self.args = self.build_args(arg_list=arg_list)
        
        self.configure_logging(args=self.args)
        
        Benchmarker.__init__(self,args=self.args)
            
    def run(self,karl=None):
        """
        Our Primary CLI subroutine to process passed arguments, take appropriate
        action, and return an integer result code.
        
        :param karl: Karl object to load settings from.
        
        :returns: (int) exit code
        """
        
        self.benchmark_start_timer("Runtime")
        
        ## Parse our arguments        
        if self.args is None:
            self.build_args()
            
        args = self.args
        self.setup(args=args)
        
        
        ## Intro logging
        logger = logging.getLogger()
        
        logger.info("Executing...")
        
        ####  Do Work Here ####

        #### End Work Here ####
        
        self.benchmark_end_timer("Runtime")
        
        ## Report our Results
        if args.stats or args.verbose > 0 or args.no_log:
            self.print_benchmarks(args=args)
    
    def configure_logging(self,args=None):
        """
        Method which will configure our logging behavior based on the 
        passed arguments. If args is ommited we will consult self.args.
        
        :param args: Arguments to consult to determine our loglevel
        :type args: argparse.Namespace object
        """
                
        if args is None:
            args = self.args
        
        log_level = logging.INFO
        log_format = "%(message)s"
        
        if args.verbose > 3:
            log_format = "%(asctime)s %(levelname)s File:%(filename)s:%(lineno)d %(message)s"            
            log_level = logging.NOTSET
        elif args.verbose > 2:
            log_format = "%(asctime)s %(levelname)s %(message)s"            
            log_level = 5
        elif args.verbose > 1:
            log_format = "%(asctime)s %(levelname)s %(message)s"            
            log_level = logging.DEBUG
        elif args.verbose == 1:
            log_format = "%(asctime)s %(message)s"
        
        logging.basicConfig(format=log_format,level=log_level)
        
        if args.verbose < 4:
            logger = logging.getLogger("boto")
            logger.setLevel(logging.INFO)
                
        logger = logging.getLogger(self.__class__.__name__)
        
        ## Warn if credential possible exposure
        if (log_level < 5):
            logger.critical("WARNING: log_level:{} !!!!CREDENTIALS MAY BE EXPOSED IN PROGRAM OUTPUT!!!!".format(log_level))
        
    def setup_parser(self):
        """
        Subroutine where we establish our Command line arguments. Populates
        self.parser. You may wish to override this if you want to implement
        any custom CLI arguments.
        """
        
        ## If we have a configured parser. Use it
        parser = self.parser
        
        if parser is None:
            parser = argparse.ArgumentParser(
            description='''Query RDS, export, process, and post data to RedShift''',
            formatter_class=argparse.RawTextHelpFormatter)
        ## Define our Arguments
        parser.add_argument("-e", "--env", default="prod",
                        choices=["prod","integ","test"],
                        help="Specify our environment")
        parser.add_argument("--integ",default=False,action="store_true",
            help=("Run against our integ environment (same as --env=integ)"))
        parser.add_argument("--test",default=False,action="store_true",
            help=("Run against our test environment (same as --env=test)"))
        parser.add_argument("-n","--no-log",default=False,action="store_true",
            help=("If passed, we will output report counts but "
                                            "will not submit records to our DB."))
        parser.add_argument("-v", "--verbose", action="count", default=0,
            help="Increase output verbosity, can be passed multiple times")
        parser.add_argument("--s3-materialset",
            help="The odin material set to use for S3 credential lookup.")
        parser.add_argument("--s3-bucketname",
            help="The bucket name to use for S3 credential lookup.")
        parser.add_argument("--s3file",help="Provide a filepath or s3 filename "
                        "containing CSV results to use as the source "
                        "for our submission (bypasses query and processing)")
        parser.add_argument("--rds-materialset",
            help="The odin material set to use for RDS credential lookup.")
        parser.add_argument("--rds-host",help="The fqdn used to access the "
            "RDS database.")
        parser.add_argument("--rds-port",help="The Port used to access the "
            "RDS database.")
        parser.add_argument("--rds-name",help="The RDS Database name")
        parser.add_argument("--redshift-materialset",
            help="The odin material set to use for Redshift credential lookup.")
        parser.add_argument("--redshift-host",help="The fqdn used to access the "
            "RedShift database.")
        parser.add_argument("--redshift-port",help="The Port used to access the "
            "RedShift database.")
        parser.add_argument("--redshift-name",help="The RedShift database name")
        parser.add_argument("--kinesis-region",help="The region hosting "
                                                    "the Kinesis stream.")
        parser.add_argument("--kinesis-materialset",help="The odin material "
                                            "set hosting Kinesis credentials.")
        parser.add_argument("--stats",help="Output performance data.",
                                                        action="store_true",
                                                        default=False)
                
        parser.add_argument("--karl-opconfig",
            default=DEFAULT_OPCONFIGNAME,
            help="The Apollo OpConfig to use for various values. Options explicitly provided via CLI args or in env_config file  will override opconfig values.")
        self.parser = parser
    
    def build_args(self,arg_list=None,env_config=None,env_filepath=None,
                                            opconfig=None,opconfig_name=None):
        """
        Method to setup runtime base on our environment configurations.
        
        :param arg_list: A list of arguments to process.
        :type arg_list: (list)
        :param env_config: A dictionary of configuration elements, keyed by
            environment name.
        :type env_config: (dict)
        """
        
        logger = logging.getLogger()
        
        ## Normalize inputs
        if not env_config:
            env_config = self.env_config
            
        if not env_filepath:
            env_filepath = self.env_filepath
            
        ## Merge in CLI argument data
        args = None
        if arg_list is not None:
            args = self.parser.parse_args(arg_list)
        else:
            args = self.parser.parse_args()
        
        ## Resolve our active env
        if not "env" in args.__dict__.keys():
            args.env = DEFAULT_ENV
        if args.integ:
            args.env = "integ"
        if args.test:
            args.env = "test"
        
        ## If we have a config file, merge in it's values
        if env_filepath is not None and os.path.exists(env_filepath):
            try:
                json_data = open(env_filepath).read()
                file_env_config = json.loads(json_data)
                logger.log(5,"build_args(): Importing file env_config:{}".format(file_env_config))
                args = self.merge_args_from_envconfig(args=args,data=file_env_config)
            except Exception as exp:
                self.logger.error("An error occurred loading our configuration!"
                            " Error:%s" % exp)
        
        ## Merge in config data
        if env_config:
            logger.log(5,"build_args(): Importing env_config:{}".format(env_config))
            args = self.merge_args_from_envconfig(args=args,data=env_config)

        ## Merge in OPConfig data
        if opconfig or opconfig_name:
            if opconfig:
                logger.log(5,"build_args(): Importing opconfig:{}".format(opconfig))
            elif opconfig_name:
                logger.log(5,"build_args(): Importing opconfig_name:{}".format(opconfig_name))
            else:
                logger.log(5,"build_args(): Importing from default opconfig")
                                
        try:
            args = self.merge_args_from_opconfig(args=args,
                                                    opconfig_name=opconfig_name,
                                                    opconfig=opconfig)
        except Exception as exp:
            logger.error("Failed to load opconfig: {}".format(exp.message))
        
        ## Proccess our Arguments, use our in-script config as a baseline,
        ## override with any explicitely passed arguments        
        my_default_env_config = None
        try:
            my_default_env_config = self.default_env_config
            if my_default_env_config:
                logger.log(5,"build_args(): Importing module:{} default config:{}".format(self.__class__.__name__,my_default_env_config))
                args = self.merge_args_from_envconfig(args=args,
                                                    data=my_default_env_config)
        except AttributeError:
            logger.log(5,"build_args(): module:{} has no default_env_config".format(self.__class__.__name__))
        except NameError:
            pass
        
        ## Proccess our Arguments, use our in-script config as a baseline,
        ## override with any explicitely passed arguments
        try:
            logger.log(5,"build_args(): clientengmetric default config:{}".format(default_env_config))
            if default_env_config and default_env_config != my_default_env_config:
                logger.log(5,"build_args(): Importing clientengmetric default config:{}".format(default_env_config))
                args = self.merge_args_from_envconfig(args=args,
                                    data=default_env_config)
        except AttributeError:
            pass
        except NameError:
            pass
        
        return args
        
    def setup(self,env=None,redshift_host=None,redshift_name=None,redshift_port=None,
                            redshift_ms=None,rds_host=None,
                            rds_name=None,rds_port=None,
                            rds_ms=None,s3_bucketname=None,s3_ms=None,
                            args=None,
                            opconfig=None,
                            opconfig_name=None):
        """
        Method which sets up our object by loading the appropriate
        credentials from odin.
        """
                            
        if args is None:
            args = self.args
        
        if redshift_host is None:
            try:
                redshift_host = args.redshift_host
            except AttributeError:
                pass
            
        if redshift_name is None:
            try:
                redshift_name = args.redshift_name
            except AttributeError:
                pass

        if redshift_port is None:
            try:
                redshift_port = args.redshift_port
            except AttributeError:
                pass
        if redshift_ms is None:
            try:
                redshift_ms = args.redshift_materialset
            except AttributeError:
                pass
                
        if rds_host is None:
            try:
                rds_host = args.rds_host
            except AttributeError:
                pass
                
        if rds_name is None:
            try:
                rds_name = args.rds_name
            except AttributeError:
                pass

        if rds_port is None:
            try:
                rds_port = args.rds_port
            except AttributeError:
                pass
                
        if rds_ms is None:
            try:
                rds_ms = args.rds_materialset
            except AttributeError:
                pass
        
        if s3_bucketname is None:
            try:
                s3_bucketname = args.s3_bucketname
            except AttributeError:
                pass
        
        if s3_ms is None:
            try:
                s3_ms = args.s3_materialset
            except AttributeError:
                pass
        
        if env is None:
            try:
                env = args.env
            except AttributeError:
                pass
        
        ## Retrieve our credentials from odin
        setup(scope=self,env=env,redshift_host=redshift_host,
                            redshift_name=redshift_name,
                            redshift_port=redshift_port,
                            redshift_ms=redshift_ms,
                            rds_host=rds_host,
                            rds_name=rds_name,
                            rds_port=rds_port,
                            rds_ms=rds_ms,
                            s3_bucketname=s3_bucketname,
                            s3_ms=s3_ms,
                            opconfig=opconfig,
                            opconfig_name=opconfig_name)
                            
        
    def merge_args_from_envconfig(self,args=None,data=None):
        """
        Method which will merge args from the provided env_config
        dataset. This will generally be a dataset represented nested 
        dictionaries, keyed by env name.
        
        :param args: Object representing input args
        :type args: py:class:`argparse.Namespace`
        :param data: Configuration data
        :type data: (dict) key=>value data.
        
        :returns py:class:`argparse.Namespace`: Modified argument data
        
        """
        
        ## Sanitize inputs
        if args is None:
            args = self.args
        
        ## Resolve our active env
        if not "env" in args.__dict__.keys():
            args.env = DEFAULT_ENV
        if self.supports_integ_env and args.integ:
            args.env = "integ"
        if self.supports_test_env and args.test:
            args.env = "test"
        
        my_config = None
        if data:
            for env_key in data.keys():
                if env_key.lower() == args.env.lower():
                    my_config = data[env_key]
                    args = self.merge_args_from_dict(args=args,
                                                    data=my_config)
        if "*" in data.keys():
            my_config = data["*"]
            args = self.merge_args_from_dict(args=args,
                                                    data=my_config)
        
        return args
        
    def merge_args_from_dict(self,args=None,data=None):
        """
        Method which will merge args with the provided dictionary 
        (a key=>value dict).
        Data established in args will be retained, data missing will be
        supplanted with values present in env_config. You will
        need to overwrite this if you want env_config support for your
        metric
        
        :param args: Key=>Value dictionary of values
        :type args: Namespace object as returned by argparse.parse_args()
        :param data: Key=>Value dictionary of values
        :type data: dict
        
        :returns py:class:`argparse.Namespace`: Modified argument data
        """
        
        ## Sanitize inputs
        if args is None:
            args = self.args
        
        ## Resolve our active env
        if not "env" in args.__dict__.keys():
            args.env = DEFAULT_ENV
        if self.supports_integ_env and args.integ:
            args.env = "integ"
        if self.supports_test_env and args.test:
            args.env = "test"
        
        if data:
            for key in args.__dict__.keys():
                if args.__dict__[key] is not None:
                    continue
                
                altkey = key.replace("_","-")
                if key in data and data[key] is not None:
                    setattr(args,key,data[key])
                elif altkey in data and data[altkey] is not None:
                    setattr(args,key,data[altkey])
            
        return args
        
    def merge_args_from_opconfig(self,mapping=None,args=None,
                                                        opconfig_name=None,
                                                        opconfig=None):
        """
        Method for importing opconfig data. This will only populate
        values into the args namespace which are not already populated.
        
        :param mapping: A dictionary mapping 
        :param args: Arguments obj to populate (defaults to self.args)
        :type args: argsparse.Namespace object
        :param str opconfig_name: Apollo opconfig name to import
        :param opconfig: An OpConfig dictionary to use in lieu of looking up via
                    Apollo (overrides opconfig_name if provided)
        :type opconfig: dict of key=>value pairs
        
        :raises apollo.ApolloError: If opconfig cannot be looked up.
        
        :returns py:class:`argparse.Namespace`: Modified argument data
        
        """
        
        logger = logging.getLogger()
        
        ## Sanitize inputs
        if args is None:
            args = self.args
        
        ## Resolve our active env
        if not "env" in args.__dict__.keys():
            args.env = DEFAULT_ENV
        if self.supports_integ_env and args.integ:
            args.env = "integ"
        if self.supports_test_env and args.test:
            args.env = "test"
        
        if opconfig_name is None:
            try:
                if args.opconfig is not None:
                    opconfig_name = args.opconfig
            except AttributeError:
                pass
        ## Convert our opconfig params into actual objects
        
        ## fetch our opconfig
        if opconfig is None and opconfig_name is not None:
            opconfig = apollo.opconfig(opconfig_name)
        elif opconfig is None:
            ## No opconfig data, nothing to do
            logger.debug("No opconfig source found to import...")
            return args
        
        
        opconfig_mapping = {"mws_marketplace" : "marketplace",
                            "mws_materialset" : "odinmaterialset",
                        }
                        
        if mapping:
            opconfig_mapping.update(mapping)
                
        the_opconfig = opconfig
        for arg_key,oc_key in opconfig_mapping.iteritems():
            arg_value = None
            oc_value = None
            try:
                arg_value = getattr(args,arg_key.replace("-","_"))
            except AttributeError:
                pass
            
            if arg_value is None:
                oc_value = opconfig_value(oc_key.lower(),
                                                env=args.env,
                                                opconfig=the_opconfig)
                
                if not oc_value:
                    logger.warning("Key:'%s' is not defined in OpConfig:'%s'"
                                        % (arg_key,opconfig_name))
                else:
                    setattr(args,arg_key.replace("-","_"),oc_value)
                    logger.debug("Loaded value:'%s' for key:'%s' from OpConfig:'%s' (env:%s)"
                            % (oc_value,arg_key,opconfig_name,args.env))
        
        return args
    
    def merge_config(self,env_config):
        """
        Method to merge our current config with the passed config. We will 
        iterate through two dimensions (top level keyed by AirWatch env name)
        
        :param env_config: Provide configuration data.
        :type env_config: (dict) Dictionary object 
        """
    
        new_config = merge_config(self.env_config,env_config)
        
        self.env_config = new_config


class KARLActionStreamMapper(object):
    """
    Class that is used to map SubjectAreas or EventTypes to their respective 
    Kinesis streams.
    
    :Example: 
        >>> map = { "MyAction" : "my_action_stream",
                    "default" : "default_stream",
                    }
        >>> sm = KARLActionStreamMapper(map=map)
        >>> sm.stream_name_for_key("MyAction")
        "my_action_stream"
        >>> sm.stream_name_for_key("blah")
        "default_stream"
        
    """
    
    def __init__(self,map=None):
        """
        Constructor
        """
        
        if map is not None:
            self.map = map
        else:
            self.map = {"default" : "default"}
    
    def stream_name_for_event(self,evt):
        """
        Returns the stream name for the given event.
        
        :param evt: The event to lookup.
        :type evt: :py:class:`event.Event`
        
        :returns str: stream name
        """
        
        stream_name = None
        
        if evt.type:
            stream_name = self.stream_name_for_key(evt.type)
        
        if not stream_name and evt.subject_area:
            stream_name = self.stream_name_for_key(evt.subject_area)
            
        if not stream_name:
            stream_name = self.stream_name_for_key("default")
            
        return stream_name
        
    def stream_name_for_key(self,key):
        """
        Returns the stream name for the given key.
        
        :param str key: The key to lookup.
        
        :returns str: A stream name if key is mapped. 
        :returns None: If no stream name could be determined.
        
        """
        
        stream_name = None
        
        for mapped_key in self.map.keys():
            if mapped_key.lower() == key.lower():
                stream_name = self.map[mapped_key]
                break
                
        return stream_name
        
    def load_from_file(self,filepath):
        """
        Method to load our routing data from the provided file
        
        :param str filepath: Path to the file to load.
        
        :raises IOError, OSError, ValueError: 
        
        """
        
        with open(filepath,"r") as fh:
            json_data = fh.read()
            self.map = json.loads(json_data)
        
        
    def save_to_file(self,filepath):
        """ 
        Method to save our state to the provided file
        
        :param str filepath: Path to the file to load.
        
        :raises IOError, OSError, ValueError: 
        
        """
        with open(filepath,"w") as fh:
            json_string = json.dumps(self.map)
            fh.write(json_string)
            
        
    def map_is_loaded(self):
        """ 
        Method which returns whether our Map is loaded.
        
        :returns bool: True if we have a loaded map.
        """
        
        if self.map:
            return True
        else:
            return False
        

#MARK: Module Functions
def setup(scope=KARL,opconfig_name=None,env=None,
                                            redshift_host=None,
                                            redshift_name=None,
                                            redshift_port=None,
                                            redshift_ms=None,
                                            rds_host=None,
                                            rds_name=None,
                                            rds_port=None,
                                            rds_ms=None,
                                            s3_bucketname=None,
                                            s3_ms=None,
                                            kinesis_ms=None,
                                            kinesis_region=None,
                                            dynamo_ms=None,
                                            dynamo_region=None,
                                            opconfig=None,
                                            required_attributes=None,
                                            **kwargs):
    """
    Method which will load values from the provided Apollo opconfig. In the
    event of an error (missing parameters, ODIN lookup failures), we will
    attempt to setup the object as much as we can.
    
    :param object scope: The scope where we will load our config. The default scope
        is the KARL class. Subsequent KARL decendents would
        then inherit the configuration. A specific instance can be passed
        for this param to load in a more refined context.
    :param str opconfig_name: The OpConfig to load (default "KARL")
    :param str env: The environment to load ('Prod'|'Integ'|'Test')
    :param str redshift_host: The address of our database. If ommitted we will pull
                    from OpConfig
    :param str redshift_name: The name of our database. If ommitted we will pull
                    from OpConfig
    :param str redshift_port: The network port for our database. If ommitted we will 
                    pull from OpConfig
    :param str rds_host: The address of our database. If ommitted we will pull
                    from OpConfig
    :param str rds_name: The name of our database. If ommitted we will pull
                    from OpConfig
    :param str rds_port: The network port for our database. If ommitted we will 
                    pull from OpConfig
    :param str rds_ms: The odin material set used for RDS access. If ommitted we will
                    pull from OpConfig
    :param str s3_bucketname: The s3 bucket name. If ommitted we will
                    pull from OpConfig
    :param str s3_ms: The odin material set used for s3 access. If ommitted we will
                    pull from OpConfig
    :param str kinesis_region: The kinesis region to use. If ommitted we will
                    pull from OpConfig
    :param str kinesis_ms: The odin material set used for kinesis access. If ommitted we will
                    pull from OpConfig
    :param opconfig: An OpConfig dictionary to use in lieu of looking up via
                    Apollo (overrides opconfig_name if provided)
    :type opconfig: dict of key=>value pairs
    :param required_attributes: List of attributes which must be setup or we
                    will raise a SetupError. 
    
    :raises ValueError: If various configuration values could not be determined
    :raises bender.apollo_error.ApolloError: If the opconfig is not set on
        this environment.
    :raises CredentialRetrievalError: If we fail to load any credentials from odin
    
    """
    
    if not opconfig_name:
        opconfig_name = DEFAULT_OPCONFIGNAME
    
    logger = logging.getLogger()
    
    logger.debug("Running Setup for PyKARL scope:'{}' env:'{}' opconfig:'{}' ".format(scope,env,opconfig_name))
    
    ## fetch our opconfig
    if opconfig is None and APOLLO_AVAILABLE:
        try:
            oc = apollo.opconfig(opconfig_name)
        except apollo.ApolloError as exp:
            logger.warning("Failed to initialize Apollo. Error: %s", exp)
            oc = {}
    elif opconfig is None and not APOLLO_AVAILABLE:
        logger.debug("Apollo is not availble, cannot load OpConfig data!")
        oc = {}
    else:
        oc = opconfig
    
    default_opconfig_index = 0
    if env and env.lower() == "integ":
        default_opconfig_index = 1
    elif env and env.lower() == "test":
        default_opconfig_index = 2
        
    ## Resolve all of our variables
    if not redshift_host:
        try:
            i = default_opconfig_index
            the_list = oc["redshiftdbhost"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            redshift_host = the_list[i].strip()
        except KeyError:
            pass
    
    if not redshift_name:
        try:
            i = default_opconfig_index
            the_list = oc["redshiftdbname"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            redshift_name = the_list[i].strip()
        except KeyError:
            pass
    
    if not redshift_port:
        try:
            i = default_opconfig_index
            the_list = oc["redshiftdbport"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            redshift_port = the_list[i].strip()
        except KeyError:
            pass
    
    if not redshift_ms:
        try:
            i = default_opconfig_index
            the_list = oc["redshiftmaterialset"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            redshift_ms = the_list[i].strip()
        except KeyError:
            pass
    
    if not rds_host:
        try:
            i = default_opconfig_index
            the_list = oc["rdsdbhost"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            rds_host = the_list[i].strip()
        except KeyError:
            pass
    
    if not rds_name:
        try:
            i = default_opconfig_index
            the_list = oc["rdsdbname"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            rds_name = the_list[i].strip()
        except KeyError:
            pass
    
    if not rds_port:
        try:
            i = default_opconfig_index
            the_list = oc["rdsdbport"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            rds_port = the_list[i].strip()
        except KeyError:
            pass
    
    if not rds_ms:
        try:
            i = default_opconfig_index
            the_list = oc["rdsmaterialset"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            rds_ms = the_list[i].strip()
        except KeyError:
            pass
    
    if not s3_bucketname:
        try:
            i = default_opconfig_index
            the_list = oc["s3bucket"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            s3_bucketname = the_list[i].strip()
        except KeyError:
            pass
            
    if not s3_ms:
        try:
            i = default_opconfig_index
            the_list = oc["s3credentialmaterialsetname"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            s3_ms = the_list[i].strip()
        except KeyError:
            pass
            
    if not kinesis_region:
        try:
            i = default_opconfig_index
            the_list = oc["kinesisregion"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            kinesis_region = the_list[i].strip()
        except KeyError:
            pass
    
    if not kinesis_ms:
        try:
            i = default_opconfig_index
            the_list = oc["kinesismaterialset"].split(",")
            while i >= len(the_list) and i >= 0:
                i -= 1
            kinesis_ms = the_list[i].strip()
        except KeyError:
            pass

    if not dynamo_region:
        try:
            i = default_opconfig_index
            the_list = oc["dynamodbregion"].split(",")
            while i >= len(the_list) and i > 0:
                i -= 1
            dynamo_region = the_list[i].strip()
        except KeyError:
            pass

    if not dynamo_ms:
        try:
            i = default_opconfig_index
            the_list = oc["dynamodbmaterialset"].split(",")
            while i >= len(the_list) and i > 0:
                i -= 1
            dynamo_ms = the_list[i].strip()
        except KeyError:
            pass
    ## Throw errors on missing criticals    
    if required_attributes is None and DB_AVAILABLE:
        required_attributes = ["redshift_host","redshift_port","redshift_name",
                                        "rds_host","rds_port","rds_name",
                                        "s3_bucketname","s3_ms",
                                        "kinesis_region","kinesis_ms", "dynamo_region", "dynamo_ms"]
    elif required_attributes is None and not DB_AVAILABLE and ODIN_AVAILBLE:
        required_attributes = ["s3_bucketname","s3_ms",
                                        "kinesis_region","kinesis_ms"]
    elif required_attributes is None:
        required_attributes = ["s3_bucketname","kinesis_region"]
    
    missing_values = []
    if not redshift_host:
        missing_values.append("redshift_host")
    else:
        scope.redshift_host = redshift_host
        logger.debug(" - setup redshift_host:{}".format(redshift_host))
    
    if not redshift_port:
        missing_values.append("redshift_port")
    else:
        scope.redshift_port = redshift_port
        logger.debug(" - setup redshift_port:{}".format(redshift_port))
    
    if not redshift_name:
        missing_values.append("redshift_name")
    else:
        scope.redshift_name = redshift_name
        logger.debug(" - setup redshift_name:{}".format(redshift_name))
    
    if not rds_host:
        missing_values.append("rds_host")
    else:
        scope.rds_host = rds_host
        logger.debug(" - setup rds_host:{}".format(rds_host))
    
    if not rds_port:
        missing_values.append("rds_port")
    else:
        scope.rds_port = rds_port
        logger.debug(" - setup rds_port:{}".format(rds_port))
    
    if not rds_name:
        missing_values.append("rds_name")
    else:
        scope.rds_name = rds_name
        logger.debug(" - setup rds_name:{}".format(rds_name))
    
    if not s3_bucketname:
        missing_values.append("s3_bucketname")
    else:
        scope.s3_bucketname = s3_bucketname
        logger.debug(" - setup s3_bucketname:{}".format(s3_bucketname))
    
    if not s3_ms:
        missing_values.append("s3_ms")
    else:
        scope.s3_bucketname = s3_bucketname
        logger.debug(" - setup s3_bucketname:{}".format(s3_bucketname))
    
    if not kinesis_region:
        missing_values.append("kinesis_region")
    else:
        scope.kinesis_region = kinesis_region
        logger.debug(" - setup kinesis_region:{}".format(kinesis_region))
    
    if not kinesis_ms:
        missing_values.append("kinesis_ms")
    else:
        scope.kinesis_ms = kinesis_ms
        logger.debug(" - setup kinesis_ms:{}".format(kinesis_ms))

    if not dynamo_region:
        missing_values.append("dynamo_region")
    else:
        scope.dynamo_region = dynamo_region
        logger.debug(" - setup dynamo_region:{}".format(dynamo_region))

    if not dynamo_ms:
        missing_values.append("dynamo_ms")
    else:
        scope.dynamo_ms = dynamo_ms
        logger.debug(" - setup dynamo_ms:{}".format(dynamo_ms))

    if ODIN_AVAILBLE:
        load_credentials(scope=scope,rds_ms=rds_ms,redshift_ms=redshift_ms,
                                                    s3_ms=s3_ms,
                                                    kinesis_ms=kinesis_ms, dynamo_ms=dynamo_ms)
    
    missing_required_attributes = []
    if len(missing_values) > 0 and required_attributes:
        for required in required_attributes:
            for missing in missing_values:
                if  required.lower() == missing.lower():
                    missing_required_attributes.append(missing)
                elif missing.split("_")[0].lower() == required.lower():
                    missing_required_attributes.append(missing)
                    
        if len(missing_required_attributes) > 0:
            raise SetupError("Failed to fully complete setup. Several parameters "
                            "were not specified and could not be resolved: %s"
                                        % ", ".join(missing_required_attributes),
                                missing_required_attributes)


def load_credentials(scope=KARL,rds_ms=None, redshift_ms=None, s3_ms=None, kinesis_ms=None, dynamo_ms=None):
                     
    """
    Method which will load values for the provided material sets.
    
    :param object scope: The scope where we will load our credentials. The 
        default scope is the KARL class. Subsequent KARL decendents would
        then inherit the configuration. A specific instance can be passed
        for this param to load in a more refined context.
    
    :param str rds_ms: The odin material set used for rds access.
    :param str s3_ms: The odin material set used for s3 access.
    :param str kinesis_ms: The odin material set used for kinesis access.
    
    :raises ValueError: If various configuration values could not be determined
    :raises bender.apollo_error.ApolloError: If the opconfig is not set on
        this environment.
    :raises CredentialRetrievalError: If we fail to load any credentials from odin
    
    """
    
    if not ODIN_AVAILBLE:
        raise SubSystemUnavailableError("Odin is not available, cannot load creds!")
    
    logging.log(15,"Loading Credentials from Odin...")
    
    credential_retrieval_failures = {}
    logger = logging.getLogger()
    
    if redshift_ms:
        logger.debug("- loading Redshift Credentials using odin ms:{}".format(redshift_ms))
        try:
            scope.redshift_username = pyodinhttp.odin_material_retrieve(
                                                    _materialName=redshift_ms,
                                                    _materialType="Principal")
            scope.redshift_password = pyodinhttp.odin_material_retrieve(
                                                    _materialName=redshift_ms,
                                                    _materialType="Credential")
        except (pyodinhttp.OdinDaemonError, pyodinhttp.OdinOperationError) as e:
            credential_retrieval_failures["redshift"] =  { "exp": e,
                                                            "ms": redshift_ms,
                                                            }
    
    if rds_ms:
        logger.debug("- loading RDS Credentials using odin ms:{}".format(rds_ms))
        try:
            scope.rds_username = pyodinhttp.odin_material_retrieve(
                                                    _materialName=rds_ms,
                                                    _materialType="Principal")
            scope.rds_password = pyodinhttp.odin_material_retrieve(
                                                    _materialName=rds_ms,
                                                    _materialType="Credential")
        except (pyodinhttp.OdinDaemonError, pyodinhttp.OdinOperationError) as e:
            credential_retrieval_failures["rds"] = { "exp": e,
                                                            "ms": rds_ms,
                                                            }    
    if s3_ms:
        logger.debug("- loading S3 Credentials using odin ms:{}".format(s3_ms))
        try:
            scope.s3_access_key_id = pyodinhttp.odin_material_retrieve(
                                                    _materialName=s3_ms,
                                                    _materialType="Principal")
            scope.s3_secret_key = pyodinhttp.odin_material_retrieve(
                                                    _materialName=s3_ms,
                                                    _materialType="Credential")
        except (pyodinhttp.OdinDaemonError, pyodinhttp.OdinOperationError) as e:
            credential_retrieval_failures["s3"] = { "exp": e,
                                                            "ms": s3_ms,
                                                            } 
    
    if kinesis_ms:
        logger.debug("- loading Kinesis Credentials using odin ms:{}".format(kinesis_ms))
        try:
            scope.kinesis_access_key_id = pyodinhttp.odin_material_retrieve(
                                                    _materialName=kinesis_ms,
                                                    _materialType="Principal")
            scope.kinesis_secret_key = pyodinhttp.odin_material_retrieve(
                                                    _materialName=kinesis_ms,
                                                    _materialType="Credential")
        except (pyodinhttp.OdinDaemonError, pyodinhttp.OdinOperationError) as e:
            credential_retrieval_failures["kinesis"] = { "exp": e,
                                                            "ms": kinesis_ms,
                                                            }
    if dynamo_ms:
        logger.debug("- loading Dynamo Credentials using odin ms:{}".format(dynamo_ms))
        try:
            scope.dynamo_access_key_id = pyodinhttp.odin_material_retrieve(
                                                _materialName=dynamo_ms,
                                                _materialType="Principal")
            scope.dynamo_secret_access_key = pyodinhttp.odin_material_retrieve(
                                                _materialName=dynamo_ms,
                                                _materialType="Credential")
        except (pyodinhttp.OdinDaemonError, pyodinhttp.OdinOperationError) as e:
            credential_retrieval_failures["dynamo"] = {"exp": e, "ms": dynamo_ms,}

    if len(credential_retrieval_failures) > 0:
        raise CredentialRetrievalError(failures=credential_retrieval_failures)
  
def load_karl_settings(source,target,load_attributes=None):
    """
    Method to load settings from a one karl instance to another

    :param karl: The KARL instance to load settings from
    :type karl: :py:class:`KARL`

    """
    
    if load_attributes is None:
        load_attributes = ["redshift_host",
                           "redshift_port",
                            "redshift_name",
                            "redshift_username",
                            "redshift_password",
                            "rds_host",
                            "rds_port",
                            "rds_name",
                            "rds_username",
                            "rds_password",
                            "s3_bucketname",
                            "s3_access_key_id",
                            "s3_secret_key",
                            "kinesis_access_key_id",
                            "kinesis_secret_key",
                            "kinesis_region",
                            "kinesis_session_token",
                            "use_temp_cred",
                            "dynamo_access_key_id",
                            "dynamo_secret_access_key",
                            "dynamo_region"
                           ]
    
    for attribute in load_attributes:
        their_value = None
        try:
            their_value = getattr(source,attribute)
        except AttributeError:
            pass
        
        if their_value is not None:
            setattr(target,attribute,their_value)

def merge_config(config,config2):
    """
    Method to merge values of the two passed configs. We will 
    iterate through two dimensions (top level keyed by env name),entries
    which pre-exist in :py:arg:`config` will not be overridden. 
    
    :param env_config: Provide configuration data.
    :type env_config: (dict) Dictionary object
    
    :Example:
        .. codeblock: python
            env_config1 = {
                "prod" : {
                    "mws-hostname" : "prodservice.amazon.com",
                },
                "*" : {
                    "mws-marketplace" : "Corp",
                    "mws-opconfig" : "MWS",
                }
            }
            env_config2 = {
                "integ" : {
                    "mws-hostname" : "integservice.amazon.com",
                },
                "*" : {
                    "mws-period" : "OneHour",
                }
            }
            
            ## Merge our inputs and report
            result_config = self.merge_config(config1,config2)
            ''' Results:
            result_config = {
                "prod" : {
                    "mws-hostname" : "prodservice.amazon.com",
                },
                "integ" : {
                    "mws-hostname" : "integservice.amazon.com",
                },
                "*" : {
                    "mws-period" : "OneHour",
                    "mws-marketplace" : "Corp",
                    "mws-opconfig" : "MWS",
                }
            }
            '''
            
    :returns: Merged configuration data 
       
    """

    my_config = {}
    
    for key,value in config.iteritems():
        my_config[key] = value

    ## If we have a conf already, merge in our passed data
    for key,value in config2.iteritems():
        if not key in my_config:
            my_config[key] = value
            continue
        else:
            rootobj = my_config[key]
            for sub_key,sub_value in value.iteritems():
                if sub_key not in rootobj.keys() or rootobj[sub_key] is None:
                    rootobj[sub_key] = sub_value
    
    return my_config

def opconfig_value(key,env="prod",opconfig=None):
    """Convenience method to return a value from our Apollo opconfig
    relevent to our current environment."""
    
    if opconfig is None and APOLLO_AVAILABLE:
        opconfig = apollo.opconfig(DEFAULT_OPCONFIGNAME)
    elif opconfig is None:
        raise SubSystemUnavailableError("Subsystem (Apollo) is not available: cannot load OpConfig data!")
    
    default_opconfig_index = 0
    if env and env.lower() == "integ":
        default_opconfig_index = 1
    elif env and env.lower() == "test":
        default_opconfig_index = 2
    
    ## Resolve all of our variables
    value = None
    try:
        i = default_opconfig_index
        the_list = opconfig[key].split(",")
        while i >= len(the_list) and i >= 0:
            i -= 1
        value = the_list[i].strip()
    except (KeyError,AttributeError):
        pass
    
    return value

#MARK: Module Exceptions
class SubSystemUnavailableError(Exception):
    """
    Thrown in the event that prerequisites for a given subsystem are unavailable.
    I.E. this will be thrown if RDS is attempted when the pg module is not 
    installed.
    """
    pass

class ConfigurationError(Exception):
    """
    Thrown in the event that there are configuration problems.
    """
    pass

class ConnectionError(Exception):
    """
    Thrown in the event that a connection fails.
    """
    pass

class CredentialRetrievalError(Exception):
    """
    Exception which is raised when PyKARL descendents cannot necessary
    credentials.
    """
    
    def __init__(self,message=None,failures=None):
        
        if message is None and failures:
            try:
                detail = map(lambda x: "{} (ms:{})".format(x,failures[x]["ms"]) ,failures.keys())
            except:
                detail = failures.keys()
            
            message = "Failed to retrieve ODIN credentials: {}".format(", ".join(detail))
        elif message is None:
            message = "Failed to retrieve ODIN credentials!"
        
        super(CredentialRetrievalError, self).__init__(message)
        self.failures = failures
    

class SetupError(Exception):
    """
    Exception which is raised when PyKARL descendents cannot load all
    necessary values.
    """
    def __init__(self,message,attributes=None):
        super(SetupError, self).__init__(message)
        self.attributes = attributes

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
FILE_DATE_FORMAT = "%Y-%m-%dT%H_%M_%S"

