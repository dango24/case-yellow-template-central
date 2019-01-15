"""
**tanium**  - Module that provides facilities for processing data queried 
live from tanium.

.. module:: pykarl.tanium
   :platform: RHEL5
   :synopsis: Includes controller class for querying data and posting to S3 and Redshift   
        
.. codeauthor:: Beau Hunter <beauhunt@amazon.com>


"""

import json
import time
import argparse
import pytanium
import logging
import uuid
import copy
import codecs
import xml.etree.ElementTree as ElementTree

from bender import apollo

from .core import setup
from .core import KARLCollector
from .core import load_credentials
from .core import opconfig_value
from .core import DATE_FORMAT,FILE_DATE_FORMAT
from .core import ConfigurationError

class TaniumCollector(KARLCollector):
    """
    Class which provides several convenience methods for dealing with Tanium
    metric gathering and publication.
    
    This class will generally be overridden with custom implementations of:
    
    * :py:func:`TaniumCollector.process_tanium_results`
    * :py:func:`TaniumCollector.report_results_to_db` 
    * :py:func:`TaniumCollector.s3file_from_results`
    * :py:func:`KARLCollector.print_results`
    
    """
    
    tanium = None
    
    supports_test_env = False
    supports_integ_env = True
    supports_prod_env = True
    
    def __init__(self,env_config=None,
                            env_filepath="configuration/env_config.conf",
                            arg_list=None):
        """
        Our class constructor which includes two options for configuration.
        Configurations specified in env_config will override those specified
        in config file at env_filepath.
        
        :param env_config: Provide configuration data.
        :type env_config: (dict) Dictionary object keyed at the top level by Tanium environment
        :param env_filepath: Path to a configuration file
        :type env_filepath: (str) Filesystem path
        
        """
        
        self.tanium = pytanium.Tanium()
        
        KARLCollector.__init__(self,env_config=env_config,
                                    env_filepath=env_filepath,
                                    arg_list=arg_list)
        
    
    def setup_parser(self):
        """
        Subroutine where we establish our Command line arguments. Populates
        self.parser. You may wish to override this if you want to implement
        any custom CLI arguments.
        """
        
        ## If we have a configured parser. Use it
        parser = self.parser
        
        if parser is None:
            KARLCollector.setup_parser(self)
            
        parser = self.parser
        
        parser.description  ='''Query Tanium, process, and post data to RedShift'''
        
        ## Define our Arguments
        parser.add_argument("--min-result-count",type=int,default=0,
            help=("If passed, we only report counts to the DB if the number of "
                "Tanium records returned equals or exceeds the provided value."))
        
        parser.add_argument("-q","--no-sleep",default=False,
            action="store_true",
            help=("If provided, we will not wait for Tanium questions to expire prior to processing."))
        parser.add_argument("--tanium-url",
            help="The Tanium url string to use for our query.")
        parser.add_argument("--tanium-materialset",
            help="The odin material set to use for Tanium credential lookup.")
        parser.add_argument("--question-id",
            help="The Tanium question id to use for the query.")
        parser.add_argument("--xml",help="Provide a filepath containing tanium "
                        "xml results to use as the source for our submission (bypasses Tanium query)")
        
        parser.add_argument("--save",help="Save copies of generated files.",
                                                        action="store_true",
                                                        default=False)
        parser.add_argument("--tanium-opconfig",
            default="Tanium",
            help="The Apollo OpConfig to use for various values. Options explicitly provided via CLI args or in env_config file  will override opconfig values.")
        self.parser = parser
    
    def setup(self,redshift_host=None,
                    redshift_name=None,redshift_port=None,redshift_ms=None,
                    rds_host=None,rds_name=None,rds_port=None,rds_ms=None,
                    s3_bucketname=None,s3_ms=None,
                    args=None,
                    tanium_url=None,
                    tanium_materialset=None,
                    opconfig=None):
        """Method which sets up our object by loading the appropriate
        credentials from odin.
        """
        
        KARLCollector.setup(self,redshift_host=redshift_host,
                                    redshift_name=redshift_name,
                                    redshift_port=redshift_port,
                                    redshift_ms=redshift_ms,
                                    rds_host=rds_host,rds_name=rds_name,
                                    rds_port=rds_port,rds_ms=rds_ms,
                                    s3_bucketname=s3_bucketname,s3_ms=s3_ms,
                                    args=args,
                                    opconfig=opconfig)
        
        if args is None:
            args = self.args
        
        if not tanium_url:
            tanium_url = args.tanium_url
        if not tanium_materialset:
            tanium_materialset = args.tanium_materialset
        
        pytanium.setup(scope=self.tanium,url=tanium_url,
                                    odin_ms=tanium_materialset)
    
    
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

        if not mapping:
            mapping = {"tanium-url" : "url",
                            "tanium-materialset": "odinmaterialset"}
                            
        return KARLCollector.merge_args_from_opconfig(self,mapping=mapping,
                                                args=args,
                                                opconfig_name="Tanium",
                                                opconfig=opconfig)
                        
    
    def validate_tanium_config(self,args=None):
        """
        Method which will sanity check our configuration for use with Tanium.
        
        :param args: Optional arguments to utilize when loading credentials.
                            If this param is ommited we will consult self.args
        :type args: Namespace Object
        
        :raises ConfigurationError: If db or tanium material sets are not defined
        
        :returns: (bool) true if tests passed (raises exception otherwise)
        
        """
        
        if args is None:
            args = self.args
        
        try:
            if not args.tanium_url:
                raise AttributeError
        except AttributeError:
            raise ConfigurationError("tanium-url not specified!")
        
        try:
            if not args.question_id:
                raise AttributeError
        except AttributeError:
            raise ConfigurationError("tanium-question-id not specified!")
        
        return True
    
        ## MARK: Primary Subroutine
    def run(self):
        """
        Our Primary CLI subroutine to process passed arguments, take appropriate
        action, and return an integer result code.
        
        :returns: (int) exit code
        """
        
        self.benchmark_start_timer("Runtime")
        
        ## Setup our environment
        if self.args is None:
            self.build_args()
        
        ## Parse our arguments
        args = self.args
        
        self.setup(args=args)
        
        ## Intro logging
        log_message = ""
        
        question = None
        
        if not args.s3file:
            if not args.xml:
                question = self.question_result_from_tanium()
            else:
                self.logger.info("Processing results from XML file:%s" % args.xml)
                
                fh = codecs.open(args.xml,"r","utf-8")
                raw_xml_data = fh.read()
                fh.close()
                
                tanium = pytanium.Tanium()
                cleaned_xml = tanium.clean_xml(raw_xml_data)
                
                xml = ElementTree.fromstring(cleaned_xml)
                
                question = pytanium.Question(xml=xml)
        
            ## Make sure that we received an adequate result set (if we're
            ## into that kindathing)
            result_count = len(question.rows)
            if result_count < args.min_result_count:
                self.logger.error("ERROR: Result count too small, exiting "
                    "(records:%s min:%s)" %(result_count,args.min_result_count))
                return 90
        
        
        ## Process our Results
        self.benchmark_start_timer("Tanium","Processing")
        
        result = self.process_tanium_results(question,args=args)
        self.benchmark_end_timer("Tanium","Processing")
        
        if not args.no_log:
            self.benchmark_start_timer("DBReport")
            self.report_results_to_db(result,args=args)
            self.benchmark_end_timer("DBReport")
        
        self.benchmark_end_timer("Runtime")
        
        ## Report our Results
        if args.stats or args.verbose > 0:
            self.print_benchmarks(args=args)
        
        if args.no_log or args.verbose > 0 or args.stats:
            self.print_results(result,args=args)
        
    def s3file_from_results(self,results,filepath=None,args=None):
        """
        Method which returns a path to save our file to. If none
        is provided we will use a temporary location.
        
        :param dict results: Our results to process
        
        :returns: Path to delimited file for upload to s3
        
        """
        
        ## Implementation here (must subclass and override)
        raise Exception("process_tanium_results() not implemented!")
    
    def question_result_from_tanium(self, args=None):
        """
        Method which returns a Question object containing relevant data,
        provided by a Tanium Query.
        """
    
        ## Validate our Tanium config
        self.validate_tanium_config()
        tanium = self.tanium
        
        if args is None:
            args = self.args
        
        if args.verbose == 0:
            log_message = ("Submitting question id:'%s' against env:'%s' "
                                        % (args.question_id,args.env))
            if args.no_log:
                log_message += "(dryrun)"
        
        elif args.verbose == 1:
            log_message = ("Submitting question id:'%s' against url:'%s' "
                                        % (args.question_id,tanium.url))
            if args.no_log:
                log_message += "(dryrun)"
        else:
            log_message = """Submitting Query:
        env: %s
        tanium-url: %s
        tanium-question-id: %s
        tanium-materialset: %s
        redshift-name: %s
        redshift-host: %s:%s
        redshift-materialset: %s
        rds-name: %s
        rds-host: %s:%s
        rds-materialset: %s
    """ % (args.env,
                tanium.url,
                args.question_id,
                tanium.materialset,
                args.redshift_name,
                args.redshift_host,
                args.redshift_port,
                args.redshift_materialset,
                args.rds_name,
                args.rds_host,
                args.rds_port,
                args.rds_materialset)
            
            if args.no_log:
                log_message += "    dryrun: True"
            else:
                log_message += "    dryrun: False"
        
        self.logger.info(log_message)
        
        self.benchmark_start_timer("Tanium","Query")
        
        ## Run our Tanium Query
        if args.save:
            tanium.save_xml_file = True
        
        question = tanium.ask_saved_question(args.question_id)
        
        # Determine if we should wait
        if question.expire_seconds > 0 and not args.no_sleep:
            if question.seconds_since_issued < question.expire_seconds:
                wait_time = (1 + question.expire_seconds
                                                - question.seconds_since_issued)
                try:
                    self.logger.info("Query Submitted. Waiting %s seconds for "
                            "the question to expire..." % wait_time)
                    time.sleep(wait_time)
                except KeyboardInterrupt:
                    self.logger.info("Keyboard Interrupt! Skipping wait...")
                
                # Rerun our query after our wait
                self.logger.info("Refreshing question data...")
                question = tanium.ask_saved_question(args.question_id)
    
        self.benchmark_end_timer("Tanium","Query")
        
        return question
    
    def process_tanium_results(self,question,args=None):
        """
        Method which processes our tanium result and outputs our resulting 
        computations. We will organize our data into an arbitrary data 
        structure that will be processed by report_results_to_db()
        and report_results()
        
        param question: Our question to process
        :type question: pytanium.Question object
        :param args: Argument namespace as generated by argparse.parse()
        :type args: Namespace
        
        :returns: (dict) Arbitrary data structure
        
        """
        
        if args is None:
            args = self.args
        
        result = {}
        
        ## Implementation here (must subclass and override)
        raise Exception("process_tanium_results() not implemented!")
        
        return result
    
    def report_results_to_db(self,results,args=None):
        """
        Method which processes our distilled results and logs them to DB.
        This should be overridden.
        
        :param results: Arbitrary data structure as produced by process_tanium_results()
        :type results: (dict)
        :param args: Argument namespace as generated by argparse.parse()
        :type args: Namespace
        
        """
        
        if args is None:
            args = self.args
        
        ## Implementation here (must subclass and override)
        raise Exception("report_results_to_db() not implemented!")
    
    def sanitize_string(self,data,max_length=None):
        """
        Method which will sanitize a piece of data.
        
        :param str data: The data to sanitize
        :param int max_length: If provided, we will trim the string to this length
        
        """
        sanitized = None
        if max_length:
            sanitized = ("%s" % data)[0:max_length-1]
        else:
            sanitized = "%s" % data
        
        sanitized = sanitized.replace("\n","")
        sanitized = sanitized.replace("|","")
        
        return sanitized
    
