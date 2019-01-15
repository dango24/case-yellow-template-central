"""
.. module:: acme.cli
    :synopsis: Module containing classes used by acme cli app.
    :platform: OSX, Ubuntu

.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>


"""

import argparse
import sys
import os
import logging
import datetime
import json
import threading

import acme.ipc as ipc
import acme.network as network
import acme.daemon
import acme.agent
import acme.compliance

import pykarl.core
import pykarl.event
from pykarl.event import Event, EventEngine
import systemprofile
import constant
import compliance
import time

#MARK: Module defaults
DEFAULT_LOG_LEVEL = logging.INFO
DEFAULT_BOTO_LOGLEVEL = logging.WARNING

PROCESS_POLLING_WAIT = 0.5  #In seconds


LOGGING_FORMAT = "%(message)s"
LOGGING_FORMAT_DETAILED = "[%(asctime)s] [%(name)s] [%(levelname)s] [file:%(filename)s:%(lineno)d] %(message)s"


class ACMECLI(object):
    """
    Our primary CLI controller class. Responsible for parsing arguments and 
    executing accordingly.
    """
    
    parser = None           #: :py:class:`argparse.Parser` object.
    arguments = None        #: list(str) of arguments passed via CLI
    args = None             #: :py:class:`argparse.Namespace` object representing
                            #: current configuration

    logger_name = "CLI"

    system_dir = acme.daemon.DEFAULT_SYSTEM_DIR

    

    def __init__(self,arguments=None,host=None,port=None):

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

        ## If no parser is passed, use our internal parser
        if parser is None:
            ## Instantiate a parser if we don't have one already.
            parser = argparse.ArgumentParser(
                            prog="ACME CLI.")
        elif parser is None:
            parser = self.parser

        cmd_group = parser.add_argument_group("ACME Interaction")
        cmd_group.add_argument("--status",action="store_true",
                        help=("Get current computer status"))
        cmd_group.add_argument("--activity",action="store_true",
                        help=("List current server activity."))
        cmd_group.add_argument("--shutdown",action="store_true",
                        help=("Shutdown the service."))
        cmd_group.add_argument("--reload",action="store_true",
                        help=("Reload configuration settings."))
        cmd_group.add_argument("--heartbeat",action="store_true",
                        help=("Send a heartbeat."))
        cmd_group.add_argument("--network-status",action="store_true",
                        help=("Output network status."))
        cmd_group.add_argument("--karl-status",action="store_true",
                        help=("Output network status."))
        cmd_group.add_argument("--list-group-cache",action="store_true",
                        help=("Output cached user group data."))
        cmd_group.add_argument("--agent-status",action="store_true",
                        help=("Output agent status."))
        cmd_group.add_argument("--compliance-status", action="store_true",
                        help=("Output compliance modules' data"))
        cmd_group.add_argument("--compliance-activity", action="store_true",
                        help=("Output compliance modules' current activity"))
        cmd_group.add_argument("--no-history",action="store_true",
                        help=("Output compliance modules' data without history. Usage --compliance-status --no-history"))
        cmd_group.add_argument("--evaluate", action="store_true",
                        help=("Run compliance evaluation. Usage --evaluate [--module <module identifier>]"))
        cmd_group.add_argument("--remediate", action="store_true",
                        help=("Run compliance remediation. Usage --remediate [--module <module identifier>]"))
        cmd_group.add_argument("--module",action="store",
                        help=("Run compliance evaluation/remediation. Usage --remediation [--module <module identifier>]"))
        cmd_group.add_argument("--module-status",
                        help=("Output module compliance status."))
        cmd_group.add_argument("--reload-modules",action="store_true",
                        help=("Reload all compliance modules.")) 
        cmd_group.add_argument("--register",action="store_true",
                        help=("Register to KARL with token. Usage --register --token '<token>'"))
        cmd_group.add_argument("--token",action="store",
                        help=("Register to KARL with token. Usage --register --token '<token>'"))
        cmd_group.add_argument("--force",action="store_true",
                        help=("Register to KARL with new request even if the system is already registered."))
        parser.add_argument("-v","--verbose",action="count",
                        help=("Increase our level of output detail."))
        parser.add_argument("-V","--version",action="store_true",
                        help=("Output the current version."))
        parser.add_argument("--json",action="store_true",
                        help=("Output in json."))
        
        cmd_group = parser.add_argument_group("ACME Client Interaction")
        cmd_group.add_argument("--client",action="store_true",
                        help=("Run commands against active ACME clients (session daemons, UI apps)"))
       
        """ Todo: construct per-module setup_parser() methods to allow
        individual modules to add their own CLI arguments
        
        for module in self.modules:
            module.setup_parser(parser=parser)
        
        """
        
        parser.add_argument_group(cmd_group)
        
        self.parser = parser
        
        return parser

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
        
        if self.parser is None:
            parser = self.setup_parser()
        else:
            parser = self.parser
        
        args = parser.parse_args(arguments)
        
        return args

    def requests(self,args=None):
        """
        Method that will return a list of requests based on the
        arguments provided to the CLI
        
        :param args: List of arguments to process
        :type args: List

        :returns: List of ipc.Request objects
        """
        
        logger = logging.getLogger(self.logger_name)
        if args is None:
            args = self.args
    
        request = None
        action = None
        options = {}
        
        if args.version:
            action = "GetVersion"
            options["ignore_ipc_failure"] = True
        elif args.status:
            action = "GetStatus"
        elif args.activity:
            action = "GetActivity"
        elif args.shutdown:
            action = "Shutdown"
        elif args.network_status:
            action = "GetNetworkStatus"
        elif args.karl_status:
            action = "GetKARLStatus"
        elif args.list_group_cache:
            action = "GetGroupCache"
        elif args.agent_status:
            action = "GetAgentStatus"
        elif args.reload:
            action = "Reload"
        elif args.compliance_status or args.compliance_activity:
            if args.no_history:
                request = ipc.Request()
                request.action = "GetComplianceStatus"
                request.options["no-history"] = True
            else:
                request = ipc.Request()
                request.action = "GetComplianceStatus"
                request.options["no-history"] = False
        elif args.evaluate:
            action = "ComplianceEvaluate"
            if args.module:
                request = ipc.Request()
                request.action = "ComplianceEvaluate"
                request.options["identifier"] = args.module
        elif args.remediate:
            action = "ComplianceRemediate"
            if args.module:
                request = ipc.Request()
                request.action = "ComplianceRemediate"
                request.options["identifier"] = args.module
        elif args.register:
            if args.token:
                request = ipc.Request()
                request.action = "RegisterWithToken"
                request.options["token"] = args.token
                if args.force:
                    request.options["force"] = True
            else:
                logger.info("Please provide token as --register --token=<token>")
            
                
        elif args.heartbeat:
            event = Event(type="HeartBeat",subject_area="ACME")
            request = ipc.Request()
            request.action = "CommitKARLEvent"
            request.options["event_data"] = event.to_json(base64encode=True)
        elif args.module_status:
            action = constant.ACTION_MODULE_STATUS
            options["identifier"] = args.module_status
        elif args.reload_modules:
            action = constant.ACTION_RELOAD_MODULES
        if not request:
            if action:
                request = ipc.Request()
                request.action = action
                request.options = options
        
        return request
            
    def configure_logging(self,verbosity=None,log_file=None):
        """
        Method which will configure our logging behavior based on the 
        passed arguments. If env is ommited we will consult os.environ.
        
        :param env: Dictionary to consult to determine our logging params. If
            not provided we will source from os.environ
        :type args: argparse.Namespace object
        """
        
        ## Establish defaults
        log_level = DEFAULT_LOG_LEVEL
        boto_log_level = DEFAULT_BOTO_LOGLEVEL
        log_format = LOGGING_FORMAT
        
        logging.addLevelName(25,"IMPORTANT")
        logging.addLevelName(15,"DETAILED")
        logging.addLevelName(9,"DEBUG2")
        logging.addLevelName(5,"API")
        logging.addLevelName(2,"API2")
        logging.addLevelName(1,"API3")
        
        ## See if verbosity was specified
        if verbosity is not None:
            if verbosity > 5:
                log_format = LOGGING_FORMAT_DETAILED            
                log_level = logging.NOTSET
                boto_log_level = logging.NOTSET
            if verbosity >= 5:
                log_format = LOGGING_FORMAT_DETAILED            
                if log_level > 5:
                    log_level = 5
                if boto_log_level > logging.DEBUG:
                    boto_log_level = logging.DEBUG
            elif verbosity >= 4:
                if log_level > 9:
                    log_level = 9
                if boto_log_level > logging.INFO:
                    boto_log_level = logging.INFO
            elif verbosity >= 3:
                if log_level > logging.DEBUG:
                    log_level = logging.DEBUG
            elif verbosity >= 2:
                if log_level > 15:
                    log_level = 15
            elif verbosity >= 1:
                if log_level > logging.INFO:
                    log_level = logging.INFO
            
        ## Setup our boto logger
        boto_logger = logging.getLogger("boto")
        boto_logger.setLevel(boto_log_level)
        
        logger = logging.getLogger()
        logger.setLevel(log_level)
        
        if log_file and not os.path.exists(os.path.dirname(log_file)):
            os.makedirs(os.path.dirname(log_file))
        
        if log_file is not None:
            fh = logging.FileHandler(log_file)
            fh.setFormatter(logging.Formatter(log_format))
            logger.addHandler(fh)
        else:
            sh = logging.StreamHandler(sys.stdout)
            sh.setFormatter(logging.Formatter(log_format))
            logger.addHandler(sh)
        
    def run(self,arguments=None):
        """
        Our primary execution point.
        """
        
        exit_code = 0
        
        if arguments is None:
            arguments = self.arguments
            
        args = self.parse_args(arguments=arguments)
        self.args = args
        
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
            self.configure_logging(verbosity=verbosity,
                                log_file=log_file)
        except Exception as exp:
            sys.stderr.write("Error! Could not setup file logging: {}".format(exp))
            return 10
        
        logger = logging.getLogger(self.logger_name)
        
        request = self.requests(args=args)
        
        if request and args.client:
            rundir = os.path.join(self.system_dir,"run")
            rundata = ipc.Client().load_runfile_details(rundir)
            try:
                exit_code = self.process_agent_request(request=request,
                                                            rundata=rundata,
                                                            args=args)
            except Exception as exp:
                logger.debug("Failed to process request:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                raise
        elif request:
            exit_code = self.process_request(request=request, args=args)
        
        else:
            logger.error("Error! No request action was specified!")
            if self.parser:
                self.parser.print_help()
            exit_code = 99
                
        return exit_code
        
    def process_agent_request(self,request,rundata=None,args=None):
        """
        Method which will process the given request against our agents
        """
        
        exit_code = 0
        
        logger = logging.getLogger(self.logger_name)
        
        if rundata is None:
            rundir = os.path.join(self.system_dir,"run")
            c = ipc.Client()
            rundata = c.load_runfile_details(rundir)
        
        data = {}
        
        for agent in rundata["Client"]:
            name = "{}.{}".format(agent["type"],agent["user"])
            port = agent["port"]
            address = agent["address"]
            
            response = None
            
            ignore_ipc_failure = False
            try:
                ignore_ipc_failure = request.options["ignore_ipc_failure"]
            except KeyError:
                pass
            
            try:
                with ipc.Client(port=port,hostname=address) as c:
                    response = c.submit_request(request)
            except Exception as exp:
                status_message = "Failed to connect to client: {}!".format(name)
                if not args.json:
                    logger.error(status_message)
                    logger.debug("Client address:({}:{}) Error:{}\nStack trace (handled cleanly):".format(
                                                        address, port, exp),
                                                    exc_info=1)
                else:
                    exit_code = 3
                    data[agent["user"]] = { "status": status_message,
                                            "status_code": ipc.StatusCode.ERROR,
                                            }
                    continue
            
            response_data = None
            response_json = None
            
            if args.json == True:
                try:
                    data[agent["user"]] = response.to_dict();
                except Exception as exp:
                    exit_code = 3
                    data[agent["user"]] = { "status": status_message,
                                            "status_code": ipc.StatusCode.ERROR,
                                            }
                continue
                
            elif (response and response.status_code == 0 
                                    or ignore_ipc_failure and not response):
                if response:
                    response_data = response.data
                    try:
                        response_json = response.to_json()
                    except Exception as exp:
                        logger.error("Failed to parse response JSON...")
            
                if request.action == "GetVersion":
                    self.print_version(response_data,name=name,args=args)
                if request.action == "GetKARLStatus":
                    self.print_karl_status(response_data,name=name,args=args)
                elif request.action == "GetAgentStatus":
                    self.print_agent_status(response_data,name=name,args=args)
                elif request.action == "GetGroupCache":
                    self.print_groupcache(response_data,name=name,args=args)
                elif request.action == "GetNetworkStatus":
                    self.print_network_status(response_data,name=name,args=args)
                elif request.action == "GetStatus":
                    self.print_status(response_data,name=name,args=args)                
                elif request.action == "Reload":
                    logger.info("System Reloaded... ({})".format(name))
                elif request.action == "":
                    logger.info("Event Submitted. ({})".format(name))
                
            elif response:
                logger.error("Error ({}): {}".format(name,response.status))
                exit_code = 2
            
            logger.log(5,"Request ({}):{}".format(name,request.to_json()))        
            
            logger.log(9,"Response ({}):{}".format(name,response_json))
                
        if not rundata["Client"] and not args.json:
            logger.info("No clients were found!")
        elif args.json:
            logger.info(json.dumps(data, indent=4))
        
        return exit_code
        
    
    def process_request(self, request, args=None, rundata=None):
        """
        Method which will process the given request against our daemon 
        """
        
        logger = logging.getLogger(self.logger_name)
        
        exit_code = 0
        
        if args is None:
            args = self.args
        
        if rundata is None:
            rundir = os.path.join(self.system_dir,"run")
            c = ipc.Client()
            rundata = c.load_runfile_details(rundir)
        
        try:
            hostname = rundata["Daemon"][0]["address"]
        except:
            hostname = ipc.DEFAULT_ADDRESS
            if not args.json:
                logger.debug("Could not determine daemon address from rundata, using default ({})!".format(
                                                                    hostname))
        try:
            port = rundata["Daemon"][0]["port"]
        except:
            port = ipc.DEFAULT_PORT
            if not args.json:
                logger.debug("Could not determine daemon port from rundata, using default ({})!".format(
                                                                    port))
        
        response = None
        ignore_ipc_failure = False
        try:
            ignore_ipc_failure = request.options["ignore_ipc_failure"]
        except KeyError:
            pass
        
        try:
            with ipc.Client(hostname=hostname, port=port) as c:
                response = c.submit_request(request)
        except Exception:
            if not ignore_ipc_failure:
                raise
            
        response_data = None
        response_json = None
        
        json_data = {}
        
        if args.json:
            json_data = response.to_dict()
        elif (response and (response.status_code == 0 or response.status_code == ipc.StatusCode.STATUS_REGISTERED_ALREADY 
                                    or response.status_code == ipc.StatusCode.STATUS_PROCESS_RUNNING)
                                    or ignore_ipc_failure and not response):
            if response:
                response_data = response.data
                try:
                    response_json = response.to_json()
                except Exception as exp:
                    logger.error("Failed to parse response JSON...")
        
            if request.action == "GetVersion":
                self.print_version(response_data, args=args)
            if request.action == "GetKARLStatus":
                self.print_karl_status(response_data,args=args)
            elif request.action == "GetAgentStatus":
                self.print_agent_status(response_data, args=args)
            elif request.action == "GetGroupCache":
                self.print_groupcache(response_data, args=args)
            elif request.action == "GetNetworkStatus":
                self.print_network_status(response_data, args=args)
            elif request.action == "GetStatus":
                self.print_status(response_data, args=args)                
            elif request.action == "Reload":
                logger.info("System Reloaded... ")
            elif request.action == "":
                logger.info("Event Submitted...")
            elif request.action == "GetComplianceStatus":
                if args.compliance_activity:
                    self.print_compliance_activity(response_data, args=args)                    
                else:
                    self.print_compliance_status(response_data, args=args)
            elif request.action == "ComplianceEvaluate":
                logger.info("Compliance evaluation started...")
                
                # If we are here, it means we have successfully created the thread to evaluate compliance.
                # Saving the original request so that we can print it in the end.
                orig_request = request
                request = ipc.Request()
                
                #Polling the evaluation status on the thread
                request.action = "getComplianceEvaluationStatus"
                response.status_code = ipc.StatusCode.STATUS_PROCESS_RUNNING
                try:
                    while response.status_code == ipc.StatusCode.STATUS_PROCESS_RUNNING:
                        try:
                            with ipc.Client(hostname=hostname, port=port) as c:
                                response = c.submit_request(request)
                        except Exception as e:
                            logger.log(5,"Failed to check evaluation status! Exception: {}".format(e))
                            response.status = "Failed to check evaluation status! Exception: {}".format(e)
                            exit_code = 12
                            if not ignore_ipc_failure:
                                raise
                        except Exception:
                            response.status = "Failed to check evaluation status for unknown reason"
                            exit_code = 2
                            if not ignore_ipc_failure:
                                raise
                        time.sleep(PROCESS_POLLING_WAIT)
                    logger.info(response.status)
                except Exception as e:
                    logger.log(5,"Failed to evaluate compliance. Exception: {}".format(e))
                    exit_code = 13
                    if not ignore_ipc_failure:
                        raise
                
                #original_request is obtained and saved.
                request = orig_request
                response_json = response.to_json()
            elif request.action == "ComplianceRemediate":
                logger.info("Compliance remediation started...")
                
                # If we are here, it means we have successfully created the thread to evaluate compliance.
                # Saving the original request so that we can print it in the end.
                orig_request = request
                request = ipc.Request()
                
                #Polling the remediation status on the thread
                request.action = "getComplianceRemediationStatus"
                response.status_code = ipc.StatusCode.STATUS_PROCESS_RUNNING
                try:
                    while response.status_code == ipc.StatusCode.STATUS_PROCESS_RUNNING:
                        try:
                            with ipc.Client(hostname=hostname, port=port) as c:
                                response = c.submit_request(request)
                        except Exception as e:
                            logger.log(5,"Failed to check remediate status! Exception: {}".format(e))
                            response.status = "Failed to check remediate status! Exception: {}".format(e)
                            exit_code = 12
                            if not ignore_ipc_failure:
                                raise
                        except Exception:
                            response.status = "Failed to check remediate status for unknown reason"
                            exit_code = 2
                            if not ignore_ipc_failure:
                                raise
                        time.sleep(PROCESS_POLLING_WAIT)
                    logger.info(response.status)
                except Exception as e:
                    logger.log(5,"Failed to remediate compliance. Exception: {}".format(e))
                    exit_code = 13
                    if not ignore_ipc_failure:
                        raise
                
                #original_request is obtained and saved.
                request = orig_request
                response_json = response.to_json()
            elif request.action == constant.ACTION_MODULE_STATUS:
                if response.status_code == ipc.StatusCode.SUCCESS:
                    logger.info(response_data)
                else:
                    logger.info("Fail to load module:{}".format(response.status))
                    exit_code = 2
            elif request.action == "RegisterWithToken":
                if response.status_code == ipc.StatusCode.STATUS_REGISTERED_ALREADY:
                    logger.info(response.status)
                    exit_code = 11
                else:
                    # If we are here, it means we have successfully created the thread to register to KARL.
                    # Saving the original request so that we can print it in the end.
                    orig_request = request
                    request = ipc.Request()
                    
                    #Polling the registration status on the thread
                    request.action = "getRegistrationStatus"
                    response.status_code = ipc.StatusCode.STATUS_PROCESS_RUNNING
                    try:
                        while response.status_code == ipc.StatusCode.STATUS_PROCESS_RUNNING:
                            logger.info("Processing the registration request......")
                            try:
                                with ipc.Client(hostname=hostname, port=port) as c:
                                    response = c.submit_request(request)
                            except Exception as e:
                                logger.log(5,"Failed to register for unknown reason: Encountered an exception: {}".format(e))
                                response.status = "Failed to register, Encountered an exception:{}".format(e)
                                exit_code = 12
                                if not ignore_ipc_failure:
                                    raise
                            except Exception:
                                response.status = "Failed to register for unknown reason"
                                exit_code = 2
                                if not ignore_ipc_failure:
                                    raise
                            time.sleep(PROCESS_POLLING_WAIT)
                        logger.info(response.status)
                    except Exception as e:
                        logger.log(5,"Failed to register for unknown reason: Encountered an exception: {}".format(e))
                        exit_code = 13
                        if not ignore_ipc_failure:
                            raise
                    
                    #original_request is obtained and saved.
                    request = orig_request
                    response_json = response.to_json()
        elif response:
            logger.error("Error: {}".format(response.status))
            exit_code = 2
        
        if not args.json:
            logger.log(5,"Request:{}".format(request.to_json()))        
            logger.log(9,"Response:{}".format(response_json))
        else:
            logger.info(json.dumps(json_data, indent=4))

        return exit_code
    
    def print_version(self,data,name=None,args=None):
        """
        Method to output GetVersion results. You should not generally 
        call this directly; it will usually be called by self.process_request
        """
        
        if args is None:
            args = self.args
        
        logger = logging.getLogger(self.logger_name)
        
        status_str = "Version: "
        
        installed_acme = acme.daemon.__version__
        installed_acme_core = acme.core.__version__
        installed_karl = pykarl.core.__version__
        
        running_acme = "N/A"
        running_karl = "N/A"
        
        try:
            running_acme = data["ACMELib"]
            if name:
                running_acme += " ({})".format(name)
        except:
            pass
        
        try:
            running_karl = data["KARLLib"]
            if name:
                running_karl += " ({})".format(name)
        except:
            pass
        
        try:
            running_acme_core = data["ACMECoreLib"]
            if name:
                running_acme_core += " ({})".format(name)
        except:
            pass
        
        if not args.verbose:
            status_str = "Version: {}".format(installed_acme)
        elif args.verbose == 1:
            status_str = "Installed ACME Version: {}".format(installed_acme)
            status_str += "\nRunning ACME Version: {}".format(running_acme)
            status_str += "\nInstalled ACMECore Version: {}".format(installed_acme_core)
            status_str += "\nRunning ACMECore Version: {}".format(running_acme_core)
        elif args.verbose > 1:
            status_str = "Installed ACME Version: {}".format(installed_acme)
            status_str += "\nRunning ACME Version: {}".format(running_acme)
            status_str += "\nInstalled KARL Version: {}".format(installed_karl)
            status_str += "\nRunning KARL Version: {}".format(running_karl)
            status_str += "\nInstalled ACMECore Version: {}".format(installed_acme_core)
            status_str += "\nRunning ACMECore Version: {}".format(running_acme_core)
        
        logger.info(status_str)

    
    def print_status(self,data,name=None,args=None):
        """
        Method to output GetStatus results. You should not generally call this
        directly; it will usually be called by self.process_request
        """
        
        if args is None:
            args = self.args
        
        logger = logging.getLogger(self.logger_name)
        
        status_str = "Status: "
        
        if data["acme_status"] == acme.daemon.STATUS_IDLE:
            status_str += "Idle"
        elif data["acme_status"] == acme.daemon.STATUS_EVALUATING | acme.compliance.ModuleStatus.EVALUATING:
            status_str += "Evaluating..."
        elif data["acme_status"] == acme.daemon.STATUS_REMEDIATING | acme.compliance.ModuleStatus.REMEDIATING:
            status_str += "Remediating..."    
        elif data["acme_status"] == acme.daemon.STATUS_AGENT_EXECUTING:
            status_str += "Agent Executing..."    
        elif data["acme_status"] == acme.daemon.STATUS_FATAL_ERROR:
            status_str += "Fatal Error!" 
        else:
            status_str += "Unknown"
        
        if name:
            status_str += "        ({})".format(name)
        
        compliance_status_str = "Compliance Status: "
        
        compliance_status = acme.compliance.ComplianceStatus.to_string(
                                            data["compliance_status"],
                                            True)

        compliance_status_str += compliance_status
        
        logger.info(status_str)
        logger.info(compliance_status_str)
        
    def print_network_status(self,data,name=None,args=None):
        """
        Method to output network status. You should not generally call this
        directly; it will usually be called by self.process_request
        """
        
        if args is None:
            args = self.args
        
        logger = logging.getLogger(self.logger_name)
            
        if name:
            logger.info("Network Status... ({})".format(name))        
        else:
            logger.info("Network Status...")        
            
        logger.info("--------------")
        network_state = network.NetworkState(dict_data=data)

        hostname_str = None
        try:
            hostname_str = systemprofile.profiler.hostname()
        except:
            pass
        
        active_network_session = network_state.active_network_session
        if active_network_session:
            ip_address_str = active_network_session.ip_address
            interface_str = active_network_session.interface
        else:
            ip_address_str = ""
            interface_str = ""
        
        site_info = network_state.site_info
        if site_info.site == site_info.last_fixed_site:
            site_str = site_info.site
        else:
            if site_info.site:
                site_str = "{} ({})".format(site_info.last_fixed_site,site_info.site)
            else:
                site_str = "{} (OffDomain)".format(site_info.last_fixed_site)
    
        if args.verbose <= 0:
            state_str = network.string_from_state(active_network_session.state)
        else:
            state_str = network.string_from_state(
                                            state=active_network_session.state,
                                            affirm_only=False)

        logger.info("{:14} {}".format("Hostname:",hostname_str))
        logger.info("{:14} {}".format("IP Address:",ip_address_str))
        logger.info("{:14} {}".format("Interface:",interface_str))
        logger.info("{:14} {}".format("Network Site:",site_str))
        logger.info("{:14} {}".format("State:",state_str))
        logger.info("")

    def print_groupcache(self,data,name=None,args=None):
        """
        Method to output cached gorup data. You should not generally call this
        directly; it will usually be called by self.process_request
        """
        
        if args is None:
            args = self.args
        
        logger = logging.getLogger(self.logger_name)
            
        if name:
            logger.info("Outputing Group Cache Data... ({})".format(name))        
        else:
            logger.info("Outputing Group Cache Data...")        
            
        try:
            cached_groups = data["cached_objects"]
            
            if not cached_groups:
                logger.info("No cached group data found!")
                return
            else:
                logger.info("{:14}  {}".format("Username", "Groups"))
                logger.info("-------------  -------------".format(" "*10))
            
            for info in cached_groups:
                count=0
                
                username = info["username"]
                groups = info["groups"]
                
                ## Remove domain prefixes if we have verbosity.
                if args.verbose <= 0:
                    groups = [x if x.find("\\") == -1 else x[x.find("\\")+1:] 
                                                                for x in groups]
                groups.sort(key=lambda item: item.lower())
                
                for group in groups:
                    
                    count += 1                                
                    if count == 1:
                        logger.info("{:14}  {}".format(username, group))
                    elif count == len(groups):
                        logger.info("{:14}  {}\n".format("", group))
                    else:
                        logger.info("{:14}  {}".format("", group))
        except Exception as exp:
            print_stack = args.verbose > 2
            logger.error("Failed to output group cache data: {}".format(exp), 
                                                    exc_info=print_stack)

    def print_karl_status(self,data,name=None,args=None):
        """
        Method to output network status. You should not generally call this
        directly; it will usually be called by self.process_request
        """
        
        if args is None:
            args = self.args
        
        logger = logging.getLogger(self.logger_name)
        
        if name:
            logger.info("KARL Status... ({})".format(name))        
        else:
            logger.info("KARL Status...")        
            
        logger.info("--------------")
        
        is_enabled = None
        try:
            is_enabled = data["enabled"]
        except:
            pass
            
        state_str = None
        try:
            state_str = pykarl.event.string_from_enginestate(data["state"])
        except:
            pass
        
        online_str = None
        offline_reasons = []
        try:
            if data["online"]:
                online_str = "True"
            else:
                online_str = "False"
                if not data["has_network_access"]:
                    offline_reasons.append("No Network")
                if not data["has_credentials"]:
                    offline_reasons.append("No Credentials")
            
            if args.verbose > 0 and offline_reasons:
                online_str += " ({})".format(", ".join(offline_reasons))
        except: 
            pass
        
        karl_id = None
        try:
            karl_id = data["source_id"]
        except:
            pass
        
        queue_length_str = 0
        try:
            queue_length_str = data["queue_length"]
        except:
            pass

        num_failed_commits_str = 0
        try:
            num_failed_commits_str = data["num_failed_commits"]
        except:
            pass

        last_failed_commit_str = "N/A"
        try:
            if data["last_failed_commit"]:
                last_failed_commit_str = data["last_failed_commit"]
        except:
            pass
        
        
        access_key_id = "N/A"
        try:
            access_key_id = data["access_key_id"]
        except:
            pass
        
        if args.verbose > 0:
            logger.info("{:14} {}".format("System ID:",karl_id))
            logger.info("{:14} {}".format("AWS Access Key ID:",access_key_id))
        
        logger.info("{:14} {}".format("Enabled:",is_enabled))
        logger.info("{:14} {}".format("State:",state_str))
        
        try:
            state = int(data["state"])
            if (state and state & (pykarl.event.ENGINE_STATE_STOPPED
                                            |pykarl.event.ENGINE_STATE_STOPPING
                                            |pykarl.event.ENGINE_STATE_RUNNING)):
                logger.info("{:14} {}".format("Online:",online_str))
                logger.info("{:14} {}".format("QueueLength:",queue_length_str))
                logger.info("{:14} {}".format("NumFailedCommits:",num_failed_commits_str))
                logger.info("{:14} {}".format("LastFailedCommit:",last_failed_commit_str))        
        except Exception:
            logger.info("{:14} {}".format("Online:",online_str))
            logger.info("{:14} {}".format("QueueLength:",queue_length_str))
            logger.info("{:14} {}".format("NumFailedCommits:",num_failed_commits_str))
            logger.info("{:14} {}".format("LastFailedCommit:",last_failed_commit_str))
        
        logger.info("")
    
    def print_table(self, my_dict, col_list=None):
        """ 
        Pretty print a list of dictionaries (my_dict) as a dynamically sized table.
        If col_list is not specified, it will take the first dict entry's keys in the list
        """
        logger = logging.getLogger(self.logger_name)
        if not col_list: col_list = list(my_dict[0].keys() if my_dict else [])
        my_list = [col_list] # 1st row = header
        for item in my_dict: my_list.append([str(item[col] or '') for col in col_list])
        col_size = [max(map(len,col)) for col in zip(*my_list)]
        format_str = ' | '.join(["{{:<{}}}".format(i) for i in col_size])
        my_list.insert(1, ['-' * i for i in col_size]) # Seperating line
        for item in my_list: logger.info(format_str.format(*item))


    def print_compliance_status(self,data,name=None,args=None):
        """
        Method to output complinace status. You should not generally call this
        directly; it will usually be called by self.process_request
        """
        
        if args is None:
            args = self.args
        
        logger = logging.getLogger(self.logger_name)
                
        if name:
            logger.info("Compliance Status... ({})".format(name))        
        else:
            logger.info("Compliance Status...")        
            
        logger.info("--------------")
        
        is_enabled = None
        try:
            is_enabled = data["running"]
        except:
            pass
                
        last_failed_commit_str = "N/A"
        try:
            if data["last_failed_commit"]:
                last_failed_commit_str = data["last_failed_commit"]
        except:
            pass
            
        tz_hours = round((datetime.datetime.now() - datetime.datetime.utcnow()).total_seconds() / 3600)
        tz_timedelta = datetime.timedelta(hours=tz_hours)
        
        
        compliance_status = acme.compliance.ComplianceStatus.to_string(
                                            data["compliance_status"],
                                            True) 
        status = acme.compliance.ModuleStatus.to_string(
                                            data["status"],
                                            True) 
        
        compliance_deadline = data["compliance_deadline"]
        if compliance_deadline:
            compliance_deadline = acme.core.DataFormatter.convert_to_date(
                                                        compliance_deadline)
            compliance_deadline += tz_timedelta
        
        isolation_deadline = data["isolation_deadline"]
        if isolation_deadline:
            isolation_deadline = acme.core.DataFormatter.convert_to_date(
                                                        isolation_deadline)
            isolation_deadline += tz_timedelta    
            
        
        
        logger.info("{:20} {}".format("Status:", status))
        logger.info("{:20} {}".format("Compliance Status:",compliance_status))
        
        if compliance_deadline:
            logger.info("{:20} {}".format("Deadline:",compliance_deadline))
        
        if isolation_deadline:
            logger.info("{:20} {}".format("Isolation:",isolation_deadline))

        logger.info("")        
        logger.info("{:20} {}".format("Loaded Modules:",len(data["modules"])))
        logger.info("{:20} {}".format("Execution Threads:",data["execution_thread_count"]))
        logger.info("{:20} {}".format("Queue Length:",data["queue_length"]))
        
        logger.info("")
        
        ## Bail out at this point if we aren't outputing with verbosity
        if not args.verbose:
            return
                
        
        
        if len(data["modules"]) > 0:
            logger.info("Loaded Modules:")
            logger.info("    {:20}{:9}{:32}{:21}{}".format("Module Name",
                                                        "Version",
                                                        "Compliance Status",
                                                        "Deadline",
                                                        "Isolation Deadline"))
            logger.info("    {:20}{:9}{:32}{:21}{}".format("-"*18,
                                                        "-"*7,
                                                        "-"*30,
                                                        "-"*19,
                                                        "-"*19))
            modules = []
            for identifier, module_data in data["modules"].iteritems():
                module = acme.compliance.BaseModule(identifier=identifier)
                module.load_dict(module_data)
                modules.append(module)
                
            for module in sorted(modules,key=lambda x: x.identifier):    
                deadline = module.compliance_deadline()
                if deadline:
                    local_deadline = "{}".format(deadline + tz_timedelta) 
                else:
                    local_deadline = "N/A"
                
                iso_deadline = module.isolation_deadline()
                if iso_deadline:
                    local_iso_deadline = "{}".format(iso_deadline + tz_timedelta)  
                else:
                    local_iso_deadline = "N/A"
                
                status = module.last_compliance_status
                
                compliance_status = acme.compliance.ComplianceStatus.to_string(
                                            module.last_compliance_status,
                                            True)
                logger.info("    {:20}{:9}{:32}{:21}{}".format(module.identifier,
                                                            module.version,
                                                            compliance_status,
                                                            local_deadline,
                                                            local_iso_deadline))
         
    def print_compliance_activity(self,data,name=None,args=None):
        """
        Method to output complinace activity. You should not generally call this
        directly; it will usually be called by self.process_request
        """
        
        if args is None:
            args = self.args
        
        logger = logging.getLogger(self.logger_name)
                
        if name:
            logger.info("Compliance Activity... ({})".format(name))        
        else:
            logger.info("Compliance Activity...")        
            
        logger.info("--------------")
        
        is_enabled = None
        try:
            is_enabled = data["running"]
        except:
            pass
        
        tz_hours = round((datetime.datetime.now() - datetime.datetime.utcnow()).total_seconds() / 3600)
        tz_timedelta = datetime.timedelta(hours=tz_hours)
        
        
        compliance_status = acme.compliance.ComplianceStatus.to_string(
                                            data["compliance_status"],
                                            True) 
        status = acme.compliance.ModuleStatus.to_string(
                                            data["status"],
                                            True) 
        
        compliance_deadline = data["compliance_deadline"]
        if compliance_deadline:
            compliance_deadline = acme.core.DataFormatter.convert_to_date(
                                                        compliance_deadline)
            compliance_deadline += tz_timedelta
        
        isolation_deadline = data["isolation_deadline"]
        if isolation_deadline:
            isolation_deadline = acme.core.DataFormatter.convert_to_date(
                                                        isolation_deadline)
            isolation_deadline += tz_timedelta    
        
        logger.info("{:20} {}".format("Status:", status))
        logger.info("{:20} {}".format("Compliance Status:",compliance_status))
        
        logger.info("")        
        logger.info("{:20} {}".format("Loaded Modules:",len(data["modules"])))
        logger.info("{:20} {}".format("Execution Threads:",data["execution_thread_count"]))
        logger.info("{:20} {}".format("Queue Length:",data["queue_length"]))
        
        logger.info("")
        
        ## Bail out at this point if we aren't outputing with verbosity
        if not args.verbose:
            return
        
        if len(data["modules"]) > 0:
            logger.info("Loaded Modules:")
            logger.info("    {:20}{:11}{:21}{}".format("Module Name",
                                                        "Status",
                                                        "Last Result",
                                                        "Last Evaluation"))
            logger.info("    {:20}{:11}{:21}{}".format("-"*18,
                                                        "-"*9,
                                                        "-"*19,
                                                        "-"*20,))
            modules = []
            for identifier, module_data in data["modules"].iteritems():
                module = acme.compliance.BaseModule(identifier=identifier)
                module.load_dict(module_data)
                modules.append(module)
                
            for module in sorted(modules,key=lambda x: x.identifier):    
                status = acme.compliance.ModuleStatus.to_string(
                                                module.status,
                                                True)
                last_status = acme.compliance.ComplianceStatus.to_string(
                                            module.last_compliance_status,
                                            True)
                
                last_result = module.last_evaluation_result
                if last_result and last_result.end_date:
                    last_evaluation = "{}".format(
                                        last_result.end_date + tz_timedelta)
                    last_status = acme.compliance.ExecutionStatus.to_string(
                                                last_result.execution_status,
                                                True)
                else:
                    last_evaluation = "N/A"
                    last_status = "N/A"
                
                logger.info("    {:20}{:11}{:21}{}".format(module.identifier,
                                                            status,
                                                            last_status,
                                                            last_evaluation))

    
            
    def print_agent_status(self,data,name=None,args=None):
        """
        Method to output agent status. You should not generally call this
        directly; it will usually be called by self.process_request
        """
        
        if args is None:
            args = self.args
        
        logger = logging.getLogger(self.logger_name)
                
        if name:
            logger.info("Agent Status... ({})".format(name))        
        else:
            logger.info("Agent Status...")        
            
        logger.info("--------------")
        
        is_enabled = None
        try:
            is_enabled = data["running"]
        except:
            pass
                
        last_failed_commit_str = "N/A"
        try:
            if data["last_failed_commit"]:
                last_failed_commit_str = data["last_failed_commit"]
        except:
            pass
        
        logger.info("{:14} {}".format("Enabled:",is_enabled))
        logger.info("{:14} {}".format("Loaded Agents:",len(data["agents"])))
        logger.info("{:14} {}".format("Execution Threads:",data["execution_thread_count"]))
        logger.info("{:14} {}".format("Queue Length:",data["queue_length"]))
        
        logger.info("")
        
        ## Bail out at this point if we aren't outputing with verbosity
        if not args.verbose:
            return
                
        tz_hours = round((datetime.datetime.now() - datetime.datetime.utcnow()).total_seconds() / 3600)
        tz_timedelta = datetime.timedelta(hours=tz_hours)
        
        if len(data["agents"]) > 0:
            logger.info("Loaded Agents:")
            logger.info("    {:32}{:15}{:10}{}".format("Agent Name",
                                                        "Status",
                                                        "Result",
                                                        "Last Execution"))
            logger.info("    {:32}{:15}{:10}{}".format("-"*30,
                                                        "-"*13,
                                                        "-"*8,
                                                        "-"*19))
            agents = []
            for agent_data in data["agents"]:
                agent = acme.agent.BaseAgent(dict_data=agent_data)
                agents.append(agent)
                
            for agent in sorted(agents,key=lambda x: x.identifier):    
                if agent.last_execution:
                    local_date = agent.last_execution + tz_timedelta
                else:
                    local_date = "Never"
                
                status = self.agent_display_status(agent.status)
                execution_status = self.agent_display_execution_status(
                                                agent.last_execution_status)
                
                logger.info("    {:32}{:15}{:10}{}".format(agent.name,
                                                            status,
                                                            execution_status,
                                                            local_date))

    
    def agent_display_status(self,status):
        """
        Method which accepts an agent status bitmask and returns human-readable
        """
        output = "Idle."
        
        if status & acme.agent.AGENT_STATUS_EXECUTING:
            output = "Executing..."
        elif status & acme.agent.AGENT_STATUS_QUEUED:
            output = "Queued..."
            
        return output
    
    def agent_display_execution_status(self,status):
        """
        Method which accepts an agent status bitmask and returns human-readable
        """
        output = "N/A"
        
        if status == acme.agent.AGENT_EXECUTION_STATUS_SUCCESS:
            output = "Success"
        elif status & acme.agent.AGENT_EXECUTION_STATUS_FATAL:
            output = "Error"
        elif status & acme.agent.AGENT_EXECUTION_STATUS_ERROR:
            output = "Failed"
                
        return output
        
if __name__ == "__main__":
    cli = ACMECLI(arguments = sys.argv[1:])
    cli.run()

