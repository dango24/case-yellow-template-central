
"""
.. module: ipc
    :synopsis: Module which provides facilities for sockets-based IPC 
        interactions. 

.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>

    :Example:
    
        >>> import acme.ipc as ipc
        >>> 
        >>> ## Setup our delegate
        >>> def request_handler(request):
        ...     response = ipc.Response()
        ...     if request.action == "GetStatus":
        ...         response.status_code = StatusCode.STATUS_SUCCESS
        ...         response.data = {"Status": "Idle"}
        ...     else:
        ...         response.status_code = ipc.StatusCode.INVALID_ACTION 
        ...         response.status = "Invalid action:{}".format(request.action)
        ...     
        ...     return response
        >>>
        >>> ## Start an IPC server daemon thread
        >>> s = ipc.Server(delegate=request_handler)
        >>> s.start()
        >>>
        >>> ## Create a request with an IPC client
        >>> with ipc.Client as c:
        >>>     req = ipc.Request(action="GetStatus")
        >>>     result = c.submit_request(req)
        >>>
        >>> print result
        

"""
import fcntl
import codecs
import datetime
import errno
import getpass
import json
import logging
import os
import socket
import sys
import time
import threading
import uuid

import core
import tempfile
from os import stat
from pwd import getpwuid

#MARK: - Defaults
SOCKET_BUFFER_SIZE = 2048
SOCKET_HEADER_DELIM = "|"
SOCKET_HEADER_MAXSIZE = 100
SOCKET_DEFAULT_TIMEOUT = datetime.timedelta(seconds=10) # Lowering causes KARL registration timeouts
SOCKET_DEFAULT_FOLLOWUP_TIMEOUT = datetime.timedelta(seconds=0)
THREAD_EXIT_TIMEOUT = datetime.timedelta(seconds=5)

DEFAULT_ADDRESS = "127.0.0.1"
DEFAULT_PORT = 9216

#MARK: - Classes
class StatusCode(core.Enum):
    SUCCESS = 0
    DEFFERRED = 1 << 1
    ERROR = 1 << 2
    SOCKET_ERROR = 1 << 3 | ERROR
    SOCKET_CLOSED = 1 << 4 | SOCKET_ERROR
    SOCKET_TIMEOUT = 1 << 5 | SOCKET_ERROR
    INVALID_HEADER = 1 << 6 | ERROR
    INVALID_ACTION = 1 << 7 | ERROR
    TARGET_REQUIRED = 1 << 8 | ERROR
    TARGET_INVALID = 1 << 9 | ERROR
    SUBSYSTEM_UNSET = 1 << 10 | ERROR
    STATUS_PROCESS_RUNNING = 1 << 11
    STATUS_REGISTERED_ALREADY = 1 << 12


class Request(core.SerializedObject):
    """
    Class which represents a request for data and/or action from a consumer 
    process.
    """
    
    def __init__(self,action=None,options={},key_map=None, secure=False, auth_token_file=None,date=None,
                                                *args, **kwargs):
        """
        Constructor method. 
        
        :param str action: Our action identifier
        :param dict options: Key/value dictionary of options
        :param bool secure: identify if request is secure or not
        :param str auth_token_file: auth token file name
        :param datetime date: request datetime
        """
        
        self.uuid = str(uuid.uuid4())
        self.action = action
        self.options = options
        self.secure = secure
        self.auth_token_file = auth_token_file
        self.date = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        if not key_map:
            key_map = { 
                        "request_uuid" : "uuid",
                        "action": None,
                        "options": None,
                        "secure": False,
                        "auth_token_file": None,
                        "date": None
                        }
        
        super(Request, self).__init__(key_map=key_map, *args, **kwargs)

    def create_auth_token(self, directory, fileprefix="com.ipc.securerequest."):
        """
        Method which will create an auth token file with a  
        random name in the provided directory. This method will
        store the generated file path in self.auth_token_file.
        
        :param directory: directory that the file will be created in.
        :type directory: string
        :param fileprefix: the file name will begin with this prefix.
        :type fileprefix: string 
        
        :raises: Exception on failure
        :returns: (str) - path to the auth token file
        """
        logger = logging.getLogger(self.logger_name)
        try:
            _, self.auth_token_file  = tempfile.mkstemp(dir = directory, prefix = fileprefix)
            with open(self.auth_token_file, "r+") as fh:
                fh.write(self.to_json())
            logger.debug("Successfully created auth token.")
        except Exception as exp:
            raise Exception("Failed to create auth token file. Error: {}".format(exp))
        return self.auth_token_file
    
    def validate_auth_token(self, owner=None, ttl=5):
        """
        Method which will validate the request against the 
        token file stored at self.auth_token_file. If an owner is specified, we
        will verify that the file is owned by the provided username.
        If TTL is provided, token validation will fail if the token
        life exceeds the provided value (default: 5 minutes). 

        :param owner: file owner
        :type owner: string
        :param ttl: token life time
        :type ttl: int
        :raises acme.ipc.TokenValidationError: In the event
               that validation fails
        :returns: (bool) True if token successfully validates. 
        """
        logger = logging.getLogger(self.logger_name)
        
        #check if auth token file exists or not.
        if self.auth_token_file is None:
            raise TokenValidationError("No auth token!", error_code=ValidationErrorType.NO_AUTH_TOKEN)
        
        #check if the owner of auth token file is specified owner or not.
        file_owner = getpwuid(stat(self.auth_token_file).st_uid).pw_name
        if owner and file_owner != owner:
            raise TokenValidationError("File is not owned by:{}".format(owner), error_code=ValidationErrorType.OWNER_ERROR)
        
        #check if the token life exceeds the provided value
        timespan = datetime.timedelta(minutes=ttl)
        ipc_request_start_date = datetime.datetime.strptime(self.date, '%Y-%m-%d %H:%M:%S.%f')
        if ipc_request_start_date + timespan < datetime.datetime.utcnow():
            raise TokenValidationError("Auth token is expired!", error_code=ValidationErrorType.TOKEN_EXPIRED)
        
        #check if auth validate the request content against the token file stored at self.auth_token_file.
        with open(self.auth_token_file, "r") as fh:
            if json.loads(fh.read()) != self.to_dict():
                raise TokenValidationError("Auth token file does not match the request!", error_code=ValidationErrorType.CONTENT_ERROR)
        logger.debug("Successfully validated the auth token.")
        
        #delete auth token after the auth token validation
        if self.auth_token_file and os.path.exists(self.auth_token_file):
            os.remove(self.auth_token_file)

        return True
    
    def delete_auth_token(self):
        """
        Method which will delete auth token.
        """
        if self.auth_token_file and os.path.exists(self.auth_token_file):
            os.remove(self.auth_token_file)
    
class Response(core.SerializedObject):
    """
    Class which represents a response form a consumer request.
    """
    
    def request_data(self):
        """
        Method to serialize our stored request object
        """
        if self.request:
            return self.request.to_dict()
            
    def load_request_data(self, data):
        if data is not None and isinstance(data, Request):
            self.request = data
        elif data is not None:
            request = Request()
            request.load_dict(data)
            self.request = request
        else:
            self.request = data
    
    def __init__(self, request=None, key_map=None, *args, **kwargs):
        """
        Class Constructor
        
        :param request: Our associated request
        :type request: ipc.Request object
        """
        
        self.request = request      #: Our originating request
        self.status = None          #: The status of our response
        self.status_code = None     #: Status code of our response
        self.data = None            #: The data payload of our response
        
        if key_map is None:
            key_map = { "request": "<getter=request_data,setter=load_request_data>",
                        "status": None,
                        "status_code": None,
                        "data": None,
                    }
        super(Response, self).__init__(key_map=key_map, *args, **kwargs)
        
class Client(object):
    """Class representing an IPC client. This class provides an interface for
    connecting to the Server."""
    
    logger_name = "ipc.client"
    
    def __init__(self,hostname=None,port=None,socket=None,run_directory=None):
        """Class constructor.
        
        :param str hostname: The address to bind to (default "127.0.0.1")
        :param int port: The port to bind to (default 9216)
        
        """
        self.port = port
        self.hostname = hostname
        self.socket = socket
        self.run_directory = run_directory
        
    def __enter__(self):
        """
        Construct method to facilitate a context-manager use-case for client objects.
        """
        
        self.connect()
        return self
         
    def __exit__(self, type, value, traceback):
        """
        Teardown method to facilitate a context-manager use-case for client objects.
        """
        
        logger = logging.getLogger("ipc.Server")
        
        try:
            self.close()
        except Exception as exp:
            logger.warning("Failed to close our client: {}".format(exp))
            pass
    
    def load_runfile_details(self, directory=None):
        """
        Method which looks for run files in the provided directory
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if directory is None:
            directory = self.run_directory
        
        if directory is None:
            raise ValueError("Cannot load runfile details: no run directory specified!")
        
        logger.log(5, "Loading IPC connection data from run directory:'{}'".format(
                                                    directory))
        
        results = { "Client":[], "Daemon":[] }
        
        for filename in os.listdir(directory):
            json_data = None
            if filename.startswith("acme.ipc"):
                filepath = os.path.join(directory,filename)
                try:
                    with codecs.open(filepath,"r") as fh:
                        string_data = fh.read()
                        json_data = json.loads(string_data)
                except Exception as exp:
                    logger.warning("Failed to load runfile data from file:'{}' Error:{}".format(
                                                                filename,exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            
            if json_data:
                if filename.startswith("acme.ipc.daemon"):
                    results["Daemon"].append(json_data)
                elif filename.startswith("acme.ipc.client"):
                    results["Client"].append(json_data)
        
        return results
    
    def connect(self, hostname=None, port=None, run_directory=None):
        """
        Method used to initiate a connection to our IPC server.
        
        :param str hostname: The address to bind to
        :param int port: The port to bind
        
        :raises: Any exception raised by socket.connect()
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if not self.socket:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(SOCKET_DEFAULT_TIMEOUT.total_seconds())
        
        if hostname is None:
            hostname = self.hostname
        
        if port is None:
            port = self.port
        
        if not run_directory:
            run_directory = self.run_directory
            
        if (not hostname or not port) and run_directory:
            rundata = self.load_runfile_details(run_directory)
            
            try:
                hostname = rundata["Daemon"][0]["address"]
            except:
                pass
            
            try:
                port = rundata["Daemon"][0]["port"]
            except:
                pass
        
        if hostname is None:
            raise IPCError(message="Service hostname was not explicitely provided and could not be determined via autodiscovery!")
        
        if port is None:
            raise IPCError(message="Service port was not explicitely provided and could not be determined via autodiscovery!")
            
        logger.log(5, "Establishing Connection to IPC socket: {}:{}".format(hostname, port))
        self.socket.connect((hostname,port))
        
    def submit_request(self,request):
        """
        Method used to submit a request object to our server.
        
        :param request: 
        :type request: :py:class:`Request`
        
        :returns: :py:class:`Response` object.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        data = request.to_json()
        
        self.send(data)
        
        try:
            d = self.read()
        except Exception as exp:
            logger.error("Failed to read request data: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            raise
        
        try:
            r = Response(json_data=d)
        except Exception as exp:
            logger.error("Failed to parse request results. Error:{}  RawJSON:{}"
                            .format(exp,d))
            raise
        
        return r
        
    def read(self):
        """
        Method used to read a response from our server service over our 
        established IPC socket.
        
        :returns: Data send by server response.
        """
        
        return read_socket_data(self.socket)
        
    def send(self, data):
        """
        Method used to send data to our server service over our established 
        IPC socket.
        
        :param str data: The data to send. 
        """
        
        send_socket_data(self.socket, data)
        
    def close(self):
        """
        Method to tear down our socket.
        """
        
        try:
            if self.socket:
                self.socket.shutdown(socket.SHUT_RDWR)
                self.socket.close()                
        except Exception as exp:
            raise
        finally:
            self.socket = None    


class RequestProcessor(threading.Thread):
    """
    Class which is responsible for asyncronously processing a request 
    and outputting results.
    """
    
    def __init__(self,socket,address,server=None):
        """
        Our primary constructor.
        
        :param socket: Our socket used for IPC.
        :type socket: :py:class:`socket.socket` object
        :param address: The address of our socket
        :type address: (str)
        :param server: The controller object used to process requests 
        
        
        """
        super(RequestProcessor,self).__init__()
        
        self.daemon = True
        self.socket = socket
        self.address = address
        self.server = server
        self.shutdown = False
        self.logger_name = self.__class__.__name__
        self.read_lock = threading.Lock()
        
    def run(self):
        """
        Method which is invoked to process the request.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.log(5,"RequestProcessor running!")
        
        response = Response()
        
        while not self.shutdown:
            request = None
            
            should_loop = True
            while should_loop: #: Setup a loop to allow multiple read attempts
                should_loop = False
                try:
                    data = self.read()
                    
                    logger.log(2,"Found response data:{}".format(data))
                    
                    request = Request()
                    request.load_from_json(json_data=data)
                    
                    if self.server:
                        response = self.server.process_request(request)
                    else:
                        response.status_code = StatusCode.SUBSYSTEM_UNSET
                        response.status = "Failed to process request: our RequestProcessor has no delegate configured!"
                        logger.error(response.status)
                        
                except IPCError as exp:                    
                    if exp.errno == StatusCode.SOCKET_CLOSED:
                        logger.log(5,"Connection closed by client, shutting down RequestProcessor thread {}...".format(self))
                        self.shutdown = True
                        break
                        
                    response.status_code = exp.errno
                    response.status = exp.message

                    logger.error("An IPC error occurred: {}".format(exp.message),exc_info=1)
                    self.shutdown = True
                    break
                
                except socket.error as exp:
                    if exp.errno == errno.EAGAIN:
                        #logger.log(5,"A socket IO error occurred: (Errno:{}) {}. Will Retry...".format(exp.errno,exp.message))
                        should_loop = not self.shutdown
                        time.sleep(.1)

                        continue
                    else:
                        response.status_code = StatusCode.ERROR
                        response.status = exp.message

                        logger.error("A socket IO error occurred: ({}) {}".format(exp.__class__.__name__,exp.message),exc_info=1)
                        self.shutdown = True
                
                except ValueError as exp:
                    response.status_code = StatusCode.ERROR
                    response.status = exp.message

                    logger.error("An error occurred serializing data: ({}) {}. Data:'''{}'''".format(
                                        exp.__class__.__name__,
                                        exp.message,
                                        data),exc_info=1)
                    self.shutdown = True
                
                except Exception as exp:
                    response.status_code = StatusCode.ERROR
                    response.status = exp.message

                    logger.error("An unknown error occurred: ({}) {}".format(exp.__class__.__name__,exp.message),exc_info=1)
                    self.shutdown = True
            
            should_loop = True
            while request is not None and should_loop:
                should_loop = False
                try:
                    self.send(response.to_json())
                    
                    logger.log(5,"RequestProcessor sent response!")
                    #self.shutdown = True
                    
                except socket.error as exp:
                    if exp.errno == errno.EAGAIN:
                        #logger.log(5,"A socket IO error occurred while sending client response: (Errno:{}) {}. Will Retry...".format(exp.errno,exp.message))
                        should_loop = not self.shutdown
                        time.sleep(.1)
                        continue
                    else:
                        logger.error("A socket IO error occurred while sending client response: (Errno:{}) {}. Will Retry...".format(exp.errno,exp.message),exc_info=1)
                        self.shutdown = True

                except Exception as exp:
                    logger.error("An error occurred sending our response to the client: {}".format(exp),exc_info=1)
                    self.shutdown = True
            
            time.sleep(.1)
                        
        if self.shutdown:
            try:
                logger.log(5,"RequestProcessor spinning down.")
                self.socket.shutdown(socket.SHUT_RDWR)
                self.socket.close()
            except socket.error as exp:
                if exp.errno == errno.ENOTCONN:
                    pass
                else:
                    logger.debug("An error occurred shutting down our connection. Error:{}".format(exp),exc_info=1)
            except Exception as exp:
                logger.debug("An error occurred shutting down our connection.",exc_info=1)
    
    def read(self):
        """
        Method that will read in response data from our IPC socket.
        """
        
        return read_socket_data(self.socket)
        
    def send(self, data):
        """
        Method used to send data to our server service over our IPC socket.
        
        :param str data: The data to send. 
        """
        
        send_socket_data(self.socket, data)

class Server(object):
    """
    Class which provides server-side connection and request processing.
    """
    
    logger_name = None
    _run_directory = None
    
    @property
    def run_directory(self):
        """
        Returns our run file directory
        """
        return self._run_directory
    
    @run_directory.setter
    def run_directory(self,value):
        """
        Sets our run file directory
        """
        self._run_directory = value
    
    def __init__(self, hostname=None, port=None, run_directory=None,
                                            delegate=None,*args,**kwargs):
        """Class constructor.

        :param str hostname: The address to bind to (default "127.0.0.1")
        :param int port: The port to bind to (default 9216)
        :param str run_directory: The directory to use for run file creation.
        :param delegate: delegate object used for references. This object should
            support the method `process_request()`
        
        :raises AttributeError: If filepath isn't provided and no run directory is configured.
        :raises IOError: On various IO related problems.
        
        """
        
        if hostname is None:
            hostname = DEFAULT_ADDRESS
        
        if port is None:
            port = DEFAULT_PORT
        
        self.port = port            #: The TCP port to listen on
        self.hostname = hostname    #: The FQDN
        self.is_acme_daemon = None  #: If true we will force to register as a daemon
        self._run_directory = run_directory
        
        self.server_thread = None
        self.threads = []
        self.threads_lock = threading.RLock()
        self.socket = None
        self.socket_timeout = SOCKET_DEFAULT_TIMEOUT
        self.delegate = delegate
        self.shutdown = False
        self.requests = {}
        self.logger_name = "ipc.Server"
    
    def create_run_file(self, filepath=None):
        """
        Method to create a run file.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if not filepath and not self.run_directory:
            raise AttributeError("Cannot create run file, filepath was not specified and no run_directory set!")
            
        
        type = None
        user = getpass.getuser()
        if user == "root" or self.is_acme_daemon:
            type = "daemon"
            filename = "acme.ipc.daemon"
        else:
            type = "client"
            filename = "acme.ipc.client.{}".format(user)
        
        if filepath is None:
            filepath = os.path.join(self.run_directory, filename)
        
        pid = os.getpid()
        
        data = {
                 "port": self.port,
                 "address": self.hostname,
                 "type": type,
                 "pid": pid
               }
                
        if type != "daemon":
            data["user"] = user
          
        logger.log(9, "Creating runfile at path:'{}'".format(filepath))
        
        with codecs.open(filepath, "w") as f:
            while True:
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    f.write("{}\n".format(json.dumps(data, indent=4)))
                    fcntl.flock(f, fcntl.LOCK_UN)
                    break
                except IOError as exp:
                    logger.info("UserSessionEndCleanUpAgent may lock this file, Try Again. {}".format(exp))
                    time.sleep(10)
    
    def destroy_run_file(self, filepath=None):
        """
        Method to delete a run file.
        """
        
        type = None
        user = getpass.getuser()
        
        if user == "root":
            type = "daemon"
            filename = "acme.ipc.daemon"
        else:
            type = "client"
            filename = "acme.ipc.client.{}".format(user)
        
        if filepath is None:
                filepath = os.path.join(self.run_directory, filename)
        
        if os.path.isfile(filepath):
            os.remove(filepath)
        
    def __enter__(self):
        """
        Construct method to facilitate a context-manager use-case for Server objects.
        """
        
        self.start()
        return self
         
    def __exit__(self, type, value, traceback):
        """
        Teardown method to facilitate a context-manager use-case for Server objects.
        """
        
        logger = logging.getLogger("ipc.Server")
        
        try:
            self.stop()
        except Exception as exp:
            logger.warn("An error occurred shutting down the Server. Error:{}".format(exp))
            
    def process_request(self,request):
        """
        Method which will process an Request object from a client. This 
        executes syncronously, and will typically be called off the main thread
        or primary server_thread to prevent blocking.

        :param request: The request to process
        :type request: ipc.Request

        :returns: Response
        :raises: IPCError
        """

        logger = logging.getLogger(self.__class__.__name__)

        response = Response(request=request)
        
        if self.delegate:
            logger.log(5,"Passing to delegate:{}".format(self.delegate.__class__.__name__))
            response = self.delegate.process_request(request)
        else:
            response.status_code = StatusCode.SUBSYSTEM_UNSET
            response.status = "Failed to process request: our Server instances has no delegate configured!"
            logger.error(response.status)
            
        return response
        
    def start(self):
        """
        Initiate our service.
        """
        
        logger = logging.getLogger("ipc.Server")
        
        logger.info("Starting IPC server ({}:{})...".format(self.hostname,
                                                            self.port))
        
        try:
            self.create_run_file()
        except Exception as exp:
            logger.warning("Failed to create run file:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        self.shutdown = False
        self.server_thread = threading.Thread(target=self.loop,
                                                    name="IPCServerThread")
        self.server_thread.daemon = True
        self.server_thread.start()
        
        time.sleep(.5)

    def stop(self):
        """
        Shutdown our service.
        """
        
        logger = logging.getLogger("ipc.Server")
        
        logger.info("Stopping IPC server...")
        
        self.shutdown = True
        
        if self.socket is not None:
            logger.debug("Closing server socket...")
            try:
                self.socket.close()
                self.socket = None
            except AttributeError:
                pass

        try:
            self.destroy_run_file()
        except Exception as exp:
            logger.warning("Failed to create run file:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        num_active_threads = len(self.threads)
        if num_active_threads > 0:
            logger.debug("Closing down request processors...")
        
        if self.server_thread and self.server_thread.is_alive():
            num_active_threads += 1
            logger.debug("Waiting for server processors...")
                
        force_shutdown_time = datetime.datetime.utcnow() + THREAD_EXIT_TIMEOUT
        now = datetime.datetime.utcnow()

        try:
            logger.log(9,"Closing down slave threads...")
            while num_active_threads > 0 and now < force_shutdown_time:
                logger.log(2,"Waiting for active threads to close...")
                now = datetime.datetime.utcnow()
                num_active_threads = 0
                for thread in self.threads:
                    thread.shutdown = True
                    if thread.is_alive():
                        num_active_threads += 1
                if self.server_thread and self.server_thread.is_alive():
                    num_active_threads += 1
                
                if num_active_threads > 0:
                    time.sleep(0.01)
            
            if now >= force_shutdown_time:
                thread_names = []
                if num_active_threads > 0:
                    thread_names = map(lambda t:t.name, self.threads)
                
                if self.server_thread and self.server_thread.is_alive():
                    thread_names.append(self.server_thread.name)
                
                logger.warning("Abandoning {} latent threads:'{}'!!...".format(
                         num_active_threads,"' ,'".join(thread_names)))
                    
        except KeyboardInterrupt:
            pass
        
        logger.info("IPC server shutdown complete...")

    def loop(self):
        """
        This is our primary run loop, in typical usage it operates on it's
        own thread, controlled by :py:func:`Server.start` and 
        :py:func:`Server.stop`
        """
        
        logger = logging.getLogger("ipc.Server")
        
        logger.log(9,"Initiating main server run loop...")
        
        ## Establish our socket and start listening.
        my_socket = self.socket
        
        try:
            my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            my_socket.settimeout(SOCKET_DEFAULT_TIMEOUT.total_seconds())
            my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            my_socket.bind((self.hostname, self.port))
            my_socket.listen(10)
        except socket.error as exp:
            logger.critical("Address already in use! ({}:{})".format(self.hostname,
                                                                self.port))
            self.shutdown = True
        except Exception as exp:
            logger.critical("An unknown error occurred:{}".format(exp.message))
            self.shutdown = True

        ## Primary run loop
        while not self.shutdown:
            try:
                (sock, addr) = my_socket.accept()
                rp = RequestProcessor(sock,addr,self)
                rp.start()
                with self.threads_lock:
                    self.threads.append(rp)
            except socket.timeout:
                pass
            except Exception as exp:
                logger.error("An error occurred: {}".format(exp))
                break

            time.sleep(0.1)
                        
            ## Prune our threads
            for thread in self.threads[:]:
                with self.threads_lock:
                    if not thread.is_alive():
                        try:
                            self.threads.remove(thread)
                        except Exception as exp:
                            logger.warning("Failed to remove thread:{} from thread list. Error: {}".format(thread,exp))
        

#MARK: - Functions
def read_socket_data(sock):
    """
    Method used to read a response from our server service over the provided
    socket.
    """
    
    logger = logging.getLogger(__name__)
    
    segments = []
    socket_bytes_read = 0
    
    if not sock:
        raise IPCError(errno=StatusCode.SUBSYSTEM_UNSET, 
                            message="Cannot read data, no socket is set.")
                            
    logger.log(2, "Reading IPC data...")
    
    ## Read in our header
    header_bytes = []
    current_byte = None
    content_len = 0
    
    start_time = time.time()
    while not current_byte == SOCKET_HEADER_DELIM:
        try:
            current_byte = sock.recv(1)
            
            if not current_byte:
                raise IPCError(errno=StatusCode.SOCKET_CLOSED, 
                    message="Failed to read data, socket appears to have closed.")
            
            header_bytes.append(current_byte)
            
            if len(header_bytes) > SOCKET_HEADER_MAXSIZE:
                raise IPCError(message="Failed to read content header: max length exceeded!",
                                    errno=StatusCode.INVALID_HEADER)
            
        except socket.error as exp:
            if exp.errno == errno.EAGAIN:
                logger.log(5,"A socket IO error occurred while reading header: {} (Errno:{}). Will Retry...".format(
                                                exp.message, exp.errno))
                time.sleep(.1)
                continue
            raise IPCError("Failed to read IPC content due to a socket error: {}".format(exp),
                errno=StatusCode.SOCKET_ERROR), None, sys.exc_info()[2]
        except IPCError as exp:
            raise
        except Exception as exp:
            raise IPCError("Failed to read content header: {}".format(exp),
                    errno=StatusCode.INVALID_HEADER), None, sys.exc_info()[2]
    
    content_len = int("".join(header_bytes[:-1]))
    
    header_end_time  = time.time()
    logger.log(2,"Read IPC header (content length:{} bytes)".format(
                                                    content_len)) 
    
    ## Read in our Content
    while socket_bytes_read < content_len:
        try:
            data = sock.recv(min(content_len - socket_bytes_read, 
                                                        SOCKET_BUFFER_SIZE))
            if not data:
                raise IPCError(errno=StatusCode.SOCKET_CLOSED, 
                    message="Failed to read data, socket appears to have closed.")
            
            socket_bytes_read += len(data)
            segments.append(data)
        except socket.error as exp:
            if exp.errno == errno.EAGAIN:
                logger.log(5,"A socket IO error occurred while reading data: {} (Errno:{}). Will Retry...".format(
                                                exp.message, exp.errno))
                time.sleep(.1)
                continue
            raise IPCError("Failed to read IPC content due to a socket error: {}".format(exp),
                errno=StatusCode.SOCKET_ERROR), None, sys.exc_info()[2]
        except socket.timeout as exp:
            raise IPCError("Failed to read IPC content due to a socket error: {}".format(exp),
                errno=StatusCode.SOCKET_ERROR), None, sys.exc_info()[2]
    
    if not segments:
        raise IPCError(errno=StatusCode.ERROR, 
                message="Failed to read data, socket appears to be dead.")
    
    end_time = time.time()
    
    data = "".join(segments)
    
    logger.log(9, "Read IPC data (len:{} reads:{} time:{})".format(
                        socket_bytes_read,
                        len(segments),
                        end_time - start_time))
    return data

def send_socket_data(sock, data):
    """
    Method used to send data to our server service over the provided socket.
    
    :param str data: The data to send. 
    
    """
    
    logger = logging.getLogger(__name__)
    
    socket_bytes_sent = 0
    
    if not socket:
        raise IPCError(errno=StatusCode.SUBSYSTEM_UNSET, 
                            message="Cannot read data, no socket is set.")
    
    logger.log(2, "Sending IPC data...")
    
    start_time = time.time()
    ## Send our header
    try:
        content_len = len(data)
        header = "{}{}".format(content_len, SOCKET_HEADER_DELIM)        
        bytes_sent = sock.send(header)
        if bytes_sent == 0:
            raise IPCError(errno=StatusCode.SOCKET_ERROR, 
                message="Failed to send IPC header data, socket appears to be broken.")
    except socket.error as exp:
        raise IPCError("Failed to send IPC header due to a socket error: {}".format(exp),
            errno=StatusCode.SOCKET_ERROR), None, sys.exc_info()[2]
    except socket.timeout as exp:
        raise IPCError("Failed to send IPC header due to a socket error: {}".format(exp),
            errno=StatusCode.SOCKET_ERROR), None, sys.exc_info()[2]
    
    header_end_time  = time.time()
    ## Send our content
    while socket_bytes_sent < content_len:
        try:
            bytes_to_send = data[socket_bytes_sent:]
            bytes_sent = sock.send(bytes_to_send)
            socket_bytes_sent += bytes_sent
            if bytes_sent == 0:
                raise IPCError(errno=StatusCode.SOCKET_ERROR, 
                    message="Failed to send IPC data, socket appears to be broken.")
        except socket.error as exp:
            if exp.errno == errno.EAGAIN:
                logger.log(5,"A socket IO error occurred while sending data: {} (Errno:{}). Will Retry...".format(
                                                exp.message, exp.errno))
                time.sleep(.1)
                continue
                time.sleep(.1)
            raise IPCError("Failed to send IPC header due to a socket error: {}".format(exp),
                errno=StatusCode.SOCKET_ERROR), None, sys.exc_info()[2]
        except socket.timeout as exp:
            raise IPCError("Failed to send IPC header due to a socket error: {}".format(exp),
                errno=StatusCode.SOCKET_ERROR), None, sys.exc_info()[2]
    
    end_time = time.time()
    
    logger.log(9,"Sent IPC data (len:{} time:{})".format(
                        socket_bytes_sent,
                        end_time - start_time))
            

#MARK: - Exceptions
class IPCError(Exception):
    """
    Exception class representing a generic IPC error.
    """
    
    def __init__(self, message="", errno=None):
        self.message = message
        self.errno = errno

    def __str__(self):
        if self.errno:
            return "An IPCError occurred (errno:{}): {}".format(self.errno,
                                                        self.message)
        else: 
            return "An IPCError occurred: {}".format(self.message)
            
class ValidationErrorType(core.Enum):
    """
    Enum which represents token validation error.
    """
    UNKNOWN = 0
    NO_AUTH_TOKEN = 1 << 1
    OWNER_ERROR = 1 << 2
    TOKEN_EXPIRED = 1 << 3
    CONTENT_ERROR = 1 << 4
    
class TokenValidationError(Exception):
    """
    Exception class representing a generic token validation error.
    """
    
    def __init__(self,message="",error_code=None):
        self.message = message
        self.error_code = error_code

    def __str__(self):
        if self.errno:
            return "An TokenValidationError occurred (error_code:{}): {}".format(self.error_code,
                                                        self.message)
        else: 
            return "An TokenValidationError occurred: {}".format(self.message)
