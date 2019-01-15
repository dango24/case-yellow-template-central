"""
**proxy** - module which provides event routing capabilities between various
    ACME-related processes. This package contains platform-specific 
    implementations for passing event notifications between various runtime
    contexts (i.e. running as root vs running in user's context). These proxies
    ensure that :py:mod:`systemevents` triggers can be utilized in either
    root or user context. 

:platform: RHEL5, OSX, Ubuntu
:synopsis: This module provides facilities to route events via IPC to related
    processes (acmed, acmed client).

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import datetime
import logging
import os
import subprocess
import threading

import acme
import acme.ipc as ipc
import acme.network as network

import acme.systemevents as systemevents

PROXY_TARGETTYPE_DAEMON = "Daemon"
PROXY_TARGETTYPE_CLIENT = "Client"

#MARK: -
#MARK: Classes
class ProxiedEvent(acme.SerializedObject):
    """
    Class which represents an event that is proxied.
    """
    
    key = None     #: Our event key "NetworkDidChange"
    data = {}
    
    def __init__(self,key=None,data=None,*args,**kwargs):
        
        key_map = { "key" : None,
                    "data": None
                    }
        
        if key is not None:
            self.key = key
            
        if data is not None:
            self.data = data
        
        super(ProxiedEvent,self).__init__(key_map=key_map,*args,**kwargs)
        
#MARK: -
class EventProxy(object):
    """
    Root Class providing basic proxy framework interface, you should overide
    methods implemented in this class with custom functionality.
    """
    
    logger_name = "EventProxy"
    target_type = PROXY_TARGETTYPE_DAEMON
    
    def handle_proxied_event(self,event):
        """
        Method to handle proxied events, called by the receiver.
        The default implementation of this method raises a RuntimeError,
        you should override and implement custom handling functionality.
        
        """
        logger = logging.getLogger(self.logger_name)
        logger.error("handle_proxied_event() not implemented!")
        
    def proxy_event(self,event):
        """
        Method to proxy the provided event to all running targets (as defined
        by self.target_type). 
        The default implementation of this method raises a RuntimeError,
        you should override and implement custom handling functionality.
        
        :param event: The event to proxy
        :type event: :py:class:`ProxiedEvent`
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        event_json = event.to_json()
        
        rundir = os.path.join(acme.BASE_DIR,"run")
        rundata = ipc.Client().load_runfile_details(rundir)
        
        request = ipc.Request(action="ProxyEvent")
        request.options["event_data"] = event.to_dict()
        
        for data in rundata[self.target_type]:
            port = data["port"]
            address = data["address"]
            
            if "type" in data:
                type = data["type"]
            else:
                type = "type:{}".format(self.target_type)
            
            with ipc.Client(port=port,hostname=address) as c:
                logger.log(9,"Proxying event:{} to {}:'{}:{}'".format(
                                                event.key,type,address,port))
                try:
                    response = c.submit_request(request)
                except Exception as exp:
                    logger.warning("Failed to proxy event:{} to daemon!".format(event.key))

#MARK: -
#MARK: Module Globals

DaemonEventProxy = None     #: Our daemon proxy class, used to proxy events to  
                            #: our daemon, and also handles events received by 
                            #: our daemon. Type: :py:class:`EventProxy` 
                            #: descendant

ClientEventProxy = None     #: Our client proxy class, used by our daemon to  
                            #: proxy events to user-context clients, and used
                            #: by user-context clients to handle events 
                            #: received from a daemon.

def configure():
    """
    Method to configure our platform proxy
    """
    
    if acme.platform == "OS X" or acme.platform == "macOS":
        configure_osx()
    elif acme.platform == "Ubuntu":
        configure_ubuntu()
    else:
        configure_default()
    
def configure_default():
    """
    Method to configure our event handlers for macOS
    """
    
    global DaemonEventProxy, ClientEventProxy
        
    DaemonEventProxy = EventProxy
    ClientEventProxy = EventProxy
    
def configure_osx():
    """
    Method to configure our event handlers for macOS
    """
    
    import proxy_osx
    global DaemonEventProxy, ClientEventProxy
        
    DaemonEventProxy = proxy_osx.DaemonEventProxyOSX
    ClientEventProxy = proxy_osx.ClientEventProxyOSX
    
def configure_ubuntu():
    """
    Method to configure our event handlers for DBus (Ubuntu)
    """
    
    import proxy_ubuntu
    global DaemonEventProxy, ClientEventProxy
        
    DaemonEventProxy = proxy_ubuntu.DaemonEventProxyUbuntu
    ClientEventProxy = proxy_ubuntu.ClientEventProxyUbuntu
    
if DaemonEventProxy is None or ClientEventProxy is None:
    configure()


