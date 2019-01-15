"""
**systemevents** - Package which is responsible for providing registration
    capabilities, monitoring and dispatching events. This module provides two
    module-level variables: :py:var:`system_handler` and 
    :py:var:`session_handler`. Upon importing this package these two variables
    will be automatically instantiated with a platform-specific class which 
    inherit from :py:class:`EventHandlerBase`, in a Daemon and user Context 
    respectively. External consumers will reference these two variables
    to register event handlers.  

:platform: RHEL5, OSX, Ubuntu
:synopsis: This is the root module that is used to establish watchers for 
    various system events and triggering registered event handlers.

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

#MARK: Imports
import copy
import datetime
import logging
import os
import subprocess
import threading

import acme
import systemprofile

#MARK: - Defaults
DELAYED_NETWORK_TIMER_INTERVAL = datetime.timedelta(seconds=4)   #: :py:class:`datetime.timedelta` object 
                                        #: which designates how long we wait for 
                                        #: network changes to settle before 
                                        #: triggering assessments.

#MARK: - Classes
class EventHandlerBase(object):
    """
    Class which provides event registration and dispatching capabilities.
    
    This class defines a base set of event triggers which should be invoked
    by various system-dependent implementations. For each platform supported
    by this system, you should inherit from this class and implement 
    hooks into the platforms basic eventing mechanisms (where possible).
    Platform specific implementations should bootstrap event_threads or other
    listener configurations via :py:func:`start_listener` and 
    :py:func:`register_subsystems` methods. Platform-specific implementations
    should also implement custom code via the :py:mod:`network` module to
    support network-based event triggering. 
    
    """
    
    registered_handlers = {"UserDidLogin" : [],
                                "UserWillLogout" : [],
                                "UserSessionLocked" : [],
                                "UserSessionUnlocked" : [],
                                "SystemResumed" : [],
                                "SystemWillSuspend" : [],
                                "SystemDidStartup" : [],
                                "SystemDidRegister" : [],
                                "ProcessDidStart" : [],
                                "SystemWillShutdown" : [],
                                "NetworkDidChange" : [],
                                "NetworkSessionDidChange" : [],
                                "NetworkSiteDidChange" : [],
                                "DidConnectToInternet" : [],
                                "DidLeaveInternet" : [],
                                "DidConnectToIntranet" : [],
                                "DidLeaveIntranet" : [],
                                "DidConnectToPublicNetwork" : [],
                                "DidLeavePublicNetwork" : [],
                                "DidConnectToVPN" : [],
                                "DidLeaveVPN" : [],

                            } #: Event handlers
    
    event_thread = None             #: Thread used to run event detection. This is not utilized on all platforms. If you plan to spin up a :py:class:`threading.Thread` for event detection, use this variable to store the thread.
    
    lock = None                     #: Lock object to ensure consistency when dealing with delegates
    
    network_change_start = None     #: :py:class:`datetime.datetime` object representing the 
                                    #: date when a current network change chain started
                                    #: This value is reset to None when the chain ends
    delayed_network_timer = None    #: A timer invoked to address network state flapping
    network_lock = None             #: A lock object to ensure consistency across network changes
    
    system_start_window = datetime.timedelta(minutes=5)  #: Timedelta object representing how long after system startup we must be to trigger a system_start event
    
    logger_name = None              #: Name used for logging output
    
    def __init__(self,registered_handlers=None,*args,**kwargs):
        """
        :param registered_handlers: Handlers that our implementation supports.
                This is a dictionary of arrays keyed by Event Type
        :type registered_handlers: Dictionary <string,list> keyed by event type.
                Each list contains callback references associated each event.
        
        """
        
        self.event_thread = None
        
        self.lock = threading.RLock()
        self.network_lock = threading.RLock()

        if registered_handlers:
            self.registered_handlers = registered_handlers
        else:
            self.registered_handlers = copy.deepcopy(EventHandlerBase.registered_handlers)
                            
        if not self.logger_name:
            self.logger_name = "EventHandlerBase"
    
    
    #MARK: - Platform Override Methods
    def start_listener(self):
        """
        Method which starts our main event loop on a background thread. 
        
        When you inherit from this EventHandlerBase class, you will want to 
        override this method if you need to spin up an event_thread or custom
        runloop. When overriding this method, you should invoke this parent 
        method after performing your custom bootstrap. 
        """
        
        logger = logging.getLogger(self.logger_name)
        
        ## Check for recent startup
        system_start_date = systemprofile.profiler.system_start_date()
        
        self.should_run = True
        
        now = datetime.datetime.utcnow()
        
        logger.debug("Events listener started, checking for recent system startup...")
        
        if system_start_date and (system_start_date 
                                        + self.system_start_window >= now):
            logger.log(5,"System has recently started up ({}) (now:{})".format(
                                                        system_start_date,
                                                        now))
            self.system_did_startup()
        else:
            logger.log(5,"System has not recently started up (last:'{}' now:'{}')".format(
                                                            system_start_date,
                                                            now))
                                                            
        self.process_did_start()
            
    def stop_listener(self):
        """
        Method which stops our main event loop. 
        
        
        When you inherit from this EventHandlerBase class, you will want to 
        override this method if you need to properly cleanup up event_thread 
        or custom runloop.       
        """
        
        return
                
    def is_listening(self):
        """
        Method which returns whether or not we are currently listening for 
        and dispatching events. The default implementation of this method
        returns True or false based upon whether self.event_thread is
        initialized and still alive.
        
        If you inherit from this EventHandlerBase class and implement your
        own custom run loop or listener logic, you should override this class
        and return true/false depending on whether you are actively running.
        
        :returns: (bool) True if our event handler is currently running/active
        
        """
        
        if self.event_thread and self.event_thread.is_alive():
            return True
        else:
            return False
    
    def register_subsystems(self):
        """
        Method used to perform registration of any event handlers. The default
        implementation of this method does nothing. 
        
        If you inherit from EventHandlerBase class and need to register
        custom observers. For instance, on our macOS module, we register
        custom NSWorkspace and SystemConfiguration listeners. In our Ubuntu
        module, this method is used to register DBUS listeners.
        """
        None
    
    def unregister_subsystems(self):
        """
        Method used to unregister/unsubscribe from any handlers. The default
        implementation of this method does nothing. 
        
        If you inherit from EventHandlerBase class and register custom observers
        via :py:func:`self.register_subsystems` that need proper cleanup/disposal,
        do that here.
        """
        None
    
    
    #MARK: - Core Methods
    def register_handler(self,key,handler):
        """
        Method used to register an event handler against the provided SsytemEvent
        key.
        
        SystemEvent Keys
        ========================
        UserDidLogin
        UserWillLogout
        UserSessionLocked
        UserSessionUnlocked
        SystemResumed
        SystemWillSuspend
        SystemDidStartup
        SystemDidRegister
        ProcessDidStart
        SystemWillShutdown
        NetworkDidChange
        NetworkSessionDidChange
        NetworkSiteDidChange
        DidConnectToInternet
        DidLeaveInternet
        DidConnectToIntranet
        DidLeaveIntranet
        DidConnectToPublicNetwork
        DidLeavePublicNetwork
        DidConnectToVPN
        DidLeaveVPN

        """
        
        logger = logging.getLogger(self.logger_name)
        
        with self.lock:
            handler_keys = filter(lambda x: x.lower() == key.lower(), 
                                                self.registered_handlers.keys())
            
            if len(handler_keys) == 0:
                raise EventKeyNotFoundError(key,self.registered_handlers.keys())
            
            for handler_key in handler_keys:
                logger.log(2,"Registering handler:'{}' for key:'{}'".format(
                                                        handler,handler_key))
                self.registered_handlers[handler_key].append(handler)
                    
    def unregister_handler(self,key,handler):
        """
        Method used to remove a registered event handler.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        with self.lock:
            handler_keys = filter(lambda x: x.lower() == key.lower(), 
                                                self.registered_handlers.keys())
            if len(handler_keys) == 0:
                return
            
            for handler_key in handler_keys:
                logger.log(2,"Unregistering handler:{} for event:{}".format(
                                                        handler,handler_key))
                try:
                    self.registered_handlers[handler_key].remove(handler)
                except Exception as exp:
                    logger.warning("Failed to unregister handler:{} for event:{}. Error:{}".format(
                                                        handler,
                                                        handler_key,
                                                        exp.message))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        
    def handlers_for_key(self,key):
        """
        Method to return all registered event handlers for the provided
        key.
        """
        
        with self.lock:
            registered_handlers = self.registered_handlers
            
            handler_keys = filter(lambda x: x.lower() == key.lower(), 
                                                registered_handlers.keys())
            
            handlers = []
        
            for handler_key in handler_keys:
                handlers = list(set(registered_handlers[handler_key] + handlers))
        
        return handlers
    
    #MARK: - System Event Callbacks    
    def user_did_login(self,username=None,*args,**kwargs):
        """
        Callback executed on user login. This method will be invoked by
        platform-specific triggers.
        
        Executes handlers registered to trigger 'UserDidLogin'
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "UserDidLogin"
        
        logger.log(9, "SystemTrigger: {} (user:{})".format(key,username))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback(username=username)
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass
    
    def user_will_logout(self,username=None,*args,**kwargs):
        """
        Callback executed on user logout. This method will be invoked by
        platform-specific triggers.
        
        Executes handlers registered to trigger 'UserWillLogout'
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "UserWillLogout"
        
        logger.log(9, "SystemTrigger: {} (user:{})".format(key,username))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback(username=username)
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass
    
    def user_session_locked(self,username=None,*args,**kwargs):
        """
        Callback executed on user screen lock. This method will be invoked by
        platform-specific triggers.
        
        Executes handlers registered to trigger 'UserSessionLocked'
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "UserSessionLocked"
        
        logger.log(9, "SystemTrigger: {} (user:{})".format(key,username))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback(username=username)
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass
    
    def user_session_unlocked(self,username=None,*args,**kwargs):
        """
        Callback executed on user screen unlock.This method will be invoked by
        platform-specific triggers.
        
        Executes handlers registered to trigger 'UserSessionUnlocked'

        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "UserSessionUnlocked"
        
        logger.log(9, "SystemTrigger: {} (user:{})".format(key,username))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback(username=username)
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass
    
    def system_resumed(self,*args,**kwargs):
        """
        Callback executed on system wake. This method will be invoked by
        platform-specific triggers.
        
        Executes handlers registered to trigger 'SystemResumed'
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "SystemResumed"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass
    
    def system_will_suspend(self,*args,**kwargs):
        """
        Callback executed prior to system sleep. This method will be invoked by
        platform-specific triggers.
        
        Executes handlers registered to trigger 'SystemWillSuspend'

        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "SystemWillSuspend"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass     
                
                
    def system_did_startup(self,*args,**kwargs):
        """
        Callback executed on system startup. This method will be invoked by
        platform-specific triggers.
        
        Executes handlers registered to trigger 'SystemDidStartup'

        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "SystemDidStartup"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass            
    
    def system_did_register(self,*args,**kwargs):
        """
        Callback executed on system registration. This method will be invoked 
        after a system successfully registers with KARL (this will generally
        be a one-time event.
        
        Executes handlers registered to trigger 'SystemDidStartup'

        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "SystemDidRegister"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass 
    
    def system_will_shutdown(self,*args,**kwargs):
        """
        Callback executed prior to system shutdown/reboot. This method will be 
        invoked by platform-specific triggers.
        
        Executes handlers registered to trigger 'SystemWillShutdown'
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "SystemWillShutdown"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass

    def process_did_start(self,*args,**kwargs):
        """
        Callback executed on process start. This method will be invoked by
        platform-specific triggers.
        
        Executes handlers registered to trigger 'SystemDidStartup'

        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "ProcessDidStart"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass            

   
    def network_did_change(self,*args,**kwargs):
        """
        Triggered by any network change (IP, interface). Calls to this method 
        should be collated & throttled by :py:func:`reset_delayed_network_timer` to ensure
        only one trigger under heavy changes. (i.e. multiple network changes
        after system wake). Event handlers should use the latter function to
        take advantage of this coalescence
        """

        logger = logging.getLogger(self.logger_name)
        
        key = "NetworkDidChange"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        with self.network_lock:
            self.delayed_network_timer = None
            
            if self.network_change_start:
                resolve_time = datetime.datetime.utcnow() - self.network_change_start
                self.network_change_start = None
                logger.debug("NetworkChange resolved in {} seconds...".format(
                                                resolve_time.total_seconds()))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception as exp:
            logger.warning("Failed to retrieve callbacks for key:{}. Error:{}".format(
                                                                key,exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
                                                                
                
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass
        
        """ Todo: pretty sure it's safe to ditch this
        try:
            systemevents.system_handler.network_change_callback()
        except Exception as exp:
            logger.error("Failed to execute callback: network_change_callback(). Error:{}".format(exp))
        """
    
    def network_session_did_change(self,new_session=None,old_session=None,
                                                            *args,**kwargs):
        """
        Triggered by any network change (IP, interface). Calls to this method 
        should be collated & throttled by :py:func:`reset_delayed_network_timer` to ensure
        only one trigger under heavy changes. (i.e. multiple network changes
        after system wake). Event handlers should use the latter function to
        take advantage of this coalescence
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "NetworkSessionDidChange"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception as exp:
            logger.warning("Failed to retrieve callbacks for key:{}. Error:{}".format(
                                                                key,exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
            logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
                
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback(new_session=new_session,old_session=old_session)
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass

    def network_site_did_change(self,new_site=None,old_site=None,
                                                            *args,**kwargs):
        """
        Triggered by any network change (IP, interface). Calls to this method 
        should be collated & throttled by :py:func:`reset_delayed_network_timer` 
        to ensure only one trigger under heavy changes. (i.e. multiple network 
        after system wake). Event handlers should use the latter function to
        take advantage of this coalescence.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "NetworkSiteDidChange"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception as exp:
            logger.warning("Failed to retrieve callbacks for key:{}. Error:{}".format(
                                                                key,exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
                
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback(new_site=new_site,old_site=old_site)
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass

    def reset_delayed_network_timer(self):
        """
        Method which will reset our delayed network timer. This is invoked by
        platform-specific triggers whenever a network change event is detected. 
        The premise is that due to resource cost we won't run network 
        assessment tests while under flux; when a network change occurs we 
        will wait a short time to ensure state has settled prior to triggering 
        any automations.
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        with self.network_lock:
            if self.delayed_network_timer is not None:
                logger.log(2,"Resetting delayed_network_timer")
                self.delayed_network_timer.cancel()
            else:
                logger.log(2,"Creating delayed_network_timer")
                self.network_change_start = datetime.datetime.utcnow()
                
            self.delayed_network_timer = threading.Timer(
                                DELAYED_NETWORK_TIMER_INTERVAL.total_seconds(),
                                self.network_did_change)
            self.delayed_network_timer.start()
    
    def did_connect_to_internet(self,*args,**kwargs):
        """
        Callback executed upon connection to the Internet. This method will be 
        invoked by platform-specific triggers.
        
        Executes handlers registered to trigger 'DidConnectToInternet'
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "DidConnectToInternet"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass
    
    def did_leave_internet(self,*args,**kwargs):
        """
        Callback executed upon connection to the corporate network. This method 
        will be invoked by platform-specific triggers.
        
        Executes handlers registered to trigger 'DidLeaveToInternet'

        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "DidLeaveInternet"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass

                      
    def did_connect_to_intranet(self,*args,**kwargs):
        """
        Callback executed upon connection to the corporate network. This method 
        will be invoked by platform-specific triggers. Executes handlers 
        registered to trigger 'DidConnectToIntranet'

        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "DidConnectToIntranet"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass
                
    def did_leave_intranet(self,*args,**kwargs):
        """
        Callback executed upon losing connectivity to the corporate network.
        This method will be invoked by platform-specific triggers.
        
        Executes handlers registered to trigger 'DidLeaveIntranet'

        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "DidLeaveIntranet"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass    
                
    def did_connect_to_public_network(self,*args,**kwargs):
        """
        Callback executed upon losing connectivity to a public internet 
        connection, such as home/public WiFi. This method will be invoked by
        platform-specific triggers.
        
        Executes handlers registered to trigger 'DidConnectToPublicNetwork'
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "DidConnectToPublicNetwork"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass
                
    def did_leave_public_network(self,*args,**kwargs):
        """
        Callback executed upon losing connectivity to a public internet 
        connection, such as home/public WiFi. 
                
        Executes handlers registered to trigger 'DidLeavePublicNetwork'
        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "DidLeavePublicNetwork"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass

    def did_connect_to_vpn(self,*args,**kwargs):
        """
        Callback executed upon establishing a VPN connection
        
        Executes handlers registered to trigger 'DidConnectToVPN'

        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "DidConnectToVPN"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}' ({})".format(
                                                    len(callbacks),
                                                    key,
                                                    map(lambda x: x.__name__, 
                                                        ", ".join(callbacks))))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass
                
    def did_leave_vpn(self,*args,**kwargs):
        """
        Callback executed upon disconnecting from VPN.
        
        Executes handlers registered to trigger 'DidLeaveVPN'

        """
        
        logger = logging.getLogger(self.logger_name)
        
        key = "DidLeaveVPN"
        
        logger.log(9, "SystemTrigger: {}".format(key))
        
        callbacks = []
        try:
            callbacks = self.handlers_for_key(key)
        except Exception:
            pass
        
        logger.log(5,"Found {} callbacks for key:'{}'".format(
                                                    len(callbacks),key))
        for callback in callbacks:
            try:
                logger.log(2,"Calling delegate:'{}' for key:'{}'".format(callback,key))
                callback()
            except Exception as exp:
                logger.warning("Callback:{} failed. Error:{}".format(callback,exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                pass   

    

#MARK: - Exceptions
class EventKeyNotFoundError(Exception):
    """
    Exception called in the event that an unknown event handler is referenced.
    """    
    def __init__(self,key=None,available_keys=None):
        self.key = key
        self.available_keys = available_keys
        
    def __str__(self):
        message = "Invalid Key:"
        if self.key:
            message += ": '{}'".format(self.key)
        if self.available_keys:
            keys = self.available_keys
            message += " Available Keys:\n\t\"{}\",\n".format(key,
                                                        "\",\n\t\"".join(keys))
        return message

#MARK: - Module vars
system_handler = None           #: Our system_handler, which operates in system space.
session_handler = None          #: Our session handler, which operates in user space.

#MARK: - Module functions
def configure():
    """
    Method to configure our event handler.
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
    
    global system_handler, session_handler
        
    system_handler = EventHandlerBase()
    session_handler = EventHandlerBase()
    
def configure_osx():
    """
    Method to configure our event handlers for macOS
    """
    
    import systemevents_osx
    global system_handler, session_handler
        
    system_handler = systemevents_osx.SystemEventHandlerOSX()
    session_handler = systemevents_osx.SessionEventHandlerOSX()
    
def configure_ubuntu():
    """
    Method to configure our event handlers for DBus (Ubuntu)
    """
    
    import systemevents_ubuntu
    global system_handler, session_handler
    
    system_handler = systemevents_ubuntu.SystemEventHandlerDbus()
    session_handler = systemevents_ubuntu.SessionEventHandlerDbus()
    
if not system_handler:
    configure()
    
