"""
**systemevents_ubuntu** - Module which shims Ubuntu dbus events for ACME.

:platform: Ubuntu
:synopsis: This module serves as a shim layer between ACME's systemevent
    system and Ubuntu's dbus
    
.. codeauthor:: Beau Hunter <beauhunt@amazon.com>, based on code by 
        Vadim Omeltchenko <omeltche@amazon.com> and Shane Terpening 
        <sterpeni@amazon.com> 

"""

import copy
import datetime
import dbus
from dbus.mainloop.glib import DBusGMainLoop
import gobject
import json
import logging
import platform
import os
import time
import threading

from . import EventHandlerBase

import systemprofile

LOGIN_DELAY_WAITTIME = datetime.timedelta(seconds=1) #: Wait one second between login event trigger and user lookup. (necessary because username is not provided by DBUS during login events and TTY might not be created if fired immediately.

class SystemEventHandlerDbus(EventHandlerBase):
    """
    Class which provides ACME-style event triggers based on DBus SystemBus
    events.
    """
    
    gobject_loop = None
    dbus_session_receiver_entries = []
    dbus_system_receiver_entries = []
    logger_name = "SystemEventHandlerDbus"
    session_path_user_map = {}

    def __init__(self):
        
        super(SystemEventHandlerDbus,self).__init__()
        
        
        self.gobject_loop = None
        
        self.setup_dbus_config()
        
                
    def setup_dbus_config(self):
        """
        Method to establish our DBUS listenter details: signals, paths,
        interfaces, and callback.
        """
        
        distro, version, id = platform.dist()
        
        # Custom Python 2.7 on Ubuntu 14 identifies as 'jessie', not 14.
        if version.startswith("14") or version.startswith("16") or version.startswith("jessie"):
            return self._setup_dbus_config_ubuntu14()
        else:
            return self._setup_dbus_config_ubuntu18()
        
    def _setup_dbus_config_ubuntu14(self):
        """
        setup_debus_config() implementation for ubuntu14
        """
        
        self.dbus_system_receiver_entries = [
                                {   
                                    "signal_name" : "SessionAdded",
                                    "callback": self.login_callback,
                                    "dbus_interface" : "org.freedesktop.DisplayManager",
                                    "path" : "/org/freedesktop/DisplayManager"
                                },
                                {
                                    "signal_name" : "SessionRemoved",
                                    "callback": self.logout_callback,
                                    "dbus_interface" : "org.freedesktop.DisplayManager",
                                    "path" : "/org/freedesktop/DisplayManager"
                                },
                                {
                                    "signal_name" : "Resuming",
                                    "callback": self.resume_callback,
                                    "dbus_interface" : "org.freedesktop.UPower",
                                    #"path" : "/org/freedesktop/UPower"
                                },
                                {
                                    "signal_name" : "Sleeping",
                                    "callback": self.suspend_callback,
                                    "dbus_interface" : "org.freedesktop.UPower",
                                    #"path" : "/org/freedesktop/UPower"
                                },
                                { 
                                    "signal_name" : "PropertiesChanged",
                                    "callback" : self.network_change_callback,
                                    "dbus_interface" : "org.freedesktop.NetworkManager",
                                    "path" : "/org/freedesktop/NetworkManager",
                                },
                                { 
                                    "signal_name" : "PropertiesChanged",
                                    "callback" : self.network_change_callback,
                                    "dbus_interface" : "org.freedesktop.NetworkManager.Device.Wireless",
                                },
                                { 
                                    "signal_name" : "InstanceAdded",
                                    "path" : "/com/ubuntu/Upstart/jobs/network_2dinterface",
                                    "callback" : self.network_change_callback,
                                },
                                { 
                                    "signal_name" : "InstanceRemoved",
                                    "path" : "/com/ubuntu/Upstart/jobs/network_2dinterface",
                                    "callback" : self.network_change_callback,
                                }
                            ]

    def _setup_dbus_config_ubuntu18(self):
        """
        setup_debus_config() implementation for ubuntu18
        """
        
        self.dbus_system_receiver_entries = [
                                {   
                                    "signal_name" : "SessionAdded",
                                    "callback": self.login_callback,
                                    "dbus_interface" : "org.freedesktop.DisplayManager",
                                    "path" : "/org/freedesktop/DisplayManager"
                                },
                                {
                                    "signal_name" : "SessionRemoved",
                                    "callback": self.logout_callback,
                                    "dbus_interface" : "org.freedesktop.DisplayManager",
                                    "path" : "/org/freedesktop/DisplayManager"
                                },
                                {
                                    "signal_name" : "Resuming",
                                    "callback": self.resume_callback,
                                    "dbus_interface" : "org.freedesktop.UPower",
                                    #"path" : "/org/freedesktop/UPower"
                                },
                                {
                                    "signal_name" : "Sleeping",
                                    "callback": self.suspend_callback,
                                    "dbus_interface" : "org.freedesktop.UPower",
                                    #"path" : "/org/freedesktop/UPower"
                                },
                                { 
                                    "signal_name" : "PropertiesChanged",
                                    "callback" : self.network_change_callback,
                                    "dbus_interface" : "org.freedesktop.NetworkManager",
                                    "path" : "/org/freedesktop/NetworkManager",
                                },
                                { 
                                    "signal_name" : "PropertiesChanged",
                                    "callback" : self.network_change_callback,
                                    "dbus_interface" : "org.freedesktop.NetworkManager.Device.Wireless",
                                },
                                { 
                                    "signal_name" : "InstanceAdded",
                                    "path" : "/com/ubuntu/Upstart/jobs/network_2dinterface",
                                    "callback" : self.network_change_callback,
                                },
                                { 
                                    "signal_name" : "InstanceRemoved",
                                    "path" : "/com/ubuntu/Upstart/jobs/network_2dinterface",
                                    "callback" : self.network_change_callback,
                                }
                            ]

    def login_callback(self, *args, **kwargs):
        """
        Triggered by login events
        """

        logger = logging.getLogger(self.logger_name)
        logger.log(5, "login_callback() - args:{} kwargs:{}".format(args, kwargs))

        user = None
        try:
            time.sleep(LOGIN_DELAY_WAITTIME.total_seconds())
            logger.log(5, "Looking up current GUI user...")
            session_path = args[0]
            bus = dbus.SystemBus()
            session_obj = bus.get_object('org.freedesktop.DisplayManager', session_path)
            session_interface = dbus.Interface(session_obj, 'org.freedesktop.DBus.Properties')
            user = session_interface.Get('org.freedesktop.DisplayManager.Session', 'UserName')
            self.session_path_user_map[session_path] = user
        except Exception as exp:
            logger.warning("Failed to lookup user for login event. Error:{}".format(exp))

        try:
            self.user_did_login(user)
        except Exception as exp:
            logger.error("Failed to execute callback: user_did_login(). Error:{}".format(exp))
            logger.log(5, "Failure stack trace (handled cleanly):", exc_info=1)

    def logout_callback(self, *args, **kwargs):
        """
        Triggered by login events
        """

        logger = logging.getLogger(self.logger_name)
        logger.log(5, "logout_callback() - args:{} kwargs:{}".format(args, kwargs))

        user = None
        try:
            logger.log(5, "Looking up user for logout event.")
            session_path = args[0]
            user = self.session_path_user_map[session_path]
        except:
            pass

        try:
            self.user_will_logout(user)
        except Exception as exp:
            logger.error("Failed to execute callback: user_did_logout(). Error:{}".format(exp))
            logger.log(5, "Failure stack trace (handled cleanly):", exc_info=1)

    
    def resume_callback(self,*args,**kwargs):
        """
        Triggered by system resume
        """

        logger = logging.getLogger(self.logger_name)
        logger.log(5,"resume_callback() - args:{} kwargs:{}".format(args,kwargs))

        try:
            self.system_resumed()
        except Exception as exp:
            logger.error("Failed to execute callback: system_resumed(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

    def suspend_callback(self,*args, **kwargs):
        """
        Triggered by system suspend
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.log(5,"suspend_callback() - args:{} kwargs:{}".format(args,kwargs))
        
        try:
            self.system_will_suspend()
        except Exception as exp:
            logger.error("Failed to execute callback: system_will_suspend(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

    def network_change_callback(self, data=None, *args, **kwargs):
        """
        Triggered by DBUS network change events
        """
        
        logger = logging.getLogger(self.logger_name)
        
        ## Ignore bitrate updates, as these are very frequent on WiFi systems
        if data and len(data) == 1 and "Bitrate" in data:
            return
        elif data and len(data) == 1 and "AvailableConnections" in data:
            return
        
        logger.log(5,"network_change_callback() - data:{} args:{} kwargs:{}".format(
                                                    data,
                                                    args,
                                                    kwargs))
        
        if not self.network_change_start:
            logger.debug("NetworkChange detected, waiting for more changes...")
        
        self.reset_delayed_network_timer()
        

        """
        try:
            systemevents.handler.network_change_callback()
        except Exception as exp:
            logger.error("Failed to execute callback: network_change_callback(). Error:{}".format(exp))
        """

    def register_subsystems(self):
        """
        Method used to perform configuration.
        """
        
        self.register_dbus_receivers()
    
    def unregister_subsystems(self):
        """
        Method used to unregister handlers.
        """
    
    def start_listener(self):
        """
        Method which starts our main event loop on a background thread.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if not self.gobject_loop:
            gobject.threads_init()
            self.gobject_loop = gobject.MainLoop()
        
        if not self.event_thread or self.event_thread.is_alive(): 
            logger.info("Starting DBUS Event Listener...")
            self.event_thread = threading.Thread(target=self.gobject_loop.run,
                                            name="dbusThread")
            self.event_thread.daemon = True
            self.event_thread.start()
    
        super(SystemEventHandlerDbus,self).start_listener()
    
    def stop_listener(self):
        """
        Method which stops our main event loop
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if self.event_thread and self.event_thread.is_alive():
            if self.gobject_loop:
                logger.info("Stopping DBUS Event Listener...")
                self.gobject_loop.quit()
        
    def is_listening(self):
        """
        Method which returns whether or not we are currently listening for 
        and dispatching events.
        """
        
        if self.event_thread and self.event_thread.is_alive():
            return True
        else:
            return False
    
    def register_dbus_receivers(self):
        """
        Method to register our DBus receivers
        """
                
        DBusGMainLoop(set_as_default=True) 
        
        systembus = dbus.SystemBus() 
        
        for receiver_entry in self.dbus_system_receiver_entries:
            self._register_dbus_receiver(receiver_entry=receiver_entry, bus=systembus)

        return

    def _register_dbus_receiver(self, receiver_entry, bus):
        """
        Method to register our DBus receiver
        """
        
        logger = logging.getLogger(self.logger_name)
        
        try:
            callback = None
            signal_name = None
            interface = None
            path = None
            
            registration_entry = copy.copy(receiver_entry)
            
            if "callback" in registration_entry:
                callback = registration_entry["callback"]
                del registration_entry["callback"]
            
            if "signal_name" in registration_entry:
                signal_name = registration_entry["signal_name"]
            
            if "dbus_interface" in registration_entry:
                interface = registration_entry["dbus_interface"]
            
            if "path" in registration_entry:
                path = registration_entry["path"]
            
            if callback:
                logger.debug("Adding signal receiver for signal:{} via interface:{}, path:{}".format(
                        signal_name,interface,path))
                logger.log(5,"Adding signal receiver for callback:{} data:{}".format(
                        callback,registration_entry))
                bus.add_signal_receiver(          		
                    callback,            		
                    **registration_entry
                )
            else:
                logger.log(5,"No callback provided! Cannot register DBUS event using config entry:{}".format(
                                            receiver_entry))
        except Exception as exp:
            if not receiver_entry:
                msg = "Failed to register signal receiver... Error: {}".format(exp) 
            else:
                msg = "Failed to register signal receiver for signal entry:{} Error: {}".format(
                                        receiver_entry,exp.message) 
            logger.error(msg)
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        return


class SessionEventHandlerDbus(SystemEventHandlerDbus):
    """
    Class which provides ACME-style event triggers based on DBus SystemBus
    events.
    """
    
    logger_name = "SessionEventHandlerDbus"
    
    def _setup_dbus_config_ubuntu14(self):
        """
        Method to register our DBUS entries
        """
        
        self.dbus_session_receiver_entries = [
                    {   
                        "signal_name" : "LockRequested",
                        "callback": self.session_locked_callback,
                        "dbus_interface" : "com.canonical.Unity.Session",
                        "path" : "/com/canonical/Unity/Session",
                    },
                    {   
                        "signal_name" : "UnlockRequested",
                        "callback": self.session_unlocked_callback,
                        "dbus_interface" : "com.canonical.Unity.Session",
                        "path" : "/com/canonical/Unity/Session",
                    },
                    {
                        "signal_name" : "Resuming",
                        "callback": self.resume_callback,
                        "dbus_interface" : "org.freedesktop.UPower",
                        #"path" : "/org/freedesktop/UPower"
                    },
                    {
                        "signal_name" : "Sleeping",
                        "callback": self.suspend_callback,
                        "dbus_interface" : "org.freedesktop.UPower",
                        #"path" : "/org/freedesktop/UPower"
                    },
                    {
                        "signal_name" : "PrepareForShutdown",
                        "callback": self.shutdown_callback,
                        "dbus_interface" : "org.freedesktop.login1.Manager",
                        "path" : "/org/freedesktop/login1"
                    },
                    {
                        "signal_name" : "LogoutRequested",
                        "callback": self.logout_callback,
                        "dbus_interface" : "com.canonical.Unity.Session",
                        "path" : "/com/canonical/Unity/Session"
                    }
                ]

    def _setup_dbus_config_ubuntu18(self):
        """
        Method to register our DBUS entries
        """
        
        self.dbus_session_receiver_entries = [
                    {
                        "signal_name" : "Resuming",
                        "callback": self.resume_callback,
                        "dbus_interface" : "org.freedesktop.UPower",
                        #"path" : "/org/freedesktop/UPower"
                    },
                    {
                        "signal_name" : "Sleeping",
                        "callback": self.suspend_callback,
                        "dbus_interface" : "org.freedesktop.UPower",
                        #"path" : "/org/freedesktop/UPower"
                    },
                    {
                        "signal_name" : "PrepareForShutdown",
                        "callback": self.shutdown_callback,
                        "dbus_interface" : "org.freedesktop.login1.Manager",
                        "path" : "/org/freedesktop/login1"
                    },
                    {
                        "signal_name" : "LogoutRequested",
                        "callback": self.logout_callback,
                        "dbus_interface" : "com.canonical.Unity.Session",
                        "path" : "/com/canonical/Unity/Session"
                    }
                ]
            
        self.dbus_system_receiver_entries = [
                    {
                        "signal_name" : "Lock",
                        "callback": self.session_locked_callback,
                        "dbus_interface" : "org.freedesktop.login1.Session",
                        #"path" : "/org/freedesktop/login1/session/_32",
                    },
                    {
                        "signal_name" : "Unlock",
                        "callback": self.session_unlocked_callback,
                        "dbus_interface" : "org.freedesktop.login1.Session",
                        #"path" : "/org/freedesktop/login1/session/_32",
                    }
                ]

    def logout_callback(self, *args, **kwargs):
        """
        Triggered by logout callback
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.log(5, "logout_callback hit!!!")
        
        username = os.environ["USER"]
        
        try:
            self.user_will_logout(username=username)
        except Exception as exp:
            logger.error("Failed to execute callback: logout_callback(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
    def session_locked_callback(self, *args, **kwargs):
        """
        Callback method triggered by screen lock under Ubuntu 14 and newer.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.log(5, "session_locked_callback() hit!!!")
        
        username = os.environ["USER"]
        
        try:
            self.user_session_locked(username=username)
        except Exception as exp:
            logger.error("Failed to execute callback: session_locked_callback(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
    def session_unlocked_callback(self, *args, **kwargs):
        """
        Callback method triggered by screen unlock under Ubuntu 14 and newer.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.log(5, "session_unlocked_callback() hit!!!")
        
        username = os.environ["USER"]
        
        try:
            self.user_session_unlocked(username=username)
        except Exception as exp:
            logger.error("Failed to execute callback: session_unlocked_callback(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
    def shutdown_callback(self,state=None,*args,**kwargs):
        """
        Triggered by system shutdown.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        logger.log(5, "shutdown_callback hit!!!")
        
        try:
            self.system_will_shutdown()
        except Exception as exp:
            logger.error("Failed to execute callback: shutdown_callback(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

    def register_dbus_receivers(self):
        """
        Method to register our DBus receivers
        """
                
        DBusGMainLoop(set_as_default=True) 
        
        sessionbus = dbus.SessionBus()
        systembus = dbus.SystemBus() 
        
        for receiver_entry in self.dbus_system_receiver_entries:
            self._register_dbus_receiver(receiver_entry=receiver_entry, bus=systembus)
         
        for receiver_entry in self.dbus_session_receiver_entries:
            self._register_dbus_receiver(receiver_entry=receiver_entry, bus=sessionbus)

        return
