"""
**systemevents_osx** - Module which shims macOS SystemConfiguration and 
        NSDistributedNotificationCenter Events for ACME.

:platform: macOS
:synopsis: This module serves as a shim layer between ACME's systemevent
    system and macOS's event notification facilites.
    
.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""

import datetime
import logging
import threading
import subprocess

from . import EventHandlerBase

from Foundation import NSDistributedNotificationCenter, NSRunLoop
from SystemConfiguration import (SCDynamicStoreCreate,
                    SCDynamicStoreSetNotificationKeys,
                    SCDynamicStoreCopyKeyList,
                    SCDynamicStoreCreateRunLoopSource)

from Cocoa import (CFRunLoopAddSource,kCFRunLoopCommonModes)

import systemprofile

class EventHandlerOSXBase(EventHandlerBase):
    """
    Class which provides a basic interface for interacting with macOS event 
    systems.
    """
    
    should_run = None           #: Flag to indicate whether our run loop 
                                #: should continue to fire. You should
                                #: invoke the start() nad stop() methods 
                                #: rather than modify this value.
    
    sc_keymap = None            #: Mapping dictionary for mapping SystemConfiguration
                                #: events to ACME keys (i.e. "network","system_load"
    sc_store = None             #: Our cached NSDynamicStore object
    
    notification_map = None     #: Dictionary mapping notifiction events to callback names
    
    def __init__(self,notification_map=None,*args,**kwargs):
        """
        :param notification_map: A dictionary which maps Notification events 
                    to callback function names.
        :type notification_map: dictionary <string,string>
        """
        
        super(EventHandlerOSXBase,self).__init__()
        
        if notification_map:
            self.notification_map = notification_map
            
        if not self.logger_name:
            self.logger_name = "BaseEventHandlerOSX"
    
    #MARK: Registration methods
    def register_subsystems(self):
        """
        Method used to perform configuration.
        """
        self.register_nsnotifcation_observers()
        self.register_scdynamicstore_notification_keys()
    
    def unregister_subsystems(self):
        """
        Method used to unregister handlers.
        """
        self.unregister_nsnotifcation_observers()
        
    #MARK: NSDistributedNotificationCenter observers
    def register_nsnotifcation_observers(self):
        """
        Method to register our NSDistributedNotificationHandlers
        """
        logger = logging.getLogger(self.logger_name)
        
        nc = NSDistributedNotificationCenter.defaultCenter()
        
        for key,callback in self.notification_map.iteritems():
            nc.addObserver_selector_name_object_(self,callback,key,None)
            logger.log(9,"Adding NSDistributedNotificationCenter observer for key:'{}' callback:'{}'".format(
                                                                key,callback))
        
    def unregister_nsnotifcation_observers(self):
        """
        Method to register our NSDistributedNotificationHandlers
        """
        
        logger = logging.getLogger(self.logger_name)
        
        nc = NSDistributedNotificationCenter.defaultCenter()
        
        for key,callback in self.notification_map.iteritems():
            nc.removeObserver_name_object_(self,key,None)
            logger.log(5,"Removing observer for trigger:{}".format(key))
            
    #MARK: SCDynamicStore notifications
    def register_scdynamicstore_notification_keys(self,sc_keymap=None,sc_store=None):
        """
        Method to register observers using the `SystemConfiguration 
        <https://developer.apple.com/library/mac/documentation/Networking/Reference/SysConfig>`_ framework, specifically the `SCDynamicStore
        <https://developer.apple.com/library/mac/documentation/Networking/Reference/SCDynamicStore>`_ component. 
        """
        
        logger = logging.getLogger(self.logger_name)
        
        if sc_keymap is None and self.sc_keymap is None:
            sc_keymap = self.build_scdynamicstore_notification_keymap(sc_store=sc_store)
        elif sc_keymap is None:
            sc_keymap = self.sc_keymap
        
        if not sc_store and not self.sc_store:
            sc_store = SCDynamicStoreCreate(None,"acmed",self.sc_callback,None)
        elif not sc_store:
            sc_store = self.sc_store
        
        ## Flatten the top level of our map to get just our keys
        keys = sorted({key for value in sc_keymap.itervalues() for key in value})
        
        SCDynamicStoreSetNotificationKeys(sc_store, None, keys)
        
        # Get a CFRunLoopSource for our store session and add it to the application's runloop:
        CFRunLoopAddSource(
            NSRunLoop.currentRunLoop().getCFRunLoop(),
            SCDynamicStoreCreateRunLoopSource(None, sc_store, 0),
            kCFRunLoopCommonModes
        )
        
        logger.log(9, "Registered SystemConfiguration NotificationKeys: \n\t\"{}\",\n".format(
                                                        "\",\n\t\"".join(keys)))
    
    def build_scdynamicstore_notification_keymap(self,sc_store=None):
        """
        Method which will interogate our SCDynamicStore and determine
        appropriate keys to observe.
        
        :returns 
        
        """
        
        key_map = {"network" : [], "system_load" : [], "user" : [],
                    "domain" : []}
        
        if not sc_store and not self.sc_store:
            sc_store = SCDynamicStoreCreate(None,"acmed",self.sc_callback,None)
        elif not sc_store:
            sc_store = self.sc_store
            
        available_keys = SCDynamicStoreCopyKeyList(sc_store,".*")
        
        for key in available_keys:
            if (key.startswith("State:/Network/Interface")
                                    and not key.endswith("LinkQuality") 
                                    and not key.endswith("SleepProxyServers")):
                key_map["network"].append(key)
            elif key.startswith("State:/IOKit/PowerManagement/SystemLoad"):
                key_map["system_load"].append(key)
            elif key == "State:/Users/ConsoleUser":
                key_map["user"].append(key)
            elif key.startswith("Kerberos:"):
                key_map["domain"].append(key)
            
        
        return key_map        
    
    ##MARK: Daemon Control
    def start_listener(self):
        """
        Method which starts our main event loop on a background thread.
        """
        
        super(EventHandlerOSXBase,self).start_listener()
                
        """ Note: scrapped this because NSRunLoop doesn't seem to work on the
        non-main thread.
        
        if not self.nsrunloop_thread or self.nsrunloop_thread.is_alive(): 
            logger.info("Starting OSX Event Listener...")
            self.should_run = True
            self.nsrunloop_thread = threading.Thread(target=self.nsrunloop,
                                                    name="NSRunLoopThread")
            self.nsrunloop_thread.daemon = True
            self.nsrunloop_thread.start()
        """
    
    def stop_listener(self):
        """
        Method which stops our main event loop
        """
        
        logger = logging.getLogger(self.logger_name)
                
        if self.should_run:
            logger.info("Stopping macOS Event Listener...")
            self.should_run = False
    
    def nsrunloop(self):
        """
        Method which will keep our NSRunLoop operating
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.log(5,"Starting NSRunLoop")
        
        run_loop = Foundation.NSRunLoop.currentRunLoop()
        while self.should_run:
            now = datetime.datetime.now() + datetime.timedelta(seconds=1)
            run_loop.runUntilDate_(now)
            
        logger.log(5,"Stopping NSRunLoop")
    
    def is_listening(self):
        """
        Method which returns whether or not we are currently listening for 
        and dispatching events.
        """
        
        if self.should_run:
            return True
        else:
            return False
    
    #MARK: Callbacks
    def sc_callback(self, store, changed_keys, info,*args,**kwargs):
        """
        Callback method to handle SystemConfiguration event notifications.
        """
        logger = logging.getLogger(self.logger_name)
        logger.log(5,"Recieved SystemConfiguration events for keys: {}".format(changed_keys))
        logger.log(5,"Recieved SystemConfiguration info: {}".format(info))
        
        
        ## If we have a registered key map, determine changed keys.
        if not self.sc_keymap:
            return
        
        changes = []
        for changed_key in changed_keys:
            for event_key,event_types in self.sc_keymap.iteritems():
                if event_key in changes:
                    continue
                elif changed_key in event_types:
                    changes.append(event_key)
        
        if "network" in changes:
            if not self.network_change_start:
                logger.debug("NetworkChange detected, waiting for more changes...")
            try:
                self.reset_delayed_network_timer()
            except Exception as exp:
                logger.error("Failed to process network change event. Error:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly)",exc_info=1)

            
        if "system_load" in changes:
            logger.debug("SystemLoad change detected...")
            
        if "user" in changes:
            logger.debug("ConsoleUser change detected...")
            try:
                self.osx_consoleuser_callback_()
            except Exception as exp:
                logger.error("Failed to process ConsoleUser change event. Error:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly)",exc_info=1)
        
        if "kerberos" in changes:
            logger.debug("Kerberos change detected...")
            
    def osx_consoleuser_callback_(self,event=None,*args,**kwargs):
        """
        Triggered by SCNotification change to /User/Console
        """
        
        logger = logging.getLogger(self.logger_name)
        if event:
            logger.log(5,"osx_consoleuser_callback_() - event:{}".format(event.object))
        else:
            logger.log(5,"osx_consoleuser_callback_()")   
            
        console_user = None         #: Tracks console user to detect login/logout changes
        
        
#MARK: -
class SystemEventHandlerOSX(EventHandlerOSXBase):
    """
    Class which provides ACME-style event triggers based on DBus
    events.
    """
    
    console_user = None         #: Tracks console user to detect login/logout changes
    
    def __init__(self,notification_map=None,*args,**kwargs):
        """
        Our constructor.
        """
        super(SystemEventHandlerOSX,self).__init__()
        
        self.notification_map = { 
                        "com.amazon.acme.userLogin" : "osx_login_callback:",
                        "com.amazon.acme.userLogout" : "osx_logout_callback:",
                                }
        
        self.sc_store = SCDynamicStoreCreate(None,"acmed",self.sc_callback,None)
        self.sc_keymap = self.build_scdynamicstore_notification_keymap(
                                                    sc_store=self.sc_store)
        
        self.logger_name = "SystemEventHandlerOSX"
        
        try:
            self.console_user = systemprofile.profiler.current_user()
        except Exception:
            pass
        
        
    #MARK: Callback handlers
    def osx_login_callback_(self,event=None,*args,**kwargs):
        """
        Triggered by login events
        """
        
        logger = logging.getLogger(self.logger_name)
        if event:
            logger.log(5,"osx_login_callback() - event:{}".format(event.object))
        else:
            logger.log(5,"osx_login_callback()")   
        
        try:
            user = systemprofile.profiler.current_user()
            self.user_did_login(username=user)
        except Exception as exp:
            logger.error("Failed to execute callback: user_did_login(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
    
    def osx_logout_callback_(self,event=None,*args,**kwargs):
        """
        Triggered by login events
        """
        
        logger = logging.getLogger(self.logger_name)
        if event:
            logger.log(5,"osx_logout_callback() - event:{}".format(event.object))
        else:
            logger.log(5,"osx_logout_callback()")            
        
        try:
            self.user_will_logout()
        except Exception as exp:
            logger.error("Failed to execute callback: user_will_logout(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
    
    
    
#MARK: -
class SessionEventHandlerOSX(EventHandlerOSXBase):
    """
    Class which provides ACME-style event triggers based on DBus
    events.
    """
    
    def __init__(self):
        """
        Our constructor.
        """
        
        self.logger_name = "SessionEventHandlerOSX"
        
        super(SessionEventHandlerOSX,self).__init__()
        
        self.notification_map = { "com.apple.screenIsUnlocked" : "osx_screen_unlock_callback:",
                        "com.apple.screenIsLocked" : "osx_screen_lock_callback:"
                        }
        self.workstation_notification_map = {
                        "NSWorkspaceDidWakeNotification":"ws_didwake_callback:",
                        "NSWorkspaceWillPowerOffNotification":"ws_willpoweroff_callback:",
                        "NSWorkspaceWillSleepNotification":"ws_willsleep_callback:",
                        }
        self.sc_store = SCDynamicStoreCreate(None,"acme-agent",self.sc_callback,None)
        self.sc_keymap = self.build_scdynamicstore_notification_keymap(
                                                    sc_store=self.sc_store)
        
    #MARK: Registration methods
    def register_subsystems(self):
        """
        Method used to perform configuration.
        """
        super(SessionEventHandlerOSX,self).register_subsystems()
        self.register_nsworkspace_observers()
        
    def unregister_subsystems(self):
        """
        Method used to unregister handlers.
        """
        super(SessionEventHandlerOSX,self).unregister_subsystems()
        self.unregister_nsworkspace_observers()
        
    def ws_callback_(self,note,*args,**kwargs):
        logger = logging.getLogger(self.logger_name)
        
        logger.debug("ws_callback() {}".format(note.object))
    
    def ws_willsleep_callback_(self,notification,*args,**kwargs):
        """
        NSWorkspaceDidWakeNotification callback shim. 
        """
        
        self.system_will_suspend()
        
    def ws_didwake_callback_(self,notification,*args,**kwargs):
        """
        NSWorkspaceDidWakeNotification callback shim. 
        """
        
        self.system_resumed()
    
    def ws_willpoweroff_callback_(self,notification,*args,**kwargs):
        """
        NSWorkspaceWillPowerOffNotification callback shim. 
        """
        
        self.system_will_shutdown()
                   
    
    #MARK: Registration methods
    def register_nsworkspace_observers(self):
        """
        Method to register our NSWorkspace observers
        """
        
        logger = logging.getLogger(self.logger_name)
        
        from AppKit import NSWorkspace
        ws = NSWorkspace.sharedWorkspace()
        nc = ws.notificationCenter()
        
        for key,callback in self.workstation_notification_map.iteritems():
            logger.log(5,"Registering NSWorkspaceNotification:{} using callback:{}".format(
                                                            key,callback))
            nc.addObserver_selector_name_object_(self,callback,key,None)
    
    def unregister_nsworkspace_observers(self):
        """
        Method to unregister our NSWorkspace
        """
        
        logger = logging.getLogger(self.logger_name)
        
        from AppKit import NSWorkspace
        ws = NSWorkspace.sharedWorkspace()
        nc = ws.notificationCenter()
        
        for key,callback in self.workstation_notification_map.iteritems():
            logger.log(5,"Unregistering NSWorkspaceNotification:{} with callback:{}".format(
                                                            key,callback))
            nc.removeObserver_name_object_(self,key,None)
    
    #MARK: Callback handlers
    def osx_screen_lock_callback_(self,event,*args,**kwargs):
        """
        Triggered by screen unlock.
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.log(5,"osx_screen_lock_callback()")
        
        try:
            user = systemprofile.profiler.current_user()
            self.user_session_locked(username=user)
        except Exception as exp:
            logger.error("Failed to execute callback: user_session_locked(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
    def osx_screen_unlock_callback_(self,event,*args,**kwargs):
        """
        Triggered by screen unlock.
        """
        logger = logging.getLogger(self.logger_name)
        logger.log(5,"osx_screen_unlock_callback()")
        
        try:
            user = systemprofile.profiler.current_user()
            self.user_session_unlocked(username=user)
        except Exception as exp:
            logger.error("Failed to execute callback: user_session_unlocked(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
    
    def osx_resume_callback_(self,event,*args,**kwargs):
        """
        Triggered by system resume
        """

        logger = logging.getLogger(self.logger_name)
        logger.log(5,"osx_resume_callback() - event:{}".format(event.object))

        try:
            self.system_resumed()
        except Exception as exp:
            logger.error("Failed to execute callback: system_resumed(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
    
    def osx_suspend_callback_(self,event,*args,**kwargs):
        """
        Triggered by system suspend
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.log(5,"osx_suspend_callback() - event:{}".format(event.object))
        
        try:
            self.system_will_suspend()
        except Exception as exp:
            logger.error("Failed to execute callback: system_will_suspend(). Error:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

    