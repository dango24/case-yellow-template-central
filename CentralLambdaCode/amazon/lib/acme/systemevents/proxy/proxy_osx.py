"""
**proxy_osx** - module which provides event routing capabilities for macOS

:platform: macOS
:synopsis: This module provides facilities on macOS to route events via IPC to 
    related processes (acmed, acmed client, UI apps).

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

from . import EventProxy, ProxiedEvent

import acme.systemevents as systemevents

PROXY_TARGETTYPE_DAEMON = "Daemon"
PROXY_TARGETTYPE_CLIENT = "Client"

#MARK: -
class DaemonEventProxyOSX(EventProxy):
    """
    Class used to proxy events to the ACME daemon.
    """
    
    logger_name = "DaemonEventProxyOSX"
    target_type = PROXY_TARGETTYPE_DAEMON
    
    def register_proxy_forwarders(self,event_handler):
        """
        Method to register our proxy forwarders. This method is invoked by
        the sender to ensure local events are forwarded.
        """
        
        event_handler.register_handler("UserDidLogin",
                                                    self.proxy_login_event)
        event_handler.register_handler("UserWillLogout",
                                                    self.proxy_logout_event)
        event_handler.register_handler("UserSessionLocked",
                                                    self.proxy_session_lock)
        event_handler.register_handler("UserSessionUnlocked",
                                                    self.proxy_session_unlock)
        event_handler.register_handler("SystemWillSuspend",
                                                    self.proxy_system_suspend)
        event_handler.register_handler("SystemResumed",
                                                    self.proxy_system_resumed)
                                                    
    def handle_proxied_event(self,event):
        """
        Method which handles proxied events. This method will be invoked by
        the receiver to handle forwarded events.
        
        :param event: The event to proxy
        :type event: :py:class:`EventProxy`
        
        """
        
        logger = logging.getLogger(self.logger_name)
        
        username = None
        data = event.data
        
        if event.key == "UserDidLogin":
            if "username" in data:
                username = data["username"]
            
            systemevents.system_handler.user_did_login(username=username)
        
        elif event.key == "UserWillLogout":
            if "username" in data:
                username = data["username"]
            
            systemevents.system_handler.user_will_logout(username=username)
        
        elif event.key == "UserSessionLocked":
            if "username" in data:
                username = data["username"]
            
            systemevents.system_handler.user_session_locked(username=username)
        
        elif event.key == "UserSessionUnlocked":
            if "username" in data:
                username = data["username"]
            
            systemevents.system_handler.user_session_unlocked(username=username)
        
        elif event.key == "SystemWillSuspend":
            systemevents.system_handler.system_will_suspend()
        
        elif event.key == "SystemResumed":
            systemevents.system_handler.system_resumed()
        
        else:
            logger.warning("Recieved unhandled ProxiedEvent ({}), will not process!".format(event.key))
    
    def proxy_login_event(self,username=None,*args,**kwargs):
        """
        Delegate method to forward login events
        """
        
        event = ProxiedEvent(key="UserDidLogin")
        event.data["username"] = username
        
        self.proxy_event(event)
    
    def proxy_logout_event(self,username=None,*args,**kwargs):
        """
        Delegate method to forward logout events
        """
        
        event = ProxiedEvent(key="UserWillLogout")
        event.data["username"] = username
        
        self.proxy_event(event)    
    
    def proxy_session_lock(self,username=None,*args,**kwargs):
        """
        Delegate method to forward session lock events
        """
        
        event = ProxiedEvent(key="UserSessionLocked")
        event.data["username"] = username
        
        self.proxy_event(event) 
    
    def proxy_session_unlock(self,username=None,*args,**kwargs):
        """
        Delegate method to forward session unlock events
        """
        
        event = ProxiedEvent(key="UserSessionUnlocked")
        event.data["username"] = username
        
        self.proxy_event(event)         

    def proxy_system_suspend(self,username=None,*args,**kwargs):
        """
        Delegate method to forward system suspend events
        """
        
        event = ProxiedEvent(key="SystemWillSuspend")        
        self.proxy_event(event) 

    def proxy_system_resumed(self,username=None,*args,**kwargs):
        """
        Delegate method to forward system wake events
        """
        
        event = ProxiedEvent(key="SystemResumed")        
        self.proxy_event(event) 

#MARK: -
class ClientEventProxyOSX(EventProxy):
    """
    Class used to proxy events to the ACME clients.
    """
    
    logger_name = "ClientEventProxyOSX"
    target_type = PROXY_TARGETTYPE_CLIENT
    
    def register_proxy_forwarders(self,event_handler):
        """
        Method to register our proxy handlers.
        """
        
        event_handler.register_handler("NetworkSessionDidChange",
                                            self.proxy_network_session_change)
        event_handler.register_handler("NetworkSiteDidChange",
                                            self.proxy_network_site_change)
        
    def handle_proxied_event(self,event):
        """
        Method which handles proxied events.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        username = None
        data = event.data
        
        if event.key == "NetworkSessionDidChange":
            new_session = None
                        
            if "new_session" in data:
                new_session = network.NetworkSession(dict_data=data["new_session"])
            
            
            network.state.update(session=new_session,
                                        site_info=network.state.site_info)
            
        elif event.key == "NetworkSiteDidChange":
            new_site = None
            
            if "new_site" in data:
                new_site = network.NetworkSiteInfo(dict_data=data["new_site"])
                
            network.state.update(site_info=new_site,
                                session=network.state.active_network_session)
    
    def proxy_network_session_change(self,new_session=None,old_session=None,
                                                            *args,**kwargs):
        """
        Delegate method to forward network session change events
        
        :param new_session: The new network session data
        :type new_session: :py:class:`acme.network.NetworkSession` instance
        :param old_session: The previous session data
        :type old_session: :py:class:`acme.network.NetworkSession` instance
        
        """
        
        event = ProxiedEvent(key="NetworkSessionDidChange")
        event.data["new_session"] = new_session.to_dict()
        event.data["old_session"] = old_session.to_dict()
        
        self.proxy_event(event)

    def proxy_network_site_change(self,new_site=None,old_site=None,
                                                            *args,**kwargs):
        """
        Delegate method to forward network site change events
        
        :param new_site: The new site data
        :type new_site: :py:class:`acme.network.NetworkSiteInfo` instance
        :param old_site: The previous site data
        :type old_site: :py:class:`acme.network.NetworkSiteInfo` instance
        
        """
        
        event = ProxiedEvent(key="NetworkSiteDidChange")
        
        event.data["new_site"] = new_site.to_dict()
        event.data["old_site"] = old_site.to_dict()
        
        self.proxy_event(event)
    
             
