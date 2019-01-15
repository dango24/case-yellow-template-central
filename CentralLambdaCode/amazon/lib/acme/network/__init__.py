
"""
**network** - Package which is responsible for handling network state 
    tracking and network change events.

:platform: RHEL5
:synopsis: Package which provides various facilities for querying network status.

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>

"""


#MARK: Imports
import collections
import datetime
import logging
import os
import uuid
import re
import acme
import systemprofile

import acme.systemevents as systemevents


#MARK: Constants
NETWORK_STATE_UNKNOWN = 0
NETWORK_STATE_ONLINE = 1 << 1
NETWORK_STATE_OFFLINE = 1 << 2
NETWORK_STATE_ONDOMAIN = 1 << 3
NETWORK_STATE_OFFDOMAIN = 1 << 4
NETWORK_STATE_ONVPN = 1 << 5
NETWORK_STATE_OFFVPN = 1 << 6

#MARK: -
#MARK: Classes
class NetworkSiteInfo(acme.SerializedObject):
    
    site = None                     #: Denotes our currently active site. 
    last_temporary_site = None      #: Denotes our last known temp site. (this may be the same value as site)
    last_fixed_site = None          #: Denotes our last known "fixed" site (this may be the same value as site)
    
    temporary_site_list = None      #: A list of site names which qualify as "Temporary" sites
    temporary_site_filter = None    #: A regex filter which is used to identify "Temporary" sites
    
    logger_name = "NetworkSiteInfo"
    
    def __init__(self,*args,**kwargs):
        self.site = None
        self.last_temporary_site = None
        self.last_fixed_site = None
        
        self.temporary_site_list = []
        self.temporary_site_filter = None
        
        key_map = {"site" : None,
                    "last_temporary_site" : None,
                    "last_fixed_site" : None,
                    "temporary_site_list" : None,
                    "temporary_site_filter" : None,
                    }
        acme.SerializedObject.__init__(self,key_map=key_map,*args,**kwargs)
        
    def update_site(self,site=None):
        """
        Method which will update our current site data.
        """
        logger = logging.getLogger(self.logger_name)
        self.site = site
        
        if site:
            if self.site_is_temporary(site):
                self.last_temporary_site = site
            else:
                self.last_fixed_site = site
    
    def load(self):
        """
        Method which will load our current network site.
        """
        try:
            site = systemprofile.profiler.directoryservice.network_site()
        except systemprofile.PermissionsDeniedError:
            logger.debug("Failed to look up AD Site: permission denied!")
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                
        self.update_site(site)
        
    
    def site_is_temporary(self,site):
        """
        Method which returns whether or not the provided site qualifies a "temporary":
        A site will qualify as temporary if it's explicitely referenced in 
        the TemporarySiteList ivar or matches RE pattern specified by 
        TemporarySiteFilter
        
        :param str site: The site to check for
        
        :returns: (bool) True if the site is temporary
        """
        
        result = False
        
        if not site:
            result = True
        
        if not result and self.temporary_site_list:
            f = filter(lambda x: x.lower() == site.lower(), self.temporary_site_list)
            if f:
                result = True
        
        if not result and self.temporary_site_filter:
            if re.match(self.temporary_site_filter,site,re.I):
                result = True
                
        return result
       
    def qualifies_for_site(self,site):
        """
        Method which returns whether or not the provided site name qualifies
        by our SiteInfo data. That is, if the provided site is currently our 
        active site, or was our last fixed site, we will return True. 
        "Temporary" sites only qualify if they are currently active.
        
        :param str site: The site to check for
        
        :returns: (bool) True if our current site or last fixed site matches 
            the provided site
        
        """
        
        result = False
        
        if site and self.site:
            if site.lower() == self.site.lower():
                result = True
        
        if not result and site and self.last_fixed_site:
            if site.lower() == self.last_fixed_site.lower():
                result = True
        
        return result
    
    def qualifies_for_site_filter(self,filter):
        """
        Method which returns whether or not our current site matches
        the provided filter.
        
        :param str filter: Our regex filter.
        """
        
        result = False
        
        p = re.compile(filter,re.I)
        
        if filter and self.site:
            if p.search(self.site):
                result = True
        
        if not result and filter and self.last_fixed_site:
            if p.search(self.last_fixed_site):
                result = True
        
        return result
    
    def active_site():
        """
        Method which returns the site currently reported by the machine.
        """
        
        logger = logging.getLogger(self.logger_name)
        try:
            systemprofile.profiler.directoryservice.network_site()
        except Exception as exp:
            logger.error("Failed to retrieve our active site. Error:{}".format(exp))

#MARK: -
class NetworkState(acme.SerializedObject):
    """
    Class used to track current network state which includes IP data, 
    Network session info and Network site data.
    
    """

    active_network_session = None   #: NetworkSession object representing our current known network state
    last_network_session = None     #: NetworkSession object representing our last known network state
    
    site_info = None                #: NetworkSiteInfo object representing our known network site state
    
    logger_name = "NetworkState"
       
    def __init__(self,*args,**kwargs):
        """
        Constructor
        """
        
        key_map = {"Site" : "site",
                    "IPAddress" : "ip_address",
                    "NetworkState" : "state_dict",}
        acme.SerializedObject.__init__(self,key_map=key_map,*args,**kwargs)
    
    
    @property
    def site(self):
        """
        Property to serialize site data. Persist the last fixed site
        on record (which may or may not be the current site)
        """
        if self.site_info:
            return site_info.last_fixed_site
        else:
            return None
    
    @site.setter
    def site(self,value):
        """
        Setter accessor to set our network site name.
        """
        if not self.site_info:
            self.site_info = NetworkSiteInfo()
        
        self.site_info.update_site(site=value)        
    
    @property
    def ip_address(self):
        """
        Property to serialize ip_address data. Persist the current IP, if set.
        """
        if self.active_network_session:
            return self.active_network_session.ip_address
        else:
            return None
            
    @ip_address.setter
    def ip_address(self,value):
        """
        Setter accessor to set IP address. This modifies last_network_session,
        as IP data for the active session is read live.
        """
        if self.active_network_session:
            self.active_network_session.ip_address = value
        else:
            self.active_network_session = NetworkSession(ip_address=value)
    
    @property
    def state_dict(self):
        """
        Property to serialize our state data. 
        """
        
        d = {}
        
        network_session = self.active_network_session
        if network_session:
            d["Session"] = network_session.to_dict()
            
        site = self.site_info
        if site:
            d["SiteInfo"] = site.to_dict()
        
        return d
        
    @state_dict.setter
    def state_dict(self,value):
        """
        Setter accessor for our state dict
        """
        
        if value and "Session" in value:
            if not self.active_network_session:
                self.active_network_session = NetworkSession()                
            self.active_network_session.load_dict(value["Session"])
        
        if value and "SiteInfo" in value:
            if not self.site_info:
                self.site_info = NetworkSiteInfo()
            self.site_info.load_dict(value["SiteInfo"])
        
    @property
    def state(self):
        """
        Property to serialize our active network state.
        """
        if self.active_network_session:
            return self.active_network_session.state
        else:
            return None
            
    @state.setter
    def state(self,value):
        """
        Setter accessor to set network state. This modifies last_network_session,
        as network state data for the active session is read live.
        """
        if self.active_network_session:
            self.active_network_session.state = state
        else:
            self.active_network_session = NetworkSession(state=value)
    
    '''
    def to_dict(self,key_map=None,*args,**kwargs):
        """
        Method to export our record in key=>value dictionary form
        """
        
        d = {"NetworkState" : {}}
        
        network_session = self.active_network_session
        if network_session:
            d["NetworkState"]["Session"] = network_session.to_dict()
            
        site = self.site_info
        if site:
            d["SiteInfo"] = site.to_dict()
        
        return d
        
    def load_dict(self,data,*args,**kwargs):
        """
        Method to load data from a dictionary.
        
        :param dict data: Dictionary of key->values to load
        """
        
        logger = logging.getLogger(self.logger_name)
        try:
            d = data["NetworkState"]["Session"]
            self.active_network_session.load_dict(d)
        except KeyError:
            pass
        except Exception as exp:
            logger.debug("Failed to load NetworkSession data:{}".format(exp))
            
        try:
            d = data["NetworkState"]["SiteInfo"]
            self.site_info.load_dict(d)
        except KeyError:
            pass
        except Exception as exp:
            logger.debug("Failed to load NetworkState data:{}".format(exp))
    '''
    def active_state(self):
        """
        Method which outputs a bitwise mask denoting our current network
        state.
        """
        
        state = NETWORK_STATE_UNKNOWN
        
        profiler = systemprofile.profiler
        
        if profiler.online():
            state |= NETWORK_STATE_ONLINE
        else:
            state |= NETWORK_STATE_OFFLINE
        
        if profiler.on_domain():
            state |= NETWORK_STATE_ONDOMAIN
        else:
            state |= NETWORK_STATE_OFFDOMAIN
        
        if profiler.on_vpn():
            state |= NETWORK_STATE_ONVPN
        else:
            state |= NETWORK_STATE_OFFVPN
        
        return state
    
    
    def update_from_file(self,filepath):
        """
        Method to update network state based on the provided file
        """
        
        state = NetworkState()
        state.load_from_file(filepath)
        
        self.update(state=state)
    
    def update(self,state=None,session=None,site_info=None,*args,**kwargs):
        """
        Method which will update our current network state, generating a new
        Network session as necessary.
        """
        
        logger = logging.getLogger(self.logger_name)
        
        old_session = self.active_network_session
        
        new_session = None
        new_site_info = None
        
        now = datetime.datetime.utcnow()
        
        if state:
            new_session = state.active_network_session
            new_site_info = state.site_info
        
        if session:
            new_session = session
        elif new_session is None:
            new_session = NetworkSession()
            try:
                logger.log(2,"Looking up network session data...")
                new_session.load()
            except Exception as exp:
                logger.warning("Failed to update NetworkState: NetworkSession failed to load: {}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        if site_info:
            new_site_info = site_info
        elif new_site_info is None:
            new_site_info = NetworkSiteInfo()
            try:
                logger.log(2,"Looking up network site...")
                new_site_info.site = systemprofile.profiler.directoryservice.network_site()
            except Exception as exp:
                logger.warning("Failed to load NetworkSite:")
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
        
        if not old_session:
            default_state = (NETWORK_STATE_OFFLINE
                                            | NETWORK_STATE_OFFDOMAIN
                                            | NETWORK_STATE_OFFVPN)
            old_session = NetworkSession(state=default_state)
            
        self.active_network_session = new_session
        
        ## Check to see if there is a change in site
        old_site_info = None
        
        if not self.site_info:
            self.site_info = new_site_info
        else:
            old_site_info = NetworkSiteInfo(dict_data=self.site_info.to_dict())
        
        current_site = new_site_info.site
        self.site_info.update_site(current_site)
        
        if old_site_info and (old_site_info.last_fixed_site 
                                        != self.site_info.last_fixed_site):
            self.network_site_did_change(new_site=self.site_info,
                                                    old_site=old_site_info)
        elif not old_site_info:
            self.network_site_did_change(new_site=self.site_info)
                        
        old_state = old_session.state
        current_state = new_session.state
        
        self.active_network_session = new_session
        self.last_network_session = old_session
        
        if new_session != old_session:
            old_session.session_end = now
            self.network_session_did_change(new_session=new_session,
                                            old_session=old_session)   
        
        ## Todo: this is a bit of a mess. Needs refactoring.
        try:
            if current_state & NETWORK_STATE_ONDOMAIN:
                if not old_state & NETWORK_STATE_ONDOMAIN:
                    self.did_connect_to_intranet()
                    
                    if old_state & NETWORK_STATE_ONLINE:
                        self.did_leave_public_network()
            
            elif not current_state & NETWORK_STATE_ONDOMAIN:
                if old_state & NETWORK_STATE_ONDOMAIN:
                    self.did_leave_intranet()
                    
                    if current_state & NETWORK_STATE_ONLINE:
                        self.did_connect_to_public_network()
            
            if current_state & NETWORK_STATE_ONVPN:
                if not old_state & NETWORK_STATE_ONVPN:
                    self.did_connect_to_vpn()
            elif current_state & NETWORK_STATE_OFFVPN:
                if old_state & NETWORK_STATE_ONVPN:
                    self.did_leave_vpn()
            
            if current_state & NETWORK_STATE_OFFLINE:
                if not old_state & NETWORK_STATE_OFFLINE:
                    self.did_leave_internet()
                    
            elif current_state & NETWORK_STATE_ONLINE:
                if not old_state & NETWORK_STATE_ONLINE:
                    self.did_connect_to_internet()
            
        except Exception as exp:
            logger.error("Failed to execute network change callback: {}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly)",exc_info=1)
    
    def network_session_did_change(self,new_session,old_session=None):
        """
        Method which is invoked when we detect changes to our network
        session.
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.debug("network_session_did_change()")
        
        if systemevents.system_handler:
            systemevents.system_handler.network_session_did_change(
                                                    new_session=new_session,
                                                    old_session=old_session)
        if systemevents.session_handler:
            systemevents.session_handler.network_session_did_change(
                                                    new_session=new_session,
                                                    old_session=old_session)
        
    def network_site_did_change(self,new_site,old_site=None):
        """
        Method which is invoked when a site change is detected.
        Note: this method should be overriden and implemented to support
        the local systems event notification systems.
        
        This method should not be called directly, but will instead be 
        called based on evaluation performed by one of the update() calls.
        
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.debug("network_site_did_change()")
        
        if systemevents.system_handler:
            systemevents.system_handler.network_site_did_change(
                                                        new_site=new_site,
                                                        old_site=old_site)       
        
        if systemevents.session_handler:
            systemevents.session_handler.network_site_did_change(
                                                        new_site=new_site,
                                                        old_site=old_site) 
        
    def did_connect_to_internet(self):
        """
        Method which is invoked when the system is detected to have gone
        online.
        """
        
        logger = logging.getLogger(self.logger_name)
        logger.debug("did_connect_to_internet()")
        
        if systemevents.system_handler:
            systemevents.system_handler.did_connect_to_internet()

        if systemevents.session_handler:
            systemevents.session_handler.did_connect_to_internet()
                    
    def did_leave_internet(self):
        """
        Method which is invoked when the system is detected to have gone
        offline.
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("did_leave_internet()")
    
        if systemevents.system_handler:
            systemevents.system_handler.did_leave_internet()
        
        if systemevents.session_handler:
            systemevents.session_handler.did_leave_internet()
    
    def did_connect_to_intranet(self):
        """
        Method which is invoked when the system is detected to have left
        the corporate intranet.
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("did_connect_to_intranet()")
        
        if systemevents.system_handler:
            systemevents.system_handler.did_connect_to_intranet()

        if systemevents.session_handler:
            systemevents.session_handler.did_connect_to_intranet()
        
    def did_leave_intranet(self):
        """
        Method which is invoked when the system is detected to have left
        the corporate intranet.
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("did_leave_intranet()")
        
        if systemevents.system_handler:
            systemevents.system_handler.did_leave_intranet()

        if systemevents.session_handler:
            systemevents.session_handler.did_leave_intranet()
        
    def did_connect_to_public_network(self):
        """
        Method which is invoked when the system is detected to have connected
        to a public network connection.
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("did_connect_to_public_network()")
        
        if systemevents.system_handler:
            systemevents.system_handler.did_connect_to_public_network()

        if systemevents.session_handler:
            systemevents.session_handler.did_connect_to_public_network()
        
    def did_leave_public_network(self):
        """
        Method which is invoked when the system is detected to have left
        a public internet connection
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("did_leave_public_network()")
        
        if systemevents.system_handler:
            systemevents.system_handler.did_leave_public_network()

        if systemevents.session_handler:
            systemevents.session_handler.did_leave_public_network()
   
    def did_connect_to_vpn(self):
        """
        Method which is invoked when the system is detected to have connected
        via VPN.
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("did_connect_to_vpn()")
        
        if systemevents.system_handler:
            systemevents.system_handler.did_connect_to_vpn()

        if systemevents.session_handler:
            systemevents.session_handler.did_connect_to_vpn()
        
    def did_leave_vpn(self):
        """
        Method which is invoked when the system is detected to have disconnected
        from VPN
        """
        logger = logging.getLogger(self.logger_name)
        logger.debug("did_leave_vpn()")
        
        if systemevents.system_handler:
            systemevents.system_handler.did_leave_vpn()

        if systemevents.session_handler:
            systemevents.session_handler.did_leave_vpn() 

#MARK: -
class NetworkSession(acme.SerializedObject):
    """
    Class which represents a network session. A network session is classified
    as a period of consistent network connection including IP address, active
    adapters, VPN presence, and domain/intranet presence
    """
    
    session_guid = None     #: Unique identifier for our session
    ip_address = None       #: Current default IP address
    state = None            #: Connection state bitmask
    interface = None        #: Our primary interface
    
    session_start = None    #: Datetime object representing our session start
    session_end = None      #: Datetime object representing our session end
    
    logger_name = "NetworkSession"
    
    @property
    def start_datestamp(self):
        """
        Property which returns a formatted datestamp representation of our 
        session start date
        """
        logger = logging.getLogger(self.logger_name)
        
        dt = None
        
        if self.session_start:
            try: 
                self.session_start.strftime(acme.DATE_FORMAT)
            except Exception as exp:
                logger.warning("Failed to output start_datestamp:{}. Error:{}".format(
                                                    self.session_start,exp))
                logger.log(5,"Failure stack trace (handled cleanly)",exc_info=1)
        else:
            return None
    
    @start_datestamp.setter
    def start_datestamp(self,value):
        """
        Setter for our session start date using a datestamp formatted as
        per DATE_FORMAT or unix timestamp (float)
        """
        
        logger = logging.getLogger(self.logger_name)
        
        the_date = None
        
        if isinstance(value,datetime.datetime):
            the_date = value
        elif value is not None:
            try:
                the_date = datetime.datetime.utcfromtimestamp(float(value))
            except (TypeError,ValueError) as exp:
                try:
                    the_date = datetime.datetime.strptime(value,acme.DATE_FORMAT)
                except (TypeError,ValueError) as exp:
                    logger = logging.getLogger(self.__class__.__name__)
                    logger.warning("Could not import start_datestamp value:{v} for model:{m}".format(v=value,m=self.__class__.__name__)) 

        self.session_start = the_date
        
    @property
    def end_datestamp(self):
        """
        Property which returns a formatted datestamp representation of our 
        session end date
        """
        
        if self.session_end:
            try: 
                self.session_end.strftime(acme.DATE_FORMAT)
            except Exception as exp:
                logger.warning("Failed to output end_datestamp:{}. Error:{}".format(
                                                    self.session_start,exp))
                logger.log(5,"Failure stack trace (handled cleanly)",exc_info=1)
        else:
            return None
        
    @end_datestamp.setter
    def end_datestamp(self,value):
        """
        Setter for our session end date using a datestamp formatted as
        per DATE_FORMAT or unix timestamp (float)
        """
        the_date = None
        
        if isinstance(value,datetime.datetime):
            the_date = value
        elif value is not None:
            try:
                the_date = datetime.datetime.utcfromtimestamp(float(value))
            except (TypeError,ValueError) as exp:
                try:
                    the_date = datetime.datetime.strptime(value,acme.DATE_FORMAT)
                except (TypeError,ValueError) as exp:
                    logger = logging.getLogger(self.__class__.__name__)
                    logger.debug("Could not import end_datestamp value:{v} for model:{m}".format(v=value,m=self.__class__.__name__))
        self.session_end = the_date
    
    def __init__(self,session_uuid=None,ip_address=None,state=None,
                                                key_map=None,
                                                *args,**kwargs):
        """
        Our constructor
        
        :param int state: Bitwise mask representing our session satte.
        """
        
        if session_uuid:
            self.session_guid = session_uuid
        else:
            self.session_guid = str(uuid.uuid4())
        
        if ip_address:
            self.ip_address = ip_address
        else:
            self.ip_address = None
        
        if state:
            self.state = state
        else:
            self.state = 0
        
        self.session_start = datetime.datetime.utcnow()
        
        if not key_map:
            key_map = { "session_uuid" : "session_guid",
                        "ip_address" : None,
                        "interface" : None,
                        "state" : None,
                        "start" : "start_datestamp",
                        "end" : "end_datestamp",
                        }
        super(NetworkSession,self).__init__(key_map=key_map,*args,**kwargs)
    
    def load(self):
        """
        Method to load our current state.
        """
        ns = NetworkState()
        
        self.ip_address = systemprofile.profiler.primary_ip_address()
        self.interface = systemprofile.profiler.primary_interface()
        self.state = ns.active_state()
        
    def __eq__(self,other):
        """
        Equality operator to compare NetworkSession objects. If two objects 
        contain the same IP address and state flags, we consider them equal.
        """
        equal = True
        try:
            if self.ip_address != other.ip_address:
                equal = False
                
            if self.state != other.state:
                equal = False
                
            if self.interface != other.interface:
                equal = False
                
        except Exception:
            equal = False
            
        return equal
        
    def __ne__(self,other):
        """
        Inequality operator to compare NetworkSession objects. If two objects 
        contain the same IP address and state flags, we consider them equal.
        """
                
        return not self.__eq__(other)

#MARK: -
#MARK: Module vars
state = NetworkState()


#MARK: Module functions
state_string_map = collections.OrderedDict() 
state_string_map[NETWORK_STATE_UNKNOWN] = "Unknown"
state_string_map[NETWORK_STATE_ONLINE] = "Online"
state_string_map[NETWORK_STATE_OFFLINE] = "Offline"
state_string_map[NETWORK_STATE_ONDOMAIN] = "OnDomain"
state_string_map[NETWORK_STATE_OFFDOMAIN] = "OffDomain"
state_string_map[NETWORK_STATE_ONVPN] = "OnVPN"
state_string_map[NETWORK_STATE_OFFVPN] = "OffVPN"
                    

def statemask_from_string(value):
    """
    Method which returns a bitmask representing the network state 
    as defined by the provided value.
    
    :param string value: The value to parse into a state (i.e. "Online, OffDomain")
    
    :returns: Integer bitmask
    
    """
    
    state = NETWORK_STATE_UNKNOWN
    
    try:
        state = int(value)
        return state
    except ValueError:
        pass
    
    state_components = re.findall(r"[\w']+", value)
        
    for map_state, map_name in state_string_map.iteritems():
        if map_name in state_components:
            state |= map_state
    
    return state


def string_from_state(state,affirm_only=True):
    """
    Method which returns a string representing the current network state.
    
    :param state: Value representing the network state to parse
    :type state: bitmask network state, or :py:class:`NetworkState` object
    :param bool affirm_only: If true, we will only output positive states
    
    """
        
    states = []
    
    affirm_only_values = ["Online", "OnDomain", "OnVPN", "Unknown"]
    
    if isinstance(state, NetworkState):
        state_int = state.state
    else:
        state_int = state
        
    if state_int == NETWORK_STATE_UNKNOWN:
        states.append("Unknown")
    
    for map_state, map_name in state_string_map.iteritems():
        if not map_state:
            continue
        
        if map_state & state_int == map_state:
            if affirm_only and map_name.lower() in [x.lower() 
                                                for x in affirm_only_values]:
                states.append(map_name)
            elif not affirm_only:
                states.append(map_name)
    
    state_string = ", ".join(states)
            
    if not state_string and (NETWORK_STATE_OFFLINE & state_int 
                                                    == NETWORK_STATE_OFFLINE):
        state_string = "Offline"
    
    elif not state_string:
        state_string = "Unknown"
        
    return state_string

def configure_osx():
    """
    Method to configure our network package for use with OS X
    """
    
    import network_osx
    global state
    
    state = network_osx.NetworkStateOSX()
    

def configure_ubuntu():
    """
    Method to configure our network package for use with Ubuntu
    """
    import network_ubuntu
    global state
    
    state = network_ubuntu.NetworkStateUbuntu()
    

#MARK: OS Configuration
if acme.platform == "OS X" or acme.platform == "macOS":
    configure_osx()
elif acme.platform == "Ubuntu":
    configure_ubuntu()

