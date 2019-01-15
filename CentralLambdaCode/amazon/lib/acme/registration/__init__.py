"""
.. module:: acme.registration
    :synopsis: Module containing classes used for ACME device registration.

    :platform: RHEL, OSX, Ubuntu

.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>


"""

#MARK: Imports
import datetime
import json
import math
import logging
import os
import re
import requests
from random import randrange
from datetime import timedelta

FIRST_RENEWAL_MARK = datetime.timedelta(days=90)
SECOND_RENEWAL_MARK = datetime.timedelta(days=60)
FINAL_RENEWAL_MARK = datetime.timedelta(days=30)
SECOND_RENEWAL_SKEW = datetime.timedelta(days=1)
FINAL_RENEWAL_SKEW = datetime.timedelta(minutes=5)
SUPER_CAP_CLASS = "com.amazon.aea.cap"
SUPER_RENEWAL_CLAIM = "com.amazon.aea.cap.renew"

## Hotfix for Ubuntu (there are issues with requests_kerberos that need to be resolved)
try:
    from requests_kerberos import HTTPKerberosAuth
except:
    HTTPKerberosAuth = None


import sys
import time

import acme
import acme.crypto

import systemprofile
import systemprofile.directoryservice.kerberos as kerberos
from uuid import uuid4

#MARK: - Classes
class AuthType(acme.Enum):
    """
    Class which represents registration authentication options
    """

    NONE = 0
    KERBEROS = 1 << 0
    KERBEROS_USER = 1 << 1 | KERBEROS
    KERBEROS_SYSTEM = 1 << 2 | KERBEROS
    TOKEN = 1 << 3

class ResultStatus(acme.Enum):
    '''
    Class which represents Registrar response status
    '''

    #it also can contain some exception message information
    FAILED = 1 #don't use it directly
    UNKNOWN_ERROR = 1 << 1 | FAILED
    KNOWN_ERROR = 1 << 2 | FAILED
    ERROR_JSON_CONTENT = 1 << 3 | FAILED
    UNKNOWN_DEVICE_CLASS = 1 << 4 | FAILED
    NEW_DEVICE = 1 << 5
    EXISTED_DEVICE = 1 << 6
    GIVEN_UUID_EXISTED = 1 << 7
    UNAVAILABLE_TOKEN = 1 << 8 | FAILED
    NEW_INSTANCE = 1 << 9
    EXISTED_INSTANCE = 1 << 10
    SUCCESS = 1 << 11
    AVAILABLE_TOKEN = 1 << 12

class ApplicantBase(acme.SerializedObject):
    """
    Class which provides device registration functionality.

    :param string uuid: The unique identifier to use for our device.

    """

    logger_name = "Applicant"

    uuid = None                         #: The UUID to use for registration.

    registrar_address = None            #: The IP or DNS name of our registrar server

    _registrar_url = None               #: Backing variable for registrar_url property
    _registration_uuid = None           #: The registration UUID provided by our registrar during registration.
    _registration_token = None          #: Token provided by our registrar during registration.

    _last_response = None


    @property
    def registrar_url(self):
        """
        Property which represents our registrar URL, with protocol
        designator ('https://') and REST route ('/api/registrar').

        This value is generally derived from :py:var:`registrar_address` and
        :py:func:`_build_base_url`.

        """

        url = None

        if self._registrar_url is None:
            if self.registrar_address:
                url = self._build_base_url()
        else:
            url = self._registrar_url

        return url

    @registrar_url.setter
    def registrar_url(self,value):
        self._registrar_url = value

    def __init__(self, uuid=None, registrar_address=None, key_map=None,
                                                            authtype=None,
                                                            *args, **kwargs):

        self.uuid = uuid
        self.registrar_address = registrar_address
        self.config_server = None
        self.config_signing_authority = None
        self.certificate = None
        self.renewal_date = None
        self.checkin_lock = None

        if authtype is None:
            self.authtype = AuthType.KERBEROS_SYSTEM
        else:
            self.authtype = authtype

        if key_map is None:
            key_map = { "registrar_address" : None,
                        "token" : "_registration_token",
                        "registrar_url" : "_registrar_url",
                        "config_server": None,
                        "config_signing_authority":None,
                        "certificate":None,
                        "renewal_date": "<type=datetime>;"
                        }

        acme.SerializedObject.__init__(self,key_map=key_map,*args, **kwargs)

    def __enter__(self):
        """
        Context manager entry method. This is a no-op.
        """

        return self

    def __exit__(self, type, value, traceback):
        """
        Context manager exit method, this is a no-op
        """

        pass

    def credential_session(self, authtype=None):
        """
        Method which returns a context manager object responsible for
        bootstrapping credentials during the registration process.

        :param authtype: The type of authentication to use for this session.
        :type authtype: AuthType enum

        .. example:
            >>> ap = Applicant()
            >>> with ap.credential_session(authtype=AuthType.KERBEROS_SYSTEM):
            ...      args = {"auth" : requests_kerberos.HTTPKerberosAuth()}
            ...      requests.get("http://site.amazon.com", **args)

        """

        if authtype is None:
            authtype = self.authtype

        if authtype == AuthType.KERBEROS_SYSTEM:
            return kerberos.SystemCredentialContextManager()
        else:
            return self

    def negotiate(self, uuid=None, url=None, authtype=None, token = None, args=None):
        """
        Method which will negotiate a UUID with our registrar.

        :param str uuid: The device UUID to negotiate
        :param str url: The URL endpoint to query (optional)
        :param authtype: The type of authentication to use for this session.
        :type authtype: AuthType enum
        :param dict args: Optional args to pass to requests

        :raises ValueError: If neither registrar_address nor registrar_url are not set.


        """
        
        logger = logging.getLogger(self.logger_name)
        
        if uuid is None:
            uuid = self.uuid
        
        if authtype is None and token is None:
            authtype = self.authtype
        
        if url is None and token is None:
            try:
                url = self.registrar_url + "/api/corp/negotiate"
            except TypeError:
                raise ValueError("Neither registrar_address nor registrar_url is set!"), None, sys.exc_info()[2]
            
        if url is None and token:
            try:
                url = self.registrar_url + "/api/token/negotiate"
            except TypeError:
                raise ValueError("Neither registrar_address nor registrar_url is set!"), None, sys.exc_info()[2]
        
        identity_data = self.build_identity_data()
        
        identity_data["uuid"] = uuid
        
        ## If Device Owner is not defined, username key won't exist
        if not "username" in identity_data:
            identity_data["username"] = "_" + systemprofile.profiler.hostname() + "_"
        
        request_args = {}
        
        if token:
            identity_data["token"] = token
        else:
            if self.authtype == AuthType.TOKEN:
                identity_data["token"] = self._registration_token
            elif self.authtype & AuthType.KERBEROS:
                if HTTPKerberosAuth:
                    request_args["auth"] = HTTPKerberosAuth()
                else:
                    raise RegistrationError("Failed to negotiate system identifier, Kerberos auth is not supported!")

        request_args["data"] = json.dumps(identity_data)

        if args:
            for key, value in args.iteritems():
                request_args[key] = value

        json_data = None
        try:
            logger.debug("Negotiating UUID with registrar at URL:{}".format(url))
            logger.log(5,"Negotiation request data:{}".format(request_args))
            
            response = requests.post(url, **request_args)
            
            self._last_response = response
            
            try:
                json_data = response.json()
                # Redact token from logging
                log_json_data = json_data.copy()
                log_json_data["token"] = "*"
                logger.log(5, "Received service response: {}".format(log_json_data))
            except Exception as exp:
                logger.error("Failed to parse server response! (Response text:'''{}''')".format(response.text))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                json_data = { "status" : ResultStatus.FAILED }
            
            if response.status_code != 200:
                raise RegistrationError("Failed to negotiate UUID, registrar returned HTTP Status: '{}'".format(
                                                    response.status_code),
                                                payload=json_data)
            
            status = json_data["status"]
            if status & ResultStatus.FAILED:
                raise RegistrationError("Failed to negotiate system identifier, registrar returned status: '{}' ({})".format(
                                                    ResultStatus.to_string(status),
                                                    status),
                                                payload=json_data)
        except Exception as exp:
            raise RegistrationError("Failed to negotiate UUID; {}".format(exp),
                                    payload=json_data), None, sys.exc_info()[2]
        
        if token:
            self._registration_token = token
        else:
            self._registration_token = json_data["token"]
        self._registration_uuid = json_data["uuid"].encode("ascii")
        
        if not json_data["uuid"]:
            raise RegistrationError("Registrar did not confirm UUID or provide a new one!", payload=json_data)
        
        if uuid is None:
            self.uuid = self._registration_uuid
        elif uuid != self._registration_uuid:
            raise RegistrationUUIDReset(old_uuid=uuid,
                                            new_uuid=self._registration_uuid)
    
    def register(self, csr_data, uuid=None, url=None, authtype=None, args=None):
        """
        Method which will register our system as a CAP with our registrar,

        :param string csr_data: Our certificate signing request, in PEM format.
        :param string uuid: Our registration UUID (optional)
        :param str url: The URL endpoint to query (optional)
        :param authtype: The type of authentication to use for this session.
        :type authtype: AuthType enum
        :param dict args: Optional args to pass to requests


        :returns: (dict) Registration data
        """

        logger = logging.getLogger(self.logger_name)

        if authtype is None:
            authtype = AuthType.TOKEN

        if not self._registration_token and not self._registration_uuid:
            logger.info("Registration session not found, performing negotiation.")
            self.negotiate(uuid=uuid)

        if not self._registration_token:
            raise RegistrationError("Cannot register system: no registration token has been established!")

        if not uuid and self._registration_uuid:
            uuid = self._registration_uuid
        else:
            uuid = self.uuid

        if url is None:
            try:
                url = self.registrar_url + "/api/token/register"
            except TypeError:
                raise ValueError("Neither registrar_address nor registrar_url is set!"), None, sys.exc_info()[2]

        username = systemprofile.profiler.owner()
        if not username:
            username = "_" + systemprofile.profiler.hostname() + "_"
        
        request_args = {}
        request_args["data"] = json.dumps({"token": self._registration_token,
                                    "cap_type" : "com.amazon.acme",
                                    "identifier" : uuid,
                                    "username": username,
                                    "csr" : csr_data,
                                })

        # request_args["verify"] = False ## WARNING: THIS DISABLES SSL, IT SHOULD NOT MAKE IT TO CR

        if authtype & AuthType.KERBEROS:
            if HTTPKerberosAuth:
                request_args["auth"] = HTTPKerberosAuth()
            else:
                raise RegistrationError("Failed to negotiate system identifier, Kerberos auth is not supported!")

        if args:
            for key, value in args.iteritems():
                request_args[key] = value

        json_data = None
        try:
            logger.debug("Signing certificate with registrar at URL: {}".format(url))
            # Redact token from logging
            log_request_args = request_args.copy()
            temp_json_data = json.loads(log_request_args["data"])
            temp_json_data["token"] = "*"
            log_request_args["data"] = json.dumps(temp_json_data)
            logger.log(5,"Registration request data:{}".format(log_request_args))

            response = requests.post(url, **request_args)

            try:
                json_data = response.json()
                logger.log(5, "Received service response: {}".format(json_data))
            except Exception as exp:
                logger.error("Failed to parse json response!")
                json_data = { "status" : ResultStatus.FAILED }

            if response.status_code != 200:
                raise RegistrationError("Registrar returned HTTP Status: '{}'".format(
                                                    response.status_code),
                                                payload=json_data)
            json_data = response.json()
        except Exception as exp:
            raise RegistrationError("Failed to sign certificate with registrar; {}".format(exp),
                                    payload=json_data), None, sys.exc_info()[2]

        status = json_data["status"]
        if status & ResultStatus.FAILED:
            raise RegistrationError("Failed to sign certificate with registrar, registrar returned status: '{}' ({})".format(
                                                ResultStatus.to_string(status),
                                                status),
                                            payload=json_data)

        if not json_data["certificate"]:
            raise RegistrationError("Failed to sign certificate with registrar, registrar did not return certificate data!",
                                                        payload=json_data)

        return json_data

    def renew(self, csr_data=None, identity=None, uuid=None, url=None, args=None):
        """
        Method which will register our system as a CAP with our registrar,

        :param string csr_data: Our certificate signing request, in PEM format.
        :param string uuid: Our registration UUID (optional)
        :param str url: The URL endpoint to query (optional)
        :param authtype: The type of authentication to use for this session.
        :type authtype: AuthType enum
        :param dict args: Optional args to pass to requests


        :returns: (dict) Registration data
        """

        logger = logging.getLogger(self.logger_name)

        if not uuid and self._registration_uuid:
            uuid = self._registration_uuid
        else:
            uuid = self.uuid

        if url is None:
            try:
                url = "https://{}/api/cert/renew".format(self.config_server)
            except TypeError:
                raise ValueError("Neither registrar_address nor registrar_url is set!"), None, sys.exc_info()[2]
        
        response = None
        
        username = systemprofile.profiler.owner()
        if not username:
            username = "_" + systemprofile.profiler.hostname() + "_"
        
        request_args = {}
        data_claims = {"platform": systemprofile.profiler.platform(),
                        "csr" : csr_data,
                        "username": username}
        
        json_data = {"name": SUPER_RENEWAL_CLAIM,
                                  "guid": str(uuid4()),
                                  "dateTime":str(datetime.datetime.utcnow().replace(microsecond=0).isoformat()),
                                  "clientChainInfo" : [{'deviceID' : systemprofile.profiler.system_identifier()}],
                                  "cap_type" : SUPER_CAP_CLASS,
                                  "cap_class" : SUPER_CAP_CLASS,
                                  "identifier" : uuid,
                                  "data_claims": data_claims,
                                  "capInfo" : {"class" : SUPER_CAP_CLASS, "version" : "1.0.0", "identifier": uuid}
                                }
        
        request_args["data"] = json.dumps({"claim_jwt":identity.get_jwt(json_data, b64encode=False)})

        # request_args["verify"] = False ## WARNING: THIS DISABLES SSL, IT SHOULD NOT MAKE IT TO CR

        if args:
            for key, value in args.iteritems():
                request_args[key] = value

        json_data = None
        try:
            logger.debug("Renewing certificate with registrar at URL: {}".format(url))
            logger.log(5,"Registration renewal request data:{}".format(request_args))
           
            handler = requests.post
            with self.checkin_lock:
                with acme.requests.RequestsContextManager(identity) as cm:
                    response = handler(url,cert=cm.temp_file.name, data=request_args["data"])
    
                try:
                    json_data = response.json()
                    logger.log(5, "Received service response: {}".format(json_data))
                except Exception as exp:
                    logger.error("Failed to parse json response!")
                    json_data = { "status" : ResultStatus.FAILED }
    
                if response.status_code != 200:
                    raise RegistrationError("Registrar returned HTTP Status: '{}'".format(
                                                        response.status_code),
                                                    payload=json_data)
        except Exception as exp:
            raise RegistrationError("Failed to sign certificate with registrar; {}".format(exp),
                                    payload=json_data), None, sys.exc_info()[2]

        status = json_data["status"]
        if status & ResultStatus.FAILED:
            raise RegistrationError("Failed to sign certificate with registrar, registrar returned status: '{}' ({})".format(
                                                ResultStatus.to_string(status),
                                                status),
                                            payload=json_data)

        if not json_data["certificate"]:
            raise RegistrationError("Failed to sign certificate with registrar, registrar did not return certificate data!",
                                                        payload=json_data)

        return json_data
    
    def _build_base_url(self):
        """
        Method which will build our URL path based on our address.
        :raises ValueError: If a URL is not set.
        """

        if self.registrar_address is None:
            raise ValueError("Cannot negotiate our system uuid: 'registrar_address' is not set!")

        url = self.registrar_address

        ## Strip trailing slash
        if url[-1:] == "/":
            url = url[:-1]

        ## Strip out http:// with https://
        matches = re.match(".*?://(.*)?",url)

        if matches is not None:
            url = "https://" + matches.groups()[0]
        else:
            url = "https://" + url

        return url

    def build_identity_data(self):
        """
        Method which returns a dictionary containing identity data, keyed
        by identity attribute.
        """

        logger = logging.getLogger(self.logger_name)

        sp = systemprofile.profiler


        data = {"device_class" : "com.amazon.acme", "cap_type": "com.amazon.acme" }


        source_items = {
                        "uuid": "system_identifier",
                        "hardware_uuid": "hardware_identifier",
                        "system_type": None,
                        "mac_address" : None,
                        "hostname": None,
                        "platform": None,
                        "architecture" : None,
                        "platform_version" : "system_version",
                        "make": "hardware_make",
                        "model": "hardware_model",
                        "serial_number": None,
                        "username": "owner",
                        "asset_tag": None,
                    }

        for key, method_name in source_items.iteritems():
            if method_name is None:
                method_name = key

            method = None
            try:
                method = getattr(sp, method_name)
            except AttributeError:
                logger.warning("Could not lookup identity data for key:{}, method:{} does not exist...".format(key, method_name))
                continue

            try:
                value = method()
                if value is not None:
                    data[key] = value
            except Exception as exp:
                logger.warning("Failed to look up key:{} using systemprofile method:{}(). Error:{}".format(key, method_name, exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        return data

    def save_to_file(self,filepath,data):
        '''
        Method to save registration information into state
        '''
        with open(filepath,"w") as fh:
            fh.write(json.dumps(data,indent=4))
            
    def is_registered(self, filepath=None):
        '''
        Method to check current user is registered
        '''
        
        logger = logging.getLogger(self.logger_name)
        if filepath and os.path.exists(filepath):
            self.load_from_file(filepath)
        
        if self.config_server and self.config_signing_authority and self.certificate:
            try:
                config_cert_flag = acme.crypto.is_certificate_not_expired(self.config_signing_authority)
            except Exception as exp:
                logger.error("An error occurred while validating config certificate expiry: {}".format(exp))
                config_cert_flag = False
            try:
                device_cert_flag = acme.crypto.is_certificate_not_expired(self.certificate)
            except Exception as exp:
                logger.error("An error occurred while validating device certificate expiry: {}".format(exp))
                device_cert_flag = False
            if config_cert_flag and device_cert_flag:
                logger.debug("Validated certificate existence and expiry")
                return True
            else:
                logger.info("Config signing authority validation status: {0} , device registration validation status: {1}".format(config_cert_flag, device_cert_flag))
        elif filepath:
            logger.info("Unable to load config signing authority cert and device cert from filepath:{0}".format(filepath))
            
        return False
    
    def get_expiry_date(self, filepath=None):
        '''
        Method to get certificate expiry date
        '''
        device_cert_expiry, config_cert_expiry = datetime.datetime.utcnow(), datetime.datetime.utcnow()
        
        logger = logging.getLogger(self.logger_name)
        if filepath and os.path.exists(filepath):
            self.load_from_file(filepath)
        
        if self.config_server and self.config_signing_authority and self.certificate:
            try:
                config_cert_expiry = acme.crypto.get_cert_expiry(self.config_signing_authority)
            except Exception as exp:
                logger.error("An error occurred while validating config certificate expiry: {}".format(exp))
            try:
                device_cert_expiry = acme.crypto.get_cert_expiry(self.certificate)
            except Exception as exp:
                logger.error("An error occurred while validating device certificate expiry: {}".format(exp))
        else:
            logger.info("Unable to load config signing authority cert and device cert from filepath:{0}".format(filepath))
        
        if device_cert_expiry < config_cert_expiry:
            return device_cert_expiry
        else:
            return config_cert_expiry
            
        return None
    
    def get_renewal_datetime(self, filepath=None):
        '''
        Method to get certificate expiry date
        '''
        renewal_datetime = datetime.datetime.utcnow()
        if filepath and os.path.exists(filepath):
            self.load_from_file(filepath)
        
        if self.renewal_date:
            return self.renewal_date
        else:
            expiry_datetime = self.get_expiry_date(filepath)
        
        now = datetime.datetime.utcnow()
        
        if now <  expiry_datetime - FIRST_RENEWAL_MARK:
            start = expiry_datetime - FIRST_RENEWAL_MARK
            end = expiry_datetime - SECOND_RENEWAL_MARK
            renewal_datetime = random_date(start, end)
        elif now <  expiry_datetime - SECOND_RENEWAL_MARK:
            start = now
            end = expiry_datetime - SECOND_RENEWAL_MARK
            renewal_datetime = random_date(start, end)
        elif now <  expiry_datetime - FINAL_RENEWAL_MARK:
            start = now
            end = now + SECOND_RENEWAL_SKEW
            renewal_datetime = random_date(start, end)
        elif now <  expiry_datetime:
            start = now
            end = now + FINAL_RENEWAL_SKEW
            renewal_datetime = random_date(start, end)
            
        return renewal_datetime

def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

#MARK: - Exceptions
class RegistrationError(Exception):
    """
    Exception thrown when registration fails.
    """
    def __init__(self,message=None, payload=None, *args, **kwargs):

        if message is None and payload:
            message = "Registrar failed!  Response:{}".format(
                    new_uuid)
        elif message is None:
            message = "Registration failed due to an unknown error!"

        self.payload = payload

        super(RegistrationError, self).__init__(message)

#MARK: - Exceptions
class RegistrationRenewalError(Exception):
    """
    Exception thrown when registration renewal fails.
    """
    def __init__(self,message=None, payload=None, *args, **kwargs):

        if message is None:
            message = "Registration renewal failed due to an unknown error!"

        self.payload = payload

        super(RegistrationRenewalError, self).__init__(message)

class RegistrationUUIDReset(Exception):
    """
    Exception thrown when registration requires a new UUID.
    """

    def __init__(self,message=None, old_uuid=None, new_uuid=None):

        if message is None and new_uuid and old_uuid:
            message = "Registrar has designated a new UUID:{} (old UUID:{})".format(
                    new_uuid, old_uuid)
        elif message is None and new_uuid and not old_uuid:
            message = "Registrar has designated a new UUID:{}".format(
                    new_uuid)
        elif message is None:
            message = "UUID:{} was rejected!".format(old_uuid)

        self.old_uuid = old_uuid
        self.new_uuid = new_uuid

        super(RegistrationUUIDReset, self).__init__(message)

#MARK: - Module logic
Applicant = ApplicantBase

def _configure_macos():
    """
    Method to configure our registration package for use with macOS
    """

    import registration_macos
    global Applicant

    Applicant = registration_macos.ApplicantMacOS


def _configure_ubuntu():
    """
    Method to configure our registration package for use with Ubuntu
    """

    import registration_ubuntu
    global Applicant

    Applicant = registration_ubuntu.ApplicantUbuntu


## OS Configuration
if acme.platform == "OS X" or acme.platform == "macOS":
    _configure_macos()
elif acme.platform == "Ubuntu":
    _configure_ubuntu()

