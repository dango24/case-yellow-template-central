import acme
import acme.crypto
from acme.crypto import KeyNotFoundError
import acme.agent as agent

import datetime
import logging
import multiprocessing
import pexpect
import pykarl.event
import re
import sys
import systemprofile
import subprocess
import time

__version__ = "1.1"

class AccountManagementAgent(agent.BaseAgent):
    """
    Agent which will periodically heartbeat to KARL while online.
    """
    
    _admin_account_name = None      #: Backing store containing the shortname of the account to manage.
    
    last_password_blob = None       #: Last known encrypted password
    last_password_rotation = None   #: Date that we last rotated our password
    last_password_escrow = None     #: Date that we last escrowed our password
    
    password_rotation_interval = datetime.timedelta(days=90)    #: Frequency in which we rotate our password
    password_escrow_interval = datetime.timedelta(days=7)     #: Frequency in which we escrow our password 
                                        #: (This is necessary until we have
                                        #: KARL event delivery affirmation)
    
    password_length = 16  #: The length of the password to generate.
    
    _password_rotation_in_progress_mp = multiprocessing.Value('i',0)
    
    @property
    def admin_account_name(self):
        """
        Property representing the admin user name to manage.
        """
        account_name = None
        if self._admin_account_name:
            account_name = self._admin_account_name
        elif acme.platform == "OS X" or acme.platform == "macOS":
            account_name = "admin"
        elif acme.platform == "Ubuntu":
            account_name = "ceadmin"
            
        return account_name
    
    @admin_account_name.setter
    def admin_account_name(self,value):
        """
        Setter accessor for property: admin_account_name
        """
        
        self._admin_account_name = value
    
    @property
    def rotation_in_progress(self):
        """
        Property which puts a boolean wrapper around our 
        :py:class:`multiprocessing.Value` object in 
        self._password_rotation_in_progress_mp
        """
        
        is_running = False
        
        if self._password_rotation_in_progress_mp.value:
            is_running = bool(self._password_rotation_in_progress_mp.value)
        
        return is_running
        
    @rotation_in_progress.setter
    def rotation_in_progress(self,value):
        """
        Setter access for our should_run property
        """
        
        is_running = int(bool(value))
        
        self._password_rotation_in_progress_mp.value = is_running
    
    
    def __init__(self,*args,**kwargs):
        
        self.identifier = "AccountManagementAgent"
        self.name = "AccountManagementAgent"
        
        self.prerequisites = agent.AGENT_STATE_ONLINE
        self.triggers = agent.AGENT_TRIGGER_SCHEDULED
        
        self.run_frequency = datetime.timedelta(days=1)
        self.run_frequency_skew = datetime.timedelta(hours=1)         
        
        self.last_password_blob = None      
        self.last_password_rotation = None
        self.last_password_escrow = None
        
        self.priority = agent.AGENT_PRIORITY_HIGH
        
        #: When subclassing, always init superclasses
        super(AccountManagementAgent,self).__init__(name=self.name,
                                            identifier=self.identifier,
                                            *args,**kwargs)
        
        self.key_map["password_rotation_interval"] = "<type=timedelta>"
        self.key_map["password_escrow_interval"] = "<type=timedelta>"
        self.key_map["last_password_rotation"] = "<type=datetime>"
        self.key_map["last_password_escrow"] = "<type=datetime>"
        self.key_map["last_password_blob"] = None
        self.key_map["password_length"] = "<type=int>"
        self.key_map["admin_account_name"] = None
        
        self.settings_keys.append("password_rotation_interval")
        self.settings_keys.append("password_escrow_interval")
        self.settings_keys.append("admin_account_name")
        self.settings_keys.append("password_length")
        
        self.state_keys.append("last_password_rotation")
        self.state_keys.append("last_password_escrow")
        self.state_keys.append("last_password_blob")
        self.state_keys.append("admin_account_name")
    
    def execute(self,trigger=None,data=None):
        """
        Method to send a KARL HeartBeat
        
        :param trigger: The trigger executing the action
        :type trigger: int: bitwise mask value. See AGENT_TRIGGER_*
        """
        logger = logging.getLogger(self.logger_name)
        
        logger.info("{} Executing!".format(self.identifier))
        
        start_time = datetime.datetime.utcnow()
        
        result = agent.AGENT_EXECUTION_STATUS_NONE
        
        ## Make sure KARL is configured
        dispatcher = pykarl.event.dispatcher
        if dispatcher.is_configured():
            if self.is_password_rotation_time():
                try:
                    self.rotate_password(username=self.admin_account_name,
                                                length=self.password_length)
                    logger.info("Successfully rotated admin account password ({})!".format(
                                                    self.admin_account_name))
                    result = agent.AGENT_EXECUTION_STATUS_SUCCESS
                except KeyNotFoundError as exp:
                    logger.fatal("A error occurred while rotating our admin password. Error: {}".format(exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                    result = agent.AGENT_EXECUTION_STATUS_FATAL
                except PasswordChangeError as exp:
                    logger.error(exp.message)
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                    result = agent.AGENT_EXECUTION_STATUS_ERROR
                except Exception as exp:
                    logger.error("An unknown error occurred while rotating our admin password. Error: {}".format(exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                    result = agent.AGENT_EXECUTION_STATUS_ERROR
            
            elif self.is_password_escrow_time():
                try:
                    self.escrow_password()
                    result = agent.AGENT_EXECUTION_STATUS_SUCCESS
                    logger.info("Successfully escrowed admin account password.")
                except PasswordEscrowError as exp:
                    logger.fatal(exp.message)
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                    result = agent.AGENT_EXECUTION_STATUS_FATAL
                except Exception as exp:
                    logger.error("An unknown error occurred while escrowing our admin password. Error: {}".format(exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                    result = agent.AGENT_EXECUTION_STATUS_ERROR
            elif self.rotation_in_progress:
                logger.warning("Password rotation is currently in progress, deferring...")
        else:
            logger.error("Will not manage the local account, KARL is not configured!")
            result = agent.AGENT_EXECUTION_STATUS_FATAL
    
        ## Cleanup
        self.last_execution_status = result 
        self.last_execution = datetime.datetime.utcnow()
        
        logger.info("{} Finished Executing!".format(self.identifier))

    def escrow_password(self):
        """
        Method to escrow our current password.
        """
        
        if not self.last_password_blob:
            raise PasswordEscrowError("Failed to escrow password: current value is unknown!")
        
        karl_payload = {"status" : "Confirm",
                        "password_length" : self.password_length,
                        "secure_blob" : self.last_password_blob,
                        "account_name" : self.admin_account_name,
                        "rotation_date" : time.mktime(
                                    self.last_password_rotation.timetuple()),
                        }
        e = pykarl.event.Event(type="LocalPasswordEscrow",subject_area="ACME")
        e.payload = karl_payload
        
        dispatcher = pykarl.event.dispatcher
        
        if dispatcher.is_configured:
            e = pykarl.event.Event(type="LocalPasswordRotation",subject_area="ACME")
            e.payload = karl_payload
            
            dispatcher.dispatch(e)
            
            self.last_password_escrow = self.last_password_rotation
        else:
            raise PasswordEscrowError("Cannot escrow password: KARL dispatcher is not configured!")
    
    
    def rotate_password(self,username=None,length=None):
        """
        Method that will perform our password rotation and reporting.
        """
        
        self.rotation_in_progress = True
        
        karl_payload = {"account_name" : username }
        
        err_exception, backtrace  = None, None
        
        if not username:
            username = self.admin_account_name
        
        if not length:
            length = self.password_length
        
        logger = logging.getLogger(self.logger_name)
        
        try:
            
            new_password = acme.crypto.generate_password(length=length)
            encrypted_password = acme.crypto.encrypt_karl_data(new_password)
            
            self.changepass(username,new_password)
            
            karl_payload["password_length"] = length
            karl_payload["status"] = "Success"
            karl_payload["secure_blob"] = encrypted_password
            
            self.last_password_rotation = datetime.datetime.utcnow()
            self.last_password_blob = encrypted_password
            
            karl_payload["rotation_date"] = time.mktime(self.last_password_rotation.timetuple())            
            
        except KeyNotFoundError as exp:
            karl_payload["status"] = "Error"
            karl_payload["error_string"] = "Could not encrypt new password for escrow: {}".format(
                                                                exp.message)
            err_exception, backtrace = exp, sys.exc_info()[2]
        
        except Exception as exp:
            karl_payload["status"] = "Error"
            karl_payload["error_string"] = exp.message
            
            err_exception, backtrace = exp, sys.exc_info()[2]
            
        finally:
            self.rotation_in_progress = False
        
        dispatcher = pykarl.event.dispatcher
        
        if dispatcher.is_configured:
            e = pykarl.event.Event(type="LocalPasswordRotation",subject_area="ACME")
            e.payload = karl_payload
            
            dispatcher.dispatch(e)
            
            self.last_password_escrow = self.last_password_rotation

        if err_exception:
            raise err_exception, None, backtrace
        
    def changepass(self,username,password):
        """
        Method to change the password to the provided value
        for the provided username.
        """
                
        min_length = 6      #: We will throw a PasswordChangeError if the 
                            #: provided password is shorter than this value
        
        if not username:
            raise PasswordChangeError("Failed to set new password, no username was provided!")
        
        if not password:
            raise PasswordChangeError("Failed to set new password, No password was provided!")
            
        if not len(password) >= min_length:
            raise PasswordChangeError("Failed to set new password, password must be at least {} digits!".format(min_length))
        
        if acme.platform == "OS X" or acme.platform == "macOS":
            return self._changepass_osx(username,password)
        elif acme.platform == "Ubuntu":
            return self._changepass_ubuntu(username,password)
                    
        
    def _changepass_ubuntu(self,username,password):
        """
        Method to change the password to the provided value
        for the provided username on an Ubuntu system. 
        
        This method is not intended to be called directly, instead call 
        self.changepass()
        """
        
        chpasswd = "/usr/sbin/chpasswd"
        
        process = subprocess.Popen([chpasswd],stdin=subprocess.PIPE,
                                                    stdout=subprocess.PIPE,
                                                    stderr=subprocess.PIPE)
        
        input_data = "{}:{}".format(username,password)
        
        stdout,stderr = None, None
        try:
            stdout,stderr = process.communicate(input_data)
        except Exception as exp:
            process.kill()
            raise PasswordChangeError("Failed to set new password for account '{}': {}".format(
                                                                    username,
                                                                    exp))
        
        if process.returncode != 0:
            if stderr:
                raise PasswordChangeError("Failed to set new password for account '{}', chpasswd exited with status:{} ({})".format(
                                                username,
                                                process.returncode,
                                                stderr))
            
            else:
                raise PasswordChangeError("Failed to set new password for account '{}', chpasswd exited with status:{}".format(
                                                username,
                                                process.returncode))
            
        
    def _changepass_osx(self,username,password):
        """
        Method to change the password to the provided value
        for the provided username on a macOS system. 
        
        This method is not intended to be called directly, instead call 
        self.changepass()
        """

        keychain_item_name = "com.amazon.adminpassword"
        keychain_service_name = "adminpassword"
        keychain_comment_string = "Local admin account passphrase"
        oldpassword = None

        ## Sanity check username (necessary as pexpect does not have
        ## input sanitization.
        pattern = "^[A-Za-z0-9_,-]*$"
        if not re.match("^[A-Za-z0-9_,-]*$",username):
            raise PasswordChangeError("Failed to set new password for account: '{}', account name failed sanity checks!".format(username))
        
        ## Try to retrieve current password from Keychain
        ## Only exists on APFS imaged systems 
        try:
            oldpassword = acme.crypto.Identity().get_passphrase(
                                keychain_item_name=keychain_item_name, 
                                keychain_service_name=keychain_service_name)
        except acme.crypto.PassphraseError:
            pass

        ## If APFS imaged system, rotate password preserving SecureToken and save to Keychain        
        if (systemprofile.profiler.get_boot_disk_details() == 'apfs' and systemprofile.profiler.check_secure_token(username) and oldpassword):
            cmd = "/usr/sbin/sysadminctl"
            args = ["-resetPasswordFor", username, "-newPassword", "-", "-adminUser", username, "-adminPassword", "-"]
            child = pexpect.spawn(cmd, args, timeout=10)
            try:
                child.expect("New password:")
                child.sendline(password)
                child.expect("Enter password for")
                child.sendline(oldpassword)
                child.read(size=-1)
                child.close()
            except pexpect.EOF as exp:
                raise PasswordChangeError("Failed to set password for account: '{}', an unknown error occurred. (EOF hit... lastline:{})".format(username,child.before))
                    
            except pexpect.TIMEOUT as exp:
                raise PasswordChangeError("Failed to set password for account: '{}', command timed out. (lastline:{})".format(username,child.before))

            if child.before.find("Operation is not permitted without secure token unlock") != -1:
                raise PasswordChangeError("Failed to set password for account: '{}'. Failed to authenticate with prior credentials.".format(username))

            acme.crypto.Identity().create_passphrase(
                                    passphrase=password,
                                    keychain_item_name=keychain_item_name, 
                                    keychain_service_name=keychain_service_name,
                                    keychain_comment_string=keychain_comment_string)

        else: ## Legacy password rotation
            cmd = ["/usr/bin/passwd", " ", username]
            child = pexpect.spawn(cmd, timeout=10)
            
            try:
                child.expect("New password:")
                child.sendline(password)
                child.expect("Retype new password")
                child.sendline(password)
                child.read(size=-1)
                child.close()
            except pexpect.EOF as exp:
                if child.before.startswith("passwd: Unknown user name"):
                    raise PasswordChangeError("Failed to set new password! Unknown account: '{}'".format(username))
                else:
                    raise PasswordChangeError("Failed to set password for account: '{}', an unknown error occurred. (EOF hit... lastline:{})".format(username,child.before))
                    
            except pexpect.TIMEOUT as exp:
                raise PasswordChangeError("Failed to set password for account: '{}', command timed out. (lastline:{})".format(username,child.before))
            
            
    def is_password_rotation_time(self):
        """
        Method which returns True if it is time to rotate our password.
        
        :returns True: If it is time to rotate our password and a rotation is
                    not currently taking place.
        
        """
        
        is_time = False
        now = datetime.datetime.utcnow()        
        
        if (not self.rotation_in_progress 
                                and now >= self.next_password_rotation_date()):
            is_time = True
        
        return is_time
        
    def is_password_escrow_time(self):
        """
        Method which returns True if it is time to escrow our password.
        
        :returns True: If it is time to escrow our password.        
        """
        
        is_time = False
        now = datetime.datetime.utcnow()
        
        if (not self.rotation_in_progress 
                                and now >= self.next_password_escrow_date()):
            is_time = True
        
        return is_time
    
    def next_password_rotation_date(self):
        """
        Method which returns the date of our next password rotation
        
        :returns: :py:class:`datetime.datetime`
        
        """
        
        ## Default date is in the past.
        date = datetime.datetime.utcnow() - datetime.timedelta(seconds=1)
        
        if self.last_password_rotation:
            date = self.last_password_rotation + self.password_rotation_interval
        
        return date
        
    def next_password_escrow_date(self):
        """
        Method which returns the date of our next password escrow
        
        :returns: :py:class:`datetime.datetime`
        
        """
        
        date = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
        
        if self.last_password_escrow:
            date = self.last_password_escrow + self.password_escrow_interval
        
        return date

class PasswordEscrowError(Exception):
    """
    Exception that is thrown if the password escrow fails.
    """
    
    pass

class PasswordChangeError(Exception):
    """
    Exception that is thrown if the password change fails.
    """
    
    pass
        
