"""
.. module:: acme.claims
    :synopsis: Module containing classes used by acme for claims generation. Also it handles clients' current state of token generation.
    :platform: OSX, Ubuntu

.. moduleauthor:: Jason Simmons <jasosimm@amazon.com>


"""

import datetime
import time
import uuid

import acme
import acme.compliance
import acme.network as network
import systemprofile

import logging

compliance = acme.compliance

class ClaimsModule(acme.SerializedObject):


    TOKEN_STATE_NONE = 0
    TOKEN_STATE_AVAILABLE = 1<<0
    TOKEN_STATE_UNAVAILABLE = 1<<1
    logger_name = "ClaimsModule"

    def __init__(self, compliance_controller=None):

        self.token_state = self.TOKEN_STATE_NONE
        self.last_token_generation_attempt = None
        self.event_type = None
        key_map = {
                   "token_state": self.TOKEN_STATE_NONE,
                   "last_token_generation_attempt" : "last_token_generation_attempt",
                   "type": "event_type"
                }
        self.compliance_controller = compliance_controller
        
        acme.SerializedObject.__init__(self,key_map=key_map)
    
    def get_compliance_trust_value(self):
        """
        Method to calculate trust value based on compliance status.

        :param integer bitmask status: current compliance status.
        
        """
        
        status = compliance.ComplianceStatus.UNKNOWN
        trust_value = 0.0

        if self.compliance_controller:
            status = self.compliance_controller.get_device_status()

            if status & compliance.ComplianceStatus.COMPLIANT:
                trust_value = 1.0
            elif status & compliance.ComplianceStatus.INGRACETIME:
                trust_value = 0.75
            elif status & compliance.ComplianceStatus.ISOLATIONCANDIDATE:
                trust_value = 0.25
            elif status & compliance.ComplianceStatus.ISOLATED:
                trust_value = 0.0
            elif status & compliance.ComplianceStatus.NONCOMPLIANT:
                trust_value = 0.5

        return trust_value


    def get_compliance_claim(self):
        """
        Method which returns compliance claim for posture cookie.
        """

        logger = logging.getLogger()
        
        platform = acme.platform
        if platform == "OS X":
            platform = "macOS"

        data_claims = {}
        data_claims["compliance.platform"] = platform
        
        if self.compliance_controller:
            for module in self.compliance_controller.list_modules():
                result = module.last_evaluation_result
                if not result:
                    continue
                
                try:
                    data_claims["compliance.{}.status".format(
                            module.identifier)] = module.compliance_status()
                    for key, file in result.support_files.iteritems():
                        if file.hash:
                            data_claims["compliance.{}.{}".format(file.name,
                                                file.hash_algo)] = file.hash
                except Exception as exp:
                    logger.warning("An error occured creating compliance claim "
                            "while processing module: '{}'. Error: {} ".format(
                                                            module.identifer,
                                                            exp.message))
                    logger.log(9,"Failure stack trace (handled cleanly):", 
                                                                    exc_info=1)
        
        compliance_claim = {}
        compliance_claim['guid'] = str(uuid.uuid4())
        compliance_claim['name'] = "com.amazon.acme.compliance"
        compliance_claim['trustvalue'] = self.get_compliance_trust_value()
        compliance_claim['dateTime'] = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat())
        compliance_claim['capInfo'] = {'class' : "com.amazon.acme", 'version' : "1.0.0"}
        compliance_claim['clientChainInfo'] = [{'deviceID' : systemprofile.profiler.system_identifier()}]
        compliance_claim['claimsInfo'] = {
                                        'dataClaims' : data_claims
                                        }

        return compliance_claim


    def create_posture_token(self, duration=900):
        """
        Method which returns our posture token.

        :param int duration: Expiry duration of token. (UNIT: seconds)
        """

        try:
            posture_token = {}
            posture_token['logged_in_username'] = systemprofile.profiler.current_user()
            posture_token['device_id'] = systemprofile.profiler.system_identifier()
            posture_token['iat'] = int(time.time()) # UTC Time in seconds
            posture_token['jti'] = str(uuid.uuid4())
            posture_token['nbf'] = int(time.time() - 60)
            posture_token['exp'] = int(time.time() + duration)
            posture_token['amazon_enterprise_access'] = True
            posture_token['claims'] = []
            posture_token['claims'].append(self.get_compliance_claim())
        except KeyError:
            posture_token = datetime.datetime.utcnow()

        return posture_token

Claims = ClaimsModule

def configure_osx():
    """
    Method to configure claims module for mac os.
    """

    import claims_osx
    global Claims
    Claims = claims_osx.ClaimsMacOS

def configure_ubuntu():
    """
    Method to configure claims module for ubuntu.
    """

    import claims_ubuntu
    global Claims
    Claims = claims_ubuntu.ClaimsUbuntu

if acme.platform == "OS X" or acme.platform == "macOS":
    configure_osx()
elif acme.platform== "Ubuntu":
    configure_ubuntu()
