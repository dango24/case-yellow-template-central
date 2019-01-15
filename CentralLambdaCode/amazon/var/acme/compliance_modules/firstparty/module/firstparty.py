import subprocess
import os
import datetime
import plistlib
import logging

import acme.compliance as compliance

__version__ = "1.0"

class FirstPartyMacOSComplianceModule(compliance.BaseModule):
    """
    Compliance Module for macOS which will evaluate first party patch compliance
    """
        
    logger_name = "FirstPartyMacOS"
    auto_remediate = False
    can_remediate = False
    firstparty_tool_path = "/usr/local/amazon/bin/acmeupdates"
    patches_pref_path = "/Library/Preferences/com.amazon.acme.updates.plist"
    firstparty_domain = "com.apple.softwareupdate"
    
    def __init__(self,key_map=None,settings_keys=None,*args,**kwargs):
                
        self.identifier = "firstparty"
        self.name = "FirstPartyMacOS"
        self.evaluation_interval = datetime.timedelta(hours=1)
        self.evaluation_skew = datetime.timedelta(minutes=15)
        self.triggers = compliance.ExecutionTrigger.SCHEDULED
        
        if key_map is None:
            key_map = {}
            key_map.update(compliance.BaseModule.key_map)
            key_map["firstparty_tool_path"] = None
            key_map["patches_pref_path"] = None
            key_map["firstparty_domain"] = None
        
        if settings_keys is None:
            settings_keys = compliance.BaseModule.settings_keys[:]
            settings_keys.append("firstparty_tool_path")
            settings_keys.append("patches_pref_path")
            settings_keys.append("firstparty_domain")
        
        super(FirstPartyMacOSComplianceModule, self).__init__(name=self.name,
                                                identifier=self.identifier,
                                                key_map=key_map,
                                                settings_keys=settings_keys,
                                                *args, **kwargs)
    
    def register_support_files(self):   
        """
        Method to register our support files
        """
        name = "com.amazon.UpdateBadger.manifest.plist"
        path = "/Library/Preferences/com.amazon.UpdateBadger.manifest.plist"
        self.support_files[name] = compliance.SupportFile(name=name, 
                                                                filepath=path)
        
        name = "com.amazon.UpdateBadger.exceptions.plist"
        path = "/Library/Preferences/com.amazon.UpdateBadger.exceptions.plist"
        self.support_files[name] = compliance.SupportFile(name=name, 
                                                                filepath=path)
        
    def evaluate_(self, *args, **kwargs):
        """
        Method to evaluate first party patch compliance    
        """
        
        status = compliance.ComplianceStatus.UNKNOWN
        self.load_settings()
        patch_count = 0
        
        logger = logging.getLogger(self.logger_name)

        if not os.path.exists(self.firstparty_tool_path):
            logger.warning("First Party Patches tool not found at {}.".format(self.firstparty_tool_path))
            status = compliance.ComplianceStatus.ERROR
        else:
            cmd = [self.firstparty_tool_path, "--listUpdates"]
            try:
                output = subprocess.check_output(cmd)
                if not os.path.exists(self.patches_pref_path):
                    logger.error("First Party Patches preferences file was not found at {}.".format(self.patches_pref_path))
                    status = compliance.ComplianceStatus.ERROR
                else:
                    plist = plistlib.readPlist(self.patches_pref_path)
                    patch_list = plist["patchList"] 
                    try:
                        last_check = datetime.datetime.fromtimestamp(plist["lastSWUpdatecheck"])
                    except KeyError:
                        last_check = datetime.datetime.utcnow() - datetime.timedelta(days=2)
                        logger.error("Missing lastSWUpdatecheck key in First Party Patches preferences file at {}. Setting stale state.".format(self.patches_pref_path))
                        status = compliance.ComplianceStatus.ERROR
                    if abs(datetime.datetime.utcnow() - last_check) >= datetime.timedelta(days=1):
                        logger.warning("LastCheck key indicates First Party Patches preferences file is stale.")
                        status = compliance.ComplianceStatus.ERROR
                    else:
                        for patch in patch_list:                
                            if self.patch_eligible(patch_list[patch], self.firstparty_domain):
                                patch_count = patch_count + 1
                        if patch_count == 0:
                            status = compliance.ComplianceStatus.COMPLIANT
                        else:
                            status = compliance.ComplianceStatus.NONCOMPLIANT
            except Exception as exp:
                logger.error("Failed to run First Party Patches tool:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                status = compliance.ComplianceStatus.ERROR
            
        return compliance.EvaluationResult(compliance_status=status, execution_status=compliance.ExecutionStatus.SUCCESS)        
    
    def patch_eligible(self,patch,domain):
        """
        Method to evaluate patch eligibility    
        """
        
        patch_is_valid = False
        patch_name = patch["name"]
        patch_domain = "invalid-source-domain"
        patch_grace_period = 0
        patch_is_firmware = False
        
        logger = logging.getLogger(self.logger_name)

        try:
            patch_domain = patch["sourceDomain"]
        except KeyError:
            logger.error("Patch {}: No sourceDomain key.".format(patch_name))
        
        if patch_domain.find(domain) == -1:
            logger.debug("Skipping patch {} for reason: not desired domain {}.".format(patch_name, domain))
        else:
            logger.debug("Considering patch {} for reason: desired domain {}.".format(patch_name, domain))
            
            try:
                patch_grace_period = int(patch["gracePeriod"])
            except KeyError:
                logger.error("Patch {}: No gracePeriod key. Assuming gracePeriod==0 (non-mandatory update).".format(patch_name))
            if patch_grace_period == 0:
                logger.debug("Skipping patch {} for reason: grace period=0).".format(patch_name))
                return patch_is_valid
            
            try:
                patch_is_firmware = patch["isFirmware"]
            except KeyError:
                logger.error("Patch {}: No isFirmware key. Assuming isFirmware==False.".format(patch_name))
            if patch_is_firmware:
                logger.debug("Skipping patch {} for reason: firmware.".format(patch_name))
                return patch_is_valid
            
            logger.debug("Patch {} is eligble.".format(patch_name))
            patch_is_valid = True
            
        return patch_is_valid
