import subprocess
import os
import datetime
import plistlib
import logging
import sys
import systemprofile
import acme.compliance as compliance

__version__ = "1.2"

class CryptoMacOSComplianceModule(compliance.BaseModule):
    """
    Compliance Module for macOS which will evaluate encryption compliance
    """
    
    logger_name = "CryptoMacOS"
    auto_remediate = False
    can_remediate = False
    
    def __init__(self,key_map=None,settings_keys=None,*args,**kwargs):
                
        self.identifier = "crypto"
        self.name = "CryptoMacOS"
        self.evaluation_interval = datetime.timedelta(hours=1)
        self.evaluation_skew = datetime.timedelta(minutes=15)
        self.triggers = compliance.ExecutionTrigger.SCHEDULED
        
        if key_map is None:
            key_map = {}
            key_map.update(compliance.BaseModule.key_map)
        
        if settings_keys is None:
            settings_keys = compliance.BaseModule.settings_keys[:]
        
        super(CryptoMacOSComplianceModule, self).__init__(name=self.name,
                                                            identifier=self.identifier,
                                                            key_map=key_map,
                                                            settings_keys=settings_keys,
                                                            *args, **kwargs)
    
    
    def evaluate_(self, *args, **kwargs):
        """
        Method to evaluate encryption compliance    
        """
        
        status = compliance.ComplianceStatus.UNKNOWN
        self.load_settings()
        
        logger = logging.getLogger(self.logger_name)
        profiler = systemprofile.profiler
        
        try:
            encryption_required, encryption_enabled, boot_volume_conversion_status = profiler.get_file_vault_status()
            # Compliant if exempt:
            if not encryption_required:
                status = compliance.ComplianceStatus.COMPLIANT
            # Compliant if required, enabled, and finished encrypting:
            if (encryption_required and encryption_enabled and boot_volume_conversion_status.lower() == "complete"):
                status = compliance.ComplianceStatus.COMPLIANT
            # Compliant if required, enabled, and currently encrypting:
            if (encryption_required and encryption_enabled and boot_volume_conversion_status.lower() == "converting"):
                status = compliance.ComplianceStatus.COMPLIANT
            # NonCompliant if required, disabled:
            if (encryption_required and not encryption_enabled):
                status = compliance.ComplianceStatus.NONCOMPLIANT
        except Exception as exp:
            logger.error("Failed to run Crypto tool:{}".format(exp))
            logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
            status = compliance.ComplianceStatus.ERROR
        return compliance.EvaluationResult(compliance_status=status, execution_status=compliance.ExecutionStatus.SUCCESS)
