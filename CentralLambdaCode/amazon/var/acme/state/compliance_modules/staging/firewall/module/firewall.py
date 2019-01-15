import datetime
import logging
import systemprofile

import acme.compliance as compliance

__version__ = "1.2"

class FirewallMacOSComplianceModule(compliance.BaseModule):
    """
    Compliance Module for macOS which will evaluate firewall compliance
    """
    
    logger_name = "FirewallMacOS"
    auto_remediate = False
    can_remediate = False
    
    def __init__(self,key_map=None,settings_keys=None,*args,**kwargs):
                
        self.identifier = "firewall"
        self.name = "FirewallMacOS"
        self.evaluation_interval = datetime.timedelta(hours=1)
        self.evaluation_skew = datetime.timedelta(minutes=15)
        self.triggers = compliance.ExecutionTrigger.SCHEDULED
        
        if key_map is None:
            key_map = {}
            key_map.update(compliance.BaseModule.key_map)
        
        if settings_keys is None:
            settings_keys = compliance.BaseModule.settings_keys[:]
        
        super(FirewallMacOSComplianceModule, self).__init__(name=self.name,
                                                 identifier=self.identifier,
                                                 key_map=key_map,
                                                 settings_keys=settings_keys,
                                                 *args, **kwargs)

    def evaluate_(self, *args, **kwargs):
        """
        Method to evaluate firewall compliance
        """
        logger = logging.getLogger(self.logger_name)
        self.load_settings()

        status = compliance.ComplianceStatus.UNKNOWN

        output, returnCode = systemprofile.profiler.firewall_status()

        if returnCode == 0:
            if output.find('Enabled') != -1:
                status = compliance.ComplianceStatus.COMPLIANT
            else:
                status = compliance.ComplianceStatus.NONCOMPLIANT
        else:
            logger.error("Cannot get the firewall status")
            status = compliance.ComplianceStatus.ERROR

        return compliance.EvaluationResult(compliance_status=status,
                                           execution_status=compliance.ExecutionStatus.SUCCESS)
