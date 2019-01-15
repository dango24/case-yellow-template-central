import subprocess
import os
import logging
import datetime

import acme.compliance as compliance

__version__ = "1.0"

class CylanceMacOSComplianceModule(compliance.BaseModule):
    """
    Compliance Module for macOS which will evaluate Cylance compliance
    """
    
    logger_name = "CylanceMacOS"
    auto_remediate = False
    can_remediate = False
    cylance_tool_path = "/Library/Application Support/Cylance/Desktop/CylanceSvc.app/Contents/MacOS/CylanceSvc"
    
    def __init__(self,key_map=None,settings_keys=None,*args,**kwargs):
                
        self.identifier = "cylance"
        self.name = "CylanceMacOS"
        self.evaluation_interval = datetime.timedelta(hours=1)
        self.evaluation_skew = datetime.timedelta(minutes=15)
        self.triggers = compliance.ExecutionTrigger.SCHEDULED
        
        if key_map is None:
            key_map = {}
            key_map.update(compliance.BaseModule.key_map)
            key_map["cylance_tool_path"] = None
        
        if settings_keys is None:
            settings_keys = compliance.BaseModule.settings_keys[:]
            settings_keys.append("cylance_tool_path")
        
        super(CylanceMacOSComplianceModule, self).__init__(name=self.name,
                                                            identifier=self.identifier,
                                                            key_map=key_map,
                                                            settings_keys=settings_keys,
                                                            *args, **kwargs)
    
    
    def evaluate_(self, *args, **kwargs):
        """
        Method to evaluate Cylance compliance    
        """
        
        status = compliance.ComplianceStatus.UNKNOWN
        self.load_settings()
        
        logger = logging.getLogger(self.logger_name)
        
        if not os.path.exists(self.cylance_tool_path):
            logger.warning("Cylance tool not found at {}.".format(self.cylance_tool_path))
            status = compliance.ComplianceStatus.ERROR
        else:
            cmd = ["/usr/bin/pgrep", "-ix", os.path.basename(self.cylance_tool_path)]
            try:
                output = subprocess.check_output(cmd)
                if output is not None:
                    status = compliance.ComplianceStatus.COMPLIANT
            except subprocess.CalledProcessError as exp:
                if exp.returncode == 1:
                    logger.warning("Cylance tool is not running.")
                    status = compliance.ComplianceStatus.NONCOMPLIANT
                else:
                    logger.error("Failed to verify Cylance tool status:{}".format(exp))
                    logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                    status = compliance.ComplianceStatus.ERROR
                    
        return compliance.EvaluationResult(compliance_status=status, execution_status=compliance.ExecutionStatus.SUCCESS)
