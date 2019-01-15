import subprocess
import os
import logging
import datetime

import acme.compliance as compliance

__version__ = "1.0"

class ManagementMacOSComplianceModule(compliance.BaseModule):
    """
    Compliance Module for macOS which will evaluate management compliance
    """
    
    logger_name = "ManagementMacOS"
    auto_remediate = False
    can_remediate = False
    management_tool_path = ["/usr/local/jamf/bin/jamf", "/Library/Tanium/TaniumClient/TaniumClient"]
    
    def __init__(self,key_map=None,settings_keys=None,*args,**kwargs):
                
        self.identifier = "management"
        self.name = "ManagementMacOS"
        self.evaluation_interval = datetime.timedelta(hours=1)
        self.evaluation_skew = datetime.timedelta(minutes=15)
        self.triggers = compliance.ExecutionTrigger.SCHEDULED
        
        if key_map is None:
            key_map = {}
            key_map.update(compliance.BaseModule.key_map)
            key_map["management_tool_path"] = None
        
        if settings_keys is None:
            settings_keys = compliance.BaseModule.settings_keys[:]
            settings_keys.append("management_tool_path")
        
        super(ManagementMacOSComplianceModule, self).__init__(name=self.name,
                                                 identifier=self.identifier,
                                                 key_map=key_map,
                                                 settings_keys=settings_keys,
                                                 *args, **kwargs)
    
    
    def evaluate_(self, *args, **kwargs):
        """
        Method to evaluate management compliance    
        """
        
        status = compliance.ComplianceStatus.UNKNOWN
        self.load_settings()
        tool_compliant_list = []
        
        logger = logging.getLogger(self.logger_name)
        
        for tool in self.management_tool_path:
            tool_is_compliant = False
            if not os.path.exists(tool):
                logger.warning("Management tool, {}, not found at {}.".format(os.path.basename(tool), tool))
                status = compliance.ComplianceStatus.ERROR
            else:
                cmd = ["/usr/bin/pgrep", "-ix", os.path.basename(tool)]
                try:
                    output = subprocess.check_output(cmd)
                    if output is not None:
                        tool_is_compliant = True
                except subprocess.CalledProcessError as exp:
                    if exp.returncode == 1:
                        logger.warning("Management tool, {}, is not running.".format(os.path.basename(tool)))
                        status = compliance.ComplianceStatus.NONCOMPLIANT
                    else:
                        logger.error("Failed to verify Management tool, {}, status:{}".format(os.path.basename(tool), exp))
                        logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                        status = compliance.ComplianceStatus.ERROR
            tool_compliant_list.append(tool_is_compliant)
                
        if not False in tool_compliant_list:
            status = compliance.ComplianceStatus.COMPLIANT
        
        return compliance.EvaluationResult(compliance_status=status, execution_status=compliance.ExecutionStatus.SUCCESS)
