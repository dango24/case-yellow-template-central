"""
**acme.compliance.compliance_macos** - Shim which is responsible for handling compliance functions for macOS.

:platform: macOS
:synopsis: Package which provides various facilities for handling Quarantine compliance.

.. codeauthor:: Jason Simmons <jasosimm@amazon.com>

"""

#MARK: Imports
import os
import plistlib
import logging
import subprocess
from . import ComplianceModule, ComplianceStatus

class ComplianceMacOS(ComplianceModule):
    """
    Class which provides compliance evaluation functionality for macOS


    """

    # Map Mac Quarantine status values to Quarantine states
    compliance_status_map = {-1 : (ComplianceStatus.ISOLATED | ComplianceStatus.NONCOMPLIANT),
                            0 : (ComplianceStatus.ISOLATIONCANDIDATE | ComplianceStatus.NONCOMPLIANT),
                            1 : ComplianceStatus.NONCOMPLIANT,
                            3 : (ComplianceStatus.INGRACETIME | ComplianceStatus.NONCOMPLIANT),
                            7 : ComplianceStatus.COMPLIANT,
                            }

    compliance_modules_plist = "/usr/local/amazon/var/quarantine/modules.plist"
    quarantine_status_plist = "/Library/Preferences/com.amazon.acme.quarantine.plist"

    logger_name = "ComplianceMacOS"


    def get_modules(self):
        """
        Method that will get active quarantine compliance modules in Mac Quarantine.

        :returns: list of active Mac Quarantine modules
        """

        logger = logging.getLogger(self.logger_name)

        installed_module_list = []
        active_module_list = []

        plist_path = self.compliance_modules_plist

        if os.path.exists(plist_path):
            try:
                cmd = ["/usr/bin/plutil","-convert","xml1","-o","-",plist_path]
                plist_data = subprocess.check_output(cmd)
                plist = plistlib.readPlistFromString(plist_data)
                installed_module_list = plist["modules"]
                for module in installed_module_list:
                    if plist[module]["isEnabled"]:
                        active_module_list.append(ComplianceModule.quarantine_to_aea_maps[module])

            except Exception as exp:
                logger.error("Failed to lookup modules:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        return active_module_list

    def get_module_status(self,module):
        """
        Method that will get compliance module status in Mac Quarantine.

        :param module: Compliance module to query compliance status.
        :type module: string
        :returns: Integer bitwise mask representing compliance state.
        """

        module = ComplianceModule.aea_to_quarantine_maps[module]

        logger = logging.getLogger(self.logger_name)

        status = None
        mapped_status = None
        module_exit_code = None

        plist_path = self.quarantine_status_plist

        if os.path.exists(plist_path):
            try:
                cmd = ["/usr/bin/plutil","-convert","xml1","-o","-",plist_path]
                plist_data = subprocess.check_output(cmd)
                plist = plistlib.readPlistFromString(plist_data)
                status = plist[module]["moduleState"]
                mapped_status = self.map_module_status(status)
                module_exit_code = plist[module]["moduleExitCode"]
                if module_exit_code != 0:
                    mapped_status = mapped_status | ComplianceStatus.ERROR

            except Exception as exp:
                logger.error("Failed to lookup module compliance:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                mapped_status = ComplianceStatus.ERROR

        return mapped_status
