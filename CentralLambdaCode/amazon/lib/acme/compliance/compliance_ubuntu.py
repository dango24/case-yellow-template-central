"""
**acme.compliance.compliance_Ubuntu** - Shim which is responsible for handling compliance functions for Ubuntu.

:platform: Ubuntu
:synopsis: Package which provides various facilities for handling Quarantine compliance.

.. codeauthor:: Jason Simmons <jasosimm@amazon.com>

"""

#MARK: Imports
import os
import logging
import subprocess
import re
from . import ComplianceModule, ComplianceStatus

class ComplianceUbuntu(ComplianceModule):
    """
    Class which provides compliance evaluation functionality for Ubuntu


    """


    # Map Ubuntu Quarantine status values to Quarantine states
    compliance_status_map = {0 : ComplianceStatus.ERROR,
                            1 : ComplianceStatus.COMPLIANT,
                            2 : (ComplianceStatus.INGRACETIME | ComplianceStatus.NONCOMPLIANT),
                            4 : (ComplianceStatus.ISOLATIONCANDIDATE | ComplianceStatus.NONCOMPLIANT),
                            6 : (ComplianceStatus.ISOLATED | ComplianceStatus.NONCOMPLIANT),
                            }

    quarantine_lib = "/opt/amazon/q/lib/common_quarantine.pl"
    quarantine_status_dir = "/var/compliance/state/module"

    logger_name = "ComplianceUbuntu"


    def get_modules(self):
        """
        Method that will get active quarantine compliance modules in Ubuntu Quarantine.

        :returns: list of active Ubuntu Quarantine modules
        """

        logger = logging.getLogger(self.logger_name)

        unmapped_module_list = []
        active_module_list = []

        if os.path.exists(self.quarantine_lib):
            try:
                with open(self.quarantine_lib, 'r') as f:
                    for line in f:
                        if "@compliance_modules =" in line:
                            match = re.search(r'\(.*\)', line).group()
                            match = match.replace('(','[').replace(')',']')
                            unmapped_module_list = eval(match) # convert match from literal string list to list
                            for module in unmapped_module_list:
                                active_module_list.append(ComplianceModule.quarantine_to_aea_maps[module])

            except Exception as exp:
                logger.error("Failed to lookup modules:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)

        return active_module_list

    def get_module_status(self,module):
        """
        Method that will get compliance module status in Ubuntu Quarantine.

        :param module: Compliance module to query compliance status.
        :type module: string
        :returns: Integer bitwise mask representing compliance state.
        """

        module = ComplianceModule.aea_to_quarantine_maps[module]

        logger = logging.getLogger(self.logger_name)

        status = None
        mapped_status = None
        module_exit_code = None

        module_state_path = os.path.join(self.quarantine_status_dir, module)

        if os.path.exists(module_state_path):
            try:
                with open(module_state_path, "r") as fh:
                    status = int(fh.read().rstrip())
                mapped_status = self.map_module_status(status)

            except Exception as exp:
                logger.error("Failed to lookup module compliance:{}".format(exp))
                logger.log(5,"Failure stack trace (handled cleanly):",exc_info=1)
                mapped_status = ComplianceStatus.ERROR

        return mapped_status

