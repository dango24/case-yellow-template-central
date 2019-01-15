"""
.. module:: acme.quarantine
    :synopsis: Module containing classes used for ACME device sending quarantine requests.

    :platform: RHEL, OSX, Ubuntu

.. moduleauthor:: Xin Du <duxin@amazon.com>

"""

import acme
import acme.daemon
import acme.requests as re
import json
import logging
import requests

class QuarantineControllerModule(acme.SerializedObject):
    '''
    Class that performes quarantine requests sending and response receiving.
    '''
    logger_name = "quarantine_controller"
    
    def __init__(self):
        acme.SerializedObject.__init__(self)
        
    def fetch_qc_data(self,options,identity):
        """
        Method to send quarantine requests and get response data from quarantine server.
        """
        logger = logging.getLogger(self.logger_name)
        response = None
        result = {}
        try:
            options["data"]['uuid'] = identity.common_name
            data=json.dumps(options["data"],indent=4)
            logger.info("Quarantine action is {}.".format(options["data"]["action"]))
            logger.info("Sending quarantine request to quarantine server: {}.".format(options["server_uri"]))
            with re.RequestsContextManager(identity) as cm:
                response=requests.post(options["server_uri"],cert=cm.temp_file.name,data=data)
            if response is not None:
                result['content'] = response._content
                result["status_code"] = response.status_code
                if result["status_code"] == 200:
                    logger.info("Quarantine response status was GRANTED {}.".format(result["status_code"]))
                elif result["status_code"] == 429:
                    logger.info("Quarantine response status was DENIED {}.".format(result["status_code"]))
                else:
                    logger.info("Quarantine response status was {}.".format(result["status_code"]))
            else:
                raise Exception("Error: Response from quarantine controller server is None.")
        except Exception as exp:
            logger.error("Fail to get response from quarantine server: {}".format(exp.message))
        return result
        
QuarantineController = QuarantineControllerModule

def _quarantine_macos():
    """
    Method to configure our quarantine for use with macOS
    """
    import quarantine_macos
    global QuarantineController
    QuarantineController = quarantine_macos.QuarantineControllerMacOS

def _quarantine_ubuntu():
    """
    Method to configure our quarantine for use with Ubuntu
    """
    import quarantine_ubuntu
    global QuarantineController
    QuarantineController = quarantine_ubuntu.QuarantineControllerUbuntu
    
if acme.platform == "OS X" or acme.platform == "macOS":
    _quarantine_macos()
elif acme.platform == "Ubuntu":
    _quarantine_ubuntu()