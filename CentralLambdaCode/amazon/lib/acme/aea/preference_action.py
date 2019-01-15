"""
 .. module:: acme.preference_action
     :synopsis: Module containing types of action we are taking on firefox preferences.
     :platform: OSX, Ubuntu

 .. moduleauthor:: Anuj Sharma <snuj@amazon.com>


 """

import acme.core
class FirefoxPreferenceAction(acme.core.Enum):
    """
    Represents action type for Firefox preferences.
    """
    ENABLE_SCOPE_PREFERENCES = 1
    REVERT_SCOPE_PREFERENCES = 2
    SETUP_CONFIG_FILE = 3
