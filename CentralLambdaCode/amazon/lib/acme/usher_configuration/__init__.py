from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)
#

from acme.configuration import *

class UsherConfigurationController(acme.configuration.ConfigurationController):
    pass
    