"""
**pykarl** - Root namespace for connecting to KARL subsystems.
   
:platform: RHEL5
:synopsis: This is the root module that can be used for loading KARL 
        credentials and resource handles. Also includes classes which 
        facilitate data import into KARL
    
:Example:
    
        >>> import pykarl.core
        >>> 
        >>> ## Instantiate our KARL object
        >>> karl = pykarl.core.KARL()
        >>> 
        >>> ## Setup configuration settings for our "test" environment.
        >>> karl.setup(env="test")
        >>>
        >>> karl.rds_host
        'karl-test.czzziwhpg9wv.us-west-2.rds.amazonaws.com'
        >>>
        >>> ## Retrieve and use a KARL subsystem
        >>> rs = karl.redshift()
        >>> rs.query("SELECT uuid,type FROM event LIMIT 1").dictresult()
        [{'uuid': 'a51fa661-b3f2-11e4-b7ae-3c15c2de0480', 'type': 'TestEvent'}]
        >>>
        >>> rds = karl.rds()
        >>> rds.query("SELECT uuid,type FROM event LIMIT 1").dictresult()
        [{'type': 'SpamAgent', 'uuid': '5bfe107d-25eb-46e5-9ae9-69145af42c42'}]
        >>>

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>


"""

from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)
