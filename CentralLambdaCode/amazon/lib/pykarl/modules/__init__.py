"""
**modules** - Package providing facilities for extending KARL processig capabilites.
   
:platform: RHEL5
:synopsis: Provides a collection of modules and subpackages which provide
            a variety of stream processing capabilities. Primary classes 
            which facilitate KARL event processing, KARL data 
            modeling, and data publishing can be found in :py:mod:`base`. 
            All pykarl modules will be structured off of root classes provided 
            by the :py:mod:`base` module.

.. codeauthor:: Beau Hunter <beauhunt@amazon.com>


"""


from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)