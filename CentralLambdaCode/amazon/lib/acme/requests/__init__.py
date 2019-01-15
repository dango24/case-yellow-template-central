import os
import requests
import requests.packages.urllib3.util
from requests.packages.urllib3.util.ssl_ import HAS_SNI as HAS_SNI
import requests.packages.urllib3.connection
from requests.packages.urllib3.exceptions import SSLError 
import warnings
from requests.packages.urllib3.util.ssl_ import create_urllib3_context
import logging
import tempfile
import OpenSSL.crypto as crypto
import acme
import acme.crypto
import threading

logger = logging.getLogger("RequestsContextManager")

CM_LOCK = threading.RLock()

class RequestsContextManager(object):
    """
    For requests custom package, when a server needs a client to verify
    themselves, they need to provide an unencrypted private key as a parameter
    in the http requests.
    This context manager that patches requests package to encrypt private key of a
    client cert if it is provided.

    Note: The patch requires python >= 2.7.9 because various function of
    SSLContext are used under the method ssl_wrap_socket, which are only
    available starting from 2.7.9
    """
    
    @classmethod
    def patch_requests(cls):
        """
        Method which will patch our requests object to support certificate
        authentication.
        """
        
        with CM_LOCK:
            if (not hasattr(requests.packages.urllib3.util, "key_passphrase") 
                    or requests.packages.urllib3.util.key_passphrase is None):
                requests.packages.urllib3.util.key_passphrase = {}
            
            if not hasattr(requests.packages.urllib3.util, "_native_ssl_wrap_socket"):
                requests.packages.urllib3.util._native_ssl_wrap_socket  = requests.packages.urllib3.util.ssl_wrap_socket
            requests.packages.urllib3.util.ssl_wrap_socket = ssl_wrap_socket
                
            if not hasattr(requests.packages.urllib3.connection, "_native_ssl_wrap_socket"):
                requests.packages.urllib3.connection._native_ssl_wrap_socket  = requests.packages.urllib3.connection.ssl_wrap_socket
            requests.packages.urllib3.connection.ssl_wrap_socket = ssl_wrap_socket
    
    @classmethod
    def unpatch_requests(cls):
        """
        Method which will restore our requests module to it's native configuration
        """
        
        with CM_LOCK:
            if requests.packages.urllib3.util.key_passphrase is None:
                requests.packages.urllib3.util.key_passphrase = {}
        
            if hasattr(requests.packages.urllib3.util, "_native_ssl_wrap_socket"):
                requests.packages.urllib3.util.ssl_wrap_socket = requests.packages.urllib3.util._native_ssl_wrap_socket
        
            if hasattr(requests.packages.urllib3.connection, "_native_ssl_wrap_socket"):
                requests.packages.urllib3.connection.ssl_wrap_socket  = requests.packages.urllib3.connection._native_ssl_wrap_socket    
    
    def __init__(self,identity):
        """
        Method to initialize needed variables for patching
        """
        
        self.identity = identity
        self.temp_file = None
        
    def __enter__(self):
        """
        Method to patch the original ssl_wrap_socket method with custom
        ssl_wrap_socket method
        """
        
        with CM_LOCK:
            self.patch_requests()
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                passphrase = acme.crypto.generate_password(length=64)
                requests.packages.urllib3.util.key_passphrase[temp.name] = passphrase
                temp.write(crypto.dump_certificate(crypto.FILETYPE_PEM,
                                                self.identity.certificate))
                temp.write(crypto.dump_privatekey(crypto.FILETYPE_PEM,  
                                                self.identity.private_key,
                                                "aes-256-cbc",
                                                passphrase))
                self.temp_file = temp
        
        return self
    
    def __exit__(self,type,value,traceback):
        """
        Method to restore the values in requests packages
        """
        with CM_LOCK:
            if self.temp_file:
                try:
                    del(requests.packages.urllib3.util.key_passphrase[self.temp_file.name])
                except (NameError, KeyError):
                    pass
                
                try:
                    os.remove(self.temp_file.name)
                except OSError as e:
                    logger.error("Failed to remove temp file {} with error {}".format(self.temp_file.name,e))
        return

def ssl_wrap_socket(sock, keyfile=None, certfile=None, cert_reqs=None,
                    ca_certs=None, server_hostname=None,
                    ssl_version=None, ciphers=None, ssl_context=None,
                    ca_cert_dir=None, *args, **kwargs):
    """
    All arguments except for server_hostname, ssl_context, and ca_cert_dir have
    the same meaning as they do when using :func:`ssl.wrap_socket`.

    :param server_hostname:
        When SNI is supported, the expected hostname of the certificate
    :param ssl_context:
        A pre-made :class:`SSLContext` object. If none is provided, one will
        be created using :func:`create_urllib3_context`.
    :param ciphers:
        A string of ciphers we wish the client to support. This is not
        supported on Python 2.6 as the ssl module does not support it.
    :param ca_cert_dir:
        A directory containing CA certificates in multiple separate files, as
        supported by OpenSSL's -CApath flag or the capath argument to
        SSLContext.load_verify_locations().
    """
    context = ssl_context
    if context is None:
        # Note: This branch of code and all the variables in it are no longer
        # used by urllib3 itself. We should consider deprecating and removing
        # this code.
        context = create_urllib3_context(ssl_version, cert_reqs,
                                         ciphers=ciphers)

    if ca_certs or ca_cert_dir:
        try:
            context.load_verify_locations(ca_certs, ca_cert_dir)
        except IOError as e:  # Platform-specific: Python 2.6, 2.7, 3.2
            raise SSLError(e)
        # Py33 raises FileNotFoundError which subclasses OSError
        # These are not equivalent unless we check the errno attribute
        except OSError as e:  # Platform-specific: Python 3.3 and beyond
            if e.errno == errno.ENOENT:
                raise SSLError(e)
            raise
    elif getattr(context, 'load_default_certs', None) is not None:
        # try to load OS default certs; works well on Windows (require Python3.4+)
        context.load_default_certs()

    if certfile:
        try:
            context.load_cert_chain(certfile, keyfile,
                    requests.packages.urllib3.util.key_passphrase[certfile])
        except (NameError, KeyError):
            ## Here if we were unable to locate the passphrase
            context.load_cert_chain(certfile, keyfile)
        
    if HAS_SNI:  # Platform-specific: OpenSSL with enabled SNI
        return context.wrap_socket(sock, server_hostname=server_hostname)

    warnings.warn(
        'An HTTPS request has been made, but the SNI (Subject Name '
        'Indication) extension to TLS is not available on this platform. '
        'This may cause the server to present an incorrect TLS '
        'certificate, which can cause validation failures. You can upgrade to '
        'a newer version of Python to solve this. For more information, see '
        'https://urllib3.readthedocs.io/en/latest/advanced-usage.html'
        '#ssl-warnings',
        SNIMissingWarning
    )
    return context.wrap_socket(sock)