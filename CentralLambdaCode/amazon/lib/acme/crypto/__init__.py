"""
.. module:: acme.crypto
    :synopsis: Module containing classes used by the ACME Agent system for
            cryptographic purposes. This includes identity management,
            encrypting/decrypting of data and random password generation.

    :platform: RHEL, OSX, Ubuntu

.. moduleauthor:: Beau Hunter <beauhunt@amazon.com>


"""

#MARK: Imports
import base64
import copy
import collections
import datetime
import hashlib
import math
import logging
import os
import random
import re
import sys
import time
import jwt
import OpenSSL
from OpenSSL import crypto

import acme
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

import systemprofile
import json

#MARK: - Module Variables
karl_public_key_path = os.path.join(acme.BASE_DIR,"public-key.pem")

X509_DATE_FORMAT = "%Y%m%d%H%M%SZ"

class Type(acme.core.Enum):
    OBJECT = 0
    PEM = crypto.FILETYPE_PEM
    ASN1 = crypto.FILETYPE_ASN1

class IdentityBase(object):
    """
    Class which provides identity loading and saving functionality.

    :param common_name: The common name of our identity. This value is used
                    for retrieval and generation functions.

    """

    logger_name = "Identity"

    identity_directory = "/usr/local/amazon/var/acme/identity"
    
    default_certificate_template = collections.OrderedDict((
                                    ("organizationalUnitName","Amazon"),
                                    ("organizationName", "Amazon"),
                                    ("stateOrProvinceName", "WA"),
                                    ("countryName", "US")
                        ))
    
    default_extensions  = {
                        "basicConstraints" : { 
                                                "critical": False,
                                                "value": "CA:FALSE"
                                            },
                        "keyUsage": {
                                        "critical": False,
                                        "value": ", ".join([
                                                        "nonRepudiation", 
                                                        "digitalSignature", 
                                                        "keyEncipherment"
                                                        ])
                                            },
                        "extendedKeyUsage" : {
                                                "critical": False,
                                                "value": "clientAuth"
                                                }
                        }
    
    ttl = datetime.timedelta(days=365)  #: Default lifespan of created certificates, csrs
                                                                
    key_length = 2048     #: (int) Our key length, in bytes 
    key_hash_algorithm = "sha256"  #: (str) hashing algorithm i.e. 'sha256', 'sha512'
        
    def __init__(self, common_name=None, certificate_template=None,
                                                    extensions=None, 
                                                    ttl=None,
                                                    key_length=None,
                                                    key_hash_algorithm=None):
        
        self.common_name = common_name
        self.certificate = None
        self.private_key = None
        
        if certificate_template is None and self.default_certificate_template:
            self.certificate_template = copy.deepcopy(self.default_certificate_template)
        elif certificate_template:
            self.certificate_template = certificate_template
        else:
            self.certificate_template = collections.OrderedDict()
        
        if extensions is None and self.default_extensions:
            self.extensions = copy.deepcopy(self.default_extensions)
        elif extensions:
            self.extensions = extensions
        else:
            self.extensions = {}
        
        if ttl is not None:
            self.ttl = ttl
        
        if key_length is not None:
            self.key_length = key_length
    
    def load(self, filepath=None, passphrase=None):
        """
        Method which will fetch the provided entity based on the provided
        common_name and load the data into 'self.certificate' and
        'self.private_key'

        :param str filepath: The filepath to the PKCS12 file to load. If
                    ommitted we will use a filepath based on our configured
                    'identity_directory' and the common name.
        :param str passphrase: The password to use to unpack the PKCS12 file.
                    If ommitted we will attempt to retrieve from the Keychain.

        :raises IdentityNotFoundError: If the certificate cannot be found.

        :returns (:py:class:`RSA`, :py:class:`RSA`): Tuple containing Public and Private key

        """

        logger = logging.getLogger(self.logger_name)

        if filepath is None:
            if self.common_name is not None:
                filepath = os.path.join(self.identity_directory,
                                            "{}.p12".format(self.common_name))
            else:
                raise IdentityNotFoundError("Could not load identity: no common name or filepath specified!")

        if not os.path.exists(filepath):
            raise IdentityNotFoundError("Could not load identity from path:'{}'".format(
                                                                filepath))

        with open(filepath, "r") as fh:
            logger.log(5, "Loading material from path:'{}'".format(filepath))
            data = fh.read()

        if passphrase is None:
            passphrase = self.get_passphrase()

        self.load_pkcs12(data=data,passphrase=passphrase)

    def save(self, filepath=None, passphrase=None):
        """
        Method which will save our identity in PKCS12 format to the specified
        filepath.

        :param str filepath: The filepath to the PKCS12 file to load. If
                    ommitted we will use a filepath based on our configured
                    'identity_directory' and the common name.
        :param str passphrase: The password to use to unpack the PKCS12 file.
                    If ommitted we will attempt to retrieve from the Keychain.

        """

        logger = logging.getLogger(self.logger_name)

        if filepath is None:
            if self.common_name is not None:

                if not os.path.exists(self.identity_directory):
                    logger.log(5, "Creating directory:'{}'".format(
                                                    self.identity_directory))
                    os.mkdir(self.identity_directory)

                filepath = os.path.join(self.identity_directory,
                                            "{}.p12".format(self.common_name))
            else:
                raise IdentityError("Could save identity: no common name or filepath specified!")

        if passphrase is None:
            try:
                passphrase = self.get_passphrase()
            except PassphraseError:
                passphrase = self.create_passphrase()

        ## Get our PKCS12 object
        p12 = self.to_pkcs12()

        data = p12.export(passphrase=passphrase)

        with os.fdopen(os.open(filepath,os.O_RDWR|os.O_CREAT,0600), "w") as fh:
            fh.write(data)

        logger.log(5, "Saved PKCS12 file to path:'{}'".format(filepath))

    def generate(self):
        """
        Method which will generate a new private key and self-signed
        certificate

        :raises IdentityError: If identity could not be created.

        """

        logger = logging.getLogger(self.logger_name)

        if self.common_name is None:
            raise IdentityError("Could not generate identity, no common name is defined!")

        logger.log(5, "Generating identity using common name:{}".format(
                                                            self.common_name))

        ## Create our private key
        key = None
        try:
            key = crypto.PKey()
            key.generate_key(crypto.TYPE_RSA, self.key_length)
            self.private_key = key
        except Exception as exp:
            raise IdentityError("Failed to create private key:{}".format(exp.message)), None, sys.exc_info()[2]
        
        ## Generate and self-sign a CSR
        csr = self.create_csr()

        ## Self sign our CSR
        cert = self.sign_csr(csr, add_v3_ski=True)
        
        ## Import our cert
        self.process_csr(cert=cert)
    
    def create_csr(self, output=Type.OBJECT, extensions=None,
                                            key_hash_algorithm=None):
        """
        Method which will create a CSR based on loaded identity details.
        
        :param int output: Specify the desired output. Type.PEM will output a 
                            base64 encoded string, Type.OBJECT will output a
                            :py:class:`crypto.X509Req` object.                  
        :param extensions: A list of :py:class:`crypto.X509Extension` objects (optional)
        :type extensions: list<:py:func:`crypto.X509Extension`>
        :param str key_hash_algorithm: The hashing algorithm to use (optional)
        
        :raises IdentityNotFoundError: If existing identity could not be found.

        :returns: CSR data in PEM format or :py:class:`crypto.X509Req` object.

        """

        logger = logging.getLogger(self.logger_name)

        key = self.private_key

        if not key:
            raise IdentityNotFoundError("Cannot create CSR: private key material is not available!")
        
        if extensions is None:
            extensions = self.extensions
            
        if key_hash_algorithm is None:
            key_hash_algorithm = self.key_hash_algorithm
        
        req = None
        result = None

        try:
            req = crypto.X509Req()

            req_subject = req.get_subject()

            req_subject.commonName = self.common_name

            for index,value in self.certificate_template.iteritems():
                try:
                    setattr(req_subject,index,value)
                except AttributeError:
                    logger.warning("Failed to set certificate subject attribute:{}, attribute is invalid!".format(index))
            
            if extensions:
                req.add_extensions(self.build_x509_extensions(extensions))
              
            ## todo: figure out why this breaks certs on macOS
            ##req.set_version(3)
            req.set_pubkey(key)
            
            req.sign(key, key_hash_algorithm)
            
            if output == Type.PEM or output == Type.ASN1:
                result = crypto.dump_certificate_request(output, req)
            else:
                result = req
        except Exception as exp:
            raise IdentityError("Failed to generate CSR:{}".format(exp.message)),None, sys.exc_info()[2]
        
        return result

    def sign_data(self, data, hash_algorithm=None, b64encode=True):
        """
        Method which will sign the provided using the private key of
        our identity.

        :param str data: Our data to sign.
        :param str hash_algorithm: Our hashing algorithm to use (i.e. "sha256")
        :param bool b64encode: Whether or not to Base64 encode the signature (default: true)

        :returns: String signature.

        """

        if hash_algorithm is None:
            hash_algorithm = self.key_hash_algorithm

        if not self.private_key:
            raise IdentityError("Could not sign data: no private key is loaded!")

        sig = crypto.sign(self.private_key, data, hash_algorithm)

        if not b64encode:
            return sig
        else:
            return base64.b64encode(sig)

    def get_jwt(self, data, algorithm = 'RS256', b64encode=True):
        """
        Get JWT for the given data with the private key of the loaded identity.
        """
        if not self.private_key:
            raise IdentityError("Could not sign data: no private key is loaded!")
        try:
            ####jwt encode doesn't accept PKey objects. Dumping them as pem private key
            pk_pem = OpenSSL.crypto.dump_privatekey(crypto.FILETYPE_PEM,self.private_key)
            jwt_data = jwt.encode(data, pk_pem, algorithm=algorithm)
        except Exception as exp:
            raise IdentityError("Encoding to jwt failed with error {}".format(exp.message))

        if not b64encode:
            return jwt_data
        else:
            return base64.b64encode(jwt_data)

    def sign_csr(self, csr, serial_number=None, common_name=None,
                                         uid=None,
                                         extensions=None,
                                         override=True,
                                         output=Type.OBJECT,
                                         key_hash_algorithm=None,
                                         ttl=None,
                                         add_v3_ski=None,
                                         add_v3_aki=None):
        """
        Method which will sign the provided CSR object and load the resulting
        cert into our object.

        :param csr: Our CSR object or PEM encoded string
        :type csr: :py:class:`crypto.X509Req` (or PEM style string)
         
        :param str output: Specify the desired output. Type.PEM will output a 
                            base64 encoded string, Type.OBJECT will output a
                            :py:class:`crypto.X509` object.
        :param extensions: A list of :py:class:`crypto.X509Extension` objects (optional)
        :type extensions: list<:py:func:`crypto.X509Extension`>
        :param str key_hash_algorithm: The hashing algorithm to use (optional)
        :param bool add_v3_extensions: If true, we will add subjectKeyIdentifier 
                            and authorityKeyIdentifier v3 X509 extensions and 
                            mark the cert as version 3

        :returns: Certificate in PEM format or :py:class:`crypto.X509` object.

        """
        
        logger = logging.getLogger(self.logger_name)
        
        if not serial_number:
            serial_number = 1
        
        if not self.private_key:
            raise IdentityError("Could not create CSR: no private key is loaded!")
        
        if extensions is None:
            extensions = copy.deepcopy(self.extensions)
        
        if key_hash_algorithm is None:
            key_hash_algorithm = self.key_hash_algorithm
        
        if isinstance(csr, basestring):
            csr = crypto.load_certificate_request(crypto.FILETYPE_PEM, csr)
        
        if ttl is None:
            ttl = self.ttl
        
        now = datetime.datetime.utcnow()
        not_before = now - datetime.timedelta(seconds=300)
        not_after = now + ttl
        
        cert = crypto.X509()
        cert.set_serial_number(serial_number)
                
        self_signed = None
        if self.certificate:
            cert.set_issuer(self.certificate.get_subject())
            self_signed = False
        else:
            cert.set_issuer(csr.get_subject())
            self_signed = True
            
        if self_signed:
            if extensions and "basicConstraints" in extensions:
                try:
                    bc_value = extensions["basicConstraints"]["value"]
                    bc_value = re.sub("CA:[^, ]{4,5}", "CA:TRUE", bc_value) 
                    extensions["basicConstraints"]["value"] = bc_value
                except KeyError:
                    extensions["basicConstraints"]["value"] = "CA:TRUE"
            elif not extensions:
                extensions = {}
                extensions["basicConstraints"] = { "value" : "CA:TRUE",
                                                    "critical": False
                                                }
        
        ## Build subject
        subject = csr.get_subject()
        if uid:
            subject.userId = uid
        
        if common_name:
            subject.commonName = common_name
        
        if override:
            for index,value in self.certificate_template.iteritems():
                    try:
                        setattr(subject,index,value)
                    except AttributeError:
                        logger.warning("Failed to set certificate subject attribute:{}, attribute is invalid!".format(index))
        
        cert.set_subject(subject)
        
        if extensions:
            logger.debug("Adding extensions: {}".format(extensions))
            cert.add_extensions(self.build_x509_extensions(extensions))
        
        cert.set_notBefore(date_to_x509_string(not_before))
        cert.set_notAfter(date_to_x509_string(not_after))
        
        if add_v3_ski:
            if self_signed:
                ski_subject = cert
            else:
                ski_subject = self.certificate
            
            cert.add_extensions([crypto.X509Extension("subjectKeyIdentifier",
                                            critical=False,
                                            value="hash",
                                            subject=ski_subject)])
        if add_v3_aki:
            if self_signed:
                aki_subject = cert
            else:
                aki_subject = self.certificate
            
            cert.add_extensions([crypto.X509Extension("authorityKeyIdentifier",
                                            critical=False,
                                            value="keyid:always",
                                            issuer=aki_subject)
                                        ])
            cert.set_version(2)
        
        cert.set_pubkey(csr.get_pubkey())        
        cert.sign(self.private_key, key_hash_algorithm)
        
        result = None
        if output == Type.PEM or output == Type.ASN1:
            result = crypto.dump_certificate(output, cert)
        else:
            result = cert

        return result

    def is_signed(self, issuer_cn=None):
        """
        Method which reports whether or not our stored certificate is signed
        by an external party.

        :param string issuer_cn: If provided, we will test to validate that
                    the issuer matches the provided CN

        .. warning:
                This is a baseline sanity check and should not considered a
                crytpographically sound evaluation.

        """

        is_signed = None

        if self.certificate:
            my_issuer_data = dict(self.certificate.get_issuer().get_components())
            my_issuer_cn = my_issuer_data["CN"]

            my_data = dict(self.certificate.get_subject().get_components())
            my_cn = my_data["CN"]

            if issuer_cn is not None:
                if issuer_cn == my_issuer_cn:
                    is_signed = True
                else:
                    is_signed = False
            elif my_issuer_cn != my_cn:
                is_signed = True
            else:
                is_signed = False

        return is_signed

    def process_csr(self, cert):
        """
        Method which will ingest the provided signed certificate data and
        import it into our certificate store.

        :param cert: Signed certificate
        :type cert: This parameter can either be a PEM encoded string, or a
                :py:class:`crypto.X509` object.

        :raises IdentityError: If certificate could not be processed/imported.

        """

        if isinstance(cert, basestring):
            try:
                cert = crypto.load_certificate(crypto.FILETYPE_PEM, cert)
            except Exception as exp:
                raise IdentityError("Failed to process certificate data: {}".format(exp)), None, sys.exc_info()[2]
        elif not isinstance(cert, crypto.X509):
            raise IdentityError("Certificate data should be PEM encoded or of type OpenSSL.crypto.X509")

        self.certificate = cert

    def to_pkcs12(self):
        """
        Method which will output our identity as a PKCS12 object.

        :returns: :py:class:`crypto.PKCS12` object

        """

        if not self.certificate:
            raise IdentityError("Could not create PKCS12 file: no certificate data loaded!")

        if not self.private_key:
            raise IdentityError("Could not create PKCS12 file: no private key is loaded!")

        p12 = crypto.PKCS12()
        p12.set_certificate(self.certificate)
        p12.set_privatekey(self.private_key)
        p12.set_friendlyname(self.common_name.encode("ascii"))

        return p12

    def load_pkcs12(self, data, passphrase=None):
        """
        Method which will load our object based on the provided pkcs12 data 
        in PEM format.
        
        Specifically, this method will populate ivars 'common_name', 'certificate'
        and 'private_key'

        :param str data: Our PKCS12 data in PEM format
        :param str passphrase: The passphrase used to protect our PKCS12 data. 
                    If ommitted we will attempt to load our passphrase from 
                    the Keychain.
        
        """

        if passphrase is None:
            passphrase = self.get_passphrase()

        p12 = crypto.load_pkcs12(data, passphrase)

        self.certificate = p12.get_certificate()
        
        subject_data = dict(self.certificate.get_subject().get_components())
        self.common_name = subject_data.get("CN")
        
        self.private_key = p12.get_privatekey()

    def get_passphrase(self):
        """
        Method used to retrieve our secret used for loading/saving certificate
        data. By default this passphrase is a combination of various
        static system elements. When possible, platform-specific
        implementations should facilitate more secure storage capabilities.
        """

        return self.create_passphrase()

    def create_passphrase(self):
        """
        Method that will generate a new passphrase.
        """

        components = []

        sp = systemprofile.profiler

        try:
            components.append("{}".format(sp.hardware_make()))
        except:
            components.append("MakeDefault")

        try:
            components.append("{}".format(sp.hardware_model()))
        except:
            components.append("ModelDefault")

        try:
            components.append("{}".format(sp.hardware_identifier()))
        except:
            components.append("HWidDefault")

        try:
            components.append("{}".format(sp.serial_number()))
        except:
            components.append("SerialNumberDefault")

        my_passphrase = "__{}__".format(".".join(components))

        return my_passphrase
    
    @classmethod
    def build_x509_extensions(cls, extensions):
        """
        Method which builds a list of crypto.X509Extension objects
        based on the provided dictionary.
        
        :param dict extensions: Dictionary of extension definitions, 
                    keyed by the extension name
                    
        .. example:
            >>> Identity.build_x509_extensions({"basicConstraints": 
                                        {
                                            "critical": False,
                                            "value": "CA:TRUE"
                                        }
                                    })
            
        """
        
        result = []
        
        for k, v in extensions.iteritems():
            result.append(crypto.X509Extension(k, **v))
        
        return result
        
    
#MARK: - Module Functions
def file_hash(filepath, hash=None):
    """
    Method to return a hash for the provided file.
    
    :param filepath: The file to hash
    :type filepath: string 
    
    :param hash: The hashing algorithm to use
    :type hash: :py:class:`hashlib.HASH` object (default 'hashlib.sha256')
    :type hash: (string) hash algorithm to use (i.e. 'sha256')
    
    :returns: string - hash 
    
    :raises: IOError on standard filesystem errors
    :raises: Exception on misc error
    
    """
    
    if hash is None:
        hash = hashlib.sha256()
    elif isinstance(hash, basestring):  
        try:
            if not hash.startswith("sha"):
                raise AttributeError()
                
            hash = getattr(hashlib, hash)()
        except AttributeError:
            raise Exception("Invalid hash type!")
    
    with open(filepath,"rb") as fh:
        hash.update(fh.read())
    
    return hash.hexdigest()

def string_hash(string, hash=None):
    """
    Method to return a hash for the provided string.
    
    :param string: The string to hash
    :type string: string 
    
    :param hash: The hashing algorithm to use
    :type hash: :py:class:`hashlib.HASH` object (default 'hashlib.sha256')
    :type hash: (string) hash algorithm to use (i.e. 'sha256')
    
    :returns: string - hash 
    
    :raises: IOError on standard filesystem errors
    :raises: Exception on misc error
    """
    if hash is None:
        hash = hashlib.sha256(string)
    elif isinstance(hash, basestring):  
        try:
            if not hash.startswith("sha"):
                raise AttributeError()
                
            hash = getattr(hashlib, hash)(string)
        except AttributeError:
            raise Exception("Invalid hash type!")
    
    return hash.hexdigest()

def encrypt_karl_data(data):
    """
    Method to encrypt data for transport to KARL.
    """

    return encrypt_data_with_key(data,karl_public_key_path)

def encrypt_data_with_key(data,key_path):
    """
    Method which encrypts the provided data using the provided key data.

    :param str data: Arbitrary data to be encrypted.
    :param str key_path: PEM File containing the encryption key

    :returns str: Encrypted data

    """

    encrypted_data = None

    if not os.path.isfile(karl_public_key_path):
        raise KeyNotFoundError(key_path=karl_public_key_path)

    with open(key_path,"r") as fh:
        key_data = fh.read()

    key = PKCS1_OAEP.new(RSA.importKey(key_data))

    encrypted_data = base64.b16encode(key.encrypt(data))

    return encrypted_data

def generate_password(length=16,character_set=None):
    """
    Method to generate a random password.

    :param int length: The length of the password.
    :param character_set: A table of acceptable characters to seed from. By
            default we use a 64 character set that eliminates easily
            misconstrued values (i.e. no lowercase l, uppercase I, capital).
    :type character_set: Set or List of strings

    :returns: Random password of the given length.

    .. note:
        With the default character set, expect approximately 6 bits of entropy
        per character:

        Length      Entropy (bits)
	=========   =============
        8           48
        12          72
        16          96

        Default Character Set:
            "A", "B", "C", "D", "E", "F", "G", "H",
            "J", "K", "L", "M", "N", "P", "Q", "R",
            "S", "T", "U", "V", "W", "X", "Y", "Z",
            "a", "b", "c", "d", "e", "f", "g", "h",
            "i", "j", "k", "m", "n", "o", "p", "q",
            "r", "s", "t", "u", "v", "w", "x", "y",
            "z", "0", "1", "2", "3", "4", "5", "6",
            "7", "8", "9", "+", "/", "$", "*", "-"

    """

    chars = []

    if not character_set:
        character_set = ["A", "B", "C", "D", "E", "F", "G", "H",
                            "J", "K", "L", "M", "N", "P", "Q", "R",
                            "S", "T", "U", "V", "W", "X", "Y", "Z",
                            "a", "b", "c", "d", "e", "f", "g", "h",
                            "i", "j", "k", "m", "n", "o", "p", "q",
                            "r", "s", "t", "u", "v", "w", "x", "y",
                            "z", "0", "1", "2", "3", "4", "5", "6",
                            "7", "8", "9", "+", "/", "$", "*", "-"]

    rand = random.SystemRandom()

    i = 0
    while i < length:
        try:
            rand_int = rand.randint(0,len(character_set)-1)
            chars.append(character_set[rand_int])
            i += 1
        except Exception:
            continue

    return "".join(chars)

def convert_certificate(data,inform=crypto.FILETYPE_ASN1,
                                                outform=crypto.FILETYPE_PEM):
    """
    Method which will convert input certificate input into the desired output.
    
    :param data: Binary (ASN1) or string (PEM) certificate data.
    :type data: Binary or string data
    :param int inform: The format of the input data (default crypto.FILETYPE_ASN1)
    :param int outform: The format of the output data (default crypto.FILETYPE_PEM)
    """
    
    certificate = crypto.load_certificate(inform, data)
    
    return crypto.dump_certificate(outform, certificate)

def date_to_x509_string(date):
    """
    Method to convert a :py:class:`datetime.datetime` object into an x509 
    friendly string.
    
    .. example:
        >>> date = datetime.datetime.utcnow()
        >>> date_to_x509_string(date)
        '20170309001549Z'
    
    """
    
    return date.strftime(X509_DATE_FORMAT)

def date_from_x509_string(date):
    """
    Method to return a :py:class:`datetime.datetime` populated from an x509 
    friendly string.
    
    .. example:
        >>> date_str = '20170309001549Z'
        >>> date_from_x509_string(date)
        datetime.datetime(2017, 3, 9, 0, 15, 49)
    
    """
    
    return datetime.datetime.strptime(date, X509_DATE_FORMAT)

def data_from_certificate(certificate):
    """
    Method that will return a dictionary of values read in from the provided
    certificate.
    """
    
    data = {}
    
    cert = certificate
    
    data["not_before"] = date_from_x509_string(certificate.get_notBefore())
    data["not_after"] = date_from_x509_string(certificate.get_notAfter())
    data["serial_number"] = certificate.get_serial_number()
    
    subject_dict = collections.OrderedDict(
                                    certificate.get_subject().get_components())
    issuer_dict = collections.OrderedDict(
                                    certificate.get_issuer().get_components())
    
    data["cn"] = subject_dict["CN"]
    data["issuer_cn"] = issuer_dict["CN"]
    
    dn = ""
    for key, value in subject_dict.iteritems():
        dn += "/{}={}".format(key,value)
    data["dn"] = dn
    
    dn = ""
    for key, value in issuer_dict.iteritems():
        dn += "/{}={}".format(key,value)
    data["issuer_dn"] = dn
    
    return data

def is_certificate_not_expired(certificate_str):
    '''
    Method to check the provided PEM format cert is expired
    @param certificate_str: certificate string in PEM format
    Return True: If cert is not expired. False if certificate expired
    '''
    validation_status = bool(certificate_str)
    # Early return if no certificate_str
    if not validation_status:
        return False
    cert_to_validate = crypto.load_certificate(crypto.FILETYPE_PEM, certificate_str)
    cert_datetime = datetime.datetime.strptime(cert_to_validate.get_notAfter(),"%Y%m%d%H%M%SZ")
    validation_status &= cert_datetime > datetime.datetime.utcnow() 
    return validation_status

def get_cert_expiry(certificate_str):
    '''
    Method to get expiry date from PEM format cert.
    @param certificate_str: certificate string in PEM format
    Return datetime: If cert is not expired. False if certificate expired
    '''
    validation_status = bool(certificate_str)
    # Early return if no certificate_str
    if not validation_status:
        return None
    cert_to_validate = crypto.load_certificate(crypto.FILETYPE_PEM, certificate_str)
    expiry_datetime = datetime.datetime.strptime(cert_to_validate.get_notAfter(),"%Y%m%d%H%M%SZ")
    return expiry_datetime


#MARK: - Exceptions
class KeyNotFoundError(Exception):
    """
    Exception thrown when a key file is missing.
    """

    def __init__(self,message=None,key_path=None):

        if message is None and key_path:
            message = "Encryption key file not present at path:'{}'".format(key_path)
        elif message is None:
            message = "Encryption key file not found!"

        super(KeyNotFoundError, self).__init__(message)
        self.key_path = key_path

class IdentityNotFoundError(Exception):
    """
    Exception thrown when a Identity cannot be found.
    """

    def __init__(self,message=None,common_name=None):

        if message is None and common_name:
            message = "Identity with common name:'{}' could not be found".format(
                    common_name)
        elif message is None:
            message = "Identity not found!"

        self.common_name = common_name
        super(IdentityNotFoundError, self).__init__(message)

class IdentityError(Exception):
    """
    Exception raised when an error occurs while loading or processing
    identity material.
    """
    pass

class PassphraseError(Exception):
    """
    Exception raised when an error occurs while loading or saving
    passphrase material.
    """
    pass


#MARK: - Module logic
Identity = IdentityBase

def _configure_macos():
    """
    Method to configure our crypto package for use with macOS
    """

    import crypto_macos
    global Identity

    Identity = crypto_macos.IdentityMacOS


def _configure_ubuntu():
    """
    Method to configure our network package for use with Ubuntu
    """

    import crypto_ubuntu
    global Identity

    Identity = crypto_ubuntu.IdentityUbuntu


## OS Configuration
if acme.platform == "OS X" or acme.platform == "macOS":
    _configure_macos()
elif acme.platform == "Ubuntu":
    _configure_ubuntu()

class JWTObject():
    """
    class which provides method to convert json data to jwt data
    """
    logger_name = "JWTObject"
    def to_jwt(self, json_string = None, b64encode = True):
        """
        Method which converts the json string to jwt
        """
        logger = logging.getLogger(self.logger_name)
        if json_string:
            system_identifier = systemprofile.profiler.system_identifier()
            if system_identifier is None:
                raise IdentityNotFoundError("No system identifier is established!")
            identity = Identity(common_name=system_identifier)
            try:
                identity.load()
                logger.debug("Found existing identity matching:{}".format(system_identifier))
            except acme.crypto.IdentityNotFoundError:
                logger.debug("Could not load identitiy matching:{}, generating identity again and saving it..".format(system_identifier))
                identity.generate()
                identity.save()

            return identity.get_jwt(json.loads(json_string), b64encode = b64encode)
