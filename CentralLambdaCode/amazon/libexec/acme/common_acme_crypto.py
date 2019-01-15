#!/System/Library/Frameworks/Python.framework/Versions/Current/bin/python

## common_acme_crypto.py
## Written by Gerrit DeWitt (gerritd@amazon.com)
## Modified by Jason Simmons (jasosimm@amazon.com)
## Modified by LJ Cline (joscline@amazon.com)
## Version 2.9.8.5 - 2018-10-25 Copyright Amazon
## Various subroutines common to ACME Assistants & Automations.

import os, hashlib, re, uuid, base64, datetime, requests, json
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
from OpenSSL import crypto
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from common_acme import *

## ROUTINES FOR GENERATING USER CERTIFICATE IDENTITY

def generateCSR(requestedCommonName, openSSLDict, keyBits, keyHash, identityKeyPath, identityReqPath):
## Create private key and CSR objects.
## Write private key and CSR objects to temporary files.
	# Defaults:
	haveReqObject = False
	wroteFilesForCSR = False
	# Step 1 - Create private key and CSR objects.
	try:
		# Generate new key object:
		keyObject = crypto.PKey()
		keyObject.generate_key( crypto.TYPE_RSA, keyBits )
		# Generate request object:
		reqObject = crypto.X509Req()
		# Add subject to request object:
		reqSubject = reqObject.get_subject()
		reqSubject.countryName = openSSLDict['countryName']
		reqSubject.stateOrProvinceName = openSSLDict['stateOrProvinceName']
		reqSubject.organizationName = openSSLDict['organizationName']
		reqSubject.organizationalUnitName = openSSLDict['organizationalUnitName']
		reqSubject.commonName = str(requestedCommonName)
		# Add constraints:
		basicConstraints = crypto.X509Extension("basicConstraints",False,openSSLDict['basicConstraints'])
		keyUsage = crypto.X509Extension("keyUsage",False,openSSLDict['keyUsage'])
		reqObject.add_extensions([basicConstraints,keyUsage])
		# Set version - essential for Windows Server 2008 DCs:
		reqObject.set_version(0)
		# Add public key to request:
		reqObject.set_pubkey(keyObject)
		# Add private key:
		reqObject.sign(keyObject,keyHash)
		# Update haveReqObject:
		haveReqObject = True
	except:
		logger.error('generateCSR: Error generating CSR or private key.')
	# Step 2 - Write private key and CSR objects to temporary files.
	if haveReqObject:
		try:
			reqPemStr = crypto.dump_certificate_request(crypto.FILETYPE_PEM,reqObject)
			privateKeyStr = crypto.dump_privatekey(crypto.FILETYPE_PEM,keyObject)
			identityReqFile=open(identityReqPath,'w')
			identityReqFile.write(reqPemStr)
			identityReqFile.close()
			identityKeyFile=open(identityKeyPath,'w')
			identityKeyFile.write(privateKeyStr)
			identityKeyFile.close()
			wroteFilesForCSR = True
		except:
			logger.error('generateUserCSR: Error writing temporary files.')
	# Return: wroteFilesForCSR TRUE implies wroteFilesForCSR and haveReqObject are both TRUE:
	return wroteFilesForCSR

def uploadCSRAndDownloadSignedCert(caRequestDetailsDict, certTemplate, identityReqPath):
## Uploads the temporary CSR file to a certificate server and gets the response HTML.
	# Defaults:
	url = caRequestDetailsDict['requestURL']
	headers = {'content-type': 'application/json'}
	uploadResult = False
	serverResponse = ''
	certificateResult = ''
	# Load CSR:
	identityReqFile=open(identityReqPath,'r')
	reqPemStr = identityReqFile.read()
	identityReqFile.close()
	# Assemble POST data dict:
	postDataDict = {'certRequest': reqPemStr, 'template': certTemplate}
	# Send the temporary CSR file and retrieve the resulting JSON:
	logger.info('uploadCSRAndDownloadSignedCert: Trying CA')
	try:
		serverResponse = json.loads(requests.post(url, auth=HTTPKerberosAuth(mutual_authentication=OPTIONAL), verify='/usr/local/amazon/var/acme/ca/amazon-root-ca.pem', data=json.dumps(postDataDict), headers=headers).text)
	except Exception as e:
		ipcSendAndPrintMessage('Error from CA: ' + str(e)  + '.',fromHelperSocketPath, azIdentifier)
	# Likely good response. No error is a null JSON 'error' object
	if not serverResponse['error']:
		uploadResult = True
		certificateResult = serverResponse['certificate']
		logger.info('uploadCSRAndDownloadSignedCert: Valid response from CA.')
	else:
		logger.error('uploadCSRAndDownloadSignedCert: Invalid response from CA. ' + serverResponse['error'])
	# Return:
	return uploadResult, certificateResult

def createPKCS12(friendlyName, certificateResult, identityKeyPath, p12FilePath):
## Downloads the certificate and creates a PKCS12 file.
	# Defaults:
	downloadedCertContents = False
	p12Created = False
	p12Password = str(uuid.uuid1()).upper().replace('-','')
	# Assume expiration is now in case of an error:
	certNotValidAfter = datetime.datetime.utcnow()
	# Step 1 - Download certificate contents by making HTTP request to the certificate download URL:
	identityCertContents = certificateResult
	downloadedCertContents = True
	# Step 2 - Create PK12 identity:
	if downloadedCertContents:
		try:
			# Read the identity key object (path, file object, contents, crypto object):
			identityKeyFile = open(identityKeyPath,'r')
			identityKeyContents = identityKeyFile.read()
			identityKeyFile.close()
			identityKeyObject = crypto.load_privatekey( crypto.FILETYPE_PEM, identityKeyContents )
			# Load identityCertContents into identityCertObject:
			identityCertObject = crypto.load_certificate( crypto.FILETYPE_PEM, identityCertContents )
			# Get the expiration date:
			certNotValidAfter = identityCertObject.get_notAfter()
			# Create PKCS12 object.
			p12Object = crypto.PKCS12()
			p12Object.set_certificate(identityCertObject)
			p12Object.set_privatekey(identityKeyObject)
			p12Object.set_friendlyname(str(friendlyName))
			# Write the pcks12 (crypto object, contents, file object):
			p12Contents = p12Object.export(passphrase=p12Password)
			p12FileObj = open(p12FilePath,'w')
			p12FileObj.write(p12Contents)
			p12FileObj.close()
			p12Created = True
		except:
			logger.error('downloadSignedCert: Error writing PKCS12 identity.')
	# Return:
	return (downloadedCertContents and p12Created), certNotValidAfter, p12Password

def mergeFolderOfPemsIntoPKCS12(pemDirectoryPath,p12FilePath):
## Creates a PKCS12 file containing PEMs from a given directory.
	# Defaults:
	pemFilePathsArray = []
	pemFileObjectsArray = []
	p12Created = False
	p12Password = str(uuid.uuid1()).upper().replace('-','')
	# Step 1 - Build array of pem files in the pemDirectoryPath:
	for fileName in os.listdir(pemDirectoryPath):
		if fileName.find('.pem') != -1:
			pemFilePathsArray.append(pemDirectoryPath + '/' + fileName)
	# Step 2 - Read the pem files and add their contents to the pemFileObjectsArray:
	for pemFilePath in pemFilePathsArray:
		try:
			# Read the cert object (path, file object, contents):
			pemFile = open(pemFilePath,'r')
			pemFileContents = pemFile.read()
			pemFile.close()
			# Load pemFileContents into pemObject:
			pemObject = crypto.load_certificate( crypto.FILETYPE_PEM, pemFileContents )
			# Append file contents to pemFileObjectsArray:
			pemFileObjectsArray.append(pemObject)
		except:
			logger.error('mergeFolderOfPemsIntoPKCS12: Error reading file:' + pemFilePath)
	# Step 3 - Add the pem objects to the p12 archive:
	# Create PKCS12 object.
	p12Object = crypto.PKCS12()
	p12Object.set_ca_certificates(pemFileObjectsArray)
	# Write the pcks12 (crypto object, contents, file object):
	p12Contents = p12Object.export(passphrase=p12Password)
	p12FileObj = open(p12FilePath,'w')
	p12FileObj.write(p12Contents)
	p12FileObj.close()
	p12Created = True
	# Return:
	return p12Created, p12Password
	
def encryptWithPubKey(message,pubKeyFilePath):
## Encrypts a given message with a specified public key.
	# Defaults:
	encryptedMessage = 'invalid-message'
	# STEP 1 - Read and load the public key:
	try:
		pubKeyFile = open(pubKeyFilePath,'r')
		pubKeyFileContents = pubKeyFile.read()
		pubKeyFile.close()
		pubKeyObj = PKCS1_OAEP.new(RSA.importKey(pubKeyFileContents))
	except:
		logger.error('encryptWithPubKey: Error with public key.')
	# STEP 2 - Encrypt message:
	try:
		encryptedMessage = pubKeyObj.encrypt(message)
		encryptedMessage = base64.b16encode(encryptedMessage) # Use b16 to keep chars alphanumeric.
	except:
		logger.error('encryptWithPubKey: Error encrypting message.')
	# STEP 3 - Return:
	return encryptedMessage

global azIdentifier
global logger

azIdentifier = 'com.amazon.acme_crypto'
logger = azLogger(azIdentifier)
