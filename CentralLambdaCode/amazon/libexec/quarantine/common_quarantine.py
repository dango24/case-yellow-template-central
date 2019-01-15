#!/System/Library/Frameworks/Python.framework/Versions/Current/bin/python

## Mac Quarantine: common_quarantine.py
## Copyright Amazon
## Written by Gerrit DeWitt (gerritd@amazon.com)
## Version 1.5.0.2 - 2017-12-11
## Project started 2013-04-22

import os, sys, syslog, subprocess, stat, datetime, plistlib, logging, logging.handlers
from SystemConfiguration import *


## CONFIG AND PREFS FILE ROUTINES ##

def azSubprocess(cmd):
# Safely run a command in such a way that we always have the output and return code.
	try:
		output = subprocess.check_output(cmd)
		returnCode = 0
	except subprocess.CalledProcessError, errorObject:
		output = ''
		returnCode = errorObject.returncode
	return output, returnCode
		
def printMessage(message):
## Print message to console and in syslog.
	syslog.syslog(syslog.LOG_INFO,message)
	print(message)

def printError(message):
## Print error message to console and in syslog.
	syslog.syslog(syslog.LOG_ERR,message)
	logging.error(message)
	
def loadMainDictionary():
## Safely load main configuration data.
	mainConfigFilePath = '/usr/local/amazon/var/quarantine/main-config.plist'
	mainDictionary = {}
	needReinstall = False
	
	if not os.path.exists(mainConfigFilePath):
		logger.error('Missing %s.' % mainConfigFilePath)
		needReinstall = True
	else:
		# Check permissions:
		mainConfigStat = os.stat(mainConfigFilePath)
		if (mainConfigStat.st_uid != 0) or (mainConfigStat.st_mode != 33188):
			logger.error('Invalid permissions on %s.' % mainConfigFilePath)
			needReinstall = True
		else:
			try:
				mainDictionary = plistlib.readPlist(mainConfigFilePath)
			except:
				logger.error('Could not read %s.' % mainConfigFilePath)

	if needReinstall:
		reinstallACME()

	# Return:
	return mainDictionary

def azLoadModulesDictionary():
# Read module configuration details.
	modulesConfigFilePath = '/usr/local/amazon/var/quarantine/modules.plist'
	try:
		modulesDict = plistlib.readPlist(modulesConfigFilePath)
	except:
		modulesDict = {}
	return modulesDict

def outputPathForModuleNamed(moduleName):
# Given a module by name, return the path to its plist output file.
	mainConfigurationDict = loadMainDictionary()
	modulesDict = azLoadModulesDictionary()
	azProductIdentifier = mainConfigurationDict['azProductIdentifier']
	azModuleOutputDir = mainConfigurationDict['azModuleOutputDir']

	return modulesDict[moduleName]['outputFile'].replace('__azModuleOutputDir__',azModuleOutputDir).replace('__azProductIdentifier__',azProductIdentifier)

def readQuarantineOutputDict():
# Returns the quarantine output dictionary.
	quarantineConfigDict = loadMainDictionary()
	azProductIdentifier = quarantineConfigDict['azProductIdentifier']
	azModuleOutputDir = quarantineConfigDict['azModuleOutputDir']

	quarantineOutputPath = azModuleOutputDir + '/' + azProductIdentifier + '.plist'
	quarantineOutputDict = plistlib.readPlist(quarantineOutputPath)
	return quarantineOutputDict


def azReadPlistFromStr(strData):
	## Converts a string of XML plist content to a dict using plistlib.
	try:
		dictionary = plistlib.readPlistFromString(strData)
	except:
		dictionary = {}
	return dictionary


def ensureDeletedSecurely(pathsArray):
## Securely deletes files from a path array, if present.
	for filePath in pathsArray:
		if os.path.exists(filePath):
			cmd = ['/usr/bin/srm',filePath]
			azSubprocess(cmd)

def ensureDeleted(pathsArray):
## Deletes files from a path array, if present.
	for filePath in pathsArray:
		if os.path.exists(filePath):
			os.unlink(filePath)


## MISCELLANEOUS MANAGEMENT ROUTINES ##

def reinstallACME():
    # Check if we're on Amazon network to reinstall ACME Tools
    domainController = findDomainController(adDomain)
    if domainController != 'invalid':
        logger.info('Reinstalling ACME Tools.')
        cmd = ['/usr/local/jamf/bin/jamf','policy','-trigger','reinstallACME']
        azSubprocess(cmd)
    else:
        logger.info('Not connected to Amazon network. Unable to reinstall ACME Tools at this time.')
    sys.exit('Quitting.')

def findDomainController(adDomain):
	## Returns the primary domain controller by DNS-SRV query.
	# Default:
	domainController = 'invalid'
	# Run DNS query.
	cmd = ['/usr/bin/dig','+time=1','SRV','+short','_ldap._tcp.pdc._msdcs.'+adDomain]
	[output,returnCode] = azSubprocess(cmd)
	if output.find(adDomain) != -1:
		# Try parsing output for hostname:
		output = output.replace('\n','').split(' ')
		for item in output:
			if item.find(adDomain) != -1:
				domainController = item
				break
	# Return:
	return domainController

def getComputerUUID():
## Returns the computer's model uuid.
	# Defaults:
	uuid = 'invalid-uuid'
	# Run system profiler and look for platform_UUID:
	cmd = ['/usr/sbin/system_profiler','SPHardwareDataType','-xml']
	[output,returnCode] = azSubprocess(cmd)
	if returnCode == 0 and output:
		systemProfilerOutputDict = azReadPlistFromStr(output)
		if systemProfilerOutputDict:
			uuid = systemProfilerOutputDict[0]['_items'][0]['platform_UUID'].lower()
	# Return:
	return uuid

def identifyComputerType():
## Returns the computer's model identifier.
	# Defaults:
	computerType = 'unknown-mac'
	# Run system profiler and look for the Model Identifier:
	cmd = ['/usr/sbin/system_profiler','SPHardwareDataType','-xml']
	[output,returnCode] = azSubprocess(cmd)
	if returnCode == 0 and output:
		systemProfilerOutputDict = azReadPlistFromStr(output)
		if systemProfilerOutputDict:
			computerType = systemProfilerOutputDict[0]['_items'][0]['machine_model'].lower()
	# Return:
	return computerType

def azInitDynamicStore():
	return SCDynamicStoreCreate(None,'com.amazon.acme.quarantine',None,None)

def getLocalHostNameFromDynamicStore():
	return SCDynamicStoreCopyLocalHostName(None) or 'invalid-host-name'
	
def azGetInterfacesList():
	allNetInterfaces = []
	for interface in SCNetworkInterfaceCopyAll():
		allNetInterfaces.append( SCNetworkInterfaceGetBSDName(interface) )
	return allNetInterfaces


#--------------------------------------------------------------------------------------------
#  Logger----COPIED FROM COMMON ACME
#--------------------------------------------------------------------------------------------

def azLogger(azIdentifier):
	## Create logger
	logger = logging.getLogger(azIdentifier)
	
	## Set log path
	log_location="/var/log/quarantine.log"
	if not len(logger.handlers):
		fh=logging.handlers.RotatingFileHandler(log_location,maxBytes=2097152,backupCount=4)
		logger.addHandler(fh)
		logger.setLevel(logging.DEBUG)
		
		## Create console handler and set level to debug
		ch = logging.StreamHandler()
		ch.setLevel(logging.DEBUG)
		
		## Create formatter
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		
		## Set logging formatter
		fh.setFormatter(formatter)
		
		## Add formatter to ch
		ch.setFormatter(formatter)
		
		## Add ch to logger
		logger.addHandler(ch)
	logger.propagate=0
	return logger

global azIdentifier
global logger
azIdentifier = 'com.amazon.acme.quarantine'
global adDomain
adDomain = 'ant.amazon.com'
global azMainConfigurationDict
azMainConfigurationDict = loadMainDictionary()
