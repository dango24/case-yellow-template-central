#!/System/Library/Frameworks/Python.framework/Versions/Current/bin/python

## common_acme.py
## Written by Gerrit DeWitt (gerritd@amazon.com)
## Modified by Jason Simmons (jasosimm@amazon.com)
## Modified by LJ Cline (joscline@amazon.com)
## Version 2.9.8.5 - 2018-10-25 Copyright Amazon
## Various subroutines common to ACME Assistants & Automations.

import subprocess, socket, os, pwd, sys, datetime, plistlib, time, uuid, base64, logging, logging.handlers, stat
from SystemConfiguration import *
import pexpect


## CONFIG AND PREFS FILE ROUTINES ##

def azLoadMainDictionary():
## Safely load main configuration data.
	mainConfigFilePath = '/usr/local/amazon/var/acme/main-config.plist'
	if not os.path.exists(mainConfigFilePath):
		logger.error('azLoadMainDictionary: Missing main configuration file.')
	else:
		# Check permissions:
		mainConfigStat = os.stat(mainConfigFilePath)
		if (mainConfigStat.st_uid != 0) or (mainConfigStat.st_mode != 33188):
			logger.error('azLoadMainDictionary: Invalid permissions on main configuration file.')
		else:
			try:
				mainDictionary = plistlib.readPlist(mainConfigFilePath)
			except:
				mainDictionary = {}
	# Return:
	return mainDictionary

def azReadPlist(plistPath):
## BEGIN: azReadPlist()
	try:
		dictionary = plistlib.readPlist(plistPath)
	except:
		dictionary = {}
	return dictionary

def azReadPlistFromStr(strData):
## Converts a string of XML plist content to a dict using plistlib.
	try:
		dictionary = plistlib.readPlistFromString(strData)
	except:
		dictionary = {}
	return dictionary

def azWritePlist(dictionary, plistPath):
## BEGIN: azWritePlist()
	try:
		plistlib.writePlist(dictionary, plistPath)
	except:
		logger.error('azWritePlist: Could not write plist.')

## ROUTINES FOR RUNNING COMMAND-LINE TOOLS ##

def azSubprocess(cmd):
## Runs a command in such a way that we always have output and return code.
	try:
		output = subprocess.check_output(cmd)
		returnCode = 0
	except subprocess.CalledProcessError, errorObject:
		output = ''
		returnCode = errorObject.returncode
	return output, returnCode

## FILE MANIPULATION ROUTINES ##

def ensureDeletedSecurely(pathsArray):
## Securely deletes files from a path array, if present.
	for filePath in pathsArray:
		if os.path.exists( filePath ):
			cmd = ['/usr/bin/srm',filePath]
			azSubprocess(cmd)

def ensureDeleted(pathsArray):
## Deletes files from a path array, if present.
	for filePath in pathsArray:
		if os.path.exists( filePath ):
			os.unlink( filePath )

def ensureMoved(pathsArray):
## Moves files from a path array, if present.
	for filePath in pathsArray:
		if os.path.exists( filePath ):
			filePathNew = filePath + '-' + str(uuid.uuid1()).upper().replace('-','')
			os.rename( filePath, filePathNew )

def waitForPathToExist(path,timeout):
## Waits for a path to exist, then exits. Gives up after timeout.
	timer = 0
	while True:
		time.sleep(1)
		timer = timer + 1
		if os.path.exists(path) or (timer > timeout):
			break
	return os.path.exists(path)

def waitForPathToNotExist(path):
## Waits for a path to cease to exist, then exits.
	while True:
		time.sleep(1)
		if not os.path.exists(path):
			break
			
def uniquePathFromPath(filePath):
## Generates a unique path given a desired prefix.
	return filePath + '-' + str(uuid.uuid1()).upper().replace('-','')

## IPC ROUTINES ##

def ipcSend(messageArray, socketPath):
## Sends a message to an existing socket.
	# Default:
	messageSent = False
	# Wait for socket:
	socketReady = waitForPathToExist(socketPath,10)
	if not socketReady:
		logger.error('ipcSend: Socket not available.')
	else:
		# If socket is present:
		# Convert messageArray to string:
		messageArrayEncoded = []
		for item in messageArray:
			messageArrayEncoded.append( base64.urlsafe_b64encode(item) )
		messageStr = ','.join(messageArrayEncoded)
		# Connect to socket:
		socketObj = socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
		try:
			socketObj.connect(socketPath)
			socketObj.sendall(messageStr)
			messageSent = True
		except:
			logger.error('ipcSend: Could not send message.')
		finally:
			socketObj.close()
	# Return:
	return messageSent

def ipcSendAndPrintMessage(messageStr, socketPath, azHelperIdentifier):
## Used by helpers to relay output to their non-privileged callers.
	# Print message (as helper):
	if messageStr != '__closeSocket__':
		azLogger(azHelperIdentifier).info(messageStr)
	# Try connecting to socket for unprivileged tool to relay message to it:
	# Wait for socket:
	socketReady = waitForPathToExist(socketPath,10)
	if not socketReady:
		logger.error('ipcSendAndPrintMessage: Socket not available.')
	else:
		# If socket is present:
		# Connect to socket and relay message to unprivileged tool for output:
		socketObj = socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
		try:
			socketObj.connect(socketPath)
			socketObj.sendall(messageStr)
		except:
			logger.error('ipcSendAndPrintMessage: Could not send message.')
		finally:
			socketObj.close()

def ipcReceive(socketPath,socketSendTimeout):
## Sets up a socket and recieves a message on it.
	# Defaults:
	shouldRecieveMessage = True
	messageArray = []
	# Create new socket:
	if os.path.exists(socketPath):
		socketLastAccessedSinceEpoch = os.path.getatime(socketPath)
		currentTimeSinceEpoch = time.time()
		if currentTimeSinceEpoch - socketLastAccessedSinceEpoch > socketSendTimeout:
		# Socket not touched in a while; probably an orphan:
			os.unlink(socketPath)
		else:
		# Socket is likely still active; don't start a duplicate helper:
			shouldRecieveMessage = False
			logger.error('ipcReceive: Socket is active by another instance of the helper.')
	# Recieve message:
	if shouldRecieveMessage:
		try:
			socketObj = socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
			socketObj.bind(socketPath)
			socketObj.settimeout(20)
			# Make socket a drop box:
			cmd = ['/bin/chmod','222',socketPath]
			azSubprocess(cmd)
			# Loop:
			while True:
				# Listen:
				socketObj.listen(1)
				[connObj, address] = socketObj.accept()
				messageStr = connObj.recv(1024)
				if messageStr:
					# Convert to messageArray:
					messageArrayEncoded = messageStr.split(',')
					for item in messageArrayEncoded:
						messageArray.append( base64.urlsafe_b64decode(item) )
					# Close and exit loop:
					connObj.close()
					break
		except socket.timeout:
			logger.error('ipcReceive: Timeout - Did not receive message.')
		except socket.error as exp:
			logger.exception('ipcReceive: Exception - Did not receive message. ' + str(exp))
	# Clean up: remove socket:
	if os.path.exists(socketPath):
		os.unlink(socketPath)
	# Return:
	return shouldRecieveMessage, messageArray

def ipcReceiveMessagesFromHelper(socketPath, timeout, azHelperIdentifier=None):
	if azHelperIdentifier is None:
		azHelperIdentifier = "HelperTool"
	
## Sets up a socket and recieves a message on it.
	# Create new socket and object:
	if os.path.exists(socketPath):
		os.unlink(socketPath)
	try:
		socketObj = socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
		socketObj.bind(socketPath)
		socketObj.settimeout(timeout)
		# Make socket a drop box:
		cmd = ['/bin/chmod','222',socketPath]
		azSubprocess(cmd)
		# Loop:
		while True:
			# Listen:
			socketObj.listen(1)
			[connObj, address] = socketObj.accept()
			try:
				messageStr = connObj.recv(1024)
				if messageStr != '__closeSocket__':
					azLogger(azHelperIdentifier).info(messageStr)
				else:
					# Close and exit loop:
					connObj.close()
					break
			except:
				logger.debug('No messages yet.')
	except socket.timeout:
		logger.error('ipcReceiveMessagesFromHelper: ' + str(azHelperIdentifier) + ' Timeout - Did not receive any messages. The helper may still be running, though! Please wait a few minutes and run this tool again.')
	except socket.error as exp:
		logger.exception('ipcReceiveMessagesFromHelper: ' + str(azHelperIdentifier) + ' Exception - Did not receive any messages. ' + str(exp))
	# Clean up: remove socket:
	if os.path.exists(socketPath):
		os.unlink(socketPath)
	
## USER ACCOUNT ROUNTINES ##

def validateUserFromSystemContext(adDomain):
## Gets user details using SCDynamicStore.
## Validates user, returns requestedUser, userHomePath and true/false.
	# Default:
	haveValidUser = False
	requestedUser = ''
	userHomePath = '' 
	# Query dyamic store for current Aqua user:
	tempDyamicStore = SCDynamicStoreCreate(None,azIdentifier,None,None)
	dyamicStoreUserDetailsArray = SCDynamicStoreCopyConsoleUser(tempDyamicStore,None,None)
	# Get user details:
	userDetailsArray = pwd.getpwuid(dyamicStoreUserDetailsArray[1])
	requestedUser = userDetailsArray[0]	
	userHomePath = userDetailsArray[5]
	# Compute haveValidUser:
	if ( requestedUser and userHomePath and userIsAdUser(requestedUser,adDomain) ):
		haveValidUser = True
	# Print messages:
	logger.debug('validateUser: ' + str(haveValidUser) )
	logger.debug('validateUser: User name: ' + str(requestedUser) )
	logger.debug('validateUser: Home directory: ' + str(userHomePath) )
	# Return:
	return haveValidUser, requestedUser, userHomePath

def validateUser(adDomain):
## Gets user details using Python routines (running as the user running the script).
## Validates user, returns requestedUser, userHomePath and true/false.
	# Default:
	haveValidUser = False
	requestedUser = ''
	userHomePath = '' 
	# Get user:
	userDetailsArray = pwd.getpwuid(os.getuid())
	requestedUser = userDetailsArray[0]
	userHomePath = userDetailsArray[5]
	# Test:
	if userIsAdUser(requestedUser,adDomain):
		haveValidUser = True
	# Print messages:
	logger.debug('validateUser: ' + str(haveValidUser) )
	logger.debug('validateUser: User name: ' + str(requestedUser) )
	logger.debug('validateUser: Home directory: ' + str(userHomePath) )
	# Return:
	return haveValidUser, requestedUser, userHomePath

def validateUserIsRoot():
## Returns true iff the user is root.
	# Default:
	haveRootUser = False
	userHomePath = ''
	# Get user:
	userDetailsArray = pwd.getpwuid(os.getuid())
	requestedUser = userDetailsArray[0]
	userHomePath = userDetailsArray[5]
	# Test:
	if (os.getuid() == 0) and (requestedUser == 'root') and (userHomePath == '/var/root'):
		haveRootUser = True
	# Return:
	return haveRootUser, userHomePath

def validateTestUser(adTestUserName,adTestUserId):
## Returns true iff the UID for adTestUserName matches adTestUserId.
	# Default:
	userValidated = False
	# Try resolving the test user.
	for tries in range(0,5):
		try:
			# Get user:
			userDetailsArray = pwd.getpwuid(int(adTestUserId))
			requestedUser = userDetailsArray[0]
			# Test:
			if requestedUser == adTestUserName:
				userValidated = True
				break
			else:
				time.sleep(5)
		except KeyError:
			time.sleep(10)
			logger.debug('validateTestUser: Failed to validate. Trying again...')
			pass
	# Return:
	return userValidated
	
def userIsAdUser(requestedUser,adDomain):
## Returns true iff the user is from Active Directory.
	# Defaults:
	isAdUser = False
	# Lookup OriginalNodeName for userName:
	cmd = [ '/usr/bin/dscl','-plist','/Local/Default','read','/Users/' + requestedUser ,'dsAttrTypeStandard:OriginalNodeName' ]
	[output,returnCode] = azSubprocess(cmd)
	if (returnCode == 0) and output:
		outputDict = azReadPlistFromStr(output)
		if outputDict:
			originalNodeNameArray = outputDict['dsAttrTypeStandard:OriginalNodeName']
			if originalNodeNameArray:
				for item in originalNodeNameArray:
					if item.find(adDomain) != -1:
						isAdUser = True
						break
	# Return:
	return isAdUser

def haveOneOrMoreCachedUsersFrom(adDomain):
## Returns true iff at least one cached user from Active Directory exists.
	# Defaults:
	cachedAcctPresent = False
	# Create user list:
	userList = pwd.getpwall()
	for aUser in userList:
		# If UID > 1000, check to see if user is from Active Directory:
		if aUser[2] > 1000:
			if userIsAdUser(aUser[0],adDomain):
				cachedAcctPresent = True
				break
	# Return:
	return cachedAcctPresent
	
def userHasTGT(requestedUser,adDomain):
## Returns true iff the user has a TGT.
	# Default:
	userHasTGT = False
	# Expected principal:
	expectedPrincipal = requestedUser + '@' + adDomain
	expectedPrincipal = expectedPrincipal.lower().replace('\n','')
	# Search for this in output:
	searchStr = 'principal:'
	# Run klist:
	cmd = ['/usr/bin/klist']
	[output,returnCode] = azSubprocess(cmd)
	output = output.lower().split('\n')
	for line in output:
		if line.find(searchStr) != -1:
			output = line.replace(searchStr,'').replace(' ','')
			if output.find(requestedUser + '@' + adDomain) != -1:
				userHasTGT = True
				break
	# Return:
	return userHasTGT


def	deleteAccountOld(accountName):
	## Delete the user account.
	# Default:
	accountDeleted = False
	#create the home folder path
	directoryRoot = '/Users/'
	homeFolderPath = os.path.join(directoryRoot,accountName)
	cmd = ['/usr/bin/dscl','/Local/Default','-delete', homeFolderPath ]
	[output, returnCode] = azSubprocess(cmd)
	returnCodes = returnCode
	# Determine createdAccount:
	if returnCodes == 0:
		accountDeleted = True
		# Remove local directory if it exists
		if os.path.exists(homeFolderPath):
			cmd = ['/bin/rm','-rf', homeFolderPath ]
			[output,returnCode] = azSubprocess(cmd)
			returnCodes = returnCodes + returnCode
	# Return:
	return accountDeleted





def deleteAccount(accountName):
	"""
	Method to delete local user account
	"""
	cmd = '/usr/sbin/sysadminctl'
	args = ['-deleteUser', accountName ]
	try:
		child = pexpect.spawn(cmd, args, timeout=10)
		child.read(size=-1)
		child.close()
	except Exception as e:
		logger.error('deleteAccount: Account ' + str(accountName) + ' is unable to be deleted')
		logger.error('deleteAccount: Error ' + str(e) + '')
	if child.before.find("not found") != -1:
		logger.error('deleteAccount: Account ' + str(accountName) + ' is not found or already deleted')
	elif child.before.find("last admin user or last secure token user neither of which can be deleted") != -1:
		logger.error('deleteAccount: Account ' + str(accountName) + ' is unable to be deleted as it is the last admin or token user on the machine')
		return False
	elif child.before.find("Disabling BTMM for user") != -1:
		logger.debug('deleteAccount: Account ' + str(accountName) + ' deleted')
		return True
	else:
		return False


## COMPUTER ACCOUNT ROUTINES ##

def validateComputerAccount(adDomainSearchPath):
## Runs tests to check AD connectivity and determine the computerAccount.
## Returns true if the computer appears to be bound to Active Directory.
	# Defaults:
	searchPathValid = False
	computerAccountFound = False
	global computerAccount
	computerAccount = ''
	# Test 1: Check authentication search path:
	cmd = ['/usr/bin/dscl','-plist','localhost','read','/Search','SearchPath']
	[output,returnCode] = azSubprocess(cmd)
	if (returnCode == 0) and output:
		outputDict = azReadPlistFromStr(output)
		if outputDict:
			searchPathArray = outputDict['dsAttrTypeStandard:SearchPath']
			if searchPathArray:
				for item in searchPathArray:
					if item == adDomainSearchPath:
						searchPathValid = True
						break
	# Test 2: Check dsconfigad for computer record:
	cmd = ['/usr/sbin/dsconfigad','-xml','-show']
	[output,returnCode] = azSubprocess(cmd)
	if (returnCode == 0) and output:
		outputDict = azReadPlistFromStr(output)
		if outputDict:
			computerAccount = outputDict['General Info']['Computer Account']
			if computerAccount:
				computerAccountFound = True
	# Print messages:
	logger.debug('validateComputerAccount: Search path OK: ' + str(searchPathValid) )
	logger.debug('validateComputerAccount: Found computer account: ' + str(computerAccountFound) )
	logger.debug('validateComputerAccount: Computer account name: ' + str(computerAccount) )
	# Return:
	return (searchPathValid and computerAccountFound), computerAccount

def getComputerAcctTGT(computerAccount,adDomain):
## Get the TGT for the computer account.
	# Default:
	gotTGT = False
	# Expected principal:
	expectedPrincipal = computerAccount + '@' + adDomain
	expectedPrincipal = expectedPrincipal.lower().replace('\n','')
	# Run kinit:
	cmd = ['/usr/bin/kinit','-k',computerAccount]
	[output,returnCode] = azSubprocess(cmd)
	# Run klist:
	cmd = ['/usr/bin/klist','--list-all']
	[output,returnCode] = azSubprocess(cmd)
	if output.lower().find(expectedPrincipal) != -1:
		gotTGT = True
	# Return:
	return gotTGT

def destroyComputerAcctTGT(computerAccount,adDomain):
## Destroy the TGT for the computer account.
	# Default:
	destroyedTGT = False
	# Expected principal:
	expectedPrincipal = computerAccount + '@' + adDomain
	expectedPrincipal = expectedPrincipal.lower().replace('\n','')
	# Run kdestroy:
	cmd = ['/usr/bin/kdestroy','-p',computerAccount]
	[output,returnCode] = azSubprocess(cmd)
	# Run klist:
	cmd = ['/usr/bin/klist','--list-all']
	[output,returnCode] = azSubprocess(cmd)
	if output.lower().find(expectedPrincipal) == -1:
		destroyedTGT = True
	# Return:
	return destroyedTGT

## OPEN DIRECTORY ROUTINES ##

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

def findCertificateServers(domainController,certTemplate):
## Queries a domain controller for a list of certificate servers for the requested template.
	# Default:
	certServersArray = []
	parsedOutput = ''
	# Run ldapsearch:
	cmd = ['/usr/bin/ldapsearch','-LLL','-h',domainController,'-b','CN=Enrollment Services,CN=Public Key Services,CN=Services,CN=Configuration,DC=ANT,DC=AMAZON,DC=COM']
	[output,returnCode] = azSubprocess(cmd)
	# Break up ldapsearch output into array of each line:
	output = output.split('\n')
	# Create string of all lines in the output that have dNSHostName or certificateTemplates:
	# String format will be:
	# dNSHostName:host__certTemplateAttrValue1__certTemplateAttrValue2dNSHostName:host__certTemplateAttrValue1 (...)
	for line in output:
		if line.find('dNSHostName: ') != -1:
			parsedOutput = parsedOutput + line
		if line.find('certificateTemplates: ') != -1:
			parsedOutput = parsedOutput + line.replace('certificateTemplates: ','__')
	# Create array from the string, splitting by dnsHostName:
	# Format will be an array of strings, where each string includes the host and certificateTemplate values (if any):
	# [host__certTemplateAttrValue1__certTemplateAttrValue2,host__certTemplateAttrValue1, ...]
	parsedOutput = parsedOutput.split('dNSHostName: ')
	# For each item in the output array, look for the desired attribute value (certificateTemplates).
	# If found, split the string item by __, get the first (0-th) entry, which is the host name.
	# Add the host name to the array of servers to return.
	for item in parsedOutput:
		if item.find(certTemplate) != -1:
			certServersArray.append( item.split('__')[0] )
	# Return:
	return certServersArray
	
def odSetLogging(logLevel):
## Sets Open Directory logging to the requested level per http://support.apple.com/kb/ht4696.
	cmd = ['/usr/bin/odutil','set','log', logLevel]
	[output, returnCode] = azSubprocess(cmd)
	if returnCode != 0:
		logger.error('odSetLogging: Could not set requested logging level.')

def ntpUpdate(ntpServer):
	## Sets system clock.
	try:
		cmd = ['/usr/sbin/ntpdate','-u', ntpServer]
		[output, returnCode] = azSubprocess(cmd)
	except:
		try:
			cmd = ['/usr/bin/sntp', '-sS', ntpServer]
			[output, returnCode] = azSubprocess(cmd)
		except:
			logger.error('ntpUpdate: Could not set system clock using '+ntpServer+'.')
	
## CONFIGURATION PROFILE ROUTINES ##

def installConfigurationProfile(profilePath):
## Install the mobileconfig profile.
	# Defaults:
	installedProfile = False
	# Install the configuration profile. Try a few times in case the XPC helper throws an error.
	for tries in range(0,5):
		cmd = [ '/usr/bin/profiles','-I', '-F', profilePath ]
		[output,returnCode] = azSubprocess(cmd)
		if returnCode == 0 :
			installedProfile = True
			break
		else:
			time.sleep(5)
	# Return:
	return installedProfile

def listConfigurationProfilesForUser(requestedUser):
## Returns an array of all installed configuration profiles for the user.
	# Defaults:
	userProfilesArray = []
	configProfileTestPath = uniquePathFromPath('/private/tmp/com.amazon.acme.configProfileTestPath')
	# Query for the user config profiles:
	cmd = [ '/usr/bin/profiles','-L','-o',configProfileTestPath ]
	[output,returnCode] = azSubprocess(cmd)
	if returnCode == 0 :
		profileTestDict = azReadPlist(configProfileTestPath)
		# Try to set userProfilesArray - error cases when blank profileTestDict or no key for the user:
		try:
			userProfilesArray = profileTestDict[requestedUser]
		except:
			userProfilesArray = []
		ensureDeleted([configProfileTestPath])
	# Return:
	return userProfilesArray

def listConfigurationProfilesForComputer():
## Returns an array of all installed configuration profiles for the computer.
	# Defaults:
	computerProfilesArray = []
	configProfileTestPath = uniquePathFromPath('/private/tmp/com.amazon.acme.configProfileTestPath')
	# Query for the computer config profiles:
	cmd = [ '/usr/bin/profiles','-Cv','-o',configProfileTestPath ]
	[output,returnCode] = azSubprocess(cmd)
	if returnCode == 0 :
		profileTestDict = azReadPlist(configProfileTestPath)
		# Try to set computerProfilesArray - error cases when blank profileTestDict or no key _computerlevel:
		try:
			computerProfilesArray = profileTestDict['_computerlevel']
		except:
			computerProfilesArray = []
		ensureDeleted([configProfileTestPath])
	# Return:
	return computerProfilesArray

def removeConfigurationProfile(expectedProfileIdentifierPrefix):
## Removes any configuration profiles with identified containing expectedProfileIdentifierPrefix.
	# Defaults:
	profilesWereRemoved = False
	removalProfilesArray = []
	cumulativeReturnCodes = 0
	problem = 'no-problem'
	computerProfilesArray = listConfigurationProfilesForComputer()
	# Get list of profiles to remove:
	for profile in computerProfilesArray:
		# Scan through computer profiles array, building removalProfilesArray: 
		if profile['ProfileIdentifier'].find(expectedProfileIdentifierPrefix) != -1:
			removalProfilesArray.append(profile['ProfileIdentifier'])
	# If empty removalProfilesArray...
	if removalProfilesArray == []:
		# ...try removing the profile by identifier prefix directly.
		cmd = [ '/usr/bin/profiles','-R','-p',expectedProfileIdentifierPrefix ]
		[output,returnCode] = azSubprocess(cmd)
		if returnCode == 0:
			profilesWereRemoved = True
			logger.debug('removeConfigurationProfile: Removed profile with identifier ' + expectedProfileIdentifierPrefix + '.')
		else:
			logger.debug('removeConfigurationProfile: No profiles to remove.')
	# If non-empty removalProfilesArray:
	else:
		for profile in removalProfilesArray:
			cmd = [ '/usr/bin/profiles','-R','-p',profile ]
			[output,returnCode] = azSubprocess(cmd)
			cumulativeReturnCodes = cumulativeReturnCodes + returnCode
		if cumulativeReturnCodes == 0:
			profilesWereRemoved = True
			logger.debug('removeConfigurationProfile: Removed ' + str(len(removalProfilesArray)) + ' profile(s).')
		else:
			logger.error('removeConfigurationProfile: Profiles binary reported an error.')
			problem = 'error-running-profiles'
	# Return:
	return profilesWereRemoved, problem

## MISCELLANEOUS MANAGEMENT ROUTINES ##

def identifyComputerType():
## Returns the computer's model identifier.
	# Defaults:
	computerType = 'unknown-mac'
	# Run sysctl:
	cmd = ['/usr/sbin/sysctl','-n','hw.model']
	[output,returnCode] = azSubprocess(cmd)
	if returnCode == 0 and output:
		computerType = output.lower()
	# Return:
	return computerType

def getHardwareAddress(networkService):
## Returns the hardware "Ethernet" address for the given network service.
	# Defaults:
	hwAddr = ''
	# Run networksetup:
	cmd = ['/usr/sbin/networksetup','-getmacaddress',networkService]
	[output,returnCode] = azSubprocess(cmd)
	if returnCode == 0:
		hwAddr = output.split(' ')[2].replace(':','').lower()
	# Return:
	return hwAddr

def computerHasBuiltInWiFi():
	## Returns true iff the Mac has built-in Wi-Fi.
	# Defaults:
	hasBuiltInWiFi = False
	networkDict = {}
	# Run system profiler and look for built-in Wi-Fi:
	# If present, its type is AirPort (though it might not be en0).
	# Preferring system_profiler as networksetup lacks XML output.
	cmd = ['/usr/sbin/system_profiler','SPNetworkDataType','-xml']
	[output,returnCode] = azSubprocess(cmd)
	if (returnCode == 0) and output:
		systemProfilerOutputDict = azReadPlistFromStr(output)
		if systemProfilerOutputDict:
			networkDict = systemProfilerOutputDict[0]['_items']
			if networkDict:
				for i in range(0,len(networkDict)):
					try:
						theInterface = networkDict[i]['interface']
					except KeyError:
						theInterface = 'invalid'
					try:
						theInterfaceHardware = networkDict[i]['hardware']
					except KeyError:
						theInterfaceHardware = 'invalid'
					if theInterfaceHardware == 'AirPort':
						hasBuiltInWiFi = True
	# Return:
	return hasBuiltInWiFi

def computerHasBuiltInEthernet():
## Returns true iff the Mac has built-in Ethernet.
	# Defaults:
	hasBuiltInEthernet = False
	networkDict = {}
	# Run system profiler and look for built-in Ethernet:
	# If present, it's always en0, and type is Ethernet.
	# On systems lacking built-in Ethernet, en0 is of type AirPort (Wi-Fi).
	# Preferring system_profiler as networksetup lacks XML output.
	cmd = ['/usr/sbin/system_profiler','SPNetworkDataType','-xml']
	[output,returnCode] = azSubprocess(cmd)
	if (returnCode == 0) and output:
		systemProfilerOutputDict = azReadPlistFromStr(output)
		if systemProfilerOutputDict:
			networkDict = systemProfilerOutputDict[0]['_items']
			if networkDict:
				for i in range(0,len(networkDict)):
					try:
						theInterface = networkDict[i]['interface']
					except KeyError:
						theInterface = 'invalid'
					try:
						theInterfaceHardware = networkDict[i]['hardware']
					except KeyError:
						theInterfaceHardware = 'invalid'
					if (theInterfaceHardware == 'Ethernet') and (theInterface == 'en0'):
						hasBuiltInEthernet = True
	# Return:
	return hasBuiltInEthernet

def pickComputerName(randomCharacterSuffix):
## Set computer name in this fashion:
## First choice: address of network configuration named Ethernet (exactly).
## Second choice: address of network configuration named Ethernet 1 (Mac Pro systems).
## Third choice: address of network configuration named Wi-Fi (MacBook Air and Retina systems).
## Appends the randomCharacterSuffix if requested.
	# Gather information:
	computerType = identifyComputerType()
	hasBuiltInEthernet = computerHasBuiltInEthernet()
	# Pick a hardware address to use as a name:
	if hasBuiltInEthernet:
		# If Ethernet, use hardware address:
		if computerType.startswith('macpro'):
			computerName = getHardwareAddress('Ethernet 1')
		else:
			computerName = getHardwareAddress('Ethernet')
	else:
		# If no Ethernet, use hardware address of Wi-Fi:
		computerName = getHardwareAddress('Wi-Fi')
	# Add random characters:
	computerName = computerName + randomCharacterSuffix
	# Return:
	return computerName

def getComputerName():
## Returns the computer's name.
	# Defaults:
	computerName = ''
	# Run scutil:
	cmd = ['/usr/sbin/scutil','--get','ComputerName']
	[output,returnCode] = azSubprocess(cmd)
	if returnCode == 0:
		computerName = output.replace('\n','')
	# Return:
	return computerName

def setComputerName(requestedName,adDomain):
## Sets the computer's name.
	# Defaults:
	haveSetComputerName = False
	cmdsArray = []
	cumulativeReturnCodes = 0
	# Build cmdsArray:
	cmdsArray.append(['/usr/sbin/scutil','--set','ComputerName',requestedName])
	cmdsArray.append(['/usr/sbin/scutil','--set','LocalHostName',requestedName])
	cmdsArray.append(['/usr/sbin/scutil','--set','HostName',requestedName+'.'+adDomain])
	# Run scutil:
	for cmd in cmdsArray:
		[output,returnCode] = azSubprocess(cmd)
		cumulativeReturnCodes = cumulativeReturnCodes + returnCode
	if cumulativeReturnCodes == 0:
		haveSetComputerName = True
	# Return:
	return haveSetComputerName

## KEYCHAIN ROUTINES ##

def storePasswordInSystemKeychain(kcItem, kcAccount, kcPassword):
## Stores the specified credentials in the system keychain.
	# Defaults:
	systemKeychainPath = '/Library/Keychains/System.keychain'
	storedSuccessfully = False
	try:
		# Delete any existing - security will exit successfully if not found:
		cmd = ['/usr/bin/security','delete-generic-password', '-s', kcItem, systemKeychainPath]
		[output,returnCode] = azSubprocess(cmd)
		# Store new item:
		cmd = ['/usr/bin/security','add-generic-password', '-a', kcAccount, '-w', kcPassword, '-s', kcItem, systemKeychainPath]
		[output,returnCode] = azSubprocess(cmd)
		# Verify:
		if getPasswordFromKeychain(kcItem, '/Library/Keychains/System.keychain') == kcPassword:
			storedSuccessfully = True
	except:
		logger.error('storePasswordInSystemKeychain: Error accessing or modifying system the keychain.')
	# Return:
	return storedSuccessfully


def getPasswordFromKeychain(kcItem,kcPath):
	## Gets the specified password from the system keychain.
	# Defaults:
	kcPassword = 'invalid-password'
	#systemKeychainPath = '/Library/Keychains/System.keychain'
	cmd = ['/usr/bin/security','find-generic-password', '-w', '-g', '-s', kcItem, kcPath]
	[output,returnCode] = azSubprocess(cmd)
	if returnCode == 0:
		kcPassword = output.replace('\n','')
	else:
		logger.error('getPasswordFromKeychain: Error accessing ' + kcItem + ' from ' + kcPath)
	# Return:
	return kcPassword

def removeCertsByHashFromKeychain(certHashList,keychainPath):
## Removes the specified certificates by has from the specified keychain.
	for certHash in certHashList:
		cmd = ['/usr/bin/security','delete-certificate', '-Z', certHash, keychainPath]
		[output,returnCode] = azSubprocess(cmd)
		
def removeIdentityPrefsFromKeychain(identityPrefsList):
## Removes the specified identity prefs.
	for identityPref in identityPrefsList:
		cmd = ['/usr/bin/security','set-identity-preference', '-n', '-s', identityPref]
		[output,returnCode] = azSubprocess(cmd)

def scheduleReboot(minutesBeforeRestart):
## Schedules a restart minutesBeforeRestart minutes from now.
	cmd = ['/sbin/shutdown','-r', '+'+str(minutesBeforeRestart)]
	subprocess.Popen(cmd,stdin=None,stdout=None,stderr=None,close_fds=True)

#--------------------------------------------------------------------------------------------
#  Logger
#--------------------------------------------------------------------------------------------

def azLogger(azIdentifier):
	## Create logger
	logger = logging.getLogger(azIdentifier)
	
	## Check for root.
	[haveRootUser, userHomePath] = validateUserIsRoot()
	## Set log path
	if haveRootUser:
		log_location="/var/log/acme-assistants.log"
	else:
		homepath=os.path.expanduser('~')
		log_location=os.path.join(homepath, "Library/Logs/acme-assistants-user.log")
		## Create Logs folder if missing. Not present for accounts never logged in through GUI
		if not os.path.exists(os.path.join(homepath, "Library/Logs")):
			try:
				## Succeeds for local accounts
				os.makedirs(os.path.join(homepath, "Library/Logs"))
			except:
				## Fails for admin-role accounts through ssh, /var/empty is their home
				log_location="/private/tmp/acme-assistants-user.log"
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

azIdentifier = 'com.amazon.acme'
logger = azLogger(azIdentifier)
