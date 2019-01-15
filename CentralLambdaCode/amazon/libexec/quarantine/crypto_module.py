#!/System/Library/Frameworks/Python.framework/Versions/Current/bin/python

## Mac Quarantine: crypto_module.py
## Copyright Amazon
## Written by Gerrit DeWitt (gerritd@amazon.com)
## Version 1.5.0.2 - 2017-12-11
## Project started 2013-04-22

import subprocess, os, sys, syslog, time, datetime, plistlib
from common_quarantine import *

def runTool():
	# Defaults:
	isCompliant = False
	encryptionRequired = True
	encryptionEnabled = False
	
	# Paths:
	oldToolPath = '/usr/local/amazon/assistant/tools/detool'
	newToolPath = '/usr/local/amazon/bin/detool'
	oldPrefsPath = '/Library/Preferences/com.amazon.assistant.detool.plist'
	newPrefsPath = '/Library/Preferences/com.amazon.acme.detool.plist'
	if os.path.exists(newToolPath):
		toolPath = newToolPath
		prefsPath = newPrefsPath
	else:
		toolPath = oldToolPath
		prefsPath = oldPrefsPath
	
	# Run detool and read its preferences.
	# Include a few retries to cover timing issues (socket for detool being unavailable, helper busy).
	for i in range(0,10):
		# Sleep and retry:
		if i >= 1:
			logger.info('Running detool again (try %s of 10)...' % str(i+1))
			time.sleep(60)
		logger.info('Running %s...' % toolPath)
		cmd = [toolPath,'status']
		[output,returnCode] = azSubprocess(cmd)
		if returnCode != 0:
			logger.error('%s exited with non-zero status!' % toolPath)

		if not os.path.exists(prefsPath):
			logger.error('Missing %s!' % prefsPath)
		else:
			try:
				prefsDict = plistlib.readPlist(prefsPath)
				detoolLastCheck = prefsDict['lastCheck']
				encryptionRequired = prefsDict['status_required']
				encryptionEnabled = prefsDict['status_encryption']
				try:
					bootVolumeConversionStatus = prefsDict['details']['bootVolumeConversionStatus'].lower()
				except KeyError:
					bootVolumeConversionStatus = 'unknown'
					logger.error('Missing bootVolumeConversionStatus key in details dict in %s.  Choosing unknown state.' % prefsPath)
				if abs(datetime.datetime.utcnow() - detoolLastCheck) >= datetime.timedelta(seconds=60):
						logger.error('Last check key indicates detool preferences file is stale.')
				else:
					# Compliant if exempt:
					if not encryptionRequired:
						isCompliant = True
					# Compliant if required, enabled, and finished encrypting:
					if (encryptionRequired and encryptionEnabled and bootVolumeConversionStatus == 'complete'):
						isCompliant = True
					# Compliant if required, enabled, and currently encrypting:
					if (encryptionRequired and encryptionEnabled and bootVolumeConversionStatus == 'converting'):
						isCompliant = True
					# Prefs present, keys present, lastCheck current.  Tool ran successfully; exit for loop:
					break
			except KeyError:
				logger.error('Missing necessary keys in %s.' % prefsPath)

	# Return:
	return isCompliant, encryptionRequired, encryptionEnabled

def main():
	## MAIN
	outputPath = outputPathForModuleNamed('Crypto')
	
	# Read output file:
	try:
		outputDict = plistlib.readPlist(outputPath)
	except:
		outputDict = {}

	# Date stamp:
	outputDict['moduleLastCheck'] = datetime.datetime.utcnow()

	# Determine compliance:
	[isCompliant, encryptionRequired, encryptionEnabled] = runTool()
	outputDict['moduleCompliant'] = isCompliant
	outputDict['fileVaultEnabled'] = encryptionEnabled
	outputDict['fileVaultShouldBeEnabled'] = encryptionRequired

	# Write outputDict:
	if outputDict != {}:
		plistlib.writePlist(outputDict,outputPath)

if __name__ == '__main__':
	global azIdentifier
	global logger
	azIdentifier = 'com.amazon.acme.quarantine.crypto_module.py'
	logger = azLogger(azIdentifier)
	try:
		main()
	except:
		logger.error('Generic exception.')
