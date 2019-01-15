#!/System/Library/Frameworks/Python.framework/Versions/Current/bin/python

## Mac Quarantine: patches_module.py
## Copyright Amazon
## Written by Gerrit DeWitt (gerritd@amazon.com)
## Version 1.5.0.2 - 2017-12-11
## Project started 2013-04-22

import subprocess, os, sys, syslog, time, datetime, plistlib
from common_quarantine import *

def runTool():
	# Default:
	prefsValid = False
	patchList = {}
	
	# Paths:
	oldToolPath = '/usr/local/amazon/bin/updatebadger'
	newToolPath = '/usr/local/amazon/bin/acmeupdates'
	oldPrefsPath = '/Library/Preferences/com.amazon.UpdateBadger.plist'
	newPrefsPath = '/Library/Preferences/com.amazon.acme.updates.plist'
	if os.path.exists(newToolPath):
		toolPath = newToolPath
		prefsPath = newPrefsPath
	else:
		toolPath = oldToolPath
		prefsPath = oldPrefsPath

	# Run acmeupdates and read its preferences:  #work: check perms on file, check date keys in file
	logger.info('Running %s...' % toolPath)
	cmd = [toolPath,'--listUpdates']
	[output,returnCode] = azSubprocess(cmd)
	if returnCode != 0:
		logger.error('%s exited with non-zero status!' % toolPath)

	if not os.path.exists(prefsPath):
		logger.error('Missing %s!' % prefsPath)
	else:
		try:
			prefsDict = plistlib.readPlist(prefsPath)
			patchList = prefsDict['patchList']
		except KeyError:
			logger.error('Missing necessary keys in %s.' % prefsPath)

	# Return:
	return prefsValid, patchList
	
def patchIsEligible(patchDict,desiredSourceDomain):
## Returns true if a patch should be counted.

	# Defaults:
	isValidPatch = False
	patchName = patchDict['name']
	patchSourceDomain = 'invalid-source-domain'
	patchGracePeriod = 0
	patchIsFirmware = False

	# Check source domain; toss this patch out if it's not what we're checking for.
	try:
		patchSourceDomain = str(patchDict['sourceDomain'])
	except KeyError:
		logger.error('No sourceDomain key; ignoring.')
		
	if patchSourceDomain.find(desiredSourceDomain) == -1:
		logger.info('Skipping %s (not desired domain).' % patchName)
	else:
		isValidPatch = True
		logger.info('Considering %s (matches desired domain).' % patchName)

		# Check grace period; toss any that have zero grace period.
		# This is a required key.  Having a zero grace period means non-mandatory update.
		# We default to a zero grace period so that malformed entries (missing this key) appear to be non-mandatory.
		try:
			patchGracePeriod = int(patchDict['gracePeriod'])
		except KeyError:
			logger.error('No gracePeriod key; assuming gracePeriod=0 (non-mandatory update).')
		if patchGracePeriod == 0:
			isValidPatch = False
			logger.info('Skipping %s (grace period indicates is optional).' % patchName)

		# Check isFirmware key; toss out updates whose isFirmware key is true.
		# This is an optional key, so default to NOT firmware:
		try:
			patchIsFirmware = patchDict['isFirmware']
		except KeyError:
			logger.error('No isFirmware key; assuming isFirmware=NO.')
			if patchIsFirmware:
				isValidPatch = False
			logger.info('Skipping %s (is firmware).' % patchName)

	# Return:
	return isValidPatch

def determineCompliance(patchList, desiredSourceDomain):
## Count required patches to determine compliance.
	# Defaults:
	isCompliant = False
	patchCount = 0

	# Count patches:
	for thePatch in patchList:
		try:
			patchDict = patchList[thePatch]
			if patchIsEligible(patchDict,desiredSourceDomain):
				patchCount = patchCount + 1
		except KeyError:
			logger.error('Invalid dict for patch.')
	logger.info('Patch count: %s' % str(patchCount))

	# Determine compliance:
	if patchCount == 0:
		isCompliant = True

	# Return:
	return isCompliant


def validateArgs():
## Validates arguments, and returns true/false.
	desiredSourceDomain = 'invalid-domain'
	outputPath = 'invalid-path'
	argsValid = False
	if len(sys.argv) == 2:
		if (sys.argv[1].find('--firstparty') != -1) or (sys.argv[1].find('--OSPatch') != -1) :
			desiredSourceDomain = 'com.apple.softwareupdate'
			outputPath = outputPathForModuleNamed('OSPatch')
			argsValid = True
	
		if (sys.argv[1].find('--thirdparty') != -1) or (sys.argv[1].find('--3PPatch') != -1) :
			desiredSourceDomain = 'com.amazon'
			outputPath = outputPathForModuleNamed('3PPatch')
			argsValid = True
	# Return:
	return argsValid, desiredSourceDomain, outputPath

def main():
## MAIN
	[argsValid, desiredSourceDomain, outputPath] = validateArgs()
		
	if not argsValid:
		print '''
-------
Quarantine module: patches_module.py
Version 1.5.0.2 Copyright Amazon
This tool is usually called by quarantine.
-------
USAGE: patches_module.py --firstparty | --thirdparty
'''
	else:
		# Read output file:
		try:
			outputDict = plistlib.readPlist(outputPath)
		except:
			outputDict = {}
		
		# Date stamp:
		outputDict['moduleLastCheck'] = datetime.datetime.utcnow()

		# Run ACME Updates and get patchList:
		[prefsValid, patchList] = runTool()

		# Determine compliance:
		outputDict['moduleCompliant'] = determineCompliance(patchList, desiredSourceDomain)

		# Write outputDict:
		if outputDict != {}:
			plistlib.writePlist(outputDict,outputPath)

if __name__ == '__main__':
	global azIdentifier
	global logger
	azIdentifier = 'com.amazon.acme.quarantine.patches_module.py'
	logger = azLogger(azIdentifier)
	try:
		main()
	except:
		logger.error('Generic exception.')
