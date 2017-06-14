#!/usr/bin/python
#
# Program: create_flight_directory.py
#
# Version: 0.1 December 8,2014
#
# Creates the directory to hold data for a given flight under the production directory
#
# Command Line Inputs:
#
#
# '-d' or '--dir':      'The path to the directory to store the flight folder, e.g. /home/ '
#
#

__author__ = 'mlucas'

import os
import os.path
import argparse
import subprocess
import sys
import re
import time
import datetime

secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)

def get_autopilot_file_list(uavPath):
    # Return a list of the names and sample date & time for all apm files in the uav_incoming directory.

    apmfilelist = []

    # Get list of files in uav staging directory

    print("Fetching list of image files...")

    filestocheck = subprocess.check_output(['ls', '-1', uavPath], universal_newlines=True)

    afile = ''
    filelist = []

    for char in filestocheck:
        if char != '\n':
            afile += char
        else:
            filelist.append(afile)
            afile = ''

# Get the subset of files that are the apm files (ideally there should be only one.)
    for f in filelist:
        k = re.search(r'apm', f)
        isapmfile = (f != '' and k != None)
        if isapmfile:
            filepath = uavPath+f
            apmfilelist.append(filepath)

    return apmfilelist

def UTCFromGps(gpsWeek, SOW, leapSecs=16):
    #  A Python implementation of GPS related time conversions.
    #
    # Copyright 2002 by Bud P. Bruegger, Sistema, Italy
    # mailto:bud@sistema.it
    # http://www.sistema.it
    #
    # Modifications for GPS seconds by Duncan Brown
    #
    # PyUTCFromGpsSeconds added by Ben Johnson
    # Converts gps week and seconds to UTC
    #
    # SOW = seconds of week
    # gpsWeek is the full number (not modulo 1024)
    #
    # The number of GPS leap seconds in 2014 is 16
    #

    secFract = SOW % 1
    epochTuple = gpsEpoch + (-1, -1, 0)
    t0 = time.mktime(epochTuple) - time.timezone  #mktime is localtime, correct for UTC
    tdiff = (gpsWeek * secsInWeek) + SOW - leapSecs
    t = t0 + tdiff
    (year, month, day, hh, mm, ss, dayOfWeek, julianDay, daylightsaving) = time.gmtime(t)

     #use gmtime since localtime does not allow to switch off daylight savings correction!!!

    return (year, month, day, hh, mm, ss + secFract)

def get_flightid_from_apm_log(fapmLog):
    fapmList = []
    with open(fapmLog,'rU') as logfile:
        log = logfile.readlines()
        for row in log:
            if row[0:3]=='GPS':
                rowFields  = row.split(',')
                gpsSecs    = round(float(rowFields[2])/1000,2)
                gpsWeek    = int(rowFields[3])
                (lyear, lmonth,lday,lhh,lmm,lsecs) = UTCFromGps(gpsWeek, gpsSecs, 16)
#                if round(math.modf(lsecs)[0],2) == 0.0:
#                    print lyear,lmonth,lday,lhh,lmm,lsecs
#                    key = str(lyear)+str(lmonth)+str(lday)+str(lhh)+str(lmm)+str(int(lsecs))
#                    print(key)
                timeAndDate = (str(lyear).zfill(4),str(lmonth).zfill(2),str(lday).zfill(2),str(lhh).zfill(2),
                               str(lmm).zfill(2),str(int(round(lsecs))).zfill(2))
                fapmList.append(timeAndDate)
    flightId =  fapmList[0][0]+fapmList[0][1]+fapmList[0][2] + '_' + \
                fapmList[0][3]+fapmList[0][4]+fapmList[0][5] + '_' + \
                fapmList[-1][0]+fapmList[-1][1]+fapmList[-1][2] + '_' + \
                fapmList[-1][3]+fapmList[-1][4]+fapmList[-1][5]
    startDate = datetime.date(int(fapmList[0][0]),int(fapmList[0][1]),int(fapmList[0][2]))
    startTime = datetime.time(int(fapmList[0][3]),int(fapmList[0][4]), int(fapmList[0][5]))
    endDate = datetime.date(int(fapmList[-1][0]),int(fapmList[-1][1]),int(fapmList[-1][2]))
    endTime = datetime.time(int(fapmList[-1][3]),int(fapmList[-1][4]), int(fapmList[-1][5]))

    return(flightId,startDate,startTime,endDate,endTime)

# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-l', '--log', help='The full path to apm log file. ')
cmdline.add_argument('-f', '--flight', help='The full path to the directory where the flight folder is to be created, e.g. /home/ ')



args = cmdline.parse_args()

flightPath = args.flight
logPath = args.log

# Determine Flight ID based on first and last GPS measurements in the APM log file

apmFiles = get_autopilot_file_list(logPath)
if len(apmFiles) == 1:
    apmFile = apmFiles[0]
    print 'Processing Log File:',apmFile
    flightInfo = get_flightid_from_apm_log(apmFile)
    logIndex = 0
else:
    if len(apmFiles) > 1:
        print
        print "*** Warning ***: More than one APM file found..."
        index = 0
        print
        for f in apmFiles:
            print index, apmFiles[index]
            index +=1
        print
        logIndex = int(input('Please select the index of the log file related to the flight of interest:'))
        apmFile = apmFiles[logIndex]
        print
        print 'Processing Log File:',apmFile
        flightInfo = get_flightid_from_apm_log(apmFile)

    else:
        print
        print '*** Error *** APM log file not found in',logPath
        print
        sys.exit()

flightID = flightInfo[0]
flightFolder = flightPath+flightID

if not os.path.exists(flightFolder):
    print 'Creating Directory :',flightFolder
    os.mkdir(flightFolder)
else:
    print
    print '*** Warning*** The folder:',flightFolder, 'already exists.'
    print

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
