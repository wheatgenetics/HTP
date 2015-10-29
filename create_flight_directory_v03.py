#!/usr/bin/python
#
# Program: create_flight_directory.py
#
#
# Version: 0.3 March 10,2015
#
# Removed return of SecFract from UTC from GPS. Caused seconds to be incremented to 60 when rounded up.
#
# Version: 0.2 February 12,2015
#
# Appended 'uas_' to the flight id to identify data collected by the uav system.
#
# Version: 0.1 December 8,2014
#
# Creates the directory to hold data for a given flight on the field computer.
#
# Command Line Inputs:
#
#
# '-l' or '--log':      'The full path to apm log file. '
# '-f' or '--flight':   'The full path to the directory where the flight folder is to be created, e.g. /home/ ')
#
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

    return (year, month, day, hh, mm, ss)

def get_flightid_from_apm_log(fapmLog):
    fapmList = []
    try:
        with open(fapmLog,'rU') as logfile:
            log = logfile.readlines()
            for row in log:
                if row[0:3]=='GPS':
                    rowFields  = row.split(',')
                    gpsSecs    = round(float(rowFields[2])/1000,2)
                    gpsWeek    = int(rowFields[3])
                    (lyear, lmonth,lday,lhh,lmm,lsecs) = UTCFromGps(gpsWeek, gpsSecs, 16)
                    timeAndDate = (str(lyear).zfill(4),str(lmonth).zfill(2),str(lday).zfill(2),str(lhh).zfill(2),
                                   str(lmm).zfill(2),str(lsecs).zfill(2))
                    fapmList.append(timeAndDate)
        flightId =  'uas_'+fapmList[0][0]+fapmList[0][1]+fapmList[0][2] + '_' + \
                    fapmList[0][3]+fapmList[0][4]+fapmList[0][5] + '_' + \
                    fapmList[-1][0]+fapmList[-1][1]+fapmList[-1][2] + '_' + \
                    fapmList[-1][3]+fapmList[-1][4]+fapmList[-1][5]
        startDate = datetime.date(int(fapmList[0][0]),int(fapmList[0][1]),int(fapmList[0][2]))
        startTime = datetime.time(int(fapmList[0][3]),int(fapmList[0][4]), int(fapmList[0][5]))
        endDate = datetime.date(int(fapmList[-1][0]),int(fapmList[-1][1]),int(fapmList[-1][2]))
        endTime = datetime.time(int(fapmList[-1][3]),int(fapmList[-1][4]), int(fapmList[-1][5]))
    except Exception,e:
        print '*** Error*** Unable to process log file. Please check that the file specified is a valid APM log. '
        print '*** Error Code:',e
        sys.exit()

    return(flightId,startDate,startTime,endDate,endTime)

# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-l', '--log', help='The full path to apm log file. ')
cmdline.add_argument('-f', '--flight',help='The full path to the directory where the flight folder is to be created.')

args = cmdline.parse_args()

logFile = args.log
flightPath = args.flight

# Determine Flight ID based on first and last GPS measurements in the APM log file

try:
    print
    print 'Processing Log File:',logFile
    print
    flightInfo = get_flightid_from_apm_log(logFile)
    flightID = flightInfo[0]
    flightFolder = flightPath+flightID
    imageFolder = flightFolder + '/' + flightID + '_images'
except:
    print
    print '*** Error *** There was a problem processing the specified file. '
    print '              Please check that the path to the file and that the file exists.'
    sys.exit()


if not os.path.exists(flightFolder):
    print 'Creating Flight Data Folder :',flightFolder
    print
    os.mkdir(flightFolder)
else:
    print
    print '*** Warning*** The folder:',flightFolder, 'already exists.'
    print
    sys.exit()

# Exit the program gracefully

print
print '*** Flight folder',flightFolder,'created.'
print '*** Please move all files associated with the flight into this folder.'
print
print 'Processing Completed. Exiting...'
sys.exit()
