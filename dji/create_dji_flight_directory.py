#!/usr/bin/python
#
# Program: create_flight_directory.py
#
#
# Version: 0.1
#
# Creates a uav flight directory for DJI log data
#
#
#
# '-l' or '--log':      'The full path to DJI flight log file. '
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

def get_flightid_from_dji_log(fdjiLog):
    fdjiList = []
    try:
        with open(fdjiLog,'rU') as logfile:
            log = logfile.readlines()
            rowCount=0
            for row in log:
                rowCount += 1
                if rowCount > 1:
                    rowFields = row.split(',')
                    lYear = rowFields[11][0:4]
                    lMonth = rowFields[11][5:7]
                    lDay = rowFields[11][8:10]
                    lhours = rowFields[11][11:13]
                    lminutes = rowFields[11][14:16]
                    lsecs = rowFields[11][17:19]
                    timeAndDate = (str(lYear).zfill(4),str(lMonth).zfill(2),str(lDay).zfill(2),str(lhours).zfill(2),
                                   str(lminutes).zfill(2),str(lsecs).zfill(2))
                    fdjiList.append(timeAndDate)
        flightId =  'uas_'+fdjiList[0][0]+fdjiList[0][1]+fdjiList[0][2] + '_' + \
                    fdjiList[0][3]+fdjiList[0][4]+fdjiList[0][5] + '_' + \
                    fdjiList[-1][0]+fdjiList[-1][1]+fdjiList[-1][2] + '_' + \
                    fdjiList[-1][3]+fdjiList[-1][4]+fdjiList[-1][5]
        startDate = datetime.date(int(fdjiList[0][0]),int(fdjiList[0][1]),int(fdjiList[0][2]))
        startTime = datetime.time(int(fdjiList[0][3]),int(fdjiList[0][4]), int(fdjiList[0][5]))
        endDate = datetime.date(int(fdjiList[-1][0]),int(fdjiList[-1][1]),int(fdjiList[-1][2]))
        endTime = datetime.time(int(fdjiList[-1][3]),int(fdjiList[-1][4]), int(fdjiList[-1][5]))
    except Exception,e:
        print '*** Error*** Unable to process log file. Please check that the file specified is a valid dji log. '
        print '*** Error Code:',e
        sys.exit()

    return(flightId,startDate,startTime,endDate,endTime)

# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-l', '--log', help='The full path to dji log file. ')
cmdline.add_argument('-f', '--flight',help='The full path to the directory where the flight folder is to be created.')

args = cmdline.parse_args()

logFile = args.log
flightPath = args.flight

# Determine Flight ID based on first and last GPS measurements in the DJI log file

try:
    print
    print 'Processing Log File:',logFile
    print
    flightInfo = get_flightid_from_dji_log(logFile)
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
