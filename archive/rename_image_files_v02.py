#!/usr/bin/python
#
# Program: rename_image_files.py
#
# Version: 0.2 February 6,2015
#
# Uses the time derived from CAM trigger signal as the image time.
#
# Version: 0.1 November 4,2014
#
# Renames Canon CR2 Image Files in HTP format for prototype
#
# The date and time used is the time that the image file was created.
#
# This will need to be changed to get the date and time from the autopilot log file when the camera trigger
# information becomes available.
#
# Command Line Inputs:
#
#
# '-c' or '--cam':      'Camera Serial Number'
# '-d' or '--dir':      'Beocat directory path to HTP image files', default='/homes/mlucas/uav_incoming/'
#


__author__ = 'mlucas'

import subprocess
import os
import time
import math
import sys
import argparse

secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)

bufsize = 1 # Use line buffering, i.e. output every line to the file.

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

# Get command line input.

cmdline = argparse.ArgumentParser()
cmdline.add_argument('-c', '--cam', help='Camera Serial Number')
cmdline.add_argument('-a', '--auto', help='Autopilot log name')
cmdline.add_argument('-d', '--dir', help='Beocat directory path to HTP image files',
                     default='/homes/mlucas/uav_incoming/')

args = cmdline.parse_args()

uavCam  = args.cam
apmLog  = args.auto
uavPath = args.dir + '/'

# Get list of files in uav staging directory

print("Fetching list of image files to rename...")

files_to_check = subprocess.check_output(['ls', '-1', uavPath], universal_newlines=True)

afile = ''
filelist = []
db_key = ''

for char in files_to_check:
    if char != '\n':
        afile += char
    else:
        filelist.append(afile)
        afile = ''

# Get the subset of files that are the image files which have naming format IMG_nnnn.CR2

imageFileList  = []
camTriggerList = []

for f in filelist:
    isimagefile = (f != '' and f[0:3] =='IMG')
    if isimagefile:
        imageFileList.append(f)

# Open the apm log file and get all CAM trigger messages.

apmDict = {}
with open(apmLog,'rU') as logfile:
    log = logfile.readlines()
    for row in log:
        if row[0:3]=='CAM':
            rowFields  = row.split(',')
            gpsSecs    = round(float(rowFields[1])/1000,2)
            gpsWeek    = int(rowFields[2])
            (lyear, lmonth,lday,lhh,lmm,lsecs) = UTCFromGps(gpsWeek, gpsSecs, 16)
            secs = math.trunc(round(lsecs))
            imageYear   = str(lyear).zfill(4)
            imageMonth  = str(lmonth).zfill(2)
            imageDay    = str(lday).zfill(2)
            imageHour   = str(lhh).zfill(2)
            imageMinute = str(lmm).zfill(2)
            imageSec    = str(secs).zfill(2)
            imageDateTime   = imageYear + imageMonth + imageDay  + '_' + imageHour + imageMinute + imageSec
            camTriggerList.append(imageDateTime)

# Compare number of CAM trigger messages to number of images. If the numbers do not match print message and exit.

if len(camTriggerList) != len(imageFileList):
    print "The number of image files is different from the number of Camera Trigger events."
    print "There were", len(imageFileList),"image files found."
    print "There were", len(camTriggerList),"camera trigger events found."
    print "Exiting..."
    sys.exit()


# Get the modification date of each file and rename the file

imageIndex=0
for f in imageFileList:
    imagefile = uavPath + f
    newfilename     = uavPath + uavCam + '_' + camTriggerList[imageIndex]+ '_'  + f
    os.rename(imagefile,newfilename)
    imageIndex+=1
    print 'old file name: ', imagefile
    print 'new file name: ', newfilename
    print ' '

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()

