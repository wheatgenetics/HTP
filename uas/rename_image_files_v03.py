#!/usr/bin/python
#
# Program: rename_image_files.py
#
# Version: 0.3 March 31,2015
#
# Handled different cases where number of CAM events in autopilot log is not equal to number of image files found.
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
#


__author__ = 'mlucas'

import subprocess
import os
import time
import math
import sys
import argparse
import exifread

# Declare Tags for image EXIF data

gpsDateTag = 'GPS GPSDate'
gpsTimeTag = 'GPS GPSTimeStamp'


secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)

bufsize = 1  # Use line buffering, i.e. output every line to the file.


def UTCFromGps(gpsWeek, SOW, leapSecs=16):
    # A Python implementation of GPS related time conversions.
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

    epochTuple = gpsEpoch + (-1, -1, 0)
    t0 = time.mktime(epochTuple) - time.timezone  # mktime is localtime, correct for UTC
    tdiff = (gpsWeek * secsInWeek) + SOW - leapSecs
    t = t0 + tdiff
    (year, month, day, hh, mm, ss, dayOfWeek, julianDay, daylightsaving) = time.gmtime(t)

    # use gmtime since localtime does not allow to switch off daylight savings correction!!!

    return year, month, day, hh, mm, ss


def getimagefilenames(fimageFileList):
    # Return a list of image files

    print("Fetching list of image files to rename...")

    files_to_check = subprocess.check_output(['ls', '-1', uavPath], universal_newlines=True)

    afile = ''
    filelist = []

    for char in files_to_check:
        if char != '\n':
            afile += char
        else:
            filelist.append(afile)
            afile = ''

            # Get the subset of files that are the image files which have naming format IMG_nnnn.CR2

    for f in filelist:
        isimagefile = (f != '' and f[0:3] == 'IMG')
        if isimagefile:
            imageFileList.append(f)

    return fimageFileList


def getcamevents(fcamTriggerList):
    with open(apmLog, 'rU') as logfile:
        log = logfile.readlines()
        for row in log:
            if row[0:3] == 'CAM':
                rowfields = row.split(',')
                fgpsSecs = round(float(rowfields[1]) / 1000, 2)
                fgpsWeek = int(rowfields[2])
                (fyear, fmonth, fday, fhh, fmm, fsecs) = UTCFromGps(fgpsWeek, fgpsSecs, 16)
                secs = math.trunc(round(fsecs))
                imageYear = str(fyear).zfill(4)
                imageMonth = str(fmonth).zfill(2)
                imageDay = str(fday).zfill(2)
                imageHour = str(fhh).zfill(2)
                imageMinute = str(fmm).zfill(2)
                imageSec = str(secs).zfill(2)
                imageDateTime = imageYear + imageMonth + imageDay + '_' + imageHour + imageMinute + imageSec
                camTriggerList.append(imageDateTime)
    return fcamTriggerList

def get_image_exif_datetime(ifilename):

    tags = exifread.process_file(image)

    try:

    # Get Camera Image Date and Time

        dateStr=str(tags[gpsDateTag])
        year,month,day = dateStr.split(':')
        fcam_sample_date=year+month+day

        timeStrLen = len(str(tags[gpsTimeTag]))-1
        timeStr = str(tags[gpsTimeTag])[1:timeStrLen]
        if '/' in timeStr:
            hrs, mins, secsFract = timeStr.split(', ')
            secsNum,secsDenom = secsFract.split('/')
            secs = str(int(secsNum)/int(secsDenom))
        else:
            hrs, mins, secs = timeStr.split(', ')

        fcam_sample_time=hrs.zfill(2)+mins.zfill(2)+secs.zfill(2)


    except Exception,e:
        print '*** Error*** Unable to process image file EXIF data.'
        print '*** Error Code:',e
        print '*** None EXIF-based column values will be generated for',ifilename

    return fcam_sample_date,fcam_sample_time

# Get command line input.

cmdline = argparse.ArgumentParser()
cmdline.add_argument('-c', '--cam', help='Camera Serial Number')
cmdline.add_argument('-a', '--auto', help='Autopilot log name')
cmdline.add_argument('-d', '--dir', help='Beocat directory path to HTP image files',
                     default='/homes/mlucas/uav_incoming/')

args = cmdline.parse_args()

uavCam = args.cam
apmLog = args.auto
#uavPath = args.dir + '/'
uavPath = args.dir

imageFileList = []
camTriggerList = []


# Get the list of image files found in the uas staging directory

getimagefilenames(imageFileList)

# Open the apm log file and get all CAM trigger messages.

getcamevents(camTriggerList)

# Compare number of CAM trigger messages to number of images. If the numbers do not match print message and exit.

print
print "There were", len(imageFileList), "image files found."
print "There were", len(camTriggerList), "camera trigger events found."
print

if len(imageFileList)==0:
    print "There were no image files found. Exiting..."
    sys.exit()

elif len(camTriggerList)==0:
    print "There were no CAM events found in the autopilot log. Exiting..."
    sys.exit()

elif len(imageFileList) == len(camTriggerList):

# Get CAM event date and time and use them to rename the file
    print "Using autopilot CAM event date time to rename images."
    print
    imageIndex = 0
    for ifile in imageFileList:
        imagefile = uavPath + ifile
        newfilename = uavPath + uavCam + '_' + camTriggerList[imageIndex] + '_' + ifile
        os.rename(imagefile, newfilename)
        imageIndex += 1
        print 'old file name: ', imagefile
        print 'new file name: ', newfilename
        print ' '
elif len(imageFileList) < len(camTriggerList):

# Get EXIF event date and time and use them to rename the file
    print "The number of image files is less than the number of CAM events"
    print "Using image EXIF date time to rename images."
    print
    for ifile in imageFileList:
        imagefile = uavPath + ifile
        with open(imagefile,'rb') as image:
            idate,itime = get_image_exif_datetime(imagefile)
        image.close()
        newfilename = uavPath + uavCam + '_' + idate + '_' + itime + '_' + ifile
        os.rename(imagefile, newfilename)
        print 'old file name: ', imagefile
        print 'new file name: ', newfilename
        print ' '
else:
    print "The number of image files is greater than the number of CAM events."
    print
    print "Please check for and remove any extraneous test images that should not be included in data set"
    print "and then re-run this program to rename image files."
    print
    print "Exiting..."
    sys.exit()

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()

