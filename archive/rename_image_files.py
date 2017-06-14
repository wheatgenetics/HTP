#!/usr/bin/python
#
# Program: rename_image_files.py
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
import sys
import argparse

# Get command line input.

cmdline = argparse.ArgumentParser()
cmdline.add_argument('-c', '--cam', help='Camera Serial Number',
                     default='CAM_432032012149')

cmdline.add_argument('-d', '--dir', help='Beocat directory path to HTP image files',
                     default='/homes/mlucas/uav_incoming/')

args = cmdline.parse_args()

uavCam  = args.cam
uavPath = args.dir

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

# Get the subset of files that are the image files

imagefilelist = []

for f in filelist:
    isimagefile = (f != '' and f[0:3] =='IMG')
    if isimagefile:
        imagefilelist.append(f)

# Get the modification date of each file and rename the file

for f in imagefilelist:
    imagefile = uavPath + f
    utcTime     = time.gmtime(os.path.getmtime(imagefile))
    print 'utc time: ', utcTime
    imageTime   = time.gmtime(os.path.getmtime(imagefile))
    imageYear   = str(imageTime.tm_year).zfill(4)
    imageMonth  = str(imageTime.tm_mon).zfill(2)
    imageDay    = str(imageTime.tm_mday).zfill(2)
    imageHour   = str(imageTime.tm_hour).zfill(2)
    imageMinute = str(imageTime.tm_min).zfill(2)
    imageSec    = str(imageTime.tm_sec).zfill(2)
    newfile     = uavPath + uavCam + '_' + imageYear + imageMonth + imageDay  + '_' + imageHour + imageMinute + imageSec + '_' + f
    os.rename(imagefile,newfile)
    print 'old file name: ', imagefile
    print 'new file name: ', newfile
    print ' '

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()

