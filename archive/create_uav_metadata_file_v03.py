#!/usr/bin/python
#
# Program: create_uav_metadata_file
#
# Version: 0.3 February 16,2015
#
# Removed experiment field from metadata file
# Added Latitude and Longitude fields to metadata file
#
# Version: 0.2 February 3,2015
#
# Modifications to use CAM trigger time instead of GPS trigger as reference for image position
# Removed get_camera_id function since camera_id is determined in the controlling bash script
# Added capability to populate flight ID using date/time of first and last GPS readings
#
# Version: 0.1 November 5,2014
#
# Creates CSV file containing image metadata to be imported into the uav_images table in the wheatgenetics database.
#
# Command Line Inputs:
#
#
# '-d' or '--dir':      'Beocat directory path to HTP image files', default='/homes/mlucas/uav_incoming/'
# '-c' or '--cam':      'Camera ID'
# '-a' or '--autop':    'Autopilot log path
# '-t' or '--type':     'Image file type, e.g. CR2, JPG'
# '-o' or '--out':      'Output file path and filename'
#

__author__ = 'mlucas'

import subprocess
import csv
import time
import math
import utm
import sys
import argparse
import hashlib

secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)

bufsize = 1  # Use line buffering, i.e. output every line to the file.


def get_image_file_list(fuavPath, fimageType):
    # Return a list of the names and sample date & time for all image files.

    imagefilelist = []

    # Get list of files in uav staging directory

    print("Fetching list of image files...")

    filestocheck = subprocess.check_output(['ls', '-1', fuavPath], universal_newlines=True)

    afile = ''
    filelist = []

    for char in filestocheck:
        if char != '\n':
            afile += char
        else:
            filelist.append(afile)
            afile = ''

            # Get the subset of files that are the image files

    for ff in filelist:
        startPos = len(ff) - 3
        endPos = len(ff)
        isimagefile = (ff != '' and ff[startPos:endPos] == fimageType)
        if isimagefile:
            imagefilelist.append(ff)

    return imagefilelist


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

    secFract = SOW % 1
    epochTuple = gpsEpoch + (-1, -1, 0)
    t0 = time.mktime(epochTuple) - time.timezone  # mktime is localtime, correct for UTC
    tdiff = (gpsWeek * secsInWeek) + SOW - leapSecs
    t = t0 + tdiff
    (year, month, day, hh, mm, ss, dayOfWeek, julianDay, daylightsaving) = time.gmtime(t)

    # use gmtime since localtime does not allow to switch off daylight savings correction!!!

    return year, month, day, hh, mm, ss + secFract


def get_image_utm_position(fgpsLatitude, fgpsLongitude):
    futmPosition = utm.from_latlon(fgpsLatitude, fgpsLongitude)

    return futmPosition


def get_apm_log_info(fapmLog):
    camEventList = []
    gpsEventList = []
    print "Reading apm log", fapmLog
    with open(fapmLog, 'rU') as logfile:
        log = logfile.readlines()
        for row in log:
            if row[0:3] == 'CAM':
                rowFields = row.split(',')
                gpsSecs = round(float(rowFields[1]) / 1000, 2)
                gpsWeek = int(rowFields[2])
                gpsLat = float(rowFields[3])
                gpsLong = float(rowFields[4])
                gpsAlt = float(rowFields[6])
                (lyear, lmonth, lday, lhh, lmm, lsecs) = UTCFromGps(gpsWeek, gpsSecs, 16)
                secs = math.trunc(round(lsecs))
                imageYear = str(lyear).zfill(4)
                imageMonth = str(lmonth).zfill(2)
                imageDay = str(lday).zfill(2)
                imageHour = str(lhh).zfill(2)
                imageMinute = str(lmm).zfill(2)
                imageSec = str(secs).zfill(2)
                camEventData = [imageYear, imageMonth, imageDay, imageHour, imageMinute, imageSec, gpsLat, gpsLong,
                                gpsAlt]
                camEventList.append(camEventData)
            else:
                if row[0:3] == 'GPS':
                    rowFields = row.split(',')
                    gpsSecs = round(float(rowFields[2]) / 1000, 2)
                    gpsWeek = int(rowFields[3])
                    (lyear, lmonth, lday, lhh, lmm, lsecs) = UTCFromGps(gpsWeek, gpsSecs, 16)
                    timeAndDate = (str(lyear).zfill(4), str(lmonth).zfill(2), str(lday).zfill(2), str(lhh).zfill(2),
                                   str(lmm).zfill(2), str(int(round(lsecs))).zfill(2))
                    gpsEventList.append(timeAndDate)
    return camEventList, gpsEventList


def hashfilelist(afile, blocksize=65536):
    hasher = hashlib.md5()
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()


def calculate_checksum(ffilename):
    checksum = hashfilelist(open(ffilename, 'rb'))
    return checksum


# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat directory path to HTP imagefiles',
                     default='/homes/mlucas/uav_incoming/')

cmdline.add_argument('-c', '--cam', help='Camera ID')

cmdline.add_argument('-a', '--auto', help='Autopilot log name',
                     default='a.log')

cmdline.add_argument('-t', '--type', help='Image file type extension, e.g. CR2, JPG',
                     default='CR2')

cmdline.add_argument('-o', '--out', help='Output file path and filename',
                     default='/homes/mlucas/uav_incoming/uav_image_metadata.csv')

args = cmdline.parse_args()

uavPath = args.dir + '/'
sensor_id = args.cam
apmLog = args.auto
imageType = args.type
uavmetfile = args.out

sequence_number = None
notes = ''
metadatalist = []

imagefiles = get_image_file_list(uavPath, imageType)
camEvents, gpsEvents = get_apm_log_info(apmLog)
flightId = gpsEvents[0][0] + gpsEvents[0][1] + gpsEvents[0][2] + '_' + \
           gpsEvents[0][3] + gpsEvents[0][4] + gpsEvents[0][5] + '_' + \
           gpsEvents[-1][0] + gpsEvents[-1][1] + gpsEvents[-1][2] + '_' + \
           gpsEvents[-1][3] + gpsEvents[-1][4] + gpsEvents[-1][5]

print 'Flight ID:', flightId

camIndex = 0
for f in imagefiles:
    filename = uavPath + f
    imagefilename = f
    sampledate = camEvents[camIndex][0] + '/' + camEvents[camIndex][1] + '/' + camEvents[camIndex][2]
    sampletime = camEvents[camIndex][3] + ':' + camEvents[camIndex][4] + ':' + camEvents[camIndex][5]
    time.sleep(0.1)
    gpsLatitude = camEvents[camIndex][6]
    gpsLongitude = camEvents[camIndex][7]
    uavUtmPosition = get_image_utm_position(gpsLatitude, gpsLongitude)
    absolute_position_x = uavUtmPosition[0]
    absolute_position_y = uavUtmPosition[1]
    absolute_position_z = camEvents[camIndex][8]
    latzone = uavUtmPosition[2]
    longzone = uavUtmPosition[3]
    f_checksum = calculate_checksum(filename)
    metadatalist.append([sequence_number, imagefilename,flightId, sensor_id, absolute_position_x, absolute_position_y,
                         absolute_position_z, latzone, longzone, gpsLatitude, gpsLongitude, sampledate, sampletime, f_checksum, notes])
    camIndex += 1

with open(uavmetfile, 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(
        ['sequence_number', 'image_file_name','flight_id', 'sensor_id', 'absolute_sensor_position_x',
         'absolute_sensor_position_y', 'absolute_sensor_position_z', 'lat_zone', 'long_zone','latitude','longitude', 'sampling_date',
         'sampling_time', 'md5sum', 'notes'])
csvfile.close()

with open(uavmetfile, 'ab') as csvfile:
    print 'Generating metadata file', uavmetfile
    for lineitem in metadatalist:
        fileline = csv.writer(csvfile)
        fileline.writerow(
            [lineitem[0], lineitem[1], lineitem[2], lineitem[3], lineitem[4], lineitem[5], lineitem[6], lineitem[7],
             lineitem[8], lineitem[9], lineitem[10], lineitem[11], lineitem[12], lineitem[13], lineitem[14]])
csvfile.close()

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
