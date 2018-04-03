#!/usr/bin/python
from __future__ import print_function

import subprocess
import csv
import sys
import argparse
import os
import exifread
import piexif
import config
import datetime
import pytz
from pytz import timezone
from tzlocal import get_localzone
from timezonefinder import TimezoneFinder
import collections
import bisect
from math import radians, cos, sin, asin, sqrt
from PIL import Image

secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)
null_date = '0000/00/00'
null_time = '00:00:00'
#bufsize = 1  # Use line buffering, i.e. output every line to the file.

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

    #secFract = SOW % 1
    epochTuple = gpsEpoch + (-1, -1, 0)
    t0 = time.mktime(epochTuple) - time.timezone  # mktime is localtime, correct for UTC
    tdiff = (gpsWeek * secsInWeek) + SOW - leapSecs
    t = t0 + tdiff
    (year, month, day, hh, mm, ss, dayOfWeek, julianDay, daylightsaving) = time.gmtime(t)

    # use gmtime since localtime does not allow to switch off daylight savings correction!!!

    return year, month, day, hh, mm, ss

def decimalDegrees2DMS(value, type):
    """
        Converts a Decimal Degree Value into
        Degrees Minute Seconds Notation.

        Pass value as double
        type = {Latitude or Longitude} as string

        returns a string as D:M:S:Direction
        created by: anothergisblog.blogspot.com
    """
    degrees = int(value)
    submin = abs((value - int(value)) * 60)
    minutes = int(submin)
    subseconds = abs((submin - int(submin)) * 60)
    direction = ""
    if type == "Longitude":
        if degrees < 0:
            direction = "W"
        elif degrees > 0:
            direction = "E"
        else:
            direction = ""
    elif type == "Latitude":
        if degrees < 0:
            direction = "S"
        elif degrees > 0:
            direction = "N"
        else:
            direction = ""
    degrees=abs(degrees) # Get rid of minus sign if present
    notation = str(degrees) + ":" + str(minutes) + ":" + \
               str(subseconds)[0:7] + "" + direction
    return notation

def get_image_file_list(uasPath, imageType,imageFileList):
    # Return a list of the names and sample date & time for all image files.

    # Get list of files in uas staging directory

    print("Fetching list of image files...")

    filestocheck = subprocess.check_output(['ls', '-1', uasPath], universal_newlines=True)

    afile = ''
    filelist = []

    for char in filestocheck:
        if char != '\n':
            afile += char
        else:
            filelist.append(afile)
            afile = ''

            # Get the subset of files that are the image files

    for f in filelist:
        imagePath=uasPath+f
        startPos = len(f) - 3
        endPos = len(f)
        isimagefile = (f != '' and f[startPos:endPos] == imageType)
        if isimagefile:
            imageFileList.append(imagePath)
    return imageFileList

def read_flight_log(flightLog):
    gpsEventDict = collections.OrderedDict()  # stores position,altitude,video status, interpolation indicator with time as key
    gpsEventList = []

    with open(flightLog, 'rU') as log:

        header = next(log)  # Skip the header row
        firstRow = next(log)  # Save the initial data row for comparison with next row
        rowFields = firstRow.split(',')
        latitude = float(rowFields[0])
        longitude = float(rowFields[1])
        tf = TimezoneFinder()
        tzone = tf.timezone_at(lng=longitude, lat=latitude)

        for row in log:  # Now start processing the rest of the log
            rowFields = row.split(',')
            latitude = float(rowFields[0])
            longitude = float(rowFields[1])
            altitude = float(rowFields[2]) * 0.3048
            lYear = rowFields[12][0:4]
            lMonth = rowFields[12][5:7]
            lDay = rowFields[12][8:10]
            lhours = rowFields[12][11:13]
            lminutes = rowFields[12][14:16]
            lsecs = rowFields[12][17:19]
            lmsecs = rowFields[12][20:]
            lDate = lYear + '/' + lMonth + '/' + lDay
            lTime = lhours + ':' + lminutes + ':' + lsecs + '.' + lmsecs
            takingPhoto = rowFields[27]
            #timestamp = int(rowFields[43])

            if takingPhoto =='0':

                # Convert the timestamp to UTC time

                #dateTimeStr = datetime.datetime.utcfromtimestamp(timestamp / 1000.0).strftime('%Y/%m/%d %H:%M:%S.%f')
                #DateStr = dateTimeStr.split(' ')[0]
                #TimeStr = dateTimeStr.split(' ')[1][0:12]
                DateStr=lDate
                TimeStr=lTime

                # Store the log data

                gpsEventList = [DateStr, TimeStr, latitude, longitude, altitude, takingPhoto]
                gpsEventDict[TimeStr]=gpsEventList
                #gpsEventDict[timestamp] = gpsEventList
                #print(gpsEventList)
    return gpsEventDict, tzone

def get_image_exif_data(filename):

    # Declare Tags for image EXIF data


    gpsDateTag          = 'GPS GPSDate'
    gpsLatTag           = 'GPS GPSLatitude'
    gpsLatRefTag        = 'GPS GPSLatitudeRef'
    gpsLongTag          = 'GPS GPSLongitude'
    gpsLongRefTag       = 'GPS GPSLongitudeRef'
    gpsTimeTag          = 'GPS GPSTimeStamp'
    exifImageDateTime   = 'EXIF DateTimeOriginal'

    tags = exifread.process_file(filename,strict=True)

    cam_position_x     = None
    cam_position_y     = None
    cam_position_z     = None
    cam_latitude       = None
    cam_longitude      = None
    cam_sample_date    = null_date
    cam_sample_time    = null_time
    cam_lat_zone       = None
    cam_long_zone      = None
    cam_altitude_ref   = None

    try:

    # Get Camera GPS Latitude and Longitude Data
        latRefStr       = str(tags[gpsLatRefTag])
        latStrLen       = len(str(tags[gpsLatTag]))-1
        latStr          = str(tags[gpsLatTag])[1:latStrLen]
        lat,latMins,latSecs = latStr.split(', ')
        if '/' in latSecs:
            latSecsNum,latSecsDenom = latSecs.split('/')
            latSecsDec = float(latSecsNum)/float(latSecsDenom)
        else:
            latSecsDec=float(latSecs)

        if latRefStr == "S":
            cam_latitude = (float(lat)+ (float(latMins)/60) + latSecsDec/3600) * (-1)
        elif latRefStr == "N":
            cam_latitude = (float(lat)+ (float(latMins)/60) + latSecsDec/3600)


        longRefStr      = str(tags[gpsLongRefTag])
        lonStrLen       = len(str(tags[gpsLongTag]))-1
        lonStr          = str(tags[gpsLongTag])[1:lonStrLen]
        lon,lonMins,lonSecs = lonStr.split(', ')
        if '/' in lonSecs:
            lonSecsNum,lonSecsDenom = lonSecs.split('/')
            lonSecsDec = float(lonSecsNum)/float(lonSecsDenom)
        else:
            lonSecsDec=float(lonSecs)

        if longRefStr == "W":
            cam_longitude = (float(lon)+ (float(lonMins)/60) + lonSecsDec/3600) * (-1)
        elif longRefStr == "E":
            cam_longitude = (float(lon)+ (float(lonMins)/60) + lonSecsDec/3600)

    # Get Camera Image Time

        if exifImageDateTime in tags:
            pass

        if gpsTimeTag in tags:

            timeStrLen = len(str(tags[gpsTimeTag]))-1
            timeStr = str(tags[gpsTimeTag])[1:timeStrLen]
            if '/' in timeStr:
                hrs, mins, secsFract = timeStr.split(', ')
                secsNum,secsDenom = secsFract.split('/')
                secs = str(int(secsNum)/int(secsDenom))
            else:
                hrs, mins, secs = timeStr.split(', ')
            cam_sample_time=hrs.zfill(2)+':'+mins.zfill(2)+':'+secs.zfill(2)

    except Exception,e:
        print('*** Error*** Unable to process image file EXIF data for ')
        print( '*** Error Code:',e)
        print('*** Null EXIF-based column values will be generated for',filename)
    return cam_latitude, cam_longitude,cam_sample_time

def get_date_taken(path):
    return Image.open(path)._getexif()[36867]

flightLog='/Users/mlucas/Desktop/Harman/2017-10-19_16-03-28/2017-10-19_16-03-28_v2.csv'
uasPath = '/Users/mlucas/Desktop/Harman/2017-10-19_16-03-28/'
uasmetfile = '/Users/mlucas/Desktop/Harman/2017-10-19_16-03-28/2017-10-19_16-03-28_metadata.csv'
imageType='jpg'

gpsEvents,localTimeZone =read_flight_log(flightLog)
sortedKeys=list(sorted(gpsEvents.keys()))

if len(gpsEvents)==0:
    print("There were no gps events found in", flightLog)
    print

imageFileList=[]
imageFiles = get_image_file_list(uasPath,imageType,imageFileList)
imageCount = len(imageFiles)
if imageCount==0:
    print("There were no image files found in ",uasPath)
    print("Exiting")
metadataList=[]
for image in imageFiles:
    #dt = subprocess.call(['exiftool', '-DateTimeOriginal', image])
    dt = subprocess.Popen(['exiftool', '-DateTimeOriginal', image],stdout=subprocess.PIPE)
    output, err = dt.communicate()
    dateTimeStr=output.split(': ')[1]
    imageTimeStr=dateTimeStr.split(' ')[1]
    imageTimeStr=imageTimeStr[0:12]
    #print(image,imageTimeStr)
    logTimeIndex = bisect.bisect_left(sortedKeys, imageTimeStr)
    gpsEventsKey = sortedKeys[logTimeIndex]
    gpsEvents[gpsEventsKey].extend((logTimeIndex, imageTimeStr,image))
    latitude=str(gpsEvents[gpsEventsKey][2])
    longitude=str(gpsEvents[gpsEventsKey][3])
    imageName=gpsEvents[gpsEventsKey][8]
    print("Image Name",imageName,"Latitude:",latitude,"Longitude:",longitude,)
    ll=subprocess.Popen(['exiftool', '-GPSLatitude='+latitude,'-GPSLongitude='+longitude, imageName], stdout=subprocess.PIPE)
    output, err = ll.communicate()
    imageFileName=imageName.split('/')[-1]
    metadataRecord=[imageFileName,latitude,longitude]
    metadataList.append(metadataRecord)
    pass

#
# Write out the metadata file
#
with open(uasmetfile, 'wb') as csvfile:
    header = csv.writer(csvfile,lineterminator = '\n')
    header.writerow(['image_file_name', 'latitude','longitude'])
csvfile.close()

with open(uasmetfile, 'ab') as csvfile:
    print('Generating metadata file', uasmetfile)
    for lineitem in metadataList:
        fileline = csv.writer(csvfile,lineterminator = ',\n')
        fileline.writerow(
            [lineitem[0], lineitem[1], lineitem[2]])

csvfile.close()

sys.exit()
