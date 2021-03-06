#!/usr/bin/python
#
# Program: create_uas_dji_x3_metadata_file
#
#
# Version: 0.1 May 23,2016
#
# Creates CSV file containing image metadata to be imported into the uas_images table in the wheatgenetics database.
#
# Command Line Inputs:
#
#
# '-d' or '--dir':      'Beocat directory path to UAV image files', default='/homes/mlucas/uas_incoming/'
# '-l' or '--log':      'Flight log path
# '-t' or '--type':     'Image file type, e.g. CR2, JPG'
# '-f' or '--framerate':'Frame rate (Hz)
# '-c' or '-camera':    'Camera Serial Number'
# '-o' or '--out':      'Output file path and filename'
#
#

__author__ = 'mlucas'

import subprocess
import csv
import time
import math
import sys
import argparse
import hashlib
import os
import exifread
import piexif

import utm
import datetime
import pytz
from pytz import timezone
from tzlocal import get_localzone
from timezonefinder import TimezoneFinder

import collections
import bisect



secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)
null_date = '0000/00/00'
null_time = '00:00:00'
epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)

bufsize = 1  # Use line buffering, i.e. output every line to the file.

# Declare Tags for image EXIF data
gpsAltTag       = 'GPS GPSAltitude'
gpsAltRefTag    = 'GPS GPSAltitudeRef'
gpsLatTag       = 'GPS GPSLatitude'
gpsLatRefTag    = 'GPS GPSLatitudeRef'
gpsLongTag      = 'GPS GPSLongitude'
gpsLongRefTag   = 'GPS GPSLongitudeRef'
TimeTag         = 'Time Codes'
DateTag         = 'EXIF DateTimeDigitized'

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
        startPos = len(f) - 3
        endPos = len(f)
        isimagefile = (f != '' and f[startPos:endPos] == imageType)
        if isimagefile:
            imageFileList.append(f)
    return imageFileList

def interpolate_time(fflightLog):
    gpsEventDict = collections.OrderedDict()  # stores position,altitude,video status, interpolation indicator with time as key
    gpsEventList = []
    rowCount = 0
    timestamp = 0
    seedTime = 0
    newTime = 0
    prevTime = 0
    nextTime = 0
    lastTime=0
    timeDelta = 0
    takingVideo = '0'
    prevLat = 0.0
    nextLat = 0.0
    prevLong = 0.0
    nextLong = 0.0
    prevAlt = 0.0
    nextAlt = 0.0
    latitude = 0.0
    longitude = 0.0
    altitude = 0.0
    with open(fflightLog, 'rU') as log:

        header = next(log)  # Skip the header row

        previous = next(log)  # Save the initial data row for comparison with next row
        rowCount+=1
        rowFields = previous.split(',')
        latitude = float(rowFields[0])
        longitude = float(rowFields[1])
        utmPosition = utm.from_latlon(latitude, longitude)
        utmPositionX = utmPosition[0]
        utmPositionY = utmPosition[1]
        utmLatZone = utmPosition[2]
        utmLongZone = utmPosition[3]
        altitude = float(rowFields[2]) * 0.3048  # Convert altitude in feet to meters
        lYear = rowFields[11][0:4]
        lMonth = rowFields[11][5:7]
        lDay = rowFields[11][8:10]
        lhours = rowFields[11][11:13]
        lminutes = rowFields[11][14:16]
        lsecs = rowFields[11][17:19]
        lmsecs = rowFields[11][20:]
        lDate = lYear + '/' + lMonth + '/' + lDay
        lTime = lhours + ':' + lminutes + ':' + lsecs + '.' + lmsecs
        takingVideo = rowFields[37]
        timestamp = int(rowFields[43])
        interpolated = False  # The row is read directly from the file and is therefore not interpolated
        tf = TimezoneFinder()
        tzone = tf.timezone_at(lng=longitude, lat=latitude)

        prevTime = timestamp
        prevLat = latitude
        prevLong = longitude
        prevAlt = altitude

        # Store the log data for first row if video was enabled

        if takingVideo == '1':
            # Convert the timestamp to UTC time

            dateTimeStr = datetime.datetime.utcfromtimestamp(timestamp / 1000.0).strftime('%Y/%m/%d %H:%M:%S.%f')
            DateStr = dateTimeStr.split(' ')[0]
            TimeStr = dateTimeStr.split(' ')[1]

            # Store the first row of the log
            gpsEventList = [DateStr, TimeStr, latitude, longitude, altitude, takingVideo, interpolated, timestamp,
                            utmPositionX, utmPositionY, utmLatZone, utmLongZone]
            gpsEventDict[timestamp] = gpsEventList

        for row in log:  # Now start processing the rest of the log
            rowCount+=1
            rowFields = row.split(',')
            latitude = float(rowFields[0])
            longitude = float(rowFields[1])
            utmPosition = utm.from_latlon(latitude, longitude)
            utmPositionX = utmPosition[0]
            utmPositionY = utmPosition[1]
            utmLatZone = utmPosition[2]
            utmLongZone = utmPosition[3]
            altitude = float(rowFields[2]) * 0.3048
            lYear = rowFields[11][0:4]
            lMonth = rowFields[11][5:7]
            lDay = rowFields[11][8:10]
            lhours = rowFields[11][11:13]
            lminutes = rowFields[11][14:16]
            lsecs = rowFields[11][17:19]
            lmsecs = rowFields[11][20:]
            lDate = lYear + '/' + lMonth + '/' + lDay
            lTime = lhours + ':' + lminutes + ':' + lsecs + '.' + lmsecs
            takingVideo = rowFields[37]
            timestamp = int(rowFields[43])
            interpolated = False
            #print utmPositionX,utmPositionY,altitude
            if takingVideo == '1':

                # Always store the data from the row read from the log

                # Convert the timestamp to UTC time

                dateTimeStr = datetime.datetime.utcfromtimestamp(timestamp / 1000.0).strftime('%Y/%m/%d %H:%M:%S.%f')
                DateStr = dateTimeStr.split(' ')[0]
                TimeStr = dateTimeStr.split(' ')[1]

                # Store the log data

                gpsEventList = [DateStr, TimeStr, latitude, longitude, altitude, takingVideo, interpolated, timestamp,
                                utmPositionX, utmPositionY, utmLatZone, utmLongZone]
                gpsEventDict[timestamp] = gpsEventList

                # Start interpolation

                timeDelta = timestamp - prevTime
                latDelta = latitude - prevLat
                longDelta = longitude - prevLong
                altDelta = altitude - prevAlt


                if seedTime == 0:
                    seedTime = round((timestamp + 5) / 1000.0, 2) * 1000
                    newTime = int(seedTime)

                # Normal sampling interval is 100ms. The interpolated sampling interval is 10 ms.
                # If the difference between the two times of interest is > 20 ms perform interpolation

                lastTime = prevTime
                if abs(timeDelta) > 10:
                    interpolated = True
                    newLat=prevLat
                    newLong=prevLong
                    newAlt=prevAlt
                    while newTime < timestamp:
                        timeFraction = (newTime - lastTime) / timeDelta
                        if latDelta > 0.0:
                            newLat += abs(latDelta) * timeFraction
                        elif latDelta < 0.0:
                            newLat -= abs(latDelta) * timeFraction
                        if longDelta > 0.0:
                            newLong += abs(longDelta) * timeFraction
                        elif longDelta < 0.0:
                            newLong -= abs(longDelta) * timeFraction
                        if altDelta > 0.0:
                            newAlt += abs(altDelta) * timeFraction
                        elif altDelta < 0.0:
                            newAlt -= abs(altDelta) * timeFraction
                        newTimeStr = datetime.datetime.utcfromtimestamp(newTime / 1000.0).strftime(
                            '%Y/%m/%d %H:%M:%S.%f')
                        newDateStr = newTimeStr.split(' ')[0]
                        newTimeStr = newTimeStr.split(' ')[1]
                        newUtmPosition = utm.from_latlon(newLat, newLong)
                        newUtmPositionX = newUtmPosition[0]
                        newUtmPositionY = newUtmPosition[1]
                        newUtmLatZone = newUtmPosition[2]
                        newUtmLongZone = newUtmPosition[3]
                        gpsEventList = [newDateStr, newTimeStr, newLat, newLong, newAlt, takingVideo, interpolated,
                                        newTime, newUtmPositionX, newUtmPositionY, newUtmLatZone, newUtmLongZone]
                        gpsEventDict[newTime] = gpsEventList
                        lastTime = newTime
                        newTime += 10
                        #print '*',newUtmPositionX, newUtmPositionY, newAlt
                        #time.sleep(0.1)
                else:
                    newTime += 0
            prevTime = timestamp
            prevLat = latitude
            prevLong = longitude
            prevAlt = altitude

    return gpsEventDict, tzone


def get_image_utm_position(fgpsLatitude, fgpsLongitude):
    futmPosition = utm.from_latlon(fgpsLatitude, fgpsLongitude)

    return futmPosition

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

def get_image_exif_data(ffilename):

    tags = exifread.process_file(image)

    try:
        # Get Camera Image Date and Time

        dateStr=str(tags[DateTag])
        localTz=timezone(localTimeZone)
        fcamTime = datetime.datetime.strptime(dateStr, "%Y:%m:%d %H:%M:%S")
        local_dt = localTz.localize(fcamTime, is_dst=None)
        utc_dt=local_dt.astimezone(pytz.utc)
        #rawCamTime=(utc_dt - epoch).total_seconds() * 1000.0
        #fcamTimeUTCInSecs = int(round(((utc_dt - epoch).total_seconds() + (int(fframeNumber) * (1.0 / 24.0)))*1000.0))
        #float_camTimeUTCInSecs=fcamTimeUTCInSecs + (int(fframeNumber) * (1.0 / 24.0))
        fcamTimeUTCInSecs = int(round(((utc_dt - epoch).total_seconds() + (int(fframeNumber) * (1.0 / framerate))) * 1000.0))
        float_camTimeUTCInSecs = fcamTimeUTCInSecs + (int(fframeNumber) * (1.0 / framerate))

    except Exception,e:
        print '*** Error*** Unable to process image file EXIF data for '
        print '*** Error Code:',e
        print '*** Null EXIF-based column values will be generated for',ffilename
    return float_camTimeUTCInSecs,fframeNumber



def init_metadata_record():
    record_id=None
    imagefilename=None
    uas_position_x=None
    uas_position_y=None
    uas_position_z=None
    uas_latitude=None
    uas_longitude=None
    uas_sample_date_utc=null_date
    uas_sample_time_utc=null_time
    uas_latzone=None
    uas_longzone=None
    uas_altitude_ref='AGL'
    cam_position_x=None
    cam_position_y=None
    cam_position_z=None
    cam_latitude=None
    cam_longitude=None
    cam_sample_date_utc=null_date
    cam_sample_time_utc=null_time
    cam_lat_zone=None
    cam_long_zone=None
    cam_altitude_ref=None
    f_checksum=None
    notes=''
    blankrow = [record_id, imagefilename, flightId, sensor_id, uas_position_x, uas_position_y,
                uas_position_z, uas_latitude, uas_longitude, uas_sample_date_utc, uas_sample_time_utc,
                uas_latzone, uas_longzone, uas_altitude_ref, cam_position_x, cam_position_y, cam_position_z,
                cam_latitude, cam_longitude, cam_sample_date_utc, cam_sample_time_utc, cam_lat_zone,
                cam_long_zone, cam_altitude_ref, f_checksum, notes]
    return blankrow


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


# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat directory path to HTP imagefiles',
                     default='/homes/mlucas/uas_incoming/')

cmdline.add_argument('-l', '--log', help='Flight Log File name')

cmdline.add_argument('-t', '--type', help='Image file type extension, e.g. CR2, JPG',
                     default='CR2')

cmdline.add_argument('-c', '--camera', help='Camera serial number')

cmdline.add_argument('-f', '--framerate',help='Frame rate (Hz)')


args = cmdline.parse_args()

uasFolderList=[]
uasFolderPaths = args.dir
uasFolderList=uasFolderPaths.split(',')

flightLog = args.log
imageType = args.type
frameRate = float(args.framerate)
frameInterval=(1.0/frameRate)*1000.0
print 'Frame Rate',frameRate,'Time Between Frames (ms)',frameInterval
sensor_id=args.camera
print ' '
print "Camera ID: ",sensor_id

record_id = None
notes = ''


gpsEvents,localTimeZone =interpolate_time(flightLog)
sortedKeys=list(sorted(gpsEvents.keys()))
#**********************************************
with open('gpsEvents.csv', 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(
        ['newTime','newDateStr', 'newTimeStr', 'newLat', 'newLong', 'newAlt', 'takingVideo', 'interpolated','newTime', 'newUtmPositionX', 'newUtmPositionY', 'newUtmLatZone', 'newUtmLongZone'])
csvfile.close()

with open('gpsEvents.csv', 'ab') as csvfile:
    print 'Generating gpsEvents file', '/Users/mlucas/Desktop/gpsEvents.csv'
    for lineitem in sorted(gpsEvents.iteritems()):
        fileline = csv.writer(csvfile)
        fileline.writerow([lineitem[0], lineitem[1][0], lineitem[1][1], lineitem[1][2], lineitem[1][3], lineitem[1][4], lineitem[1][5], lineitem[1][6],lineitem[1][7], lineitem[1][8], lineitem[1][9], lineitem[1][10],lineitem[1][11]])
csvfile.close()
#**********************************************

# Get the list of image files available for the flight/
imageFileList = []
metadatalist = []

for uasPath in uasFolderList:
    imagefiles = get_image_file_list(uasPath,imageType,imageFileList)


if len(imagefiles)==0:
    print "There were no image files found in ",uasPath
    print "Exiting"
    sys.exit(10)

if len(gpsEvents)==0:
    print "There were no gps events found in", flightLog
    print

fltStartString=datetime.datetime.utcfromtimestamp(min(gpsEvents.keys())/1000.0)
fltEndString=datetime.datetime.utcfromtimestamp(max(gpsEvents.keys())/1000.0)
fltStart=time.gmtime(min(gpsEvents.keys())/1000.0)
fltEnd=time.gmtime(max(gpsEvents.keys())/1000.0)
print ''
print 'Flight Start: ',fltStartString
print 'Flight End: ',fltEndString

flightId='uas_'+ str(fltStart[0])+str(fltStart[1]).zfill(2)+str(fltStart[2]).zfill(2)+ '_' + \
         str(fltStart[3]).zfill(2) + str(fltStart[4]).zfill(2) + str(fltStart[5]).zfill(2) + '_' + \
         str(fltEnd[0]) + str(fltEnd[1]).zfill(2) + str(fltEnd[2]).zfill(2) + '_' + \
         str(fltEnd[3]).zfill(2) + str(fltEnd[4]).zfill(2) + str(fltEnd[5]).zfill(2)
print ''
print 'Flight ID:', flightId

#
# Define UAS Metadata Output File Path
#
uasMetadataFile= uasPath + flightId + '_metadata.csv'
print ''
print 'UAS Metadata Output File: ',uasMetadataFile

#Initialize timestamp to the first timestamp in the log

timestamp=sortedKeys[0] + 5
#print timestamp
frameIndex = 0
logTimeIndex= 0
try:
    for f in imagefiles:
        metadata_record=init_metadata_record()
        imagefilename=f
        metadata_record[0]=record_id
        metadata_record[2]=flightId
        metadata_record[3]=sensor_id
    #
    #Get Date, Time and Frame Number From Image File EXIF metadata
    #
    #    with open(filename_dng,'rb') as image:
    #        print "Processing ",filename_dng
    #        cam_time_in_seconds,cam_frame_number=get_image_exif_data(filename_dng)
    #    image.close()
    #
    # Interpolate Longitude,Latitude and Altitude of image
    # Need to convert date/time/frame to timestamp
    #

    # Find the time index in GPS events that is less than or equal to the frame time

        #logTimeIndex = bisect.bisect_right(sortedKeys, timestamp + 5)
        a=timestamp + 5
        logTimeIndex=0
        logTimeIndex = bisect.bisect_right(sortedKeys, timestamp + 5)
        gpsEventsKey = gpsEvents.keys()[logTimeIndex]
        #print logTimeIndex, "{0:.3f}".format(gpsEventsKey), "{0:.3f}".format(timestamp), "{0:.3f}".format(a)
        gpsEvents[gpsEventsKey].extend((logTimeIndex,timestamp, abs(gpsEventsKey - timestamp),imagefilename))
        uas_position_x=gpsEvents[gpsEventsKey][8]
        uas_position_y=gpsEvents[gpsEventsKey][9]
        uas_position_z=gpsEvents[gpsEventsKey][4]
        uas_latitude=gpsEvents[gpsEventsKey][2]
        uas_longitude=gpsEvents[gpsEventsKey][3]
        uas_sample_date_utc=gpsEvents[gpsEventsKey][0]
        uas_sample_time_utc=gpsEvents[gpsEventsKey][1]
        uas_latzone=gpsEvents[gpsEventsKey][10]
        uas_longzone=gpsEvents[gpsEventsKey][11]

        metadata_record[0]=record_id
        metadata_record[2]=flightId
        metadata_record[3]=sensor_id
        metadata_record[4] = uas_position_x
        metadata_record[5] = uas_position_y
        metadata_record[6] = uas_position_z
        metadata_record[7] = uas_latitude
        metadata_record[8] = uas_longitude
        metadata_record[9] = uas_sample_date_utc
        metadata_record[10] = uas_sample_time_utc
        metadata_record[11] = uas_latzone
        metadata_record[12] = uas_longzone

        imgDate = uas_sample_date_utc[0:4] + uas_sample_date_utc[5:7] + uas_sample_date_utc[8:10]
        imgTime = uas_sample_time_utc[0:2] + uas_sample_time_utc[3:5] + uas_sample_time_utc[6:8]
        newimagefilepath = uasPath + sensor_id + '_' + imgDate + '_' + imgTime + '_' + imagefilename
        newimagefilename = sensor_id + '_' + imgDate + '_' + imgTime + '_' + imagefilename
        oldimagefilepath = uasPath + imagefilename
        os.rename(oldimagefilepath, newimagefilepath)
        print 'old file name: ', oldimagefilepath
        print 'new file name: ', newimagefilepath
        print ' '


        metadata_record[24] = calculate_checksum(newimagefilepath)
        metadata_record[0] = record_id
        metadata_record[1] = newimagefilename

        metadatalist.append(metadata_record)
        print ''
        #
        # Update the EXIF GPS data for the JPEG images
        #
        latDMS=decimalDegrees2DMS(uas_latitude,'Latitude')
        latFields=latDMS.split(':')
        latDegrees=int(latFields[0])
        latMins=int(latFields[1])
        latSecs=float(latFields[2][0:(len(latFields[2])-1)])
        latRef=latFields[2][-1]
        lonDMS=decimalDegrees2DMS(uas_longitude,'Longitude')
        lonFields=lonDMS.split(':')
        lonDegrees=int(lonFields[0])
        lonMins=int(lonFields[1])
        lonSecs = float(lonFields[2][0:(len(lonFields[2]) - 1)])
        lonRef = lonFields[2][-1]
        referenceAltitude = 311.0 # Rocky Ford Altitude in Meters
        altRef=0 # Above Sea Level
        altMeters=uas_position_z+referenceAltitude
        #print uas_position_z,referenceAltitude,altMeters
        exif_sample_date_utc=uas_sample_date_utc[0:4]+'-'+uas_sample_date_utc[5:7]+'-'+ uas_sample_date_utc[8:10]
        exif_sample_time_utc=uas_sample_time_utc[0:12]
        dateTimeStr=exif_sample_date_utc + ' ' + exif_sample_time_utc
        #print dateTimeStr
        try:
            with open(newimagefilepath, 'rb') as image:
                print "Writing EXIF data for ", newimagefilepath
                print ''
                exif_dict=piexif.load(newimagefilepath)
                exif_dict['GPS'][piexif.GPSIFD.GPSLatitudeRef]=latRef
                exif_dict['GPS'][piexif.GPSIFD.GPSLatitude]=((latDegrees,1),(latMins,1),(int(latSecs * 1000.0),1000))
                exif_dict['GPS'][piexif.GPSIFD.GPSLongitudeRef]=lonRef
                exif_dict['GPS'][piexif.GPSIFD.GPSLongitude] = ((lonDegrees, 1), (lonMins, 1), (int(lonSecs * 1000.0), 1000))
                exif_dict['GPS'][piexif.GPSIFD.GPSAltitudeRef]=altRef
                exif_dict['GPS'][piexif.GPSIFD.GPSAltitude]=(int(altMeters * 1000.0), 1000)
                exif_dict['Exif'][piexif.ExifIFD.DateTimeOriginal] = dateTimeStr
                exif_bytes=piexif.dump(exif_dict)
                piexif.insert(exif_bytes,newimagefilepath)
            image.close()

        except:
            pass
        timestamp += frameInterval
        frameIndex += 1

except Exception, e:
    print '*** Error*** Unable to process image file ',newimagefilepath
    print '*** Error Code:', e
    print '*** Trying to continue...'
    pass


with open(uasMetadataFile, 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(
        ['record_id', 'image_file_name','flight_id', 'sensor_id', 'uas_position_x',
         'uas_position_y', 'uas_position_z', 'uas_latitude','uas_longitude','uas_sampling_date_utc','uas_sampling_time_utc',
         'uas_lat_zone', 'uas_long_zone','uas_altitude_reference','cam_position_x','cam_position_y', 'cam_position_z',
         'cam_latitude','cam_longitude','cam_sampling_date_utc','cam_sampling_time_utc','cam_lat_zone', 'cam_long_zone',
         'cam_altitude_reference', 'md5sum', 'notes'])
csvfile.close()

with open(uasMetadataFile, 'ab') as csvfile:
    print 'Generating metadata file', uasMetadataFile
    for lineitem in metadatalist:
        fileline = csv.writer(csvfile)
        fileline.writerow(
            [lineitem[0], lineitem[1], lineitem[2], lineitem[3], lineitem[4], lineitem[5], lineitem[6], lineitem[7],
             lineitem[8], lineitem[9], lineitem[10], lineitem[11], lineitem[12], lineitem[13], lineitem[14],
             lineitem[15],lineitem[16],lineitem[17],lineitem[18],lineitem[19],lineitem[20],lineitem[21],lineitem[22],
             lineitem[23],lineitem[24]])
csvfile.close()

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
