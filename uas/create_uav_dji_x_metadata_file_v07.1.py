#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import unicode_literals
#
# Program: create_uas_dji_x_metadata_file
#
# Version 0.7.1 Modifications to write metadata file compatible with revised uas_images database table.
# Made Latitude/Longitude degree correction command line parameters. Eliminated unused functions.
#
#
# Version 0.7 March 29,2018 Modifications to operate with revised latitude/longitude based database plot_map table
#
# Version 0.6 July 25,2017 Added capability to find nearest plot for a point associated with an image and to generate
# an output data file containing an ordered list of plots that the flight path actually intersected for each range.
#
# Version 0.6 June 30,2017 Added capability to offset latitude and longitude degrees -
# Currently hard-coded but will be added as parameters later. Corrected ordering of lat_zone and long_zone
# parameters.
#
# Version 0.5 June 2,2017 Added capability to define multi-line flight path for more accurate plot intersection
# calculation. Added parameters for experiment e.g. 17ASH, lat zone and long zone
#
# Version 0.4 May 25,2017 Added capability to compute plot intersections for each range and store in file.
#                    Added capability to define output path for output files instead of using input path.
#
# Version: 0.3 May 22,2017 Interpolate position assuming equal time between images over the range flown, instead of
# using image timestamps.
#
# Version: 0.2 May 9,2017 Update to handle multiple image folders.
#
# Version: 0.1 May 23,2016
#
# This program is used to pre-process image data acquired by a DJI X5R camera. The program uses the DJI log file  to
# assign a position to each image and renames the images in order to provide a unique name for each image. It also
# creates a CSV file containing image metadata to be imported into the uas_images table in the wheatgenetics database.
#
# The X5R flight data is segmented by range. This means that the video recording is turned on at the beginning of
# a range and then turned off when the end of a range is reached. This pattern is repeated for each range that is
# covered by the overall flight plan. The log parameter isTakingVideo (0 or 1) is used to determine the set of log
# entries associated with each range: isTakingVideo = 1 means the UAV is flying over a range.
#
# The images for each range are stored in a specific folder. The folder naming convention is:
#
# DJI_<Camera Sensor ID>_<Range Sequence Number>_<Date (yyyymmdd(
#
# Example:  DJI_A01733_C001_20170502 (First range in flight)
#           DJI_A01733_C002_20170502 (Second range in flight)
#           DJI_A01733_C003_20170502 (Third range in flight)
#
# Notes:
# 1. The position data that is recorded by the DJI X5R camera (EXIF) has been found to be inaccurate. Therefore, the
#    position of the UAV at a specific time is determined from the DJI log file position data.
# 2. The sampling frequency of the DJI log file is 10Hz.
# 3. The images are acquired at a frequency of 24 hz.
# 4. Therefore,The log file position data is interpolated at a frequency of 100 Hz in order to obtain a more accurate
#    position at a specific time.
# 5. The image timestamp is then matched to the timestamp in the logfile (to the nearest 10ms) in order
#    to identify and assign a position to the image at that time.
#
# Command Line Inputs:
#
# '-d' or '--dir':      'Directory path to folder containing UAV image and log files'
# '-t' or '--type':     'Image file type extension, e.g. DNG,CR2, JPG', default='DNG'
# '-o' or '--out':      'Output file path'
# '-r' or '--rename':   'Rename image files Y or N',default='N
# '-x' or '--debug':    'Dump interpolated log file Y or N',default='N'
# '-e' or '--expt':     'Plot prefix for experiment',default='18ASH%')
# '-y' or '--lonoffset':'Longitude offset in degrees',default=0.0)  Used to correct error in log file longitude
# '-z' or '--latoffset':'Latitude offset in degrees',default=0.0)   User to correct error in log file latitude
# '-u' or '--update':   'Update EXIF poisition data',default='N'
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
import glob
import piexif
import config
import mysql.connector
from mysql.connector import errorcode

import utm
import datetime
import pytz
from timezonefinder import TimezoneFinder

import collections
import bisect
from scipy.spatial import cKDTree
import numpy

from shapely import wkt
from shapely.wkt import dumps
from shapely.geometry import Point, LineString,Polygon

secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)
null_date = '0000/00/00'
null_time = '00:00:00'
epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)

log='LOG' # GPS position obtained from log file
img='EXIF' # GPS position obtained from images
gcp_world_coords = None
gcp_pixel_coords = None
uas_position_source=None
uas_altitude_ref='AGL'

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
    #
    # Return a list of the names and sample date & time for all image files.
    #

    # Get list of files in uas staging directory

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
    #
    # The default sampling frequency in the log file is 10 Hz.
    # This function interpolates the UAV position at a frequency of 100 hz.
    # The interpolated position data stored in dictionary with the image timestamp as a key.
    # The function returns the dictionary gpsEventDict and the timezone associated with the log file times.
    #
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
    segment=0
    with open(fflightLog, 'rU') as log:

        header = next(log)  # Skip the header row

        previous = next(log)  # Save the initial data row for comparison with next row
        rowCount+=1
        rowFields = previous.split(',')
        latitude = float(rowFields[0]) + lonOffset
        longitude = float(rowFields[1]) + latOffset
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

        for row in log:  # Now start processing the rest of the log
            rowCount+=1
            rowFields = row.split(',')
            latitude = float(rowFields[0]) + latOffset
            longitude = float(rowFields[1]) + lonOffset
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
            if takingVideo == '1':

                # Start interpolation

                timeDelta = timestamp - prevTime
                latDelta = latitude - prevLat
                longDelta = longitude - prevLong
                altDelta = altitude - prevAlt


                if seedTime == 0:
                    seedTime = round((timestamp) / 1000.0, 2) * 1000
                    newTime = int(seedTime)
                    segment+=1

                # Convert the timestamp to UTC time

                dateTimeStr = datetime.datetime.utcfromtimestamp(timestamp / 1000.0).strftime('%Y/%m/%d %H:%M:%S.%f')
                DateStr = dateTimeStr.split(' ')[0]
                TimeStr = dateTimeStr.split(' ')[1]

                # Store the log data

                gpsEventList = [DateStr, TimeStr, latitude, longitude, altitude, takingVideo, interpolated,
                                timestamp,segment]
                gpsEventDict[timestamp] = gpsEventList

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
                        gpsEventList = [newDateStr, newTimeStr, newLat, newLong, newAlt, takingVideo, interpolated,
                                        newTime, segment]
                        gpsEventDict[newTime] = gpsEventList
                        rowCount+=1
                        lastTime = newTime
                        newTime += 10
                else:
                    newTime += 0

            else:
                seedTime = 0

            prevTime = timestamp
            prevLat = latitude
            prevLong = longitude
            prevAlt = altitude

    return gpsEventDict, tzone


def hashfilelist(afile, blocksize=65536):
    # Function used to compute an MD5 checksum
    hasher = hashlib.md5()
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()


def calculate_checksum(ffilename):
    # Function used to assign an MD5 checksum to each image.
    checksum = hashfilelist(open(ffilename, 'rb'))
    return checksum

def init_metadata_record():
    record_id=None
    imagefilename=None
    uas_sample_date_utc=null_date
    uas_sample_time_utc=null_time
    uas_position=None
    uas_altitude=None
    f_checksum=None
    notes=''
    blankrow = [record_id, imagefilename, flightId, sensor_id,uas_sample_date_utc, uas_sample_time_utc,uas_position,
                uas_altitude,uas_altitude_ref,gcp_world_coords,gcp_pixel_coords,f_checksum, uas_position_source,notes]
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

def calculate_range_flight_durations(gpsEvents):
    #
    # This function calculates the duration of the flight time across each range in the experiment
    #
    segRef=0
    segStartTime=0
    for event in sorted(gpsEvents.items()):
        segment=event[1][-1]
        if segment != segRef:
            segStartTime=int(event[0])
        segEndTime=int(event[0])
        segDuration=segEndTime-segStartTime
        segRef=segment
        rangeDurations[segment]=[segStartTime,segEndTime,segDuration]
        pass
    return(rangeDurations)

def open_db_connection(config):

    # Connect to the HTP database
        try:
            cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD,
                                          host=config.HOST, port=config.PORT,
                                          database=config.DATABASE)
            print('Connecting to Database: ' + cnx.database)

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
                sys.exit()
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
                sys.exit()
            else:
                print(err)
        else:
            print('Connected to MySQL database:' + cnx.database)
            cursor = cnx.cursor(buffered=True)
        return cursor,cnx


def commit_and_close_db_connection(cursor,cnx):

    # Commit changes and close cursor and connection

    try:
        cnx.commit()
        cursor.close()
        cnx.close()

    except Exception as e:
            print('There was a problem committing database changes or closing a database connection.')
            print('Error Code: ' + e)

    return


# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Directory path to X5 flight folder',
                     default='/cygdrive/f/uav_staging/')

cmdline.add_argument('-t', '--type', help='Image file type extension, e.g. DNG,CR2, JPG',
                     default='DNG')

cmdline.add_argument('-o' , '--out', help= 'Output folder path')

cmdline.add_argument('-r', '--rename',help='Rename image files Y or N',default='N')

cmdline.add_argument('-x', '--debug',help='Dump interpolated log file Y or N',default='N')

cmdline.add_argument('-e', '--expt',help='Plot prefix for experiment',default='18ASH%')

cmdline.add_argument('-y', '--lonoffset',help='Longitude offset in degrees',default=0.0)

cmdline.add_argument('-z', '--latoffset',help='Latitude offset in degrees',default=0.0)

cmdline.add_argument('-u', '--update',help='Update EXIF poisition data',default='N')


args = cmdline.parse_args()


uasFolderPath = args.dir
uasFolderList=[]

# Search for data set folders that need to be processed and store in a list
# NB '' item in os.path.join adds an os-independent trailing slash character

uasFolderList=[os.path.join(uasFolderPath,name,'') for name in os.listdir(uasFolderPath) if os.path.isdir(os.path.join(uasFolderPath,name))]
if uasFolderList==[]:
    print("No uas data sets found in uav_staging...Exiting")
    sys.exit()

csvFlightLog=glob.glob(uasFolderPath+'*_v2.csv')
if len(csvFlightLog) > 1:
    print("More than one flight log was found ...Exiting")
else:
    flightLog=csvFlightLog[0]


outPath=args.out
plotRangePath = outPath + 'RangePlotIntersections.csv'
lineSegmentsPath = outPath + 'RangeLineSegments.csv'
metadataSqlFilePath=outPath + 'uasMetadataLoad.sql'
plotPrefix=args.expt
lonOffset=args.lonoffset # Parameter used in interpolation function to correct error in longitude (degrees)
latOffset=args.latoffset # Parameter used in interpolation function to correct error in latitude (degrees)
updateExif=args.update

imageType = args.type
renameImages=args.rename
debugMode=args.debug

record_id = None
notes = ''
imageFileList = []
metadatalist = []
rangeDurations={}
rangeLineSegments={}
plots={}

# Initialize a new SQL metadata load file
with open(metadataSqlFilePath, 'w') as sqlFile:
    print("Initialized SQL Metadata Load File.")
    sqlFile.close()

# Read the log file and extract the list of timestamped events required to determine the UAV position at a given time.
gpsEvents,localTimeZone =interpolate_time(flightLog)
sortedKeys=list(sorted(gpsEvents.keys()))

if len(gpsEvents)==0:
    print("There were no gps events found in", flightLog)
    print()

fltStartString=datetime.datetime.utcfromtimestamp(min(gpsEvents.keys())/1000.0)
fltEndString=datetime.datetime.utcfromtimestamp(max(gpsEvents.keys())/1000.0)
fltStart=time.gmtime(min(gpsEvents.keys())/1000.0)
fltEnd=time.gmtime(max(gpsEvents.keys())/1000.0)
print()
print('Flight Start: ',fltStartString)
print('Flight End: ',fltEndString)

flightId='uas_'+ str(fltStart[0])+str(fltStart[1]).zfill(2)+str(fltStart[2]).zfill(2)+ '_' + \
         str(fltStart[3]).zfill(2) + str(fltStart[4]).zfill(2) + str(fltStart[5]).zfill(2) + '_' + \
         str(fltEnd[0]) + str(fltEnd[1]).zfill(2) + str(fltEnd[2]).zfill(2) + '_' + \
         str(fltEnd[3]).zfill(2) + str(fltEnd[4]).zfill(2) + str(fltEnd[5]).zfill(2)
print()
print('Flight ID:', flightId)

#**********************************************
# This code section will dump a file containing log file events including interpolated position events.
if debugMode == 'Y':
    debugPath=outPath+ 'gpsDebugEvents.csv'
    #with open('/bulk/mlucas/test/gpsEvents.csv', 'wb') as csvfile:
    with open(debugPath, 'wb') as csvfile:
        header = csv.writer(csvfile)
        header.writerow(
            ['newTime','newDateStr', 'newTimeStr', 'newLat', 'newLong', 'newAlt', 'takingVideo', 'interpolated','newTime', 'newUtmPositionX', 'newUtmPositionY', 'newUtmLatZone', 'newUtmLongZone','segment'])
    csvfile.close()

    #with open('/bulk/mlucas/test/gpsEvents.csv', 'ab') as csvfile:
    with open(debugPath, 'ab') as csvfile:
        print('Generating gpsEvents file', debugPath)
        for lineitem in sorted(gpsEvents.iteritems()):
            fileline = csv.writer(csvfile)
            fileline.writerow([lineitem[0], lineitem[1][0], lineitem[1][1], lineitem[1][2], lineitem[1][3], lineitem[1][4], lineitem[1][5], lineitem[1][6],lineitem[1][7], lineitem[1][8], lineitem[1][9], lineitem[1][10],lineitem[1][11],lineitem[1][12]])
    csvfile.close()
#**********************************************

# Query the database for the plot polygons and store them in a dictionary with plot_id as key

cursor, cnx = open_db_connection(config)
plotQuery=("SELECT record_id,plot_id,ST_AsText(plot_polygon) FROM plot_map WHERE plot_id LIKE %s")

try:
    cursor.execute(plotQuery, (plotPrefix,))
    if cursor.rowcount != 0:
        for row in cursor:
            plotId=row[1]
            plotPolygon=row[2]
            plt=wkt.loads(plotPolygon)
            plots[plotId] = [plotId, plt, plt.centroid]
    else:
        print("There were no plots found in the database with plot prefix of " + plotPrefix)
        print("Exiting...")
        sys.exit()
except Exception as e:
    #print('Unexpected error during database query:', sys.exc_info()[0])
    print('Unexpected error during database query:', e)
    print('Exiting...')
    sys.exit()

print ('Closing connection to database table: plot_map ')
commit_and_close_db_connection(cursor, cnx)

# Build the KD Tree of plot coordinates that will be used to compute the nearest plot for a point


plotCentroids = [] # Stores plot coordinates used to build KD Tree
plotCentroidID=[] # Stores plot coordinates and plotID used to identify the name of the nearest plot
for plotID in plots.items():
    plotLon = plotID[1][2].centroid.x
    plotLat = plotID[1][2].centroid.y
    plotCentroids.append((plotLon,plotLat))
    plotCentroidID.append((plotLon,plotLat,plotID[1][0]))
plotPoints = numpy.array(plotCentroids)
plotKDTree = cKDTree(plotPoints, leafsize=100)

# Get the list of image files available for the flight/

rangeDurations=calculate_range_flight_durations(gpsEvents)

#Get the timestamp of first log entry i.e. when isTakingVideo is first true.

firstLogEntry = int(sortedKeys[0])
rangeSegment=1
intersectedPlots={}

# Open the output file that will contain the ordered list of plots flown over by the UAV

with open(plotRangePath, 'w') as plotRangeFile:
    plotRangeFile.write('range_id' + ',' + 'plot_id' + ',' + 'plot' + '\n')

# Process each image folder found in the input folder

    for uasPath in uasFolderList:
        print()
        folder = uasPath.split('/')[-2]
        print("Processing Image Folder:",folder)
        sensor_id = folder.split('_')[1]
        print("Sensor ID: ", sensor_id)
        image_set = folder.split('_')[2]
        print("Image Set: ",image_set)
        print()
        imagefiles = get_image_file_list(uasPath,imageType,imageFileList)
        imageCount = len(imagefiles)
        if imageCount==0:
            print("There were no image files found in ",uasPath)
            print("Exiting")
        else:
            frameInterval=float(rangeDurations[rangeSegment][2])/imageCount
            frameRate = 1000.0/frameInterval
            print()
            print("Range Flight Duration(secs): ",rangeDurations[rangeSegment][2]/1000.0)
            print("Frame Rate (frames/s): ",frameRate)
            print("Frame Interval(ms): ",frameInterval)
            print("Image Count ",imageCount)
    #
    # Define UAS Metadata Output File Path
    #
        uasMetadataFile= outPath + flightId + '_' + image_set +'_metadata.csv'
        print()
        print('UAS Metadata Output File: ',uasMetadataFile)
        frameIndex = 0
        logTimeIndex= 0
        timestamp = rangeDurations[rangeSegment][0]

        try:
            for f in imagefiles:
                metadata_record=init_metadata_record()
                imagefilename=f
                imagefilepath=uasPath+imagefilename

            # Interpolate Longitude,Latitude and Altitude of image
            # Convert date/time/frame to timestamp
            #

            # Find the time index in GPS events that is less than or equal to the frame time

                logTimeIndex = bisect.bisect_left(sortedKeys, timestamp)
                gpsEventsKey = sortedKeys[logTimeIndex]
                gpsEvents[gpsEventsKey].extend((logTimeIndex,timestamp, abs(gpsEventsKey - timestamp),imagefilename))

                uas_latitude=gpsEvents[gpsEventsKey][2]
                uas_longitude=gpsEvents[gpsEventsKey][3]
                # Create a WKT representation of the position POINT object using shapely dumps function
                uas_position = dumps(Point(uas_longitude, uas_latitude))
                uas_position_source = img # Position data derived from image EXIF

                uas_altitude = gpsEvents[gpsEventsKey][4]
                uas_sample_date_utc=gpsEvents[gpsEventsKey][0]
                uas_sample_time_utc=gpsEvents[gpsEventsKey][1]

                metadata_record[0]=record_id
                metadata_record[2]=flightId
                metadata_record[3]=sensor_id
                metadata_record[4] = uas_sample_date_utc
                metadata_record[5] = uas_sample_time_utc
                metadata_record[6] = uas_position
                metadata_record[7] = uas_altitude
                metadata_record[8] = uas_altitude_ref
                metadata_record[9] = gcp_world_coords
                metadata_record[10] = gcp_pixel_coords
                metadata_record[12] = log

            # Determine whether the point corresponding to the image position lies within a plot

                pt=numpy.array([uas_longitude,uas_latitude]) # UAS position
                nearest = plotKDTree.query(pt, k=1, distance_upper_bound=6) # Find the nearest plot centroid for pt
                plotID=plotCentroidID[nearest[1]][2] # Lookup the plot given the plot centroid
                # Build the list of plots intersected by points on the flight path of a range
                if plotID not in intersectedPlots and plots[plotID][1].contains(Point(uas_longitude,uas_latitude)) :
                    intersectedPlots[plotID]=[rangeSegment,plotID,plots[plotID][1].wkt,timestamp]
                    print('*',rangeSegment,plotID,plots[plotID][1].wkt,timestamp)
                    rangePlotLine = str(image_set) + ',' + plotID + ',"' + plots[plotID][1].wkt + '"\n'
                    plotRangeFile.write(rangePlotLine)

            # Rename image files if specified on command line input

                if renameImages == 'Y':
                    imgDate = uas_sample_date_utc[0:4] + uas_sample_date_utc[5:7] + uas_sample_date_utc[8:10]
                    imgTime = uas_sample_time_utc[0:2] + uas_sample_time_utc[3:5] + uas_sample_time_utc[6:8]
                    newimagefilepath = uasPath + sensor_id + '_' + imgDate + '_' + imgTime + '_' + imagefilename
                    newimagefilename = sensor_id + '_' + imgDate + '_' + imgTime + '_' + imagefilename
                    metadata_record[1] = newimagefilename
                    oldimagefilepath = uasPath + imagefilename
                    os.rename(oldimagefilepath, newimagefilepath)
                    print('old file name: ', oldimagefilepath)
                    print('new file name: ', newimagefilepath)
                    print()
                else:
                    oldimagefilepath = uasPath + imagefilename
                    newimagefilepath = oldimagefilepath
                    metadata_record[1] = imagefilename

                metadata_record[11] = calculate_checksum(newimagefilepath)
                metadata_record[0] = record_id

                metadatalist.append(metadata_record)
                #
                # Update the EXIF GPS data in each image using exiftool
                #

                try:
                    if updateExif='Y':
                        imageName=imagefilepath
                        if uas_longitude < 0:
                            uas_longitude_ref='W'
                        else:
                            uas_longitude_ref='E'
                        if uas_latitude < 0:
                            uas_latitude_ref= 'S'
                        else:
                            uas_latitude_ref='N'
                        uas_altitude_ref = '+'  # Above Sea Level
                        print("Updating GPS EXIF for Image Name", imageName, "Latitude:", uas_latitude,
                              "Longitude:", uas_longitude, "Latitude REf:", uas_latitude_ref,
                              "Longitude Ref:", uas_longitude_ref,"Altitude:", uas_altitude,
                              "Altitude REf:", uas_altitude_ref, )
                        ll = subprocess.Popen(["exiftool",
                                               "-GPSLatitude=" + str(uas_latitude),
                                               "-GPSLatitudeRef=" + uas_latitude_ref,
                                               "-GPSLongitude=" + str(uas_longitude),
                                               "-GPSLongitudeRef=" + uas_longitude_ref,
                                               "-GPSAltitude=" + str(uas_altitude),
                                               "-GPSAltitudeRef=" + (uas_altitude_ref),
                                               imageName],stdout=subprocess.PIPE)
                        output, err = ll.communicate()
                        ll.kill()
                except:
                    pass
                timestamp += frameInterval
                frameIndex += 1

        except Exception as e:
            print('*** Error*** Unable to process image file ',imagefilename)
            print('*** Error Code:', e)
            print('*** Trying to continue...')
            pass

        #with open(uasMetadataFile, 'wb') as csvfile:
        with open(uasMetadataFile, 'w') as csvfile:
            header = csv.writer(csvfile)
            header.writerow(
                ['record_id', 'image_file_name','flight_id', 'sensor_id', 'date_utc','time_utc','position',
                 'altitude','altitude_ref','gcp_world_coords','gcp_pixel_coords','md5sum','position_source' 'notes'])
        csvfile.close()

        #with open(uasMetadataFile, 'ab') as csvfile:
        with open(uasMetadataFile, 'a') as csvfile:
            print('Generating metadata file', uasMetadataFile)
            for lineitem in metadatalist:
                fileline = csv.writer(csvfile)
                fileline.writerow(
                    [lineitem[0], lineitem[1], lineitem[2], lineitem[3], lineitem[4], lineitem[5], lineitem[6], lineitem[7],
                     lineitem[8], lineitem[9], lineitem[10], lineitem[11], lineitem[12], lineitem[13]])
            # Kludge to get rid of blank last line in the file which causes an empty row to be loaded into the database
            # when using LOAD DATA INFILE procedure!!
            #csvfile.seek(-2, os.SEEK_END)
            #csvfile.truncate()
        csvfile.close()


        # Create the SQL command file to load the uas_images metadata

        print('Generating SQL file for DJI metadata:', metadataSqlFilePath)
        #SET uas_position=ST_PointFromText(CONCAT('POINT(',uas_position_x,' ',uas_position_y,')')),uas_sampling_date_utc=STR_TO_DATE(@uas_sampling_date_utc,'%Y-%m-%d');"""
        loadMetCmd = """LOAD DATA LOCAL INFILE '""" + uasMetadataFile + """' INTO TABLE uas_images FIELDS TERMINATED BY ','""" + """ LINES TERMINATED BY '\\r' IGNORE 1 LINES (record_id,image_file_name,flight_id,sensor_id,uas_position_x,uas_position_y,uas_altitude,uas_latitude,uas_longitude,@uas_sampling_date_utc,uas_sampling_time_utc,uas_lat_zone,uas_long_zone,uas_altitude_reference,cam_position_x,cam_position_y,cam_position_z,cam_latitude,cam_longitude,cam_sampling_date_utc,cam_sampling_time_utc,cam_lat_zone,cam_long_zone,cam_altitude_reference,md5sum,notes) SET uas_sampling_date_utc=STR_TO_DATE(@uas_sampling_date_utc,'%Y-%m-%d');\n"""
        with open(metadataSqlFilePath, 'a+') as sqlFile:
            sqlFile.write(loadMetCmd)

        imageFileList = []
        metadatalist = []
        rangeSegment+=1

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
