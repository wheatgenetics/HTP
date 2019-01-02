#!/usr/bin/python
from __future__ import print_function
from __future__ import unicode_literals

#
# Program: archive_micasense_images.py
#
#
# Version: 0.1 April 10,2017 - Based on create_uav_metadata_file.py
# Version: 0.2 December 1,2017 - Removed reference to experiment_id
# Version: 0.3 April 26,2018 - Added capability to check image set integrity
#
# This program will search for Micasense flight data folders in the specified directory, validate and rename all image
# files for each flight and move them to the specified output folder. It will also update the wheatgenetics uas_run
# table with summary information about each flight and update the wheatgenetics uas_images table with metadata about
# each image in each flight.
#
# Note: It is necessary to execute the program collate_micasense_flight_data for each flight folder in order to
# transform the raw data into the standard format required by archive_micasense_images.
#
# Command Line Inputs:
#
#
# '-d' or '--dir':      'Beocat directory path to HTP image files', default='/bulk/jpoland/images/staging/uav_incoming/'
# '-t' or '--type':     'Image file type, e.g. TIF, JPG,DNG,default=TIF'
# '-o' or '--out':      'Output file path and filename,default='/bulk/jpoland/images/staging/uav_processed/'
#

__author__ = 'mlucas'

import csv
import mysql.connector
from mysql.connector import errorcode
import test_config
import math
import time
import sys
import os
import argparse
from imagepreprocess import *
from shapely import wkt
from shapely.wkt import dumps
from shapely.geometry import Point,Polygon,MultiPoint
from decimal import *
import shutil
import pytz
from tzlocal import get_localzone
from timezonefinder import TimezoneFinder
import datetime
from collections import defaultdict
import exifread

getcontext().prec = 8
getcontext()

def parse_flight_folder_name(flightFolder):
    dataSetParams=flightFolder.split('_')
    dateStr     = dataSetParams[0]
    timeStr     = dataSetParams[1]
    plElevStr   = dataSetParams[2][:-1]
    camStr      = dataSetParams[3]
    angleStr    = dataSetParams[4]
    imgTypeStr  = dataSetParams[5]
    seqStr      = dataSetParams[6]
    return dateStr,timeStr,plElevStr,camStr,angleStr,imgTypeStr,seqStr

def create_flightId_from_logfile(flightLog):
    flightLogList=[]
    with open(flightLog, 'rU') as log:
        header = next(log)  # Skip the header row
        for row in log:
            flightLogList.append(row)
        firstRow=flightLogList[0]
        rowFields = firstRow.split(',')
        lYear = rowFields[11][0:4]
        lMonth = rowFields[11][5:7]
        lDay = rowFields[11][8:10]
        lhours = rowFields[11][11:13]
        lminutes = rowFields[11][14:16]
        lsecs = rowFields[11][17:19]

        utcStartDate = lYear + lMonth + lDay
        utcStartTime = lhours + lminutes + lsecs

        lastRow=flightLogList[-1]
        rowFields = lastRow.split(',')
        lYear = rowFields[11][0:4]
        lMonth = rowFields[11][5:7]
        lDay = rowFields[11][8:10]
        lhours = rowFields[11][11:13]
        lminutes = rowFields[11][14:16]
        lsecs = rowFields[11][17:19]

        utcEndDate = lYear + lMonth + lDay
        utcEndTime = lhours + lminutes + lsecs

        flightID = 'uas_'+utcStartDate+'_'+utcStartTime+'_'+utcEndDate+'_'+utcEndTime

    return flightID,utcStartDate,utcStartTime,utcEndDate,utcEndTime

def create_flightId_from_image_datetime(metadataList,longitude,latitude):

    # Create flight ID from UTC dates and Times.
    # Also return the local start date and start time

    utcStartDate=str(metadataList[0][3])
    startYear, startMonth, startDay = utcStartDate.split("/")
    utcStartDate=utcStartDate.replace('/','')
    utcStartTime=str(metadataList[0][4])
    startHour, startMinute, startSecond = utcStartTime.split(':')
    utcStartTime=utcStartTime.replace(':','')
    utcEndDate=str(metadataList[-1][3])
    utcEndDate=utcEndDate.replace('/','')
    utcEndTime=str(metadataList[-1][4])
    utcEndTime=utcEndTime.replace(':','')

    flight_id='uas_'+utcStartDate+'_'+utcStartTime+'_'+utcEndDate+'_'+utcEndTime

    tf = TimezoneFinder()
    tzone = tf.timezone_at(lng=longitude, lat=latitude)
    tz = pytz.timezone(tzone)
    utc_dt = datetime.datetime(int(startYear), int(startMonth), int(startDay), int(startHour), int(startMinute),
                               int(startSecond), tzinfo=pytz.utc)
    dt = utc_dt.astimezone(tz)

    localDate = dt.date()
    localTime = dt.time()

    return flight_id,utcStartDate,utcStartTime,utcEndDate,utcEndTime,localDate,localTime

def check_manifest(manifest):
    # TBD
    # Check for presence of manifest file and that all data set files are present
    manifest_status=False
    return manifest_status

def validate_micasense_images(subFolder,imageFileList):

    #pathToImages = os.path.join(subFolder + '*.' + imageType)

    print("Number of images before validation", len(imageFileList))

    validImageList = []
    invalidImageList=[]
    imageCheckDict = defaultdict(list)

    for f in sorted(imageFileList):

        imageName = subFolder + f
        a = f.split('/')[-1]
        primaryImageName = f.rpartition('_')[0]
        imageSize = os.stat(imageName).st_size
        imageCheckDict[primaryImageName].append([f, imageSize])

    for k, v in sorted(imageCheckDict.items()):
        truncatedImage = False
        missingImage = False

    # Check for image sets which have less than 5 images or more than 5 images and remove them from the valid list if found

        if len(v) != 5:
            missingImage=True

    # Check for images that have been truncated (less than  2Mb or 2097152 bytes) and remove the set from the valid list if found

        for i in imageCheckDict[k]:
            imageSize=i[1]
            if imageSize < 2400000:
                truncatedImage = True

        if truncatedImage or missingImage:
            for i in imageCheckDict[k]:
                invalidImageList.append(i[0])
            imageCheckDict.pop(k, )
            if truncatedImage:
                print('***Deleted Image Set' + k + ' Due to Truncated Image', k)
                with open(logname, 'a') as logoutput:
                    logoutput.write('***Deleted Image Set ' + k + ' Due to Truncated Image' + '\n')
            elif missingImage:
                print('***Deleted Image Set'+ k + ' Image set does not have 5 images.')
                with open(logname, 'a') as logoutput:
                    logoutput.write('***Deleted Image Set ' + k + ' Image set does not have 5 images.' + '\n')

    # Build the list of valid images to be processed further

    for k, v in sorted(imageCheckDict.items()):
        for i in range(0, 5):
            validImageList.append(v[i][0])

    print("Number of images that passed validation:", len(validImageList))
    invalidImageList.sort()

    return validImageList,invalidImageList

def get_original_flight_dataset_name(flightDataSetPath):
    with open(flightDataSetPath)as f:
        flightDataSet=f.readline().rstrip('\n')
    return flightDataSet


# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat directory path to HTP imagefiles',
                     default='/bulk/jpoland/images/staging/uav_incoming/')

cmdline.add_argument('-t', '--type', help='Image file type extension, e.g. TIF, JPG, DNG',
                     default='TIF')

cmdline.add_argument('-o', '--out', help='Output file path and filename',
                     default='/bulk/jpoland/images/staging/uav_processed/')

args = cmdline.parse_args()

uasPath = args.dir
if uasPath[-1] != '/':
    uasPath+='/'

imageType = args.type

uasOutPath=args.out
if uasOutPath[-1] != '/':
    uasOutPath+='/'

uasFolderPathList=[]
uasSubFolderList=[]
uasLogFileList=[]



# Search for data set folders that need to be processed and store in a list
# NB '' item in os.path.join adds an os-independent trailing slash character


uasFolderPathList=[os.path.join(uasPath,name,'') for name in os.listdir(uasPath) if os.path.isdir(os.path.join(uasPath,name))]
if uasFolderPathList==[]:
    print("No uas data sets found in uav_staging...Exiting")
    sys.exit()

# Process each data set folder ie. 0000SET,0001SET,00002SET ...nnnnSET

for uasFolder in sorted(uasFolderPathList):
    logFile = 'Log_' + datetime.datetime.now().strftime("%y%m%d_%H%M%S") + '.txt'
    logname = os.path.join(uasFolder, logFile)
    print("Processing Data Set: " + uasFolder)
    record_id = None
    notes = ''
    metadataRecord = []
    metadataList = []

    flightDataSetPath = os.path.join(uasFolder, 'flightMetadata.txt')
    flightDataSetFolder = get_original_flight_dataset_name(flightDataSetPath)

    # Parse the flight folder name to extract parameters needed for the uas_run table entry

    flightFolder=os.path.split(uasFolder[:-1])
    dateStr, timeStr, plannedElevation, camStr, cameraAngle, imgTypeStr, seqStr = parse_flight_folder_name(flightFolder[1])

    # Process each image sub-folder within each Micasense data set i.e. 000,001,002...nnn

    uasSubFolderList=[]
    uasSubFolderList=[os.path.join(uasFolder,name)+'/' for name in os.listdir(uasFolder) if os.path.isdir(os.path.join(uasFolder,name))]

    for subFolder in sorted(uasSubFolderList):
        print("Processing Data Set sub-folder: "+subFolder)

        # Get the list of image files in the sub-folder

        #imagefiles = get_micasense_image_list(subFolder, imageType)

        imageFileList = sorted(os.listdir(subFolder))

        imagefiles, invalidImageFiles = validate_micasense_images(subFolder,imageFileList)

        if len(imagefiles) == 0:
            print("There were no image files found in ", uasPath)
            pass
        else:
            print("Number of images in " + subFolder + "=" + str(len(imagefiles)))
            for f in imagefiles:
                filename = subFolder + f
                imagefilename = f
                #
                # Get Image File EXIF metadata
                #
                with open(filename, 'rb') as image:
                    position_x, position_y, altitudeFeet, latitude, longitude, dateUTC, \
                    timeUTC, lat_zone, long_zone, altitudeRef, cam_serial_no = get_image_exif_data(image)
                image.close()
                flightId=None
                y = dateUTC[0:4]
                m = dateUTC[5:7]
                d = dateUTC[8:10]
                dateString = y + m + d
                h = timeUTC[0:2]
                mm = timeUTC[3:5]
                s = timeUTC[6:8]
                timeString = h + mm + s
                sensorId = 'CAM_' + cam_serial_no
                # Create a WKT representation of the position POINT object using shapely dumps function
                position = dumps(Point(longitude, latitude))
                positionRef = 'EXIF'
                notes = None
                # Rename image files
                imageFileName = sensorId + '_' + dateString + '_' + timeString + '_' + imagefilename
                oldImageFilePath = subFolder + imagefilename
                newImageFilePath = subFolder + imageFileName
                os.rename (oldImageFilePath,newImageFilePath)
                # Populate the metadata data structure for the renamed image
                metadataRecord = []
                altitude = float(altitudeFeet) * 0.3048
                md5sum = calculate_checksum(newImageFilePath)
                metadataRecord = [imageFileName, flightId, sensorId, dateUTC, timeUTC, position, altitude, altitudeRef,
                                  md5sum, positionRef, notes]
                metadataList.append(metadataRecord)

    # Compute the flight ID from the image timestamps to support case where logfile is not available

    flightId,utcStartDate,utcStartTime,utcEndDate,utcEndTime,localDate,localTime=\
       create_flightId_from_image_datetime(metadataList,longitude,latitude)
    #flightId, utcStartDate, utcStartTime, utcEndDate, utcEndTime = \
    #   create_flightId_from_image_datetime(metadataList, longitude, latitude)

    print("")
    print("Connecting to Database...")

    try:
        cnx = mysql.connector.connect(user=test_config.USER, password=test_config.PASSWORD, host=test_config.HOST,
                                      port=test_config.PORT,database=test_config.DATABASE)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursorA = cnx.cursor(buffered=True)
        cursorB = cnx.cursor(buffered=True)
        cursorC = cnx.cursor(buffered=True)
        cursorD = cnx.cursor(buffered=True)

    gcp_coords=None
    pixel_coords=None
    record_id=None
    notes=flightDataSetFolder

    db_insert = "INSERT INTO uas_images(record_id,image_file_name,flight_id,sensor_id,date_utc,time_utc,position,altitude,altitude_ref,md5sum,position_source,notes) VALUES (%s,%s,%s,%s,%s,%s,ST_PointFromText(%s),%s,%s,%s,%s,%s)"

    db_check = "SELECT record_id FROM uas_images WHERE flight_id LIKE %s"

    get_flight_coords = "SELECT ST_X(position),ST_Y(position) from uas_images where flight_id like %s"

    db_insert_run = "INSERT INTO uas_run (record_id,flight_id,start_date_utc,start_time_utc,end_date_utc," \
                    "end_time_utc,start_time_local,start_date_local,flight_filename,planned_elevation_m,sensor_id," \
                    "camera_angle,notes,flight_polygon,image_count) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_PolygonFromText(%s),%s)"

    insertCount=0
    for lineitem in metadataList:
        dataRow=(record_id,lineitem[0], flightId, lineitem[2],lineitem[3], lineitem[4], lineitem[5], lineitem[6],lineitem[7],lineitem[8], lineitem[9], lineitem[10])
        cursorA.execute(db_insert,dataRow )
        cnx.commit()
        insertCount+=1
    cursorB.execute(db_check, (flightId, ))
    checkCount = cursorB.rowcount
    cursorC.execute(get_flight_coords,(flightId, ))

    pointList=[]
    if cursorC.rowcount != 0:
        for row in cursorC:
            pointList.append((row[0],row[1]))

    imageCount=insertCount
    flightPolygon=dumps((MultiPoint(pointList)).convex_hull)
    flightRow=(record_id,flightId,utcStartDate,utcStartTime,utcEndDate,utcEndTime,localTime,localDate,
               flightFolder[1],plannedElevation,sensorId,cameraAngle,notes,flightPolygon,imageCount)
    cursorD.execute(db_insert_run,flightRow)
    cnx.commit()
    cursorA.close()
    cursorB.close()
    cursorC.close()
    cursorD.close()
    print("")
    print("Number of rows to be inserted into the database", len(metadataList))
    print("Number of metadata records inserted", insertCount)
    print("Number of database records returned on insert check query", checkCount)
    print("")
    print('Closing database connection...')
    print("")

    cnx.close()

# Move all of the image files to the top level of the sub-folder

for uasFolder in uasFolderPathList:

# Process each image sub-folder
    try:
        uasSubFolderList = []
        uasSubFolderList = [os.path.join(uasFolder, name) + '/' for name in os.listdir(uasFolder) if os.path.isdir(os.path.join(uasFolder, name))]

        for subFolder in uasSubFolderList:
            # Get the list of image files in the sub-folder
            imagefiles = get_image_file_list(subFolder, imageType)
            if len(imagefiles) == 0:
                print("There were no image files found in ", subFolder)
                pass
            else:
                print("Moving " + str(len(imagefiles)) + " images from " + subFolder + " to " + uasFolder)
                for f in imagefiles:
                    oldFilename = subFolder + f
                    newFilename = uasFolder + f
                    shutil.move(oldFilename,newFilename)
                print("Cleaning up...")

                # Check for bad files with names of the form ._IMG* and remove them
                badFiles = [os.path.join(subFolder,i) for i in os.listdir(subFolder) if os.path.isfile(os.path.join(subFolder, i)) and '._IMG' in i]
                removeBadFiles = [os.remove(f) for f in badFiles]
                # Remove the subfolder if empty
                os.rmdir(subFolder)

    # Move the data set to the uav_processed folder

        print("Moving processed data sets from " + uasFolder + " to " + uasOutPath)
        shutil.move(uasFolder,uasOutPath)

    except Exception as e:
        print('Unexpected error occurred while processing image folder:', subFolder)
        print('Error: ',e)
        print('Exiting...')
        sys.exit()


# Exit the program gracefully

print('Processing Completed. Exiting...')
sys.exit()
