#!/usr/bin/python


from __future__ import unicode_literals

#
# Program: create_uas_metadata_file_micasense
#
# Version: 0.1 April 10,2017 - Based on create_uav_metadata_file.py
#
# Creates CSV file containing image metadata to be imported into the uas_images table in the wheatgenetics database.
#
# Command Line Inputs:
#
#
# '-d' or '--dir':      'Beocat directory path to HTP image files', default='/homes/mlucas/uas_incoming/'
# '-t' or '--type':     'Image file type, e.g. CR2, JPG'
# '-o' or '--out':      'Output file path and filename'
#
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
import imagepreprocess
from imagepreprocess import *
from shapely import wkt
from shapely.wkt import dumps
from shapely.geometry import Point,Polygon,MultiPoint
from decimal import *
import shutil

#print imagepreprocess.__name__
#print sys.path

getcontext().prec = 8
getcontext()

def parse_flight_folder_name(flightFolder):
    dataSetParams=flightFolder.split('_')
    dateStr     = dataSetParams[0]
    timeStr     = dataSetParams[1]
    exptStr     = dataSetParams[2]
    plElevStr      = dataSetParams[3][:-1]
    camStr      = dataSetParams[4]
    angleStr    = dataSetParams[5]
    imgTypeStr  = dataSetParams[6]
    seqStr      = dataSetParams[7]
    return dateStr,timeStr,exptStr,plElevStr,camStr,angleStr,imgTypeStr,seqStr

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

        startDate = lYear + lMonth + lDay
        startTime = lhours + lminutes + lsecs

        lastRow=flightLogList[-1]
        rowFields = lastRow.split(',')
        lYear = rowFields[11][0:4]
        lMonth = rowFields[11][5:7]
        lDay = rowFields[11][8:10]
        lhours = rowFields[11][11:13]
        lminutes = rowFields[11][14:16]
        lsecs = rowFields[11][17:19]

        endDate = lYear + lMonth + lDay
        endTime = lhours + lminutes + lsecs

        flightID = 'uas_'+startDate+'_'+startTime+'_'+endDate+'_'+endTime

    return flightID,startDate,startTime,endDate,endTime

def create_flightId_from_image_datetime(metadataList):

    startDate=str(metadataList[0][3])
    startDate=startDate.replace('/','')
    startTime=str(metadataList[0][4])
    startTime=startTime.replace(':','')
    endDate=str(metadataList[-1][3])
    endDate=endDate.replace('/','')
    endTime=str(metadataList[-1][4])
    endTime=endTime.replace(':','')

    flight_id='uas_'+startDate+'_'+startTime+'_'+endDate+'_'+endTime

    return flight_id,startDate,startTime,endDate,endTime




# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat directory path to HTP imagefiles',
                     default='/home/mlucas/uav_staging/')

cmdline.add_argument('-t', '--type', help='Image file type extension, e.g. TIF, JPG, DNG',
                     default='TIF')

#cmdline.add_argument('-o', '--out', help='Output file path and filename',
#                     default='/bulk/jpoland/images/staging/uas_staging/uas_image_metadata.csv')
#
args = cmdline.parse_args()

uasPath = args.dir
if uasPath[-1] != '/':
    uasPath+='/'

imageType = args.type
#uasmetfile = args.out

uasFolderPathList=[]
uasFolderList=[]
uasSubFolderList=[]
uasLogFileList=[]


uasFolderPathList=[os.path.join(uasPath,name)+'/' for name in os.listdir(uasPath) if os.path.isdir(os.path.join(uasPath,name))]
if uasFolderPathList==[]:
    print "No uas data sets found in uav_staging...Exiting"
    sys.exit()


#uasFolderList=[os.path.basename(name) for name in os.listdir(uasPath) if os.path.isdir(os.path.join(uasPath,name))]

for uasFolder in uasFolderPathList:
    record_id = None
    notes = ''
    metadataRecord = []
    metadataList = []

    # Calculate flightID using the first and last records in the DJI log file - Deprecated due to log file unavailability

    #uasLogFileList = [os.path.join(uasFolder, logfile) for logfile in os.listdir(uasFolder) if logfile.endswith(".csv")]
    #uasLogFile= uasLogFileList[0]
    #flightId,startDate,startTime,endDate,endTime=create_flightId_from_logfile(uasLogFile)

    print

    # Parse the flight folder name to extract parameters needed for the uas_run table entry

    flightFolder=os.path.split(uasFolder[:-1])
    dateStr, timeStr, experimentId, plannedElevation, camStr, cameraAngle, imgTypeStr, seqStr = parse_flight_folder_name(flightFolder[1])


    # Process each image sub-folder

    uasSubFolderList=[]
    uasSubFolderList=[os.path.join(uasFolder,name)+'/' for name in os.listdir(uasFolder) if os.path.isdir(os.path.join(uasFolder,name))]

    for subFolder in uasSubFolderList:
        print "Processing "+subFolder
        # Get the list of image files in the sub-folder
        imagefiles = get_image_file_list(subFolder, imageType)
        if len(imagefiles) == 0:
            print "There were no image files found in ", uasPath
            print "Exiting"
            sys.exit(10)
        print "Number of images in " + subFolder + "=" + str(len(imagefiles))
        print
        for f in imagefiles:
            filename = subFolder + f
            imagefilename = f
            #
            # Get Image File EXIF metadata
            #
            with open(filename, 'rb') as image:
                # print "Processing ",filename
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
            positionRef = 1
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
            #md5sum = calculate_checksum(oldImageFilePath)
            # metadataRecord=[imageFileName,flightId,sensorId,dateUTC,timeUTC,position.wkt,altitude,altitudeRef,md5sum,positionRef,notes]
            metadataRecord = [imageFileName, flightId, sensorId, dateUTC, timeUTC, position, altitude, altitudeRef,
                              md5sum, positionRef, notes]
            metadataList.append(metadataRecord)

    print

    # Compute the flight ID from the image timestamps to support case where logfile is not available

    flightId,startDate,startTime,endDate,endTime=create_flightId_from_image_datetime(metadataList)

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

    #db_insert = "INSERT INTO uas_images_test (record_id,image_file_name,flight_id,sensor_id,date_utc,time_utc,position,altitude,altitude_ref,md5sum,position_source,notes) VALUES (%s,%s,%s,%s,%s,%s,ST_PointFromText(%(position)s),%s,%s,%s,%s,%s)"
    db_insert = "INSERT INTO uas_images_test (record_id,image_file_name,flight_id,sensor_id,date_utc,time_utc,position,altitude,altitude_ref,md5sum,position_source,notes) VALUES (%s,%s,%s,%s,%s,%s,ST_PointFromText(%s),%s,%s,%s,%s,%s)"

    db_check = "SELECT record_id FROM uas_images_test WHERE flight_id LIKE %s"

    #get_flight_coords = "SELECT MIN(ST_X(position)), MAX(ST_X(position)),MIN(ST_Y(position)), MAX(ST_Y(position)) FROM uas_images_test WHERE flight_id LIKE %s"

    get_flight_coords = "SELECT ST_X(position),ST_Y(position) from uas_images_test where flight_id like %s"

    #get_flight_coords = "SELECT position from uas_images_test where flight_id like %s"

    db_insert_run = "INSERT INTO uas_run_test (record_id,flight_id,start_date,start_time,end_date,end_time,flight_filename,experiment_id,planned_elevation_m,sensor_id,camera_angle,flight_polygon) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_PolygonFromText(%s))"

    insertCount=0
    for lineitem in metadataList:
        dataRow=(record_id,lineitem[0], flightId, lineitem[2],lineitem[3], lineitem[4], lineitem[5], lineitem[6],lineitem[7],lineitem[8], lineitem[9], lineitem[10])
        cursorA.execute(db_insert,dataRow )
        cnx.commit()
        insertCount+=1
    cursorB.execute(db_check, (flightId, ))
    checkCount = cursorB.rowcount
    cursorC.execute(get_flight_coords,(flightId, ))
    #if cursorC.rowcount != 0:
    #    for row in cursorC:
    #        longMin=float(row[0])
    #        longMax=float(row[1])
    #        latMin=float(row[2])
    #        latMax=float(row[3])
    pointList=[]
    if cursorC.rowcount != 0:
        for row in cursorC:
            pointList.append((row[0],row[1]))
    #print pointList
    #flightPolygon=dumps(Polygon([(longMin,latMin),(longMin,latMax),(longMax,latMax),(longMax,latMin),(longMin,latMin)]))
    #flightPolygon=dumps(MultiPoint(pointList)).convex_hull
    flightPolygon=dumps((MultiPoint(pointList)).convex_hull)
    flightRow=(record_id,flightId,startDate,startTime,endDate,endTime, flightFolder[1],experimentId,plannedElevation,sensorId,cameraAngle,flightPolygon)
    cursorD.execute(db_insert_run,flightRow)
    cnx.commit()
    cursorA.close()
    cursorB.close()
    cursorC.close()
    cursorD.close()
    print ("")
    print ("Number of rows to be inserted into the database", len(metadataList))
    print("Number of metadata records inserted", insertCount)
    print("Number of database records returned on insert check query", checkCount)
    print("")
    print('Closing database connection...')

    cnx.close()

# Move all of the image files to the top level of the sub-folder

for uasFolder in uasFolderPathList:

# Process each image sub-folder

    uasSubFolderList = []
    uasSubFolderList = [os.path.join(uasFolder, name) + '/' for name in os.listdir(uasFolder) if os.path.isdir(os.path.join(uasFolder, name))]

    for subFolder in uasSubFolderList:
        print "Moving images from  " + subFolder
        # Get the list of image files in the sub-folder
        imagefiles = get_image_file_list(subFolder, imageType)
        if len(imagefiles) == 0:
            print "There were no image files found in ", uasPath
            print "Exiting"
            sys.exit(10)
        print "Number of images in " + subFolder + "=" + str(len(imagefiles))
        print
        for f in imagefiles:
            oldFilename = subFolder + f
            newFilename = uasFolder + f
            shutil.move(oldFilename,newFilename)
        print "Deleting sub-folder " + subFolder
        os.rmdir(subFolder)

#****************************************

#
# Write out the metadata file
#
#with open(uasmetfile, 'wb') as csvfile:
#    header = csv.writer(csvfile,lineterminator = '\n')
#    header.writerow(['image_file_name', 'flight_id', 'sensor_id', 'date_utc', 'time_utc', 'position','altitude',
#                     'altitude_ref', 'md5sum', 'position_source', 'notes'])
#csvfile.close()

#with open(uasmetfile, 'ab') as csvfile:
#    print 'Generating metadata file', uasmetfile
#    for lineitem in metadataList:
#        fileline = csv.writer(csvfile,quoting=csv.QUOTE_ALL,lineterminator = ',\n')
#        fileline.writerow(
#            [lineitem[0], lineitem[1], lineitem[2],lineitem[3], lineitem[4], lineitem[5], lineitem[6], lineitem[7],
#             lineitem[8], lineitem[9], lineitem[10]])
#
#
#csvfile.close()


# Connect to the wheatgenetics database


# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
