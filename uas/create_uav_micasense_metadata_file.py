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
from shapely.geometry import Point
from decimal import *




#print imagepreprocess.__name__
#print sys.path

# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat directory path to HTP imagefiles',
                     default='/bulk/jpoland/images/staging/uas_staging/')

cmdline.add_argument('-t', '--type', help='Image file type extension, e.g. TIF, JPG, DNG',
                     default='JPG')

cmdline.add_argument('-o', '--out', help='Output file path and filename',
                     default='/bulk/jpoland/images/staging/uas_staging/uas_image_metadata.csv')

args = cmdline.parse_args()

uasPath = args.dir
if uasPath[-1] != '/':
    uasPath+='/'

imageType = args.type
uasmetfile = args.out

record_id = None
notes = ''
metadataRecord=[]
metadataList = []
getcontext().prec = 8
getcontext()

imagefiles = get_image_file_list(uasPath, imageType)
if len(imagefiles)==0:
    print "There were no image files found in ",uasPath
    print "Exiting"
    sys.exit(10)

# Calculate flightID using date and time from EXIF in the first image in the list.

firstImage=uasPath + imagefiles[0]
with open(firstImage,'rb') as image:
     position_x, position_y, altitudeFeet, latitude, longitude, dateUTC, \
     timeUTC, lat_zone, long_zone, altitudeRef, cam_serial_no = get_image_exif_data(image)
image.close()
y = dateUTC[0:4]
m = dateUTC[5:7]
d = dateUTC[8:10]
dateString = y + m + d
h = timeUTC[0:2]
mm = timeUTC[3:5]
s = timeUTC[6:8]
timeString = h + mm + s
flightId = 'uas_' + dateString + '_' + timeString

camIndex = 0
for f in imagefiles:

    filename = uasPath + f
    imagefilename = f
    #
    #Get Image File EXIF metadata
    #
    with open(filename,'rb') as image:
         #print "Processing ",filename
         position_x, position_y,altitudeFeet, latitude, longitude, dateUTC, \
         timeUTC, lat_zone,long_zone,altitudeRef,cam_serial_no=get_image_exif_data(image)
    image.close()

    # FlightID calculation is incorrect - Should only use the first image date and time

    y=dateUTC[0:4]
    m=dateUTC[5:7]
    d=dateUTC[8:10]
    dateString = y+m+d
    h= timeUTC[0:2]
    mm= timeUTC[3:5]
    s= timeUTC[6:8]
    timeString=h+mm+s
    sensorId = 'CAM_'+ cam_serial_no
    getcontext().prec=7
    lonDec=Decimal(str(longitude))
    latDec=Decimal(str(latitude))
    position = dumps(Point(lonDec, latDec),rounding_precision=7)
    #position = Point(lonDec, latDec)
    positionRef = 1
    notes=None
    # Rename image files
    imageFileName=sensorId + '_' + dateString + '_' + timeString+ '_' +  imagefilename
    oldImageFilePath= uasPath + imagefilename
    newImageFilePath= uasPath + imageFileName
    os.rename (oldImageFilePath,newImageFilePath)
    # Populate the metadata data structure for the renamed image
    #metadata_record = init_metadata_record(flightId, sensorId)
    metadataRecord=[]
    altitude=float(altitudeFeet) * 0.3048
    md5sum = calculate_checksum(newImageFilePath)
    #metadataRecord=[imageFileName,flightId,sensorId,dateUTC,timeUTC,position.wkt,altitude,altitudeRef,md5sum,positionRef,notes]
    metadataRecord = [imageFileName, flightId, sensorId, dateUTC, timeUTC, position, altitude, altitudeRef, md5sum,positionRef, notes]
    metadataList.append(metadataRecord)
    metadataList.append(metadataRecord)
    camIndex += 1
#
# Write out the metadata file
#
with open(uasmetfile, 'wb') as csvfile:
    header = csv.writer(csvfile,lineterminator = '\n')
    header.writerow(['image_file_name', 'flight_id', 'sensor_id', 'date_utc', 'time_utc', 'position','altitude',
                     'altitude_ref', 'md5sum', 'position_source', 'notes'])
csvfile.close()

with open(uasmetfile, 'ab') as csvfile:
    print 'Generating metadata file', uasmetfile
    for lineitem in metadataList:
        fileline = csv.writer(csvfile,quoting=csv.QUOTE_ALL,lineterminator = ',\n')
        fileline.writerow(
            [lineitem[0], lineitem[1], lineitem[2],lineitem[3], lineitem[4], lineitem[5], lineitem[6], lineitem[7],
             lineitem[8], lineitem[9], lineitem[10]])

csvfile.close()


# Connect to the wheatgenetics database

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

gcp_coords=None
pixel_coords=None
record_id=None

#db_insert = "INSERT INTO uas_images_test (record_id,image_file_name,flight_id,sensor_id,date_utc,time_utc,position,altitude,altitude_ref,md5sum,position_source,notes) VALUES (%s,%s,%s,%s,%s,%s,ST_PointFromText(%(position)s),%s,%s,%s,%s,%s)"
db_insert = "INSERT INTO uas_images_test (record_id,image_file_name,flight_id,sensor_id,date_utc,time_utc,position,altitude,altitude_ref,md5sum,position_source,notes) VALUES (%s,%s,%s,%s,%s,%s,ST_PointFromText(%s),%s,%s,%s,%s,%s)"

db_check = "SELECT record_id FROM uas_images_test WHERE flight_id LIKE %s"

insertCount=0
for lineitem in metadataList:
    dataRow=(record_id,lineitem[0], lineitem[1], lineitem[2],lineitem[3], lineitem[4], lineitem[5], lineitem[6],lineitem[7],lineitem[8], lineitem[9], lineitem[10])
    cursorA.execute(db_insert,dataRow )
    cnx.commit()
    insertCount+=1
cursorB.execute(db_check, (flightId, ))
checkCount = cursorB.rowcount

cursorA.close()
cursorB.close()
print ("")
print ("Number of rows to be inserted into the database", len(metadataList))
print("Number of metadata records inserted", insertCount)
print("Number of database records returned on insert check query", checkCount)
print("")
print('Closing database connection...')

cnx.close()

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
