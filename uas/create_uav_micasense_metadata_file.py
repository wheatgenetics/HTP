#!/usr/bin/python
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
import math
import time
import sys
import os
import argparse
import imagepreprocess
from imagepreprocess import *

print imagepreprocess.__name__
print sys.path

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
metadatalist = []

imagefiles = get_image_file_list(uasPath, imageType)
if len(imagefiles)==0:
    print "There were no image files found in ",uasPath
    print "Exiting"
    sys.exit(10)

# Calculate flightID using date and time from EXIF in the first image in the list.

firstImage=uasPath + imagefiles[0]
with open(firstImage,'rb') as image:
    cam_position_x, cam_position_y, cam_position_z, cam_latitude, cam_longitude, cam_sample_date_utc, \
    cam_sample_time_utc, cam_lat_zone, cam_long_zone, cam_altitude_ref, cam_serial_no = get_image_exif_data(image)
image.close()
y = cam_sample_date_utc[0:4]
m = cam_sample_date_utc[5:7]
d = cam_sample_date_utc[8:10]
dateString = y + m + d
h = cam_sample_time_utc[0:2]
mm = cam_sample_time_utc[3:5]
s = cam_sample_time_utc[6:8]
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
        print "Processing ",filename
        cam_position_x, cam_position_y,cam_position_z, cam_latitude, cam_longitude, cam_sample_date_utc, \
        cam_sample_time_utc, cam_lat_zone,cam_long_zone,cam_altitude_ref,cam_serial_no=get_image_exif_data(image)
    image.close()

    # FlightID calculation is incorrect - Should only use the first image date and time

    y=cam_sample_date_utc[0:4]
    m=cam_sample_date_utc[5:7]
    d=cam_sample_date_utc[8:10]
    dateString = y+m+d
    h= cam_sample_time_utc[0:2]
    mm= cam_sample_time_utc[3:5]
    s= cam_sample_time_utc[6:8]
    timeString=h+mm+s
    sensor_id = cam_serial_no
    # Rename image files
    imagename='CAM_' + sensor_id + '_' + dateString + '_' + timeString+ '_' +  imagefilename
    oldImageFilePath= uasPath + imagefilename
    newImageFilePath= uasPath + imagename
    os.rename (oldImageFilePath,newImageFilePath)
    # Populate the metadata data structure for the renamed image
    metadata_record = init_metadata_record(flightId, sensor_id)
    metadata_record[24] = calculate_checksum(newImageFilePath)
    metadata_record[0]=record_id
    metadata_record[1]=imagename
    metadata_record[2]=flightId
    metadata_record[3]=sensor_id
    metadata_record[14]=cam_position_x
    metadata_record[15]=cam_position_y
    metadata_record[16]=cam_position_z
    metadata_record[17]=cam_latitude
    metadata_record[18]=cam_longitude
    metadata_record[19]=cam_sample_date_utc
    metadata_record[20]=cam_sample_time_utc
    metadata_record[21]=cam_lat_zone
    metadata_record[22]=cam_long_zone
    metadata_record[23]=cam_altitude_ref
    time.sleep(0.1)
    metadatalist.append(metadata_record)
    camIndex += 1

#
# Write out the metadata file
#
with open(uasmetfile, 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(
        ['record_id', 'image_file_name','flight_id', 'sensor_id', 'uas_position_x',
         'uas_position_y', 'uas_position_z', 'uas_latitude','uas_longitude','uas_sampling_date_utc','uas_sampling_time_utc',
         'uas_lat_zone', 'uas_long_zone','uas_altitude_reference','cam_position_x','cam_position_y', 'cam_position_z',
         'cam_latitude','cam_longitude','cam_sampling_date_utc','cam_sampling_time_utc','cam_lat_zone', 'cam_long_zone',
         'cam_altitude_reference', 'md5sum', 'notes'])
csvfile.close()

with open(uasmetfile, 'ab') as csvfile:
    print 'Generating metadata file', uasmetfile
    for lineitem in metadatalist:
        fileline = csv.writer(csvfile)
        fileline.writerow(
            [lineitem[0], lineitem[1], lineitem[2],lineitem[3], lineitem[4], lineitem[5], lineitem[6], lineitem[7],
             lineitem[8], lineitem[9], lineitem[10], lineitem[11], lineitem[12], lineitem[13], lineitem[14],
             lineitem[15],lineitem[16],lineitem[17],lineitem[18],lineitem[19],lineitem[20],lineitem[21],lineitem[22],
             lineitem[23],lineitem[24]])

csvfile.close()

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
