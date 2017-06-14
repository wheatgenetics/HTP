#!/usr/bin/python
#
# Program: update_uav_metadata_db.py
#
# Version: 0.1 November 55,2014
#
# Updates the UAV flight metadata in the uav_images table in the wheatgenetics database.
# Also inserts record in flight table of the wheatgenetics database.
#
# Command Line Inputs:
#
#
# '-d' or '--dir':      'Beocat directory path to HTP image files', default='/homes/mlucas/uav_incoming/'
# '-l' or '--log':      'Operator log name'
# '-a' or '--autop':    'Autopilot log name
# '-t' or '--type':     'Image file type, e.g. CR2, JPG'
# '-o' or '--out':      'Output file path and filename'
#

__author__ = 'mlucas'

import subprocess
import os
import csv
import time
import math
import datetime
import utm
import sys
import argparse
import hashlib

secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)

bufsize = 1 # Use line buffering, i.e. output every line to the file.

def get_camera_id(opLogFile):
    # Lookup the camera serial number in the operator log.
    camera_id = 'CAM_432032012149'
    return camera_id

def get_image_file_list(uavPath,imageType):
    # Return a list of the names and sample date & time for all image files.

    imagefilelist = []

    # Get list of files in uav staging directory

    print("Fetching list of image files...")

    filestocheck = subprocess.check_output(['ls', '-1', uavPath], universal_newlines=True)

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
        startPos = len(f)-3
        endPos   = len(f)
        isimagefile = (f != '' and f[startPos:endPos] == imageType)
        if isimagefile:
            imagefilelist.append(f)

    return imagefilelist

def get_image_date_time(filename):
    isecFract = secsInWeek % 1
    (iyear, imonth, iday, ihh, imm, iss, idayOfWeek, ijulianDay, idaylightsaving) = time.gmtime(os.path.getmtime(filename))
    return (iyear, imonth, iday, ihh, imm, iss + isecFract)


def UTCFromGps(gpsWeek, SOW, leapSecs=16):
    #  A Python implementation of GPS related time conversions.
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
    t0 = time.mktime(epochTuple) - time.timezone  #mktime is localtime, correct for UTC
    tdiff = (gpsWeek * secsInWeek) + SOW - leapSecs
    t = t0 + tdiff
    (year, month, day, hh, mm, ss, dayOfWeek, julianDay, daylightsaving) = time.gmtime(t)

     #use gmtime since localtime does not allow to switch off daylight savings correction!!!

    return (year, month, day, hh, mm, ss + secFract)

def get_image_utm_position(fgpsLatitude,fgpsLongitude):

    futmPosition = utm.from_latlon(fgpsLatitude,fgpsLongitude)

    return futmPosition

def get_apm_gps_coords(fapmLog):
    fapmDict = {}
    with open(fapmLog,'rU') as logfile:
        log = logfile.readlines()
        for row in log:
            if row[0:3]=='GPS':
                rowFields  = row.split(',')
                gpsSecs    = round(float(rowFields[2])/1000,2)
                gpsWeek    = int(rowFields[3])
                gpsLat     = float(rowFields[6])
                gpsLong    = float(rowFields[7])
                gpsAlt     = float(rowFields[8])
                #print gpsSecs,gpsWeek,gpsLat,gpsLong,gpsAlt
                (lyear, lmonth,lday,lhh,lmm,lsecs) = UTCFromGps(gpsWeek, gpsSecs, 16)
                if round(math.modf(lsecs)[0],2) == 0.0:
                    #print lyear,lmonth,lday,lhh,lmm,lsecs
                    key = str(lyear)+str(lmonth)+str(lday)+str(lhh)+str(lmm)+str(int(lsecs))
                    #print(key)
                    fapmDict[key]=(lyear,lmonth,lday,lhh,lmm,lsecs,gpsLat,gpsLong,gpsAlt)
    return(fapmDict)

def hashfilelist(afile,blocksize=65536):
    hasher = hashlib.md5()
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()

def calculate_checksum(filename):
    checksum = hashfilelist(open(filename,'rb'))
    return checksum


# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat directory path to HTP imagefiles',
                     default='/homes/mlucas/uav_incoming/')

cmdline.add_argument('-l', '--log', help='Operator log name',
                     default='o.log')

cmdline.add_argument('-a', '--auto', help='Autopilot log name',
                     default='a.log')

cmdline.add_argument('-t', '--type', help='Image file type extension, e.g. CR2, JPG',
                     default='CR2')

cmdline.add_argument('-o', '--out', help='Output file path and filename',
                     default='/homes/mlucas/uav_incoming/uav_image_metadata.csv')

args = cmdline.parse_args()

uavPath     = args.dir
apmLog      = args.auto
opLog       = args.log
imageType   = args.type
uavmetfile  = args.out
exptid      = ''
flightid    = ''
sequence_number = None
notes       = ''
metadatalist    = []

sensor_id    = get_camera_id(opLog)
imagefiles   = get_image_file_list(uavPath,imageType)
apmDict      = get_apm_gps_coords(apmLog)

for f in imagefiles:
    filename = uavPath + f
    imagefilename = f
    (fyear, fmonth, fday, fhh, fmm, fsecs)  = get_image_date_time(filename)
    sampledate = str(fyear)+'/'+str(fmonth)+'/'+str(fday)
    sampletime = str(fhh)+':'+str(fmm)+ ':' +str(int(fsecs)).zfill(2)
    gpsKey = str(fyear)+str(fmonth)+str(fday)+str(fhh)+str(fmm)+str(int(fsecs))
    time.sleep (0.1)
    gpsLatitude     = apmDict[gpsKey][6]
    gpsLongitude    = apmDict[gpsKey][7]
    uavUtmPosition  = get_image_utm_position(gpsLatitude,gpsLongitude)
    absolute_position_x = uavUtmPosition[0]
    absolute_position_y = uavUtmPosition[1]
    absolute_position_z = apmDict[gpsKey][8]
    latzone  = uavUtmPosition[2]
    longzone = uavUtmPosition[3]
    f_checksum = calculate_checksum(filename)
    print sequence_number,imagefilename,exptid,flightid,sensor_id,absolute_position_x,absolute_position_y,absolute_position_z,latzone,longzone,sampledate,sampletime,f_checksum,notes
    metadatalist.append([sequence_number,imagefilename,exptid,flightid,sensor_id,absolute_position_x,absolute_position_y,absolute_position_z,latzone,longzone,sampledate,sampletime,f_checksum,notes])

with open(uavmetfile,'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(['sequence_number','image_file_name','experiment_id','flight_id', 'sensor_id','absolute_sensor_position_x','absolute_sensor_position_y','absolute_sensor_position_z','lat_zone', 'long_zone','sampling_date','sampling_time','md5sum','notes'])
csvfile.close

with open(uavmetfile,'ab') as csvfile:
    for lineitem in metadatalist:
        fileline = csv.writer(csvfile)
        fileline.writerow([lineitem[0],lineitem[1],lineitem[2],lineitem[3],lineitem[4],lineitem[5],lineitem[6],lineitem[7],lineitem[8],lineitem[9],lineitem[10],lineitem[11],lineitem[12]])
csvfile.close

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
