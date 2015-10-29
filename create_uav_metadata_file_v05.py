#!/usr/bin/python
#
# Program: create_uas_metadata_file
#
# Version: 0.5 March 31,2015
#
# Added capability to handle cases where number of autopilot log CAM events is different from number of image files.
#
# N.B. glob.glob function does not work on OS X.
#
# Version: 0.4 February 27,2015
#
# Added in capability to read camera exif metadata to retrieve GPS latitude, longitude, date and time.
# This information will be stored in an updated uas_images metadata file to allow comparison between
# UAS Position and Camera Position.
#
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
# Creates CSV file containing image metadata to be imported into the uas_images table in the wheatgenetics database.
#
# Command Line Inputs:
#
#
# '-d' or '--dir':      'Beocat directory path to HTP image files', default='/homes/mlucas/uas_incoming/'
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
import exifread

secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)
null_date = '0000/00/00'
null_time = '00:00:00'

bufsize = 1  # Use line buffering, i.e. output every line to the file.

# Declare Tags for image EXIF data
gpsAltTag       = 'GPS GPSAltitude'
gpsAltRefTag    = 'GPS GPSAltitudeRef'
gpsDateTag      = 'GPS GPSDate'
gpsLatTag       = 'GPS GPSLatitude'
gpsLatRefTag    = 'GPS GPSLatitudeRef'
gpsLongTag      = 'GPS GPSLongitude'
gpsLongRefTag   = 'GPS GPSLongitudeRef'
gpsMapDatumTag  = 'GPS GPSMapDatum'
gpsStatusTag    = 'GPS GPSStatus'
gpsTimeTag      = 'GPS GPSTimeStamp'


def get_image_file_list(fuasPath, fimageType):
    # Return a list of the names and sample date & time for all image files.

    imagefilelist = []

    # Get list of files in uas staging directory

    print("Fetching list of image files...")

    filestocheck = subprocess.check_output(['ls', '-1', fuasPath], universal_newlines=True)

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

    #secFract = SOW % 1
    epochTuple = gpsEpoch + (-1, -1, 0)
    t0 = time.mktime(epochTuple) - time.timezone  # mktime is localtime, correct for UTC
    tdiff = (gpsWeek * secsInWeek) + SOW - leapSecs
    t = t0 + tdiff
    (year, month, day, hh, mm, ss, dayOfWeek, julianDay, daylightsaving) = time.gmtime(t)

    # use gmtime since localtime does not allow to switch off daylight savings correction!!!

    return year, month, day, hh, mm, ss


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
                gpsAlt = float(rowFields[6]) # Use absolute altitude (Above Ground Level), Above sea level is index 5.
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
                                   str(lmm).zfill(2), str(lsecs).zfill(2))
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

def get_image_exif_data(ffilename):
    tags = exifread.process_file(image)
    fcam_position_x     = None
    fcam_position_y     = None
    fcam_position_z     = None
    fcam_latitude       = None
    fcam_longitude      = None
    fcam_sample_date    = null_date
    fcam_sample_time    = null_time
    fcam_lat_zone       = None
    fcam_long_zone      = None
    fcam_altitude_ref   = None
    try:

    # Get Camera GPS Altitude
        if gpsAltTag in tags:
            altStr          = str(tags[gpsAltTag])
            if '/' in altStr:
                altNum,altDenom = altStr.split('/')
                fcam_position_z  = str(float(altNum)/float(altDenom))
            else:
                fcam_position_z = altStr
        else:
            fcam_position_z = '0'


    #    Get Camera GPS Altitude reference MSL - Mean Sea Level BMSL = Below Mean Sea Level

        if gpsAltRefTag in tags:
            altRefStr = str(tags[gpsAltRefTag])
            if altRefStr == '0':
                fcam_altitude_ref='AMSL'
            else:
                fcam_altitude_ref='BMSL'
        else:
            fcam_altitude_ref='Not Available'


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
            fcam_latitude = (float(lat)+ (float(latMins)/60) + latSecsDec/3600) * (-1)
        elif latRefStr == "N":
            fcam_latitude = (float(lat)+ (float(latMins)/60) + latSecsDec/3600)


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
            fcam_longitude = (float(lon)+ (float(lonMins)/60) + lonSecsDec/3600) * (-1)
        elif longRefStr == "E":
            fcam_longitude = (float(lon)+ (float(lonMins)/60) + lonSecsDec/3600)

    # Get Camera UTM Position
        camUtmPosition  = get_image_utm_position(fcam_latitude, fcam_longitude)
        fcam_position_x = camUtmPosition[0]
        fcam_position_y = camUtmPosition[1]

    # Get Camera Latitude and Longitude Zone

        fcam_lat_zone   = camUtmPosition[2]
        fcam_long_zone  = camUtmPosition[3]

    # Get Camera Image Date and Time

        dateStr=str(tags[gpsDateTag])
        year,month,day = dateStr.split(':')
        fcam_sample_date=year+'/'+month+'/'+day

        timeStrLen = len(str(tags[gpsTimeTag]))-1
        timeStr = str(tags[gpsTimeTag])[1:timeStrLen]
        if '/' in timeStr:
            hrs, mins, secsFract = timeStr.split(', ')
            secsNum,secsDenom = secsFract.split('/')
            secs = str(int(secsNum)/int(secsDenom))
        else:
            hrs, mins, secs = timeStr.split(', ')

        fcam_sample_time=hrs.zfill(2)+':'+mins.zfill(2)+':'+secs.zfill(2)


    except Exception,e:
        print '*** Error*** Unable to process image file EXIF data for '
        print '*** Error Code:',e
        print '*** Null EXIF-based column values will be generated for',ffilename
    return fcam_position_x, fcam_position_y,fcam_position_z, fcam_latitude, fcam_longitude, fcam_sample_date,\
           fcam_sample_time, fcam_lat_zone, fcam_long_zone, fcam_altitude_ref

def get_image_log_data(ffilename,fcamEvents,fcamIndex):
    try:
        fsampledate     = null_date
        fsampletime     = null_time
        fuas_latitude   = None
        fuas_longitude  = None
        fuas_position_x = None
        fuas_position_y = None
        fuas_position_z = None
        fuas_latzone    = None
        fuas_longzone   = None
        fuas_altitude_ref = None
        ff_checksum     = None
        fsampledate = fcamEvents[fcamIndex][0] + '/' + fcamEvents[fcamIndex][1] + '/' + fcamEvents[fcamIndex][2]
        fsampletime = fcamEvents[fcamIndex][3] + ':' + fcamEvents[fcamIndex][4] + ':' + fcamEvents[fcamIndex][5]
        fuas_latitude = fcamEvents[fcamIndex][6]
        fuas_longitude = fcamEvents[fcamIndex][7]
        fuasUtmPosition = get_image_utm_position(fuas_latitude, fuas_longitude)
        fuas_position_x = fuasUtmPosition[0]
        fuas_position_y = fuasUtmPosition[1]
        fuas_position_z = fcamEvents[fcamIndex][8]
        fuas_latzone = fuasUtmPosition[2]
        fuas_longzone = fuasUtmPosition[3]
        fuas_altitude_ref = 'AGL'
        #ff_checksum = calculate_checksum(ffilename)
    except Exception,e:
        print '*** Error*** Unable to process image file autopilot log data.'
        print '*** Error Code:',e
        print '*** Null APM log-based column values will be generated for',ffilename

    return fsampledate,fsampletime,fuas_latitude,fuas_longitude,fuas_position_x,fuas_position_y,fuas_position_z, fuas_latzone, fuas_longzone,fuas_altitude_ref

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
    uas_altitude_ref=None
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
    blankrow=[record_id, imagefilename,flightId, sensor_id, uas_position_x, uas_position_y,
                         uas_position_z, uas_latitude, uas_longitude, uas_sample_date_utc, uas_sample_time_utc,
                         uas_latzone, uas_longzone, uas_altitude_ref, cam_position_x, cam_position_y, cam_position_z,
                         cam_latitude, cam_longitude, cam_sample_date_utc, cam_sample_time_utc, cam_lat_zone,
                         cam_long_zone,cam_altitude_ref, f_checksum, notes]
    return blankrow

# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat directory path to HTP imagefiles',
                     default='/homes/mlucas/uas_incoming/')

cmdline.add_argument('-c', '--cam', help='Camera ID')

cmdline.add_argument('-a', '--auto', help='Autopilot log name',
                     default='a.log')

cmdline.add_argument('-t', '--type', help='Image file type extension, e.g. CR2, JPG',
                     default='CR2')

cmdline.add_argument('-o', '--out', help='Output file path and filename',
                     default='/homes/mlucas/uas_incoming/uas_image_metadata.csv')

args = cmdline.parse_args()

#uasPath = args.dir + '/'
uasPath = args.dir
sensor_id = args.cam
apmLog = args.auto
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
camEvents, gpsEvents = get_apm_log_info(apmLog)

if len(imagefiles) > len(camEvents):
    print "The number of image files is greater than the number of CAM events."
    print
    print "Please check for and remove any extraneous test images that should not be included in data set"
    print "and then re-run this program to rename image files."
    print
    print "Exiting..."
    sys.exit()

if len(camEvents)==0:
    print "There were no CAM events found in", apmLog
    print "Attempting to use image file EXIF GPS data"
    print

flightId = 'uas_'+ gpsEvents[0][0] + gpsEvents[0][1] + gpsEvents[0][2] + '_' + \
           gpsEvents[0][3] + gpsEvents[0][4] + gpsEvents[0][5] + '_' + \
           gpsEvents[-1][0] + gpsEvents[-1][1] + gpsEvents[-1][2] + '_' + \
           gpsEvents[-1][3] + gpsEvents[-1][4] + gpsEvents[-1][5]

print 'Flight ID:', flightId

camIndex = 0
for f in imagefiles:
    metadata_record=init_metadata_record()
    filename = uasPath + f
    imagefilename = f
    metadata_record[24]=calculate_checksum(filename)
    metadata_record[0]=record_id
    metadata_record[1]=imagefilename
    metadata_record[2]=flightId
    metadata_record[3]=sensor_id
    #
    #Get GPS Data From Image File EXIF metadata
    #
    with open(filename,'rb') as image:
        print "Processing ",filename
        cam_position_x, cam_position_y,cam_position_z, cam_latitude, cam_longitude, cam_sample_date_utc, \
        cam_sample_time_utc, cam_lat_zone,cam_long_zone,cam_altitude_ref=get_image_exif_data(filename)
    image.close()
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

    #
    # Get GPS data from the AutoPilot logs CAM events
    #
    if len(imagefiles) == len(camEvents):
        uas_sample_date_utc,uas_sample_time_utc,uas_latitude,uas_longitude,uas_position_x,uas_position_y,\
        uas_position_z,uas_latzone, uas_longzone,uas_altitude_ref=get_image_log_data(filename,camEvents,camIndex)
        metadata_record[4]=uas_position_x
        metadata_record[5]=uas_position_y
        metadata_record[6]=uas_position_z
        metadata_record[7]=uas_latitude
        metadata_record[8]=uas_longitude
        metadata_record[9]=uas_sample_date_utc
        metadata_record[10]=uas_sample_time_utc
        metadata_record[11]=uas_latzone
        metadata_record[12]=uas_longzone
        metadata_record[13]=uas_altitude_ref

    time.sleep(0.1)

    #metadatalist.append([record_id, imagefilename,flightId, sensor_id, uas_position_x, uas_position_y,
    #                     uas_position_z, uas_latitude, uas_longitude, uas_sample_date_utc, uas_sample_time_utc,
    #                     uas_latzone, uas_longzone, uas_altitude_ref, cam_position_x, cam_position_y, cam_position_z,
    #                     cam_latitude, cam_longitude, cam_sample_date_utc, cam_sample_time_utc, cam_lat_zone,
    #                     cam_long_zone,cam_altitude_ref, f_checksum, notes])
    metadatalist.append(metadata_record)
    camIndex += 1
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
            [lineitem[0], lineitem[1], lineitem[2], lineitem[3], lineitem[4], lineitem[5], lineitem[6], lineitem[7],
             lineitem[8], lineitem[9], lineitem[10], lineitem[11], lineitem[12], lineitem[13], lineitem[14],
             lineitem[15],lineitem[16],lineitem[17],lineitem[18],lineitem[19],lineitem[20],lineitem[21],lineitem[22],
             lineitem[23],lineitem[24]])
csvfile.close()

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
