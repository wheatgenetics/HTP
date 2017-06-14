#
# Version 0.1 June
#
#
#
# This is a module that contains commonly used functions for HTP programs

import subprocess
import time
import utm
import hashlib
import exifread

secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)
null_date = '0000/00/00'
null_time = '00:00:00'
bufsize = 1  # Use line buffering, i.e. output every line to the file.

def get_image_file_list(uasPath, imageType):
    # Return a list of the names and sample date & time for all image files.

    imagefilelist = []

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
            imagefilelist.append(f)

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

def get_image_utm_position(gpsLatitude, gpsLongitude):

    utmPosition = utm.from_latlon(gpsLatitude, gpsLongitude)

    return utmPosition

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

def get_image_exif_data(filename):

    # Declare Tags for image EXIF data

    gpsAltTag           = 'GPS GPSAltitude'
    gpsAltRefTag        = 'GPS GPSAltitudeRef'
    gpsDateTag          = 'GPS GPSDate'
    gpsLatTag           = 'GPS GPSLatitude'
    gpsLatRefTag        = 'GPS GPSLatitudeRef'
    gpsLongTag          = 'GPS GPSLongitude'
    gpsLongRefTag       = 'GPS GPSLongitudeRef'
    gpsMapDatumTag      = 'GPS GPSMapDatum'
    gpsStatusTag        = 'GPS GPSStatus'
    gpsTimeTag          = 'GPS GPSTimeStamp'
    exifCamSerialNo     = 'EXIF BodySerialNumber'
    exifImageDateTime   = 'EXIF DateTimeOriginal'

    tags = exifread.process_file(filename)

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

    # Get Camera GPS Altitude
        if gpsAltTag in tags:
            altStr          = str(tags[gpsAltTag])
            if '/' in altStr:
                altNum,altDenom = altStr.split('/')
                cam_position_z  = str(float(altNum)/float(altDenom))
            else:
                cam_position_z = altStr
        else:
            cam_position_z = '0'


    #    Get Camera GPS Altitude reference MSL - Mean Sea Level BMSL = Below Mean Sea Level

        if gpsAltRefTag in tags:
            altRefStr = str(tags[gpsAltRefTag])
            if altRefStr == '0':
                cam_altitude_ref='AMSL'
            else:
                cam_altitude_ref='BMSL'
        else:
            cam_altitude_ref='Not Available'


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

    # Get Camera UTM Position
        camUtmPosition  = get_image_utm_position(cam_latitude, cam_longitude)
        cam_position_x = camUtmPosition[0]
        cam_position_y = camUtmPosition[1]

    # Get Camera Latitude and Longitude Zone

        cam_lat_zone   = camUtmPosition[2]
        cam_long_zone  = camUtmPosition[3]

    # Get Camera Image Date and Time

        if gpsDateTag in tags:
            dateStr=str(tags[gpsDateTag])
            year,month,day = dateStr.split(':')
            cam_sample_date=year+'/'+month+'/'+day

            timeStrLen = len(str(tags[gpsTimeTag]))-1
            timeStr = str(tags[gpsTimeTag])[1:timeStrLen]
            if '/' in timeStr:
                hrs, mins, secsFract = timeStr.split(', ')
                secsNum,secsDenom = secsFract.split('/')
                secs = str(int(secsNum)/int(secsDenom))
            else:
                hrs, mins, secs = timeStr.split(', ')
            cam_sample_time=hrs.zfill(2)+':'+mins.zfill(2)+':'+secs.zfill(2)
        elif exifImageDateTime in tags:
            dateStr=str(tags[exifImageDateTime]).split(' ')[0]
            year, month, day = dateStr.split(':')
            cam_sample_date = year + '/' + month + '/' + day
            timeStr =str(tags[exifImageDateTime]).split(' ')[1]
            hrs, mins, secs= timeStr.split(':')
            cam_sample_time = hrs.zfill(2) + ':' + mins.zfill(2) + ':' + secs.zfill(2)

        if exifCamSerialNo in tags:
            cam_serial_no = str(tags[exifCamSerialNo])


    except Exception,e:
        print '*** Error*** Unable to process image file EXIF data for '
        print '*** Error Code:',e
        print '*** Null EXIF-based column values will be generated for',filename
    return cam_position_x, cam_position_y,cam_position_z, cam_latitude, cam_longitude, cam_sample_date,\
           cam_sample_time, cam_lat_zone, cam_long_zone, cam_altitude_ref,cam_serial_no

def init_metadata_record(flightId,sensor_id):
    record_id           = None
    imagefilename       = None
    uas_position_x      = None
    uas_position_y      = None
    uas_position_z      = None
    uas_latitude        = None
    uas_longitude       = None
    uas_sample_date_utc = null_date
    uas_sample_time_utc = null_time
    uas_latzone         = None
    uas_longzone        = None
    uas_altitude_ref    = None
    cam_position_x      = None
    cam_position_y      = None
    cam_position_z      = None
    cam_latitude        = None
    cam_longitude       = None
    cam_sample_date_utc = null_date
    cam_sample_time_utc = null_time
    cam_lat_zone        = None
    cam_long_zone       = None
    cam_altitude_ref    = None
    md5_checksum        = None
    notes=''
    blankrow=[record_id, imagefilename,flightId, sensor_id, uas_position_x, uas_position_y,
                         uas_position_z, uas_latitude, uas_longitude, uas_sample_date_utc, uas_sample_time_utc,
                         uas_latzone, uas_longzone, uas_altitude_ref, cam_position_x, cam_position_y, cam_position_z,
                         cam_latitude, cam_longitude, cam_sample_date_utc, cam_sample_time_utc, cam_lat_zone,
                         cam_long_zone,cam_altitude_ref, md5_checksum, notes]
    return blankrow


