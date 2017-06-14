__author__ = 'mlucas'

import exifread
import argparse

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
gpsVersionTag   = 'GPS GPSVersionID'

# Get command line input.

cmdline = argparse.ArgumentParser()
cmdline.add_argument('-p', '--path', help='Full Path to Image File')


args = cmdline.parse_args()

imageFile  = args.path

f = open(imageFile,'rb')
tags = exifread.process_file(f)
print tags

altStr          = str(tags[gpsAltTag])
altNum,altDenom = altStr.split('/')
camAltitude     = str(float(altNum)/float(altDenom))
print gpsAltTag,camAltitude

altRefStr          = str(tags[gpsAltRefTag])
if altRefStr== '0':
    camAltitudeRef='MSL'
else:
    camAltitudeRef='BMSL'
print gpsAltRefTag,camAltitudeRef

dateStr=str(tags[gpsDateTag])
year,month,day = dateStr.split(':')
camDate=year+'-'+month+'-'+day
print gpsDateTag,camDate


latStrLen       = len(str(tags[gpsLatTag]))-1
latStr          = str(tags[gpsLatTag])[1:latStrLen]
lat,latMins,latSecs = latStr.split(', ')
print lat, latMins,latSecs
latSecsNum,latSecsDenom = latSecs.split('/')
latSecsDec = float(latSecsNum)/float(latSecsDenom)
latDecimal      = float(lat)+ (float(latMins)/60) + latSecsDec/3600
print latDecimal

latRefStr       = str(tags[gpsLatRefTag])
print latRefStr

lonStrLen       = len(str(tags[gpsLongTag]))-1
lonStr          = str(tags[gpsLongTag])[1:lonStrLen]
lon,lonMins,lonSecs = lonStr.split(', ')
print lon, lonMins,lonSecs
lonSecsNum,lonSecsDenom = lonSecs.split('/')
lonSecsDec = float(lonSecsNum)/float(lonSecsDenom)
lonDecimal      = float(lon)+ (float(lonMins)/60) + lonSecsDec/3600
print lonDecimal

longRefStr      = str(tags[gpsLongRefTag])
print longRefStr

mapDatumStr  =str(tags[gpsMapDatumTag])
print mapDatumStr

statusStr= str(tags[gpsStatusTag])
print statusStr

timeStrLen = len(str(tags[gpsTimeTag]))-1
timeStr = str(tags[gpsTimeTag])[1:timeStrLen]
hrs, mins, secs = timeStr.split(', ')
camTime=hrs.zfill(2)+':'+mins.zfill(2)+':'+secs.zfill(2)
print camTime



