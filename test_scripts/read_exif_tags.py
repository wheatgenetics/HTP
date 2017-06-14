#!/usr/bin/python
__author__ = 'mlucas'

import subprocess
import csv
import time
import math
import sys
import argparse
import hashlib
import exifread
import piexif
import datetime
import uas.imagepreprocess

print uas.imagepreprocess.__name__

DateTag         = 'EXIF DateTimeOriginal'


#filename_jpg='/Users/mlucas/Desktop/16ASH_LK_20160526/16ASH_LK_20160526_images/DJI_A01733_C001_20160526_009750.jpg'
filename_tiff='/Users/mlucas/Desktop/Micasense/IMG_0003_1.tif'

with open(filename_tiff, 'rb') as image:
    print "Processing ", filename_tiff
    tags = exifread.process_file(image)
    dateStr = str(tags[DateTag])
    print ''
    print 'Body Serial Number: ' + str(tags['EXIF BodySerialNumber'])
    print 'Date Time Original: ' + str(tags['EXIF DateTimeOriginal'])
    print 'GPS Latitude:       ' + str(tags['GPS GPSLatitude'])
    print 'GPS Latitude Ref:   ' + str(tags['GPS GPSLatitudeRef'])
    print 'GPS Longitude:      ' + str(tags['GPS GPSLongitude'])
    print 'GPS Longitude Ref:  ' + str(tags['GPS GPSLongitudeRef'])
    print 'GPS Altitude:       ' + str(tags['GPS GPSAltitude'])
    print 'GPS Altitude Ref:   ' + str(tags['GPS GPSAltitudeRef'])
    print 'GPS GPSTimeStamp:   ' + str(tags['GPS GPSTimeStamp'])

image.close()



#filename_jpg2='/Users/mlucas/Desktop/Video_extraction_20161107/20161103_frame_2315.jpg'
#exif_dict={}
#with open(filename_jpg2, 'rb') as image2:
#    exif_dict = piexif.load(filename_jpg2)
#    print "Processing ", filename_jpg2
#    dateStr2='2016:05:26 13:40:21.125'
#    #tags2 = exifread.process_file(image2)
#    exif_dict['Exif'][piexif.ExifIFD.DateTimeOriginal] = dateStr2
#    exif_bytes = piexif.dump(exif_dict)
#    piexif.insert(exif_bytes, filename_jpg2)
#image2.close()



