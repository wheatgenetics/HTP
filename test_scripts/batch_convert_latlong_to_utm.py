import subprocess
import csv
import time
import math
import sys
import argparse
import hashlib
import exifread
import piexif

import utm
import datetime
import pytz
from pytz import timezone
from tzlocal import get_localzone
import collections
import bisect

# Get command line input.

#cmdline = argparse.ArgumentParser()


#cmdline.add_argument('-l', '--log', help='Flight Log File name')

#args = cmdline.parse_args()

flightLog = '/Users/mlucas/Desktop/2016-11-15_13-01-08_v2/2016-11-15_13-01-08_v2_latlong.csv'
utmOutFile= '/Users/mlucas/Desktop/2016-11-15_13-01-08_v2/2016-11-15_13-01-08_v2_utm.csv'
metadatalist = []
rowCount=0
with open(flightLog, 'rU') as logfile:
    log = logfile.readlines()
    for row in log:
        rowCount += 1
        if rowCount > 1:
            rowFields = row.split(',')
            gpsLatitude = float(rowFields[0])
            gpsLongitude = float(rowFields[1])
            altitude = float(rowFields[2]) * 0.3048
            utmPosition = utm.from_latlon(gpsLatitude, gpsLongitude)
            utmPositionX = utmPosition[0]
            utmPositionY = utmPosition[1]
            utmLatZone = utmPosition[2]
            utmLongZone = utmPosition[3]
            print gpsLatitude,gpsLongitude,altitude,utmPositionX,utmPositionY,utmLatZone,utmLongZone
            metadatalist.append([gpsLatitude,gpsLongitude,altitude,utmPositionX,utmPositionY,utmLatZone,utmLongZone])

with open(utmOutFile, 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(
        ['latitude', 'longitude','altitude', 'utm x', 'utmy','utm lat zone','utm long zone'])
csvfile.close()

with open(utmOutFile, 'ab') as csvfile:
    print 'Generating utm coordinate file', utmOutFile
    for lineitem in metadatalist:
        fileline = csv.writer(csvfile)
        fileline.writerow(
            [lineitem[0], lineitem[1], lineitem[2], lineitem[3], lineitem[4], lineitem[5], lineitem[6]])
csvfile.close()

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
