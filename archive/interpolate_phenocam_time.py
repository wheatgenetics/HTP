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

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat directory path to phenocam files',
                     default='/homes/jpoland/images/staging/')

cmdline.add_argument('-i', '--imagein', help='Image log input file name')

cmdline.add_argument('-o', '--imageout', help='Image log output file name')

args = cmdline.parse_args()

phenoCamPath = args.dir
imageInputLogFile=phenoCamPath+args.imagein
imageOutputLogFile=phenoCamPath+args.imageout

inputCount=0
current_time=''
previous_time=0.0
new_time=0.0
correctedImageList=[]

with open(imageInputLogFile, 'rU') as logfile:
    log = logfile.readlines()
    for row in log:

        cam1=row.split(',')[0]
        cam2=row.split(',')[1]
        cam3=row.split(',')[2]
        current_time=row.split(',')[3]
        cam1=row.split(',')[0]
        if current_time == '000000.000':
            correct_time=str(previous_time + 0.8)
            previous_time=float(correct_time)
        else:
            correct_time=current_time
            previous_time=float(correct_time)
            #print cam1,cam2,cam3,correct_time
        correctedImageRecord=[cam1,cam2,cam3,correct_time]
        correctedImageList.append(correctedImageRecord)
with open(imageOutputLogFile, 'wb') as csvfile:
    print "Initializing output file"
csvfile.close()

with open(imageOutputLogFile, 'ab') as csvfile:
    print 'Generating corrected image log file', imageOutputLogFile
    for lineitem in correctedImageList:
        fileline = csv.writer(csvfile)
        fileline.writerow(
            [lineitem[0], lineitem[1], lineitem[2], lineitem[3]])
csvfile.close()

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()

