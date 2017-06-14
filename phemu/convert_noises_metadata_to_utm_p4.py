#!/usr/bin/python
#
# Program: convert_noises_metadata_to_utm_p4

# Version: 0.1 June 5,2017
#
# This program will convert one of the pheMU Noises metadata file to standard p4 output file.
# It will calculat utm_x and utm_y and will generate a new p4 header for the file.
# This is a special program that will have no value after this season.
#

from math import radians, cos, sin, asin, sqrt
from scipy.spatial import cKDTree
import numpy
import sys
import datetime
import subprocess
import argparse
import time
import os
import logging
import utm
import csv

# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-i', '--infiles', help='Comma separated list of input file paths')

cmdline.add_argument('-o' , '--outfiles', help= 'Output file path')

args = cmdline.parse_args()
inFolderPaths = args.infiles
inFolderList=inFolderPaths.split(',')
outPath=args.outfiles
noisesList=[]

for inPath in inFolderList:
    inFileName=inPath.split('/')[-1].split('.')[-2]
    with open(inPath,'rb') as infile:
        reader=csv.reader(infile)
        next(reader, None) # Skip header row
        for row in reader:
            plotId=''
            imageFileName=row[1]
            lon=float(row[2])
            lat=float(row[3])
            utmPosition = utm.from_latlon(lat,lon)
            utmX = str(utmPosition[0])
            utmY = str(utmPosition[1])
            samplingTimeUtc=row[5]
            newRow=[plotId,imageFileName,lon,lat,utmX,utmY,samplingTimeUtc]
            noisesList.append(newRow)

    outFilePath=outPath+inFileName+'_p4.csv'


    # Determine which plots intersect the range segment

    with open(outFilePath,'w') as p4File:
        print 'Writing Noises p4 File:',outFilePath
        fileline = csv.writer(p4File)
        fileline.writerow(['Plot_id', 'image_file_name', 'longitude', 'latitude', 'UTM_x', 'UTM_y', 'sampling_UTC'])
        for row in noisesList:
            fileline.writerow([row[0], row[1], row[2], row[3], row[4], row[5], row[6]])

    noisesList=[]


sys.exit()


