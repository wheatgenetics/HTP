#!/usr/bin/python
from __future__ import print_function
from __future__ import unicode_literals

#
# Program: organize_micasense_flight_data
#
# Checks Micasense flight data sets for completeness and organizes files in a standard directory structure
#
# Version 0.1 May 11,2018

import os
import sys
import argparse
import datetime
import re
import subprocess
import piexif # Use this to find start date and time of the 100th image
import shutil
import time
from pathlib import Path



# Command Line Inputs:
#
#
# '-d' or '--dir':      'Absolute path to flight data set'
# '-o' or '--out':      'Output file path'
#
#

def validate_date_string(flightDate):
    validDate=True
    try:
        if len(flightDate) != 8:
            validDate=False
        else:
            hyphen='-'
            year=flightDate[0:4]
            month=flightDate[4:6]
            day=flightDate[6:8]
            dateSequence=(year,month,day)
            dateString = hyphen.join(dateSequence)
            datetime.datetime.strptime(dateString, '%Y-%m-%d')
    except:
        raise ValueError("Incorrect data format, should be YYYYMMDD")
        validDate=False

    return validDate

def validate_camera_type(camera):
    validCamera=True

    if camera in ['RedEdge','rededge','Rededge']:
        cameraType='MRE'
    else:
        print('Invalid camera type :', camera)
        validCamera=False
        cameraType=None

    return validCamera, cameraType


def validate_elevation(elevation):
    validElevation = True

    match = re.match(r"([0-9]+)([a-z]+)", elevation, re.I)
    if match:
        items = match.groups()
        plannedElevation=elevation
    else:
        validElevation=False

    return validElevation,plannedElevation


def validate_lens_angle(angle):
    validAngle=True

    if abs(int(angle)) not in range (0,181):
        print('Invalid lens angle :',angle)
        validAngle=False
        lensAngle=None
    else:
        lensAngle=angle

    return validAngle,lensAngle


def validate_image_type(imgType):
    validImageType=True

    if imgType in ['still','Still']:
        imageType='still'
    elif imgType in ['video','Video']:
        imageType='Video'
    else:
        print('Invalid image type :',imgType)
        validImageType=False
        imageType=None

    return validImageType,imageType


def validate_flight_number(fltNumber):
    validFlightNumber=True

    match = re.match(r"([a-z]+)([0-9]+)", fltNumber, re.I)
    if match:
        items = match.groups()
        flightNumber=items[1]
    else:
        print('Invalid flight number :', fltNumber)
        validFlightNumber=False
        flightNumber=None

    return validFlightNumber,flightNumber

def getFlight_start_date_time(flightSet):
    imageList = []
    imageFileType = 'tif'
    imagePath = os.path.join(flightSet, '000')
    imageFileList = get_image_file_list(imagePath, imageFileType, imageList)
    for i in imageFileList:
        imageID = os.path.join(imagePath, imageFileList[1])
        exif_dict = piexif.load(imageID)
        dateTimeStr = exif_dict['Exif'][piexif.ExifIFD.DateTimeOriginal].decode('utf-8')
        dateStr = dateTimeStr.split(' ')[0].replace(':', '')
        timeStr = dateTimeStr.split(' ')[1].replace(':', '')
        if dateStr[0:4] > '2015':
            print("Flight Date and Start Time for Flight: ",flightSet ,dateStr, timeStr)
            print()
            break
    return dateStr,timeStr

def get_image_file_list(uasPath, imageType, imageFileList):
    # Return a list of the names and sample date & time for all image files.

    # Get list of files in uas staging directory

    #print("Fetching list of image files...")

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
        imageFileType=f.split('.')[1]
        isimagefile = (f != '' and imageFileType == imageType)
        if isimagefile:
            imageFileList.append(f)
    return imageFileList

# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Absolute path to flight data set folder')

cmdline.add_argument('-o', '--out', help='Output path')

args = cmdline.parse_args()

inputPath=args.dir
#inputPath=os.path.join(inputPath,'')
outputPath=args.out
underscore='_'

# Get the flight folder name and validate that the name conforms to naming convention

inputFlightFolder=os.path.basename(inputPath)

flightParams=inputFlightFolder.split('_')
if len(flightParams) != 8:
    print('InputFolderName is not in the correct format...Exiting.')
    sys.exit()

flightDate=flightParams[0]
validDate=validate_date_string(flightDate)

location=flightParams[1]
experiments=flightParams[2]

camera=flightParams[3]
validCamera,cameraType=validate_camera_type(camera)

elevation=flightParams[4]
validElevation,plannedElevation=validate_elevation(elevation)

angle=flightParams[5]
validAngle,lensAngle=validate_lens_angle(angle)

imgType=flightParams[6]
validImageType,imageType=validate_image_type(imgType)

fltNumber=flightParams[7]
validFlightNumber,flightNumber=validate_flight_number(fltNumber)

if not validDate or not validCamera or not validElevation or not validImageType or not validFlightNumber:
    print('Input folder name does not conform to naming conventions...Exiting.')

# Determine the number of flight sets (nnnnSET) in the flight folder

flightSets=[]
flightSets=[os.path.join(inputPath,name,'') for name in os.listdir(inputPath) if os.path.isdir(os.path.join(inputPath,name))]
flightSets.sort()

if flightSets==[]:
    print("No flight data sets found in uav_staging...Exiting")
    sys.exit()

# Formulate the flight folder names according to the naming standard

try:
    flightIndex=0
    for count,flight in enumerate(flightSets):
        originalFlightPath=flightSets[flightIndex]
        flightDate,flightStart=getFlight_start_date_time(originalFlightPath)
        validatedFlightParams = (flightDate, flightStart, cameraType, plannedElevation, lensAngle, imageType, str(flightIndex))
        newFlightPath = os.path.join(outputPath, underscore.join(validatedFlightParams))
        flightMetadataPath = os.path.join(originalFlightPath, "flightMetadata.txt")
        print("Original Flight Folder: ",originalFlightPath)
        print('New Flight Folder:', newFlightPath)
        print("Flight metadata file: ",flightMetadataPath,inputFlightFolder)
        with open(flightMetadataPath, 'w') as out:
            out.write(inputFlightFolder + '\n')
        os.rename(originalFlightPath, newFlightPath)
        print()
        flightIndex+=1

    # if the original flight folder is empty, delete it
    files = os.listdir(inputPath)
    if len(files) == 0:
        print
        "Removing empty folder:", inputPath
        os.rmdir(inputPath)
    else:
        print("Did not remove ", inputPath)
        print("Directory is not empty.")
except Exception as e:
    print('Unexpected error occurred while processing flight sets:', e)
    print('Exiting...')
    sys.exit()

sys.exit()
