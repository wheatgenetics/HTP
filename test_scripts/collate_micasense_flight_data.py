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

# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Absolute path to flight data set folder')

cmdline.add_argument('-o', '--out', help='Output path')

args = cmdline.parse_args()

inputPath=args.dir
outputPath=args.out

inputFlightFolder=os.path.basename(inputPath)

flightParams=inputFlightFolder.split('_')
if len(flightParams) != 8:
    print('InputFolderName is not in the correct format. Exiting...')
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

# Need to determine flightStart here....
flightStart='173456'

validatedFlightParams=(flightDate,flightStart,cameraType,plannedElevation,lensAngle,imageType,flightNumber)
underscore='_'
outputFlightPath=os.path.join(outputPath,underscore.join(validatedFlightParams))
print('Output Folder:',outputFlightPath)

sys.exit()
