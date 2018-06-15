#!/usr/bin/python
from __future__ import print_function
from __future__ import unicode_literals

#
# Program: organize_micasense_flight_data
#
# Checks Micasense flight data sets for completeness and organizes files in a standard directory structure
#
# Parameters:
#
# '-d', '--dir', help='Absolute path to flight data set folder to be archived '
#
# '-o', '--out', help='Output path for the validated flight folders to be archived'
#
# Input flight data set folders should have a name in the following format:
#
#  <dateyyyymmdd>_<location>_<experiments>_<camera_type>_<planned_elevation>_<lens_angle>_<image_type>_<flight_number>
#
# Example:	20180404_18ASH_BYD0BYD2_Rededge_20m_-90_Still_Flight1
#
# Output folders (ready to archive) will have a name in the following format:
#
# 20180504_163838_MRE_20m_-90_still_0
# 20180504_164811_MRE_20m_-90_still_1
# 20180504_165507_MRE_20m_-90_still_2
#
# There is one output folder produced for each SET file in the Micasense input folder. In the example above,
# there were 3 SETS in the original input flight data folder:
#
# 0000SET, 0001SET and 0002 SET
#
# Version 0.1 May 11,2018

import os
import sys
import argparse
import datetime
import re
import subprocess
import piexif # Use this to find start date and time of the 100th image

# Command Line Inputs:
#
#
# '-d' or '--dir':      'Absolute path to flight data set'
# '-o' or '--out':      'Output file path'
#
#

def validate_date_string(flightDate):
    # Check that the date part of the input folder name is a valid date.
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
    # Check that the camera type part of the input folder name is a valid Micasense name.
    validCamera=True

    if camera in ['RedEdge','rededge','Rededge']:
        cameraType='MRE'
    else:
        raise ValueError("Invalid camera type :", camera)
        validCamera=False
        cameraType=None

    return validCamera, cameraType


def validate_elevation(elevation):
    # Check that the elevation part of the input folder name is a valid number 0f meters (example '20m').

    validElevation = True
    plannedElevation = elevation

    match = re.match(r"([0-9]+)([a-z]+)", elevation, re.I)
    if match:
        items = match.groups()

    else:
        raise ValueError("Incorrect elevation format, should be a numeric value in meters e.g. 20m")
        validElevation=False

    return validElevation,plannedElevation


def validate_lens_angle(angle):
    # Check that the lens angle is a valid number of degrees in the range 0 to 180.
    # Note that the lens angle is implicitly assumed to be negative (i.e. pointing downwards.)
    validAngle=True
    try:
        if abs(int(angle)) in range (0,181):
            lensAngle=str(abs(int(angle)))
        else:
            print("Lens angle must be in the range 0 to -180 (degrees)")
    except:
        raise ValueError("Incorrect lens angle, should be a numeric value in the range 0 to -180")
        lensAngle = None

    return validAngle,lensAngle


def validate_image_type(imgType):
    # Check that the image type is either 'still' or 'video'
    validImageType=True

    if imgType in ['still','Still']:
        imageType='Still'
    elif imgType in ['video','Video']:
        imageType='Video'
    else:
        raise ValueError("Invalid image type :",imgType)
        validImageType=False
        imageType=None

    return validImageType,imageType


def validate_flight_number(fltNumber):
    # Check that the flight number is of the format Flight<n> (Example: 'Flight1')
    validFlightNumber=True

    match = re.match(r"([a-z]+)([0-9]+)", fltNumber, re.I)
    if match:
        items = match.groups()
        flightNumber=items[1]
        int(flightNumber)
    else:
        raise ValueError("Invalid flight number :", fltNumber)
        validFlightNumber=False
        flightNumber=None

    return validFlightNumber,flightNumber

def getFlight_start_date_time(flightSet):
    # Determine the time of the first image in each flight SET.
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

cmdline.add_argument('-d', '--dir', help='Absolute path to flight data set folder to be archived')

cmdline.add_argument('-o', '--out', help='Output path for the validated flight folders to be archived')


try:

    args = cmdline.parse_args()

    inputPath=args.dir
    outputPath=args.out
    underscore='_'

# Get the input flight folder name and validate that the name conforms to naming convention

    inputFlightFolder=os.path.basename(inputPath)

    flightParams=inputFlightFolder.split('_')
    if len(flightParams) != 8:
        print('Input folder name does not conform to naming conventions:')
        print('<dateyyyymmdd>_<location>_<experiments>_<camera_type>_<planned_elevation>_<lens_angle>_<image_type>_<flight_number>')
        print('Example:	20180404_18ASH_BYD0BYD2_Rededge_20m_-90_Still_Flight1')
        print('Exiting.')
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
        print('Input folder name does not conform to naming conventions:')
        print('<dateyyyymmdd>_<location>_<experiments>_<camera_type>_<planned_elevation>_<lens_angle>_<image_type>_<flight_number>')
        print('Example:	20180404_18ASH_BYD0BYD2_Rededge_20m_-90_Still_Flight1')
        print('Exiting.')
        sys.exit()

    # Determine the number of flight SETs (nnnnSET) in the flight folder

    flightSets=[]
    flightSets=[os.path.join(inputPath,name,'') for name in os.listdir(inputPath) if os.path.isdir(os.path.join(inputPath,name))]
    flightSets.sort()

    if flightSets==[]:
        print("No flight data sets found in uav_staging...Exiting")
        sys.exit()

# Formulate the flight folder names according to the naming standard and rename the SET folders using the new names

    flightIndex=0
    for count,flight in enumerate(flightSets):
        originalFlightPath=flightSets[flightIndex]
        flightDate,flightStart=getFlight_start_date_time(originalFlightPath)
        validatedFlightParams = (flightDate, flightStart, plannedElevation, cameraType, lensAngle, imageType, str(flightIndex))
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
