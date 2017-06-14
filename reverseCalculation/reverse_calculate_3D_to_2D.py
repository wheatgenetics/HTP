#!/usr/bin/python
#
# Version: 0.1 July 20,2016
#
# Converts a set of 3D coordinates (X,Y,Z) of a point cloud into 2D coordinates (u,v)
# of a set of undistorted images.
#
# Command Line Inputs:
#
# '-p','--pcloud', help='Full path to the point cloud input file.'
# '-m','--pmatrix', help='Full path to the projection pmatrix input file'
# '-i','--images', help='Full path to folder containing images'
# '-w','--width',help='Camera Sensor Width in pixels'
# '-t','--height',help='Camera Sensor Height in pixels'
# '-o','--out', help='Full path to the output file of pixel coordinates.'
#
#
#

import cv2
import numpy as np
import sys
import subprocess
import argparse
import csv
import math
from plyfile import PlyData, PlyElement
import time
import exifread
from qgis.core import *

def get_image_file_list(fuasPath, fimageType):

    # Return a list of the names, date & time for all image files.

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

# Main program body
# Get the path to the file to be imported from command line

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-p','--pcloud', help='Full path to the point cloud input file.')
cmdline.add_argument('-m','--pmatrix', help='Full path to the projection pmatrix input file')
cmdline.add_argument('-i','--images', help='Full path to folder containing images')
cmdline.add_argument('-o','--out', help='Full path to the output folder')

args = cmdline.parse_args()

# Assign variables to command line inputs/

pcloudFile = args.pcloud
pcFileText = pcloudFile.split('.')
pcloudXYZFile=pcFileText[0]+'.xyz'
pmatrixFile=args.pmatrix
imagePixelFolder = args.out
uasPath=args.images
#imageType='jpg'
imageType='dng'
tempText=pcloudFile.split('.')
tempTextLen=len(tempText[0])
plotName=tempText[0][-10:]
imagePixelFile=imagePixelFolder+plotName+'_uvCoordinates.csv'
print "Processing point cloud segment for plot:",plotName


# Open the PLY formatted point cloud file and store as a list of x.y.z coordinates

lineCount = 0
pcloud=[]
pixelCoords=[]
print ''
print("Opening Point Cloud PLY file ", pcloudFile)
print("Loading PLY coordinates...")

pcFile = PlyData.read(pcloudFile)
numPoints=pcFile.elements[0].count

for i in range(numPoints):
    xi=float(pcFile.elements[0].data[i][0])
    yi=float(pcFile.elements[0].data[i][1])
    zi=float(pcFile.elements[0].data[i][2])
    red=int(pcFile.elements[0].data[i][03])
    green = int(pcFile.elements[0].data[i][04])
    blue = int(pcFile.elements[0].data[i][05])
    pcloud.append([xi,yi,zi,red,green,blue])
    lineCount +=1
    #print ("PLY Coordinates",xi,yi,zi)
    #time.sleep(0.01)

# Get the list of images to be processed so that image dimensions (width,length) can be determined.
# The image dimensions are required to filter out the pixel coordinates derived from the reverse calculation
# that do not fall within the range of the sensor size. The image dimensions can vary from image to image
# due to portrait/landscape orientation/

imagefiles = get_image_file_list(uasPath,imageType)

if len(imagefiles)==0:
    print "There were no image files found in ",uasPath
    print "Exiting"
    sys.exit(10)

# Find the EXIF width and height for each image.
# Some may be in portrait orientation and others may be in landscape orientation.

imageSizes={}

print "Reading image sizes..."

for f in imagefiles:
    imageFileName=uasPath+f
    with open(imageFileName, 'rb') as image:
        #print "Reading Image Size for ", imageFileName
        tags = exifread.process_file(image)
#       imageWidth=tags['EXIF ExifImageWidth']
#       imageLength = tags['EXIF ExifImageLength']
        imageWidth = tags['Image ImageWidth']
        imageLength = tags['Image ImageLength']
        #print f,imageWidth,imageLength
        imageSizes[f]=[int(imageWidth.values[0]),int(imageLength.values[0])]
    image.close()

# Open the Projection Matrix file for read access

print ''
print("Opening Projection Matrix file ", pmatrixFile)

lineCount=0

with open(pmatrixFile) as pmFile:
    for pmline in csv.reader(pmFile,delimiter=' '):
        image=pmline[0]
        pmatrix=np.array([[pmline[1],pmline[2],pmline[3],pmline[4]],
                          [pmline[5],pmline[6],pmline[7],pmline[8]],
                          [pmline[9],pmline[10],pmline[11],pmline[12]]],dtype=np.float64)


# Perform the 3D to 2D reverse calculation

        print("Processing Point Cloud for Image",image)
        imgName=image.split('.')
        imgKey=imgName[0] + '.dng'
        sensorWidth = imageSizes[imgKey][0]
        sensorHeight = imageSizes[imgKey][1]
        print ("Image Width", sensorWidth, "Image Height", sensorHeight)
        pixelCount=0

        for point in pcloud:
            lineCount+=1
            pcVector=np.array([point[0],point[1],point[2],1], dtype=np.float64)
            red=point[3]
            green=point[4]
            blue=point[5]
            uiVector = pmatrix.dot(pcVector)
            x = uiVector[0]
            y = uiVector[1]
            z = uiVector[2]
            u = int(math.floor(x / z))
            v = int(math.floor(y / z))

            if (u >=0 and u <=sensorWidth) and (v > 0 and v<=sensorHeight):
                pixelCount+=1
                #print (lineCount,pixelCount, image, "2D Coordinates:", u,v, "RGB Colors:",red,green,blue)
                pixelCoords.append([image,u,v,red,green,blue])
                #time.sleep(0.01)

# Write out the results of the 3D to 2D reverse calculation to a file.

with open(imagePixelFile,'wb') as csvFile:
    header = csv.writer(csvFile)
    header.writerow(['image_file', 'u', 'v', 'R', 'G', 'B'])
csvFile.close()
with open(imagePixelFile,'ab') as csvFile:
    print ''
    print 'Generating pixel coordinate file', imagePixelFile
    for lineitem in pixelCoords:
        fileline = csv.writer(csvFile)
        fileline.writerow([lineitem[0],lineitem[1], lineitem[2], lineitem[3], lineitem[4], lineitem[5]])

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()