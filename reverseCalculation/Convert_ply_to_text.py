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
from decimal import *
import math

getcontext().prec=15
# Main program body
# Get the path to the file to be imported from command line

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-p','--pcloud', help='Full path to the point cloud input file.')
cmdline.add_argument('-d','--offset',help='Full path the utm offsets file')
cmdline.add_argument('-o','--out', help='Full path to the output folder')

args = cmdline.parse_args()

# Assign variables to command line inputs

pcloudFile = args.pcloud
pcFileText = pcloudFile.split('.')
pcloudXYZFile=pcFileText[0]+'.xyz'
offsetsFile=args.offset



# Open offsets file to get Pix4D offsets used to convert from local coordinates to UTM

print 'Reading utm coordinate offset file ',offsetsFile

with open(offsetsFile) as f:
    line=f.read()
    offsets=line.split(' ')
    x_offset=float(offsets[0])
    y_offset=float(offsets[1])
    z_offset=float(offsets[2])
print 'x offset=',x_offset
print 'y_offset=',y_offset
print 'z_offset=',z_offset


# Open the PLY formatted point cloud file and store as a list of x.y.z coordinates
print ''
print "Processing point cloud segment for:",pcloudFile

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
    #easting=float(pcFile.elements[0].data[i][6])
    #northing= float(pcFile.elements[0].data[i][7])
    easting = xi + x_offset
    northing=yi+y_offset
    pcloud.append([xi,yi,zi,red,green,blue,easting,northing])
    lineCount +=1
print ''

with open(pcloudXYZFile,'wb') as csvFile:
    header = csv.writer(csvFile)
    header.writerow(['x', 'y', 'z', 'R', 'G', 'B','Easting','Northing'])
csvFile.close()
with open(pcloudXYZFile,'ab') as csvFile:
    print ''
    print 'Generating ASCII point cloud file', pcloudXYZFile
    for lineitem in pcloud:
        fileline = csv.writer(csvFile)
        fileline.writerow([lineitem[0],lineitem[1], lineitem[2], lineitem[3], lineitem[4], lineitem[5],lineitem[6],lineitem[7]])
sys.exit()
