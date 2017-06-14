#!/usr/bin/python
#
# Version: 0.1 September 7,2016
#
# Loads a PLY file containing a point cloud in local coordinates and geo-references it using
# the ASCII (XYZ) file containing the same point cloud in UTM coordinates.
#
# Segments a geo-referenced point cloud into plots using data from the wheatgenetics plot_map table.
#
# Loads a PLY file containing a point cloud in local coordinates and geo-references it using
# the ASCII (XYZ) file containing the same point cloud in UTM coordinates.
#
#
# Command Line Inputs:
#
# '-p','--pcloud', help='Full path to the PLY point cloud input file.'
# '-e','--expt', help='The experiment that identifies the set of plots to retrieve e.g. 16ASH.'
# '-o','--out', help='Full path to the folder that will contain the plot segments'
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
import local_config
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag
from qgis.core import *

# Initialize the qgis environment
# supply path to qgis install location
QgsApplication.setPrefixPath("/Applications", True)

# create a reference to the QgsApplication
# set the second argument to True enables the GUI, which is required for custom QGIS applications
#

qgs = QgsApplication([], True)

# load QGIS providers
qgs.initQgis()

# QGIS environment initialization is now complete

# Main program body
# Get the path to the file to be imported from command line

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-p','--pcloud', help='Full path to the point cloud input file.')
cmdline.add_argument('-e','--expt', help='The experiment that identifies the set of plots to retrieve e.g. 16ASH.')
cmdline.add_argument('-o','--out', help='The path to the output file')

args = cmdline.parse_args()

# Assign variables to command line inputs/

pcloudFile = args.pcloud
pcFileText = pcloudFile.split('.')
pcloudXYZFile=pcFileText[0]+'.xyz'
experiment=args.expt + '%'
outPath=args.out

# Load vector layer of polygon data from MySQL
#
#uri = "/Users/mlucas/desktop/zenmuse_georeferencing/plot_map_16ASH.csv?delimiter=%s&crs=epsg:4723&wktField=%s" % (",", "plot_polygon")
#vlayer=QgsVectorLayer(uri, "plots", "delimitedtext" )
#allAttrs = vlayer.pendingAllAttributesList()
#vlayer.select(allAttrs)
# Get all the features to start
#allfeatures = {feature.id(): feature for (feature) in vlayer}
#sys.exit()


# Connect to the wheatgenetics database

print("Connecting to Database...")

try:
    cnx = mysql.connector.connect(user=local_config.USER, password=local_config.PASSWORD, host=local_config.HOST,
                                 port=local_config.PORT,database=local_config.DATABASE,client_flags=[ClientFlag.LOCAL_FILES])
#   cnx = mysql.connector.connect(user=config.USER,password=config.PASSWORD,host=config.HOST,database=config.DATABASE,client_flags=[ClientFlag.LOCAL_FILES])

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursor  = cnx.cursor(buffered=True)

# Execute the query to retrieve the plot polygon from the plot_map table

plot_query=("SELECT plot_id, C2_1_x,C2_1_y,C2_2_x,C2_2_y,C1_2_x,C1_2_y,C1_1_x,C1_1_y FROM plot_map WHERE plot_id LIKE %s")

cursor.execute(plot_query, (experiment, ))
plotCount=cursor.rowcount
print("")
print "Number of plot records found for experiment",experiment, ":",plotCount

# Convert plot coordinates to a polygon and store each plot polygon in a dictionary with plot_id as key.

plot_polygons = {}

#index = QgsSpatialIndex()

print "Creating plot polygons for each plot..."

for (plot_id, C2_1_x,C2_1_y,C2_2_x,C2_2_y,C1_2_x,C1_2_y,C1_1_x,C1_1_y) in cursor:
    plot_shape=QgsGeometry.fromPolygon([[QgsPoint(C2_1_x,C2_1_y), QgsPoint(C2_2_x,C2_2_y), QgsPoint(C1_2_x,C1_2_y),
                                          QgsPoint(C1_1_x,C1_1_y), QgsPoint(C2_1_x,C2_1_y)]])
    plt_id = str(plot_id)
    plot_polygons[plt_id]= plot_shape
#    index.insertFeature(plot_polygons[plt_id])

cursor.close()

# Open the PLY formatted point cloud file and store as a list of x.y.z coordinates

lineCount = 0
pcloud=[]
pixelCoords=[]
print ''
print "Opening Point Cloud PLY file ", pcloudFile
print "Loading PLY coordinates..."

pcFile = PlyData.read(pcloudFile)
numPoints=pcFile.elements[0].count
pcFileHeader=pcFile.header

print "Loaded",numPoints,"points."
print "Header",pcFileHeader

for i in range(numPoints):
    xi=float(pcFile.elements[0].data[i][0])
    yi=float(pcFile.elements[0].data[i][1])
    zi=float(pcFile.elements[0].data[i][2])
    red=int(pcFile.elements[0].data[i][03])
    green = int(pcFile.elements[0].data[i][04])
    blue = int(pcFile.elements[0].data[i][05])
    pcloud.append([xi,yi,zi,red,green,blue])
    lineCount +=1

# Open the ASCII formatted point cloud file and store as a list of UTM x.y.z coordinates
# Matching a row in the ASCII file with the PLY file effectively geo-references the point cloud
# so that the plot segmentation be performed

print ''
print "Opening Point Cloud UTM file ", pcloudXYZFile
print "Loading UTM coordinates..."

pointCount=0
with open(pcloudXYZFile) as pcFile:
   for line in csv.reader(pcFile, delimiter=','):
        x=float(line[0])
        y=float(line[1])
        z=float(line[2])
        r=int(line[3])
        g=int(line[4])
        b=int(line[5])
        pt=QgsGeometry.fromPoint(QgsPoint(x,y))
        if (pcloud[pointCount][3]==r and
            pcloud[pointCount][4]==g and
            pcloud[pointCount][5]==b):
            pcloud[pointCount].extend([x,y,z,r,g,b,pt])
        else:
            print "Mismatch found between PLY RGB values and XYZ RGB values...exiting."
            sys.exit()
        pointCount += 1
print ''

# Initialize dictionary plot points used to store point cloud points per plot.

plot_points={}

for plt_id,plt in plot_polygons.iteritems():
    plot_points[plt_id]=[]

# Iterate through the point cloud to find which plots the points fall into.

print "Segmenting point cloud..."

pointCount=0

for p in pcloud:
    if pointCount%10000==0:
        print ("Processing point",pointCount,"of",numPoints)
    pointCount += 1
    pt=p[12]
    for plt_id,plt in plot_polygons.iteritems():
        if plt.contains(pt):
            #plot_points[plt_id].append((p[0],p[1],p[2],p[3],p[4],p[5]))
            plot_points[plt_id].append((p[0], p[1], p[2], p[3], p[4], p[5], p[6],p[7]))

for plt_id,point_list in plot_points.iteritems():
    if len(point_list) > 0:
        pcloudPlotSegmentFile = pcFileText[0] + '_' + plt_id + '.ply'
        print ''
        vertex=np.array([])
        vertex = np.array(point_list, dtype=[('x', 'f4'), ('y', 'f4'), ('z', 'f4'),('diffuse_red', 'u1'), ('diffuse_green', 'u1'), ('diffuse_blue', 'u1'), ('easting','f4'),('northing','f4')])
        #vertex = np.array(point_list,dtype=[('x', 'f4'), ('y', 'f4'), ('z', 'f4'), ('diffuse_red', 'u1'), ('diffuse_green', 'u1'),('diffuse_blue', 'u1')])

        # Write out plot segments to files using the plot name to identify each plot seqment in the point cloud
        print "Writing Plot File ", pcloudPlotSegmentFile,'for plot',plt_id
        el = PlyElement.describe(vertex, 'vertex')
        PlyData([el]).write(pcloudPlotSegmentFile)

# Cleanup: call exitQgis() to remove the provider and layer registries from memory
qgs.exitQgis()

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()