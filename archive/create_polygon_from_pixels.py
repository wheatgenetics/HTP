import fiona
#import pylab as pl
import shapely.geometry as geometry
from shapely.geometry import mapping, Polygon
import csv
from shapely.geometry import Point
import shapely
import sys
from descartes import PolygonPatch
import cv2
import numpy as np

print 'Running Python Version ',sys.version_info
print ''

# Main program body
# Get the path to the file to be imported from command line

#cmdline = argparse.ArgumentParser()

#cmdline.add_argument('-p','--pixel', help='Full path to the pixel coordinate input file.')
#cmdline.add_argument('-o','--out', help='Full path to the output folder')

#args = cmdline.parse_args()
#pixelCoordFile=args.pixel
pixelCoordFile='/Users/mlucas/Desktop/zenmuse_georeferencing/test4_pc_processing/16ASH20580_uvCoordinates.csv'
plotPixels=[]
pointCount=0
with open(pixelCoordFile) as pcFile:
    for line in csv.reader(pcFile, delimiter=','):
        if 'DJI_A01733_C006_20160518_001014' in line[0]:
            row = int(line[2])
            column = int(line[1])
            point=Point(row,column)
            pointCount+=1
            plotPixels.append(point)

print 'Pixel Coordinate File Imported',pointCount,'points'

point_collection = geometry.MultiPoint(list(plotPixels))
point_collection.envelope

convex_hull_polygon = point_collection.convex_hull

image = cv2.imread("/Users/mlucas/Documents/pix4dmapper/test4/1_initial/images/undistorted_images/DJI_A01733_C006_20160518_001014.tif")
cv2.imwrite('/Users/mlucas/Desktop/zenmuse_georeferencing/test4_pc_processing/DJI_A01733_C006_20160518_001014.jpg',image)

plot_image=np.zeros_like(image)
pointCount=0
pointsFound=0
rows=image.shape[0]
cols=image.shape[1]
#numPoints=4096*2160
numPoints=rows*cols
for i in range(0, rows):
    for j in range(0, cols):
        point=Point(i,j)
        if convex_hull_polygon.contains(point):
            pointsFound+=1
            plot_image[i, j] = image[i, j]
        else:
            plot_image[i,j] = [128,128,128]
        pointCount+=1

        if pointCount % 100000 == 0:
            pct = float(pointCount / numPoints) * 100.0
            print "Processing point ",pointCount, pct,'% of', numPoints

cv2.imwrite('/Users/mlucas/Desktop/zenmuse_georeferencing/test4_pc_processing/16ASH20581_img_001014_intersection.jpg',plot_image)
sys.exit()

