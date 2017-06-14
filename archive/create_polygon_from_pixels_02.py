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
import argparse

print 'Running Python Version ',sys.version_info
print ''

# Main program body
# Get the path to the file to be imported from command line

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-p','--pixel', help='Full path to the pixel coordinate input file.')
cmdline.add_argument('-i','--images',help="Full path to the folder containing undistorted images")
cmdline.add_argument('-o','--out', help='Full path to the output folder')

args = cmdline.parse_args()
pixelCoordFile=args.pixel
imageFolder=args.images
outFolder=args.out
plotID=pixelCoordFile[-28:-18]
#pixelCoordFile='/Users/mlucas/Desktop/zenmuse_georeferencing/test4_pc_processing/16ASH20574_uvCoordinates.csv'
print "Processing pixel coordinates for plot", plotID

plotPixels=[]
plotConvexHulls={}
pointCount=0
lineCount=0
currentImage=''
nextImage=''
with open(pixelCoordFile) as pcFile:
    line = csv.reader(pcFile)
    next(line, None) # skip the header line
    for line in csv.reader(pcFile, delimiter=','):
        nextImage=line[0]
        imageName= nextImage.split('.')[0]
        if imageName !=currentImage:
            if currentImage != '':
                print "Finished processing points for",currentImage
                print "Point count for ",currentImage,pointCount
                print ''
                point_collection = geometry.MultiPoint(list(plotPixels))
                point_collection.envelope
                convex_hull_polygon = point_collection.convex_hull
                plotConvexHulls[currentImage] = convex_hull_polygon
            pointCount=0
            print "Starting processing points for ", currentImage
            currentImage=imageName
            plotPixels=[]
        else:
            row = int(line[2])
            column = int(line[1])
            point=Point(row,column)
            pointCount+=1
            plotPixels.append(point)

# Finish processing the data for the last image
print "Finished processing points for",currentImage
print "Point count for ",currentImage,pointCount
print ''
point_collection = geometry.MultiPoint(list(plotPixels))
point_collection.envelope
convex_hull_polygon = point_collection.convex_hull
plotConvexHulls[currentImage] = convex_hull_polygon

print ''
print 'Starting Image Clipping...'

for imageKey,cvHull in plotConvexHulls.iteritems():
    imageType=".tif"
    imageFileName=imageFolder+imageKey+imageType
    print "Clipping Image",imageFileName
    image = cv2.imread(imageFileName)
    imageType=".jpg"
    imageFileName=outFolder+imageKey+imageType
    cv2.imwrite(imageFileName,image)
    plot_image=np.zeros_like(image)
    pointCount=0
    pointsFound=0
    rows=image.shape[0]
    cols=image.shape[1]
    #numPoints=4096*2160
    numPoints=rows*cols
    convex_hull_polygon=cvHull
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
    outImageFile=outFolder+plotID+imageKey+"_intersection.jpg"
    print "Writing clipped image",outImageFile
    cv2.imwrite(outImageFile,plot_image)
sys.exit()

