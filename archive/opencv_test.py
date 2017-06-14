#from __future__ import print_function
import cv2
import numpy as np
#import matplotlib.pyplot as plt
from scipy.spatial import ConvexHull
import csv
import sys

# load the image and show it
image = cv2.imread("/Users/mlucas/Documents/pix4dmapper/test4/1_initial/images/undistorted_images/DJI_A01733_C006_20160518_001000.tif")
d=image.shape
t=image.dtype
print ("Image Array Shape:", d, "Image Array Type:",t)
print ("Image First pixel:",image[0,0],"Image Last Pixel:",image [2159,4095])
imageCoords=np.zeros_like(image)
#imageCoords[:] = np.NAN
d1=image.shape
t1=image.dtype
plot_image=np.zeros_like(image)
#plot_image[:]=np.NAN
print ("Plot Coordinates Array Shape:", d1, "Plot Coordinates Array Type:",t1)
print ("Plot First pixel:",plot_image[0,0],"Plot Last Pixel:",plot_image [2159,4095])

#image = cv2.imread("/Users/mlucas/Desktop/2016-05-18_14-07-31_v2/DJI_A01733_C006_20160518_JPEG/DJI_A01733_C006_20160518_001014.jpg")
#cv2.imshow("original",image)
#cv2.waitKey(0)
#imageCoords=[]

pixelCoordFile='/Users/mlucas/Desktop/zenmuse_georeferencing/test4_pc_processing/16ASH20574_uvCoordinates.csv'
#points=np.array([])
with open(pixelCoordFile) as pcFile:
    for line in csv.reader(pcFile, delimiter=','):
        if 'DJI_A01733_C006_20160518_001014' in line[0]:
            #imageCoords.append((int(line[1]),int(line[2])))
            row=int(line[2])
            column=int(line[1])
            red=int(line[3])
            green=int(line[4])
            blue=int(line[5])
            imageCoords[row,column]=(red,green,blue)
            #print ("Pixel Coordinate:",row,column,"value is ",imageCoords[row,column])
#a=np.where(imageCoords == 0)
rows=image.shape[0]
cols=image.shape[1]
for i in range(0, rows):
    for j in range(0, cols):
        v=imageCoords[i,j]
        if np.all(v==0):
            #plot_image[i, j] = imageCoords[i, j]
            #plot_image[i, j] = [np.NAN,np.NAN,np.NAN]
            plot_image[i, j] = [128,128,128]
        else:
            plot_image[i, j] = image[i, j]
#plot_image=cv2.bitwise_and(imageCoords,image)
#plot_image=cv2.addWeighted(image,0.7,imageCoords,0.3,0)
#plot_image=imageCoords
#plot_image_hsv=cv2.cvtColor(plot_image, cv2.COLOR_BGR2HSV)
#plot_image_hsv += 100
cv2.imwrite('/Users/mlucas/Desktop/zenmuse_georeferencing/test4_pc_processing/16ASH20574_img_001000_intersection.jpg',plot_image)
#cv2.imshow('Undistorted Image',plot_image)
#cv2.waitKey(0)
#cv2.destroyAllWindows()
sys.exit()

#points=np.array(imageCoords)
#hull = ConvexHull(points)
#sys.exit()


