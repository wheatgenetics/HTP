# import the necessary packages
import argparse
import cv2
import numpy as np

# construct the argument parser and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-i", "--image", required=True,
                help="Path to the image to be thresholded")
ap.add_argument("-t", "--threshold", type=int, default=128,
                help="Threshold value")
args = vars(ap.parse_args())

# load the image and convert it to grayscale
img = cv2.imread(args["image"])
gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

# Apply initial threshold

#(ret, thresh) = cv2.threshold(gray, args["threshold"], 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
(ret, thresh) = cv2.threshold(gray, args["threshold"], 255, cv2.THRESH_BINARY_INV)

# noise removal
kernel = np.ones((3,3),np.uint8)
opening = cv2.morphologyEx(thresh,cv2.MORPH_OPEN,kernel, iterations = 2)

# identify the background area
sure_bg = cv2.dilate(opening,kernel,iterations=3)

# Identify the foreground area

dist_transform = cv2.distanceTransform(opening,cv2.DIST_L2,5)
(ret, sure_fg) = cv2.threshold(dist_transform,0.7*dist_transform.max(),255,0)

# Finding unknown region
sure_fg = np.uint8(sure_fg)
unknown = cv2.subtract(sure_bg,sure_fg)

# Marker labelling
(ret, markers) = cv2.connectedComponents(sure_fg)

#contours, hierarchy = cv2.findContours(thresh,cv2.RETR_TREE,cv2.CHAIN_APPROX_SIMPLE)
#cnt = contours[4]
#cv2.drawContours(thresh, [cnt], 0, (0,255,0), -1)

# Add one to all labels so that sure background is not 0, but 1
markers = markers+1

# Now, mark the region of unknown with zero
markers[unknown==255] = 0

markers = cv2.watershed(img,markers)
img[markers == -1] = [255,0,0]

imC = cv2.applyColorMap(img, cv2.COLORMAP_JET)

cv2.imshow('segmented',sure_bg)
cv2.waitKey(0)
#cv2.imwrite('/Users/mlucas/Desktop/DJI_A01733_C006_20160518_000212_thresh.png',thresh)
cv2.imwrite('/Users/mlucas/Desktop/DJI_A01733_C006_20160518_000212_seg.png',img)
cv2.destroyAllWindows()