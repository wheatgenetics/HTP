'''
Created on Nov 14, 2017

@author: xuwang
'''
import os
import argparse
from datetime import datetime
import errno
import exiftool
import shutil
import numpy
import cv2 # Installed with pip3 install opencv-python
import matplotlib
matplotlib.use('agg') # This is needed to get matplotlib find tkinter on Beocat.
import matplotlib.pyplot as plt
import micasense.metadata as metadata
import micasense.utils as msutils
from pyimagesearch.shapedetector import ShapeDetector
import imutils
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

import config
import mysql.connector # For successful installation, need to run pip3 install -U setuptools,pip install -U wheel
# and then pip3 install mysql-connector-python-rf
from mysql.connector import errorcode

import sys
from collections import defaultdict
#------------------------------------------------------------------------
# Panel Detection Parameter settings, MAY NEED TO MODIFY
black_th=110
# cont_th=7000
cont_th=4000
#------------------------------------------------------------------------
def panelDetect(image,b_th,ct_th):
    image = cv2.imread(image)
    resized = imutils.resize(image, width=640, height=480)
    ratio = image.shape[0] / float(resized.shape[0])
    gray = cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)
    thresh = cv2.threshold(blurred, b_th, 255, cv2.THRESH_BINARY)[1]
    cnts = cv2.findContours(thresh.copy(), cv2.RETR_EXTERNAL,
        cv2.CHAIN_APPROX_SIMPLE)
    cnts = cnts[0] if imutils.is_cv2() else cnts[1]
    sd = ShapeDetector()
    # loop over the contours
    sq=0
    for c in cnts:
        shape = "unidentified"
        M = cv2.moments(c)
        if M["m00"] != 0:
            cX = int(round((M["m10"] / M["m00"]))) # * ratio
            cY = int(round((M["m01"] / M["m00"]))) # * ratio
            shape = sd.detect(c)
        if shape == "square":
            if cv2.contourArea(c)>ct_th:
                sq +=1
                c = c.astype("float")
                c *= ratio
                c = c.astype("int")
                peri = cv2.arcLength(c, True)
                approx = cv2.approxPolyDP(c, 0.04 * peri, True)
                cv2.drawContours(image, [c], -1, (0, 255, 0), 1)
                cv2.putText(image, shape, (cX, cY), cv2.FONT_HERSHEY_SIMPLEX,0.5, (255, 255, 255), 1)
    if sq == 1:
        print("Panel Found:",approx)
        return approx
    else:
        return [[[0,0]],[[0,0]],[[0,0]],[[0,0]]]
#------------------------------------------------------------------------
def open_db_connection(config):

    # Connect to the HTP database
        try:
            cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD,
                                          host=config.HOST, port=config.PORT,
                                          database=config.DATABASE)
            print('Connecting to Database: ' + cnx.database)

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print('Something is wrong with your user name or password')
                with open(logname, 'a') as logoutput:
                    logoutput.write('Something is wrong with your user name or password\n')
                sys.exit()
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print('Database does not exist')
                with open(logname, 'a') as logoutput:
                    logoutput.write('Database does not exist')
                sys.exit()
            else:
                print(err)
        else:
            print('Connected to MySQL database:' + cnx.database)
            with open(logname, 'a') as logoutput:
                logoutput.write('Connected to MySQL database' + cnx.database + '\n')
            cursor = cnx.cursor(buffered=True)
        return cursor,cnx

#------------------------------------------------------------------------
def commit_and_close_db_connection(cursor,cnx):

    # Commit changes and close cursor and connection

    try:
        cnx.commit()
        cursor.close()
        cnx.close()

    except Exception as e:
            print('There was a problem committing database changes or closing a database connection.')
            print('Error Code: ' + e)
            with open(logname, 'a') as logoutput:
                logoutput.write('There was a problem committing database changes or closing a database connection.\n')
                logoutput.write('Error Code: ' + e,'\n')

    return
#------------------------------------------------------------------------
def validate_micasense_images(imageFileList):

    #pathToImages = os.path.join(subFolder + '*.' + imageType)

    print("Number of images before validation", len(imageFileList))

    validImageList = []
    invalidImageList=[]
    imageCheckDict = defaultdict(list)

    #imageFileList = os.listdir(subFolder)
    #imageFileList.sort()

    for f in imageFileList:

        #imageName = subFolder + f
        imageName=f
        a = f.split('/')[-1]
        primaryImageName = f.rpartition('_')[0]
        imageSize = os.stat(imageName).st_size
        imageCheckDict[primaryImageName].append([f, imageSize])

    for k, v in sorted(imageCheckDict.items()):
        truncatedImage = False
        missingImage = False

    # Check for image sets which have less than 5 images or more than 5 images and remove them from the valid list if found

        if len(v) != 5:
            missingImage=True

    # Check for images that have been truncated (less than  2Mb or 2097152 bytes) and remove the set from the valid list if found

        for i in imageCheckDict[k]:
            imageSize=i[1]
            if imageSize < 2097152:
                truncatedImage = True

        if truncatedImage or missingImage:
            for i in imageCheckDict[k]:
                invalidImageList.append(i[0])
            imageCheckDict.pop(k, )
            if truncatedImage:
                print('***Deleted Image Set' + k + 'Due to Truncated Image', k)
                with open(logname, 'a') as logoutput:
                    logoutput.write('***Deleted Image Set ' + k + ' Due to Truncated Image' + '\n')
            elif missingImage:
                print('***Deleted Image Set'+ k + ' Image set does not have 5 images.')
                with open(logname, 'a') as logoutput:
                    logoutput.write('***Deleted Image Set ' + k + ' Image set does not have 5 images.' + '\n')

    # Build the list of valid images to be processed further

    for k, v in sorted(imageCheckDict.items()):
        for i in range(0, 5):
            validImageList.append(v[i][0])

    print("Number of images that passed validation:", len(validImageList))
    invalidImageList.sort()

    return validImageList,invalidImageList

#------------------------------------------------------------------------

# construct the argument parse and parse the arguments

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-p', '--path', help='Path to Micasense flight data set directory',
                     default='/bulk/jpoland/images/staging/uas_staging/')
cmdline.add_argument('-c', '--panel', help='Calibration panel serial number')
cmdline.add_argument('-e', '--exift', help='Path to exiftool executable')
cmdline.add_argument('-b', '--black', help='Black Threshold',default=110)
cmdline.add_argument('-ct', '--contour', help='Contour Threshold',default=4000)
cmdline.add_argument('-r', '--rename', help='Rename images (Y or N)',default='N')

args = cmdline.parse_args()

filePath=args.path
calibPanel=args.panel
panelCalibration={}
exiftoolPath = args.exift
black_th = args.black
cont_th = args.contour
renameImages=args.rename

#------------------------------------------------------------------------

# Define the output LogFile

logFile = 'Log_'+datetime.now().strftime("%y%m%d_%H%M%S")+'.txt'
logname = os.path.join(filePath,logFile)
with open(logname, 'a') as logoutput:
    logoutput.write("File path is: %s\n" % filePath)
print("File path is: %s" % filePath)

#------------------------------------------------------------------------

# Query the database calibration table for calibration parameters

cursor, cnx = open_db_connection(config)
calibQuery=("SELECT parameter_type,parameter_value from calibration where serial_number LIKE %s")

try:
    cursor.execute(calibQuery, (calibPanel,))
    if cursor.rowcount != 0:
        for row in cursor:
            parameter_type=row[0]
            parameter_value=float(row[1])
            if parameter_type=='blue':
                panelCalibration['Blue']=parameter_value
            elif parameter_type=='green':
                panelCalibration['Green'] = parameter_value
            elif parameter_type == 'red':
                panelCalibration['Red'] = parameter_value
            elif parameter_type == 'red_edge':
                panelCalibration['Red edge'] = parameter_value
            elif parameter_type == 'nir':
                panelCalibration['NIR'] = parameter_value
            else:
                print("Unknown calibration parameter type: " + parameter_type)
                with open(logname, 'a') as logoutput:
                    logoutput.write("Unknown calibration parameter type: " + parameter_type + '\n')

    else:
        print("There were no calibration parameters found in the database for " + calibPanel)
        print("Exiting...")
        with open(logname, 'a') as logoutput:
            logoutput.write("There were no calibration parameters found in the database for " + calibPanel + '\n')
            logoutput.write("Exiting...\n")
        sys.exit()
except Exception as e:
    print('Unexpected error during database query:'+ e)
    print('Exiting...')
    with open(logname, 'a') as logoutput:
        logoutput.write('Unexpected error during database query:'+ e + '\n')
        logoutput.write("Exiting...\n")
    sys.exit()

print('Closing connection to database table: calibration')

with open(logname, 'a') as logoutput:
    logoutput.write("Closing connection to database table: calibration \n")
commit_and_close_db_connection(cursor, cnx)

#------------------------------------------------------------------------

# Create list of all images
exten = '.tif'
imList=[]
for dirpath, dirnames, files in os.walk(filePath):
    for name in files:
        if name.lower().endswith(exten):
            imList.append(os.path.join(dirpath, name))

with open(logname, 'a') as logoutput:
    logoutput.write("Total images in the path: %d\n" % len(imList))
print("Total images in the path: %d" % len(imList))

#------------------------------------------------------------------------
# Filter questionable images

finalImList=[]
questionImList=[]
finalImList,questionImList=validate_micasense_images(imList)

#------------------------------------------------------------------------
with open(logname, 'a') as logoutput:
    logoutput.write("Total effective images in the path: %d\n" % len(finalImList))
    logoutput.write("Images removed from path after validation:" + "\n")
    for i in questionImList:
        logoutput.write(i +"\n")
print("Total effective images in the path: %d" % len(finalImList))
#------------------------------------------------------------------------
# Create renamed path
try:
    os.makedirs(filePath + os.sep + "renamed") # os.sep for platform independence
    print("Creating Renamed directory.")
except OSError as exception:
    if exception.errno != errno.EEXIST:
        raise
# Rename and copy into Renamed directory
alti = [] # altitude
finalImList.sort()
for im in finalImList:
    imObj = im.split(os.sep)  # os.sep for platform independence
    numOfObj = len(imObj)
    imFile = imObj[numOfObj-1]
    if renameImages=='Y':
        with exiftool.ExifTool() as et:
            dtTags = et.get_tag('EXIF:DateTimeOriginal',im)
            exifAlti = float(et.get_tag('GPS:GPSAltitude',im))
            if exifAlti > 0:
                alti.append(exifAlti)
        dtTags = ''.join(dtTags.split(":")).replace(" ","_")
        tgFile = filePath + os.sep + "renamed" + os.sep + dtTags +"_" + imFile  # os.sep for platform independence
        newFile = shutil.copy2(im,tgFile)
        print("Copying %s" % newFile)
    else:
        tgFile=filePath + os.sep + "renamed" + os.sep + imFile
        archivedImage=shutil.copy2(im,tgFile)
        print('pass')
#------------------------------------------------------------------------
# Calculate altitude
alti_min = numpy.min(alti)
alti_mean = numpy.mean(alti)
alti_th = alti_min + 0.5*(alti_mean-alti_min)
with open(logname, 'a') as logoutput:
    logoutput.write("Altitude threshold: %.3f\n" % alti_th)
print("Altitude threshold: %.3f" % alti_th)
# Create low path
try:
    os.makedirs(filePath + os.sep + "low_altitude")  # os.sep for platform independence
    print("Creating LOW directory.")
except OSError as exception:
    if exception.errno != errno.EEXIST:
        raise
renamedIm = os.listdir(filePath + os.sep + "renamed")
blueIm = []
for im in renamedIm:
    if im.find("_1.tif") != -1:
        blueIm.append(im)
acc = 0
with exiftool.ExifTool() as et:
    for im in blueIm:
        alti = float(et.get_tag('GPS:GPSAltitude',filePath + os.sep + "renamed" + os.sep+im))
        if alti < alti_th:
            # Move to low directory
            newFile = shutil.move(filePath + os.sep + "renamed" + os.sep+im, filePath + os.sep + "low_altitude" + os.sep+im)
            print("Moving %s" % newFile)
            newFile = shutil.move(filePath + os.sep + "renamed" + os.sep+im.replace("_1.tif","_2.tif"), filePath + os.sep + "low_altitude" + os.sep+im.replace("_1.tif","_2.tif"))
            print("Moving %s" % newFile)
            newFile = shutil.move(filePath + os.sep + "renamed" + os.sep+im.replace("_1.tif","_3.tif"), filePath + os.sep + "low_altitude" + os.sep+im.replace("_1.tif","_3.tif"))
            print("Moving %s" % newFile)
            newFile = shutil.move(filePath + os.sep + "renamed" + os.sep+im.replace("_1.tif","_4.tif"), filePath + os.sep + "low_altitude" + os.sep+im.replace("_1.tif","_4.tif"))
            print("Moving %s" % newFile)
            newFile = shutil.move(filePath + os.sep + "renamed" + os.sep+im.replace("_1.tif","_5.tif"), filePath + os.sep + "low_altitude" + os.sep+im.replace("_1.tif","_5.tif"))
            print("Moving %s" % newFile)
            acc += 5
print("%d files moved to low_altitude" % acc)
with open(logname, 'a') as logoutput:
    logoutput.write("%d files moved to low_altitude\n" % acc)
#------------------------------------------------------------------------
#Calculate converting parameters

if acc > 0:
    imageFiles = os.listdir(filePath + os.sep + "low_altitude" )
    exiftoolPath = None
    if os.name == 'nt':
        exiftoolPath = 'D:/ExifTool/exiftool.exe'
    # Sum of each band's radiance 
    sbr_B = 0
    sbr_G = 0
    sbr_R = 0
    sbr_E = 0
    sbr_N = 0
    # Num of each band's radiance 
    nbr_B = 0
    nbr_G = 0
    nbr_R = 0
    nbr_E = 0
    nbr_N = 0
    for im in imageFiles:
        # Read raw image DN values
        imageName = filePath + os.sep + "low_altitude" + os.sep+im
        imageRaw=plt.imread(imageName)
        print("Processing %s" % imageName)
        meta = metadata.Metadata(imageName, exiftoolPath=exiftoolPath)
        bandName = meta.get_item('XMP:BandName')
        radianceImage, L, V, R = msutils.raw_image_to_radiance(meta, imageRaw)
        panel_coords = panelDetect(imageName, black_th, cont_th)
        print('Panel Coords',panel_coords[0][0][0])
        # Extract coordinates
        if panel_coords[0][0][0]:
            nw_x = int(panel_coords[0][0][0])
            nw_y = int(panel_coords[0][0][1])
            sw_x = int(panel_coords[1][0][0])
            sw_y = int(panel_coords[1][0][1])
            se_x = int(panel_coords[2][0][0])
            se_y = int(panel_coords[2][0][1])
            ne_x = int(panel_coords[3][0][0])
            ne_y = int(panel_coords[3][0][1])
            x_min = numpy.min([nw_x,sw_x,ne_x,se_x])
            x_max = numpy.max([nw_x,sw_x,ne_x,se_x])
            y_min = numpy.min([nw_y,sw_y,ne_y,se_y])
            y_max = numpy.max([nw_y,sw_y,ne_y,se_y])
            panelPolygon = Polygon([(sw_x, sw_y), (nw_x, nw_y), (ne_x, ne_y), (se_x, se_y)])
            numPixel = 0
            sumRadiance = 0
            for x in range(x_min,x_max):
                for y in range(y_min,y_max):
                    if panelPolygon.contains(Point(x,y)):
                        numPixel += 1
                        sumRadiance = sumRadiance+radianceImage[y,x]
            meanRadiance = sumRadiance/numPixel
            if bandName == 'Blue':
                sbr_B = sbr_B + meanRadiance
                nbr_B += 1
            elif bandName == 'Green':
                sbr_G = sbr_G + meanRadiance
                nbr_G += 1
            elif bandName == 'Red':
                sbr_R = sbr_R + meanRadiance
                nbr_R += 1
            elif bandName == 'Red edge':
                sbr_E = sbr_E + meanRadiance
                nbr_E += 1
            else:
                sbr_N = sbr_N + meanRadiance
                nbr_N += 1
    if nbr_B != 0:
        meanRadiance_B = sbr_B / nbr_B
    else:
        meanRadiance_B = 0
    if nbr_G != 0:
        meanRadiance_G = sbr_G / nbr_G
    else:
        meanRadiance_G = 0
    if nbr_R != 0:
        meanRadiance_R = sbr_R / nbr_R
    else:
        meanRadiance_R = 0
    if nbr_E != 0:
        meanRadiance_E = sbr_E / nbr_E
    else:
        meanRadiance_E = 0
    if nbr_N != 0:
        meanRadiance_N = sbr_N / nbr_N
    else:
        meanRadiance_N = 0
    # Select panel region from radiance image
    print("Mean Radiance of each band B-G-R-N-E: %.3f, %.3f, %.3f, %.3f,%.3f" % (meanRadiance_B,meanRadiance_G,meanRadiance_R,meanRadiance_N,meanRadiance_E))
    with open(logname, 'a') as logoutput:
        logoutput.write("Mean Radiance of each band B-G-R-N-E: %.3f, %.3f, %.3f, %.3f,%.3f\n" % (meanRadiance_B,meanRadiance_G,meanRadiance_R,meanRadiance_N,meanRadiance_E))
    if (meanRadiance_B == 0.0 or
        meanRadiance_G == 0.0 or
        meanRadiance_R == 0.0 or
        meanRadiance_N == 0.0 or
        meanRadiance_E == 0.0):
        with open(logname, 'a') as logoutput:
            logoutput.write("One or more bands had a Mean Radiance of 0.0...Exiting")
        print("One or more bands had a Mean Radiance of 0.0...Exiting")
        sys.exit()
    radianceToReflectance_B = panelCalibration["Blue"] / meanRadiance_B
    radianceToReflectance_G = panelCalibration["Green"] / meanRadiance_G
    radianceToReflectance_R = panelCalibration["Red"] / meanRadiance_R
    radianceToReflectance_N = panelCalibration["NIR"] / meanRadiance_N
    radianceToReflectance_E = panelCalibration["Red edge"] / meanRadiance_E
    print("Radiance to reflectance conversion factor of each band B-G-R-N-E: %.5f, %.5f, %.5f, %.5f,%.5f" % (radianceToReflectance_B,radianceToReflectance_G,radianceToReflectance_R,radianceToReflectance_N,radianceToReflectance_E))
    with open(logname, 'a') as logoutput:
        logoutput.write("Radiance to reflectance conversion factor of each band B-G-R-N-E: %.5f, %.5f, %.5f, %.5f,%.5f\n" % (radianceToReflectance_B,radianceToReflectance_G,radianceToReflectance_R,radianceToReflectance_N,radianceToReflectance_E))
#------------------------------------------------------------------------
# Calibrate Images
# Create calibrated path
radianceToReflectance = {
    "Blue": radianceToReflectance_B,
    "Green": radianceToReflectance_G,
    "Red": radianceToReflectance_R,
    "Red edge": radianceToReflectance_E,
    "NIR": radianceToReflectance_N
}
try:
    os.makedirs(filePath + os.sep + "calibrated")
    print("Creating Calibrated directory.")
except OSError as exception:
    if exception.errno != errno.EEXIST:
        raise
rawImages = os.listdir(filePath + os.sep + "renamed")
rawImages.sort()
for im in rawImages:
    print("Calibrating: %s" % filePath + os.sep + "renamed" + os.sep+im)
    flightImageRaw=plt.imread(filePath + os.sep + "renamed" + os.sep+im)
    meta = metadata.Metadata(filePath + os.sep + "renamed" + os.sep+im, exiftoolPath=exiftoolPath)
    bandName = meta.get_item('XMP:BandName')
    bitsPerPixel = meta.get_item('EXIF:BitsPerSample')
    dnMax = float(2**bitsPerPixel)
    flightRadianceImage, _, _, _ = msutils.raw_image_to_radiance(meta, flightImageRaw)
    flightReflectanceImage = flightRadianceImage * radianceToReflectance[bandName]
    flightReflectanceImage_u16=flightReflectanceImage*dnMax
    flightReflectanceImage_u16=flightReflectanceImage_u16.astype(numpy.uint16)
    cv2.imwrite(filePath + os.sep + "calibrated" + os.sep + im, flightReflectanceImage_u16)
#------------------------------------------------------------------------
# Copy EXIF attributes
# Copy EXIF:
renamePath=os.path.join(filePath,"renamed",'')
calPath = os.path.join(filePath, "calibrated",'')
rawImages = os.listdir(renamePath)
rawImages.sort()
for im in rawImages:
    renameImgPath=os.path.join(renamePath,im)
    calImgPath=os.path.join(calPath,im)
    os.system("exiftool -tagsFromFile %s %s" % (renameImgPath,calImgPath)) # Update EXIF from /renamed/IMG_nnnn_n.tif and Creates IMG_nnnn_n.tif_original
    tif_origPath = calImgPath.replace(".tif", ".tif_original")# Replace path IMG_nnnn_n.tif with IMG_nnnn_n.tif_original
    os.remove(tif_origPath)# Delete temporary file IMG_nnnn_n.tif_original
    # Copy XMP:
    os.system("exiftool -xmp -b %s > %s" % (renameImgPath, calImgPath.replace(".tif", ".xmp"))) # Create temporary file IMG_nnnn_n.xmp
    os.system("exiftool -tagsfromfile %s -xmp %s" % (calImgPath.replace(".tif", ".xmp"), calImgPath)) # Creates IMG_nnnn_n.tif from IMG_nnnn_n.xmp
# Cleanup: Remove temporary files *.xmp and *.tif_original
for img in os.listdir(calPath):
    if img.endswith(".xmp"):
        os.remove(os.path.join(calPath,img))
    elif img.endswith(".tif_original"):
        os.remove(os.path.join(calPath,img))

sys.exit()


