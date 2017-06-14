#!/usr/bin/python
#
# Program: create_uas_dji_x3_metadata_file
#
#
# Version: 0.1 May 23,2016
#
# Creates CSV file containing image metadata to be imported into the uas_images table in the wheatgenetics database.
#
# Command Line Inputs:
#
#
# '-d' or '--dir':      'Beocat directory path to source file folder, default='/homes/mlucas/uas_incoming/'
# '-s' or '--srate':    'Sample rate Hz
# '-o' or '--out':      'Output folder path'
#
#

__author__ = 'mlucas'

import subprocess
import os
import shutil
import sys
import argparse

bufsize = 1  # Use line buffering, i.e. output every line to the file.


def get_image_file_list(fuasPath, fimageType):
    # Return a list of the names and sample date & time for all image files.

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





# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Directory path to image file folder')

cmdline.add_argument('-t', '--type', help='Image file type extension, e.g. CR2, JPG',
                     default='JPG')

cmdline.add_argument('-s', '--srate', help='Sub-Sample Rate (Hz)')

cmdline.add_argument('-o', '--out', help='Beocat output folder path')

args = cmdline.parse_args()

inPath = args.dir
print 'Processing image file folder: ',inPath
imageType=args.type
print 'Image Type:', imageType
subSampleRate = int(args.srate)
print 'Sub-sample rate:', subSampleRate
outPath = args.out
print 'Creating output file folder:',outPath
if not os.path.exists(outPath):
    os.mkdir(outPath)

# Get the list of image files available for the flight/

imagefiles = get_image_file_list(inPath,imageType)
if len(imagefiles)==0:
    print "There were no image files found in ",inPath
    print "Exiting"

frameCount = 0
try:
    for f in imagefiles:
        imagefilename=f
        if frameCount % subSampleRate == 0:
            newfile=outPath+imagefilename
            oldfile=inPath+imagefilename
            shutil.copyfile(oldfile, newfile)
            print 'Copying:',imagefilename
        frameCount += 1

except Exception, e:
    print '*** Error*** Unable to process image file ',imagefilename
    print '*** Error Code:', e
    print '*** Trying to continue...'
    pass

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
