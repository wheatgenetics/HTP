import cv2
import sys
import os
import argparse

# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='(Beocat) directory path to HTP imagefiles',
                     default='/homes/mlucas/uas_incoming/')

cmdline.add_argument('-v', '--video', help='Video filename')

cmdline.add_argument('-s', '--skip', help='Number of Video Frames to Skip')

cmdline.add_argument('-o','--out',help="Path to folder to contain output files")

args = cmdline.parse_args()

uasPath = args.dir
videoPath = args.dir + args.video
skipCount=int(args.skip)
outPath = uasPath + args.out
os.chdir(outPath)


print(cv2.__version__)
vidcap = cv2.VideoCapture(videoPath)
frameCount = 0
success = True
while success:
  success,image = vidcap.read()
  if frameCount%skipCount==0:
    print 'Extracting frame: ', frameCount, success
    countStr=str(frameCount).zfill(6)
    cv2.imwrite("frame_%s.jpg" % countStr, image)     # save frame as JPEG file
  frameCount += 1
