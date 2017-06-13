#!/usr/bin/python
#
# Program: segment_x5_video_by_range_kd_tree
#
# Version: 0.1 February 22,2017
#
# This program will generate the data necessary to segment a video taken by a UAV which traverses a field by range.
#
# It first constructs a KD tree of all timestamped positions recorded in a DJI UAV log file and then queries the
# KD Tree for each waypoint in the UAV flight plan to determine the point in the logfile that is nearest to the
# waypoint.
#
#Command Line Inputs:
#
# '-l' or '--log':      'Full path to the DJI log file'
# '-p' or '--plan':     'Full path to the flight plan file containing the positions of the way points.'
# '-v' or '--video':    'Full path to the video files to be segmented'
# '-o' or '--out':      'Output file path to the file containing the timestamped endpoints of each range'

from math import radians, cos, sin, asin, sqrt
from scipy.spatial import cKDTree
import numpy
import sys
import datetime
import subprocess
import argparse

def filter_log_by_isTakingVideo(logPath): # Generator function to filter log to include only lines with isTakingVideo=1
    with open(logPath, 'rU') as log:
        header = next(log)  # Skip the header row
        for row in log:
            rowFields = row.split(',')
            isTakingVideo = rowFields[37]
            if isTakingVideo == '1':
                yield row


def haversine_distance(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def get_video_exif(videoFileName):
    videoLat = ''
    videoLon = ''
    videoDate = ''
    videoTime = ''
    videoDuration = ''
    exifPath = ''

#    whichCmd=['which','exiftool']
#    exiftoolPath=subprocess.check_output(['which','exiftool'])
#    for char in exiftoolPath:
#        if char != '\n':
#            exifPath += char
#    print exifPath

#    proc = subprocess.Popen(['which','exiftool'],stdout=subprocess.PIPE)
#    tmp = proc.stdout.read()

    latCmd=['/usr/local/bin/exiftool', '-n', '-gpsLatitude', videoFileName]
    gpsVideoLat=subprocess.check_output(latCmd)
    for char in gpsVideoLat:
        if char != '\n':
            videoLat += char
    vlat = float(videoLat.split(': ')[1])

    lonCmd=['/usr/local/bin/exiftool', '-n', '-gpsLongitude', videoFileName]
    gpsVideoLon=subprocess.check_output(lonCmd)
    for char in gpsVideoLon:
        if char != '\n':
            videoLon += char
    vlon = float(videoLon.split(': ')[1])

    dateCmd=['/usr/local/bin/exiftool', '-n', '-trackcreatedate', videoFileName]
    gpsVideoDate=subprocess.check_output(dateCmd)
    for char in gpsVideoDate:
        if char != '\n':
            videoDate += char
    vdateTime = videoDate.split(': ')[1]
    vdate=vdateTime.split(' ')[0]
    vtime = vdateTime.split(' ')[1]
    timeCmd=['/usr/local/bin/exiftool', '-n', '-trackcreatetime', videoFileName]

    durationCmd = ['/usr/local/bin/exiftool', '-n', '-duration', videoFileName]
    gpsVideoDuration = subprocess.check_output(durationCmd)
    for char in gpsVideoDuration:
        if char != '\n':
            videoDuration += char
    vduration = videoDuration.split(': ')[1]

    return vlat,vlon,vdate,vtime,vduration

waypointRendezvous={}
rangeSegments={}

# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-i', '--inp', help='Full Path to Folder Containing Flight Data')
cmdline.add_argument('-l', '--log', help='Flight Log File Name')
cmdline.add_argument('-p', '--plan', help='Flight Plan File Name')
cmdline.add_argument('-v', '--vid', help='Flight Video File Name')
cmdline.add_argument('-c', '--cmd', help='Video Segmentation Command File Path')

args = cmdline.parse_args()


flightDataPath= args.inp
logFile = args.log
logPath = args.inp + logFile
planFile = args.plan
planPath = args.inp + planFile
videoFile = args.vid
videoPath = args.inp + videoFile
cmdPath = args.cmd

# Determine the video start position, date, time and video duration from the video EXIF

latitude,longitude,createDate,createTime,videoDuration=get_video_exif(videoPath)

print ''
print 'Video Start Time (local):            ' , createDate,createTime
print 'Video Duration:                      ' , videoDuration
print 'Video Start Position (lat/long):     ' , latitude,longitude
print ''

# Determine the video start position, date, time and video duration from the DJI log file

with open (logPath,'rU') as log:
    header = next(log)  # Skip the header row
    for row in log:
        rowFields = row.split(',')
        isTakingVideo=rowFields[37]

        if isTakingVideo == '1':

            # Record the time elapsed in ms since the flight started
            elapsedTime = rowFields[10]
            startTime = float(elapsedTime)

            # Convert the timestamp to UTC time
            timestamp = int(rowFields[43])
            dateTimeStr = datetime.datetime.utcfromtimestamp(timestamp / 1000.0).strftime('%Y/%m/%d %H:%M:%S.%f')
            #dateTimeStrLocal = datetime.datetime.fromtimestamp(timestamp / 1000.0).strftime('%Y/%m/%d %H:%M:%S.%f')
            startDateStr = dateTimeStr.split(' ')[0]
            startTimeStr = dateTimeStr.split(' ')[1]

            # Record the position that the UAV started taking video
            startLat = float(rowFields[0])
            startLon = float(rowFields[1])
            break

print ''
print 'Log Video Start Time (utc):          ' , startDateStr,startTimeStr
print 'Log Video Start Timestamp:           ' , timestamp
print 'Log Video Start Position (lat/long): ',startLat,startLon
print 'Elapsed Time to Start of Video (ms): ',elapsedTime
print ''

# Compute the delta in start position and time between video EXIF data and the log file

deltaDistance=haversine_distance(float(longitude),float(latitude),startLon,startLat) * 1000
deltaLat=haversine_distance(float(longitude),float(latitude),float(longitude),startLat) * 1000
deltaLon=haversine_distance(float(longitude),float(latitude),startLon,float(latitude)) * 1000

print 'Difference in video start GPS position between video EXIF and log file:', round(deltaDistance,2), 'meters'
print 'Difference in latitude between video EXIF and log file:                ',round(deltaLat,2),'meters'
print 'Difference in longitude between video EXIF and log file:               ',round(deltaLon,2),'meters'
print ''



# Load the flight log and flight plan data into an array structure

videoLines=filter_log_by_isTakingVideo(logPath)
flightLog=numpy.loadtxt(videoLines,delimiter=',',usecols=(0,1),skiprows=1)
videoTimeLines=filter_log_by_isTakingVideo(logPath)
flightLogTime=numpy.loadtxt(videoTimeLines,dtype={'names': ('lat', 'long','elapsed_ms'),'formats': (float, float, int)},delimiter=',',usecols=(0,1,10),skiprows=1)

#flightLog=numpy.loadtxt(logPath,delimiter=',',usecols=(0,1),skiprows=1)
#flightLogTime=numpy.loadtxt(logPath,dtype={'names': ('lat', 'long','elapsed_ms'),'formats': (float, float, int)},delimiter=',',usecols=(0,1,10),skiprows=1)
flightPlan=numpy.loadtxt(planPath,dtype={'names': ('waypoint', 'lat', 'long'),'formats': ('|S5', float, float)},delimiter=',',usecols=(0,1,2),skiprows=1)
flightPlan=numpy.loadtxt(planPath,dtype={'names': ('lat', 'long'),'formats': (float, float)},delimiter=',',usecols=(0,1,2),skiprows=1)

# Build the K-tree structure containing all points in the DJI log file

flightKDTree=cKDTree(flightLog, leafsize=100)

# Determine the nearest neighboring point (and associated timestamp) in the log file for each way point
i=1
for item in flightPlan:
    waypointID=i
    waypointLat=item[0]
    waypointLon=item[1]
    waypointLatLon=(item[0],item[1])
    nearest = flightKDTree.query(waypointLatLon, k=1, distance_upper_bound=6)
    nearestLat=flightLogTime[nearest[1]][0]
    nearestLon=flightLogTime[nearest[1]][1]
    nearestTime=flightLogTime[nearest[1]][2]
    waypointRendezvous[waypointID]=(waypointLat,waypointLon,nearestLat,nearestLon,nearestTime)
    i+=1

# Correct the first waypoint data to correspond to the time that the UAS started taking video

waypointRendezvous[1] = (waypointRendezvous[1][0],waypointRendezvous[1][1],startLat,startLon,int(startTime))

#for waypoint,values in sorted(waypointRendezvous.items()):
#    print waypoint,values[4]

# Determine the start time and duration of each video segment associated with a pair of waypoints that correspond to
# a range
lastWP=1
lastTime=0
timeOffset=startTime # time offset is the time from the start of the flight to when video started.
for waypointID, values in sorted(waypointRendezvous.items()):
    nextWP=waypointID
    nextTime=values[4]
    if waypointID % 2 == 0:
    #if waypointID > 1:
        segStart = lastTime - timeOffset
        #print lastWP,nextWP,lastTime,nextTime,nextTime-lastTime
        rawSegDuration = nextTime - lastTime
        segDuration = round(rawSegDuration / 1000.0, 3)
        segID = 'WP_' + str(lastWP).zfill(2) + '-' + str(nextWP).zfill(2)
        rangeSegments[segID] = (segStart, segDuration)
    lastTime = nextTime
    lastWP = nextWP

print ''


# Generate the avconv command file (.sh) required to split the video into segments by range.

videoFileName = videoFile.split('.')[0]
vidExt= '.' + videoFile.split('.')[1]
cmdFilePath = flightDataPath + 'segment_' + videoFileName + '.sh'
with open(cmdFilePath,'w') as cmdFile:
    shebang='#!/bin/bash' + '\n'
    cmdFile.write(shebang)
    for segID,segTimes in sorted(rangeSegments.items()):

        segStart = segTimes[0]
        segStartSecs = segStart/1000.0
        segStartMinutes,segStartSeconds = divmod(segStartSecs,60)
        segStartHours,SegStartMinutes = divmod(segStartMinutes,60)
        segStartStr= str(int(segStartHours)).zfill(2) + ':' + str(int(segStartMinutes)).zfill(2) + ':' + str(round(segStartSeconds,3)).zfill(2)

        segDuration=segTimes[1]
        segDurationMinutes,segDurationSeconds=divmod(segDuration,60)
        segDurationHours, SegDurationMinutes = divmod(segDurationMinutes, 60)
        segDurationStr = str(int(segDurationHours)).zfill(2) + ':' + str(int(segDurationMinutes)).zfill(2) + ':' + str(round(segDurationSeconds, 3)).zfill(2)

        if segTimes[1] < 0.0:
            print '***Positioning Error***'
            break
        else:
            print segID, segTimes[0], segTimes[1]
            vidPath= cmdPath + videoFile
            segFile = cmdPath + videoFileName + '_' + segID + vidExt
            avCmd = 'avconv -i ' + vidPath + ' -ss '+ segStartStr + ' -t ' + segDurationStr + ' -codec copy ' + segFile + '\n'
            cmdFile.write(avCmd)

    cmdEnd='exit'
    cmdFile.write(cmdEnd)

sys.exit()


