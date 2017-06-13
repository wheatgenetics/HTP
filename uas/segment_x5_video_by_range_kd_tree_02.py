#!/usr/bin/python
#
# Program: segment_x5_video_by_range_kd_tree
#
# Version: 0.2 May 4,2017  Added capability to identify plots associated with each video segment.
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

import mysql.connector
from mysql.connector import errorcode
import config
import time
import os
import logging
import utm

from shapely import wkt
from shapely.geometry import Point, LineString, Polygon

def convert_polygon_coord_system(plt):
    LonLatCoordString = 'POLYGON(('
    coordString=plt[9:-2]
    coords=coordString.split(',')
    for pos in coords:
        coordPair=str(pos).split(' ')
        x=float(coordPair[0])
        y=float(coordPair[1])
        latLonPosition=utm.to_latlon(x,y,43,'R')
        latCoord=str(latLonPosition[0])
        lonCoord=str(latLonPosition[1])
        LonLatCoordString+=lonCoord + ' ' + latCoord + ','
    LonLatPlt=LonLatCoordString[0:-1] + '))'
    return LonLatPlt


def setup_logging(output_path, file_log_level, console_log_level):
    '''Setup logging. Always to log file, optionally to command line if console_level isn't None. Return log.'''

    log = logging.getLogger("videoSegmentation")
    log.setLevel(logging.DEBUG)
    handler = logging.FileHandler(os.path.join(output_path, time.strftime("videoSegmentation.log")))
    handler.setLevel(file_log_level)
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)-5.5s]  %(message)s'))
    log.addHandler(handler)

    if console_log_level is not None:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(console_log_level)
        handler.setFormatter(logging.Formatter('[%(levelname)-5.5s] %(message)s'))
        log.addHandler(handler)

    return log


def log():
    '''Return main log that should be used by this package.'''
    return logging.getLogger('videoSegmentation')

def __init__(self, log):
    '''Constructor'''

    # The log is from the standard python logging module. Use it (+ set the command line arguments) instead of print statements.
    self.log = log


def open_db_connection(config):

    # Connect to the HTP database
        try:
            cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD,
                                          host=config.HOST, port=config.PORT,
                                          database=config.DATABASE)
            log().info('Connecting to Database: ' + cnx.database)

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                log().error("Something is wrong with your user name or password")
                sys.exit()
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                log().error("Database does not exist")
                sys.exit()
            else:
                log().error(err)
        else:
            log().info('Connected to MySQL database:' + cnx.database)
            cursor = cnx.cursor(buffered=True)
        return cursor,cnx


def commit_and_close_db_connection(cursor,cnx):

    # Commit changes and close cursor and connection

    try:
        cnx.commit()
        cursor.close()
        cnx.close()

    except Exception as e:
            log().info('There was a problem committing database changes or closing a database connection.')
            log().error('Error Code: ' + e)

    return



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
rangeLines={}
plots={}
plotPrefix='17LDH%' # Need to make this a command line parameter


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
plotRangePath=flightDataPath + 'RangePlotIntersections.csv'
lineSegmentsPath=flightDataPath + 'RangeLineSegments.csv'



# Query the database for the plot polygons and store them in a dictionary with plot_id as key

cursor, cnx = open_db_connection(config)
plotQuery=("SELECT plot_id,ST_AsText(plot_polygon) FROM plot_map WHERE plot_id LIKE %s")

try:
    cursor.execute(plotQuery, (plotPrefix,))
    if cursor.rowcount != 0:
        for row in cursor:
            plotId=row[0]
            plotPolygon=row[1]
            LonLatPlt = convert_polygon_coord_system(plotPolygon)
            plt=wkt.loads(LonLatPlt)
            plots[plotId]=plt
except:
    print 'Unexpected error during database query:', sys.exc_info()[0]
    sys.exit()

log().info('Committing changes and closing connection to database table: plot_map ')
commit_and_close_db_connection(cursor, cnx)

# Determine the video start position, date, time and video duration from the video EXIF

latitude,longitude,createDate,createTime,videoDuration=get_video_exif(videoPath)

print ''
print 'Video Start Time (local):            ' , createDate,createTime
print 'Video Duration:                      ' ,






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
endTime=nearestTime # The last time a nearest neighbour match was found

# Correct the first waypoint data to correspond to the time that the UAS started taking video

waypointRendezvous[1] = (waypointRendezvous[1][0],waypointRendezvous[1][1],startLat,startLon,int(startTime))

#for waypoint,values in sorted(waypointRendezvous.items()):
#    print waypoint,values[4]

# Determine the start time and duration of each video segment associated with a pair of waypoints that correspond to
# a range
rangeSegments={}
i=1
AId=0
BId=1
Atime=0
Btime=0
timeOffset=startTime # time offset is the time from the start of the flight to when video started.
for waypointID, values in sorted(waypointRendezvous.items()):
    nearestLat = values[0]
    nearestLon = values[1]
    nearestTime = values[4]
    if waypointID%2 == 0:
        pointB = Point(nearestLon,nearestLat)
        BId=waypointID
        rangeId='WP_'+str(AId).zfill(2)+ '-' + str(BId).zfill(2)
        if nearestTime >= Btime:
            print rangeId,nearestTime,Btime
            rangeLine = wkt.loads(LineString([pointA, pointB]).wkt)
            rangeLines[rangeId]=rangeLine
        else:
            break

        Btime = nearestTime
        segStart = Atime - timeOffset
        rawSegDuration = Btime - Atime
        segDuration = round(rawSegDuration / 1000.0, 3)
        segID = 'WP_'+str(AId).zfill(2)+ '-' + str(BId).zfill(2)
        rangeSegments[segID] = (segStart, segDuration)
        #print 'Range Segment:',segID,rangeSegments[segID][0],rangeSegments[segID][1]

    else:
        pointA = Point(nearestLon,nearestLat)
        AId=waypointID
        Atime=nearestTime
    i+=1

print ''


with open(lineSegmentsPath,'w') as lineSegmentsFile:
    lineSegmentsFile.write('range_id' + ',' + 'line_segment' + '\n')
    for rangeId in sorted(rangeLines.items()):
        lineSegment=rangeId[0] + ',"' + rangeId[1].wkt + '"\n'
        lineSegmentsFile.write(lineSegment)

# Determine which plots intersect the range segment

with open(plotRangePath,'w') as plotRangeFile:
    plotRangeFile.write('range_id' + ',' + 'plot_id' + ',' +'plot' + '\n')
    for rangeId in sorted(rangeLines.items()):
        for plotId in sorted(plots.items()):
            if rangeId[1].intersects(plotId[1]):
                #print 'Range Id ' + rangeId[0] +  ' intersects Plot ID ' + plotId[0] + ' , ' + plotId[1].wkt
                rangePlotLine=rangeId[0]+','+ plotId[0] + ',"' + plotId[1].wkt+'"\n'
                plotRangeFile.write(rangePlotLine)

#sys.exit()

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
            print segID, segStart, segDuration
            vidPath= cmdPath + videoFile
            segFile = cmdPath + videoFileName + '_' + segID + vidExt
            avCmd = 'avconv -i ' + vidPath + ' -ss '+ segStartStr + ' -t ' + segDurationStr + ' -codec copy ' + segFile + '\n'
            cmdFile.write(avCmd)

    cmdEnd='exit'
    cmdFile.write(cmdEnd)

segTimingPath=flightDataPath + 'segment_' + videoFileName +'_times'+ '.sh'
with open(segTimingPath,'w') as segTimingFile:
    for segID,segTimes in sorted(rangeSegments.items()):

        segStart = str(int(segTimes[0]))


        segDuration=str(int(round(segTimes[1]*1000.0,3)))

        if segTimes[1] < 0.0:
            print '***Positioning Error***'
            break
        else:
            print segID, segStart, segDuration
            vidPath= cmdPath + videoFile
            segFile = cmdPath + videoFileName + '_' + segID + vidExt
            timingCmd = 'avconv -i ' + vidPath + ' -ss '+ segStartStr + ' -t ' + segDurationStr + ' -codec copy ' + segFile + '\n'
            segTimingLine=segID+','+segStart+','+segDuration+'\n'
            segTimingFile.write(segTimingLine)


sys.exit()


