#!/usr/bin/python
# -*- coding: UTF-8 -*-

import math

def calculate_initial_compass_bearing(pointA, pointB):
    #
    # Calculates the bearing between two points.
    #
    #The formulae used is the following:
    #    θ = atan2(sin(Δlong).cos(lat2),
    #             cos(lat1).sin(lat2) − sin(lat1).cos(lat2).cos(Δlong))
    #
    #:Parameters:
    #  - `pointA: The tuple representing the latitude/longitude for the
    #    first point. Latitude and longitude must be in decimal degrees
    #  - `pointB: The tuple representing the latitude/longitude for the
    #    second point. Latitude and longitude must be in decimal degrees
    #
    #:Returns:
    #  The bearing in degrees
    #
    #:Returns Type:
    #  float
    if (type(pointA) != tuple) or (type(pointB) != tuple):
        raise TypeError("Only tuples are supported as arguments")

    lat1 = math.radians(pointA[0])
    lat2 = math.radians(pointB[0])

    diffLong = math.radians(pointB[1] - pointA[1])

    x = math.sin(diffLong) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1)
            * math.cos(lat2) * math.cos(diffLong))

    initial_bearing = math.atan2(x, y)

    # Now we have the initial bearing but math.atan2 return values
    # from -180° to + 180° which is not what we want for a compass bearing
    # The solution is to normalize the initial bearing as shown below
    initial_bearing = math.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360

    return compass_bearing,initial_bearing

coordsA=(39.230267,-96.578674) #line 841
coordsB=(39.230265,-96.578679) #line 842

bearing=calculate_initial_compass_bearing(coordsA,coordsB)
print "Bearing calculated from lat/long:",bearing[1]
print "Compass Bearing from lat/long:",bearing[0]

bearingFromVelocity=math.atan2(-9.62,-5.37)
bearingFromVelocityDegrees = bearingFromVelocity*180.0/math.pi
print "Bearing calculated from x,y velocity:",bearingFromVelocityDegrees

uas_metadata_file='/Volumes/PolandLabHD/2016-05-26_13-32-54_v2/DJI_A01733_C001_20160526/uas_20160526_183335_20160526_184126_metadata.csv'


with open (uas_metadata_file,'rU') as log:
    header = next(log)  # Skip the header row
    start = next(log)
    rowFields = start.split(',')
    print rowFields[1],rowFields[7],rowFields[8],rowFields[10]
    start_frame = rowFields[1]
    start_lat = float(rowFields[7])
    start_lon = float(rowFields[8])
    start_time = rowFields[10]
    for row in log:
        rowFields = row.split(',')
        next_frame = rowFields[1]
        next_lat = float(rowFields[7])
        next_lon = float(rowFields[8])
        next_time = rowFields[10]

        coordStart = (start_lat,start_lon)
        coordNext  = (next_lat,next_lon)
        bearing = calculate_initial_compass_bearing(coordStart, coordNext)
        print "Bearing:",next_frame, next_time,bearing[1]

        start_frame=next_frame
        start_lat=next_lat
        start_lon=next_lon
        start_time=next_time





