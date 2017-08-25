#!/usr/bin/python
#
# Program: convert_plot_coordinate_syste,

# Version: 0.1 February 22,2017
#
# This program will query a set of plots,convert the coordinate system to long/lat and save the results to a file.
#

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
        latLonPosition=utm.to_latlon(x,y,longZone,latZone)
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

plots={}
plotPrefix='17LDH%' # Need to make this a command line parameter
longZone=43
latZone='R'

plotPath= '/Users/mlucas/Desktop/LongLatPlots.csv'


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


# Determine which plots intersect the range segment

with open(plotPath,'w') as plotFile:
    print 'Writing Plot File'
    plotFile.write('plot_id' + ',' +'plot' + '\n')
    for plotId in sorted(plots.items()):
        longLatPlot=plotId[0] + ',"' + plotId[1].wkt+'"\n'
        plotFile.write(longLatPlot)

sys.exit()


