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
import csv

from shapely import wkt
from shapely.geometry import Point, LineString, Polygon

def convert_polygon_coord_system(plt):
    utmCoordString = 'POLYGON(('
    coordString=plt[10:-2]
    coords=coordString.split(',')
    for pos in coords:
        coordPair=str(pos).split(' ')
        y=float(coordPair[0])
        x=float(coordPair[1])
        utmPosition=utm.from_latlon(x,y)
        xCoord=str(utmPosition[0])
        yCoord=str(utmPosition[1])
        utmCoordString+=xCoord + ' ' + yCoord + ','
    utmPlt=utmCoordString[0:-1] + '))'
    return utmPlt

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

plots=[]
plotPrefix='17ASH%' # Need to make this a command line parameter

plotInPath= '/Users/mlucas/Desktop/2017_Ash_borders_4flightplan.csv'
plotOutPath='/Users/mlucas/Desktop/2017_Ash_border_plots.csv'


with open(plotInPath,'rb') as infile:
    preader=csv.reader(infile)
    next(preader, None)
    for row in preader:
        plt=row[0]
        pltId=row[1]
        utmPlot=convert_polygon_coord_system(plt)
        print utmPlot,pltId
        plt = wkt.loads(utmPlot)
        plots.extend([(pltId,plt)])

# Determine which plots intersect the range segment

with open(plotOutPath,'w') as plotFile:
    print 'Writing Plot File'
    plotFile.write('plot_id' + ',' +'plot_polygon' + '\n')
    for plot in plots:
        utmPlot=plot[0] + ',"' + plot[1].wkt+'"\n'
        plotFile.write(utmPlot)

sys.exit()


