#!/usr/bin/python
#
# Program: calculate_experiment_id_uas_run

# Version: 0.1 October 6,2017
#
# This program will query a set of experiments,convert the coordinate system to long/lat and save the results to a file.
#
#
# TO DO - Make the search based on - later than planting date for given year e.g. '18%' and make these command line
# inputs

import sys
import mysql.connector
from mysql.connector import errorcode
import config
import os
from pyproj import Proj, transform
import csv
from shapely.geometry import Point, LineString, Polygon
from shapely.wkt import dumps,loads

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


def open_db_connection(config):

    # Connect to the HTP database
        try:
            cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD,
                                          host=config.HOST, port=config.PORT,database=config.DATABASE)
            print('Connecting to Database: ' + cnx.database)

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
                sys.exit()
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
                sys.exit()
            else:
                print(err)
        else:
            print('Connected to MySQL database:' + cnx.database)
            cursor = cnx.cursor(buffered=True)
        return cursor,cnx


def commit_and_close_db_connection(cursor,cnx):

    # Commit changes and close cursor and connection

    try:
        cnx.commit()
        cursor.close()
        cnx.close()

    except Exception as e:
            print('There was a problem committing database changes or closing a database connection.')
            print('Error Code: ' + e)

    return


exptList=[]
exptListItem=[]

uasRunList=[]
uasRunListItem=[]

uasRunPath= '/Users/mlucas/Desktop/HTP_Database_Updates/uas_run/uas_run_experiment.csv'

WGS84Proj = Proj(init='epsg:4326')


# Query the database for the experiment records

cursorA, cnxA = open_db_connection(config)
cursorB, cnxB = open_db_connection(config)

exptQuery=("SELECT record_id,ST_AsText(experiment_polygon),experiment_id FROM experiment WHERE experiment_id LIKE %s")
uasRunQuery=("SELECT record_id,ST_AsText(flight_polygon),flight_id FROM uas_run WHERE flight_id LIKE %s")
exptYear='18%'
flightYear='uas_2017%'
try:
    cursorA.execute(exptQuery, (exptYear,))

    if cursorA.rowcount != 0:
        for row in cursorA:
            exptListItem=[]
            recordId=str(int(row[0]))
            expt=row[1]
            exptId=row[2]
            if row[1] !=None:
                experimentPolygon=loads(row[1])
                exptListItem=[recordId,experimentPolygon,exptId]
                exptList.append(exptListItem)

    cursorB.execute(uasRunQuery,(flightYear,) )

    if cursorB.rowcount != 0:
        for row in cursorB:
            uasRunListItem = []
            recordId = str(int(row[0]))
            flight=row[1]
            flightId=row[2]
            if flight != None:
                flightPolygon = loads(row[1])
                uasRunListItem = [recordId, flightPolygon,flightId]
                uasRunList.append(uasRunListItem)

except:
    print 'Unexpected error during database query:', sys.exc_info()[0]
    print errorcode
    sys.exit()

print('Closing connection to database table: experiment ')
commit_and_close_db_connection(cursorA, cnxA)

print('Closing connection to database table: uas_run ')
commit_and_close_db_connection(cursorB, cnxB)

exptIntersections=[]
for flight in uasRunList:
    recordId=flight[0]
    flightPolygon=flight[1]
    flightId=flight[2]
    for experiment in exptList:
        experimentPolygon=experiment[1]
        exptId=experiment[2]
        intersectionArea = flightPolygon.intersection(experimentPolygon).area
        intersectionPercentage=(intersectionArea/experimentPolygon.area)*100
        if intersectionPercentage > 10.0:
            print(recordId,flightId,exptId,intersectionArea,intersectionPercentage)
            exptIntersectionRow=[recordId,flightId,exptId,intersectionPercentage]
            exptIntersections.append(exptIntersectionRow)

# Write out the experiment file with long/lat coords

with open(uasRunPath,'wb') as csvFile:
    print 'Writing uas_run experiment_id File'
    header=csv.writer(csvFile)
    header.writerow(['record_id', 'flight_polygon','experiment_id','percent_intersection'])
csvFile.close()

with open(uasRunPath,'ab') as csvFile:
    for row in exptIntersections:
        fileline = csv.writer(csvFile,quoting=csv.QUOTE_ALL,lineterminator = ',\n')
        fileline.writerow([row[0], row[1], row[2],row[3]])
    # Kludge to get rid of blank last line in the file which causes an empty row to be loaded into the database
    # when using LOAD DATA INFILE procedure!!
    csvFile.seek(-2, os.SEEK_END)
    csvFile.truncate()
csvFile.close()
sys.exit()


