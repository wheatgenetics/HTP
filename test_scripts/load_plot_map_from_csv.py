#!/usr/bin/python
from __future__ import print_function
from __future__ import unicode_literals

import csv
import mysql.connector
from mysql.connector import errorcode
import config
import math
import sys
import os
import argparse
from shapely import wkt
from shapely.wkt import dumps
from shapely.geometry import Point,Polygon,MultiPoint


mapFile="/Users/mlucas/Desktop/fieldmap_18am3_shk_wFill_video.csv"

print("")
print("Connecting to Database...")

try:
    cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD, host=config.HOST,
                                  port=config.PORT, database=config.DATABASE)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursorA = cnx.cursor(buffered=True)

plot_insert = "INSERT INTO plot_map (record_id,plot_id,plot_polygon,C1_1_long,C1_1_lat,C2_1_long,C2_1_lat,C2_2_long,C2_2_lat,C1_2_long,C1_2_lat) VALUES (%s,%s,ST_PolygonFromText(%s),%s,%s,%s,%s,%s,%s,%s,%s)"


plotMapList=[]
plotPointList=[]
recordId=None
insertCount=0
with open(mapFile, 'rU') as plotMapFile:
    header = next(plotMapFile)  # Skip the header row
    for row in plotMapFile:
        plotRow = row.split(',')
        plotId = plotRow[0]
        c1_1_long=plotRow[1]
        c1_1_lat=plotRow[2]
        plotPointList.append((float(c1_1_long), float(c1_1_lat)))
        c2_1_long=plotRow[3]
        c2_1_lat=plotRow[4]
        plotPointList.append((float(c2_1_long), float(c2_1_lat)))
        c2_2_long=plotRow[5]
        c2_2_lat=plotRow[6]
        plotPointList.append((float(c2_2_long), float(c2_2_lat)))
        c1_2_long=plotRow[7]
        c1_2_lat=plotRow[8].rstrip()
        plotPointList.append((float(c1_2_long), float(c1_2_lat)))
        plotPolygon = dumps((MultiPoint(plotPointList)).convex_hull)
        plotMapRow=(recordId,plotId,plotPolygon,c1_1_long,c1_1_lat,c2_1_long,c2_1_lat,c2_2_long,c2_2_lat,c1_2_long,c1_2_lat)
        cursorA.execute(plot_insert, plotMapRow)
        cnx.commit()
        insertCount+=1
        print(insertCount,plotMapRow)
        plotMapList.append([recordId,plotId,plotPolygon,c1_1_long,c1_1_lat,c2_1_long,c2_1_lat,c2_2_long,c2_2_lat,c1_2_long,c1_2_lat])
        plotPointList = []
    pass
cursorA.close()
cnx.close()
print(insertCount,"records inserted into plot_map table")
sys.exit()