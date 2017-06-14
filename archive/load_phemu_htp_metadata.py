from __future__ import print_function

__author__ = 'mlucas'

import local_config
import mysql.connector
from mysql.connector import errorcode
import argparse
import csv
import sys

# Get the path to the file to be imported from command line

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-p', '--path', help='Full path to the metadata file to be imported')
cmdline.add_argument('-r', '--run', help='The run ID associated with the metadata file')

args = cmdline.parse_args()

fname = args.path
runID = args.run

# Connect to the wheatgenetics database

print("Connecting to Database...")

try:
    cnx = mysql.connector.connect(user=local_config.USER, password=local_config.PASSWORD, host=local_config.HOST,
                                  port=local_config.PORT,database=local_config.DATABASE)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursorA = cnx.cursor(buffered=True)
    cursorB = cnx.cursor(buffered=True)

db_insert = "INSERT INTO phemu_htp (run_id,sensor_id,sensor_observation,absolute_sensor_position_x," \
            "absolute_sensor_position_y,absolute_sensor_position_z,sampling_time_utc,sampling_date," \
            "left_utc,left_elevation,left_long,left_lat,long_zone,lat_zone,left_utm_x,left_utm_y," \
            "right_utc,right_elevation,right_long,right_lat,right_utm_x,right_utm_y," \
            "sensor_offset_x_from_left_gps,sensor_offset_y_from_left_gps,sensor_offset_z_from_left_gps) " \
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

db_check = "SELECT record_id FROM phemu_htp WHERE run_id LIKE %s"

# Open the file for read access

print("Opening file", fname)

with open(fname) as tsv:
    lcount = 0
    for line in csv.reader(tsv, delimiter="\t"):
        if lcount >=1:
            sensorid = line[0]
            sensorobservation = line[1]
            absolutesensorpositionx = line[2]
            absolutesensorpositiony = line[3]
            absolutesensorpositionz = line[4]
            samplingtimeutc = line[5][0:8]
            samplingdate = line[6]
            leftutc = line[7]
            leftelevation = line[8]
            leftlong = line[9]
            leftlat = line[10]
            longzone = line[11]
            latzone = line[12]
            leftutmx = line[13]
            leftutmy = line[14]
            rightutc = line[15]
            rightelevation = line[16]
            rightlong = line[17]
            rightlat = line[18]
            rightutmx = line[19]
            rightutmy = line[20]
            sensoroffsetxfromleftgps = line[21]
            sensoroffsetyfromleftgps = line[22]
            sensoroffsetzfromleftgps = line[23]
            data_insert = (runID, sensorid,sensorobservation, absolutesensorpositionx, absolutesensorpositiony,
                        absolutesensorpositionz,samplingtimeutc, samplingdate, leftutc, leftelevation, leftlong, leftlat,
                        longzone, latzone, leftutmx,leftutmy, rightutc, rightelevation, rightlong, rightlat, rightutmx,
                        rightutmy, sensoroffsetxfromleftgps,sensoroffsetyfromleftgps, sensoroffsetzfromleftgps)
            cursorA.execute(db_insert, data_insert)
            cnx.commit()
        lcount += 1
        print ("Processing record",lcount,end='\r')
cursorB.execute(db_check, (runID, ))
rcount = cursorB.rowcount

cursorA.close
cursorB.close

print("Number of metadata records processed", lcount-1)
print("Number of database records inserted", rcount)
print("")
print('Closing database connection...')

cnx.close()  # Exit the program gracefully

print('Processing Completed. Exiting...')

sys.exit()