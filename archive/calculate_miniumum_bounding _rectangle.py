#!/usr/bin/python
#
# Program: calculate_minimum_bounding_rectangle.pu
#
# Version: 0.1 December 4,2015 Initial Version
#
# Calculates the utm coordinates of the minimum bounding rectangle for an HTP run data set
#
# Command Line Inputs:
#
#
# '-r' or '--runTable':      'Run Table Name e.g; phemu_run
# '-o' or '--outputPath':    'Output path for mbr data file. Default /Users/mlucas/Desktop/run_table_mbr.csv'
#
# N.B Have not implemented command line input processing yet!


__author__ = 'mlucas'

import argparse
import local_config_htp
import csv
import sys
import mysql.connector
from mysql.connector import errorcode

# Start of main program

# Get command line input.

cmdline = argparse.ArgumentParser()
cmdline.add_argument('-r', '--runTable', help='Run Table Name e.g; phemu_run')
cmdline.add_argument('-o', '--outputPath',help ='Output path for mbr data file', default='/Users/mlucas/Desktop/mbr_per_run.csv')

args = cmdline.parse_args()

runtable= args.runTable
outfile = args.outputPath

# Connect to the wheatgenetics database

print ' '
print "Connecting to database:",local_config_htp.DATABASE
try:
  #cnx = mysql.connector.connect(user=config.USER,password=config.PASSWORD,host=config.HOST,port=config.PORT,database=config.DATABASE,buffered=True)
  cnx = mysql.connector.connect(user=local_config_htp.USER,password=local_config_htp.PASSWORD,host=local_config_htp.HOST,database=local_config_htp.DATABASE,buffered=True)
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print "Something is wrong with your user name or password."
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print "Database does not exist."
  else:
    print "Unknown Error Code: ",err
else:
 cursor1 = cnx.cursor(buffered=True)
 cursor2 = cnx.cursor(buffered=True)

# Execute the query to get run_ids from the ground_vehicle_run

print "Querying database:",local_config_htp.DATABASE

query1=("SELECT record_id,run_id FROM ground_vehicle_run")
cursor1.execute(query1)

#query2=("SELECT MIN(northing),MAX(northing),MIN(easting),MAX(easting) INTO @min_northing,@max_northing,@min_easting,@max_easting FROM sensor_measurements WHERE run_id=%s")
query2=("SELECT MIN(northing),MAX(northing),MIN(easting),MAX(easting) FROM sensor_measurements WHERE run_id=%s")
for record_id,run_id in cursor1:
    cursor2.execute(query2,(run_id,))
    for min_northing,max_northing,min_easting,max_easting in cursor2:
        print record_id,run_id,min_easting,max_easting,min_northing,max_northing

# Release the cursors
print ('')
print ('Closing cursors')
# noinspection PyStatementEffect
cursor1.close
cursor2.close

# Release the database connection

print ("Closing database connection")
# noinspection PyUnboundLocalVariable
cnx.close()


# Exit the program gracefully
print ('Processing Completed. Exiting...')
sys.exit()