__author__ = 'mlucas'
#!/usr/bin/python
#
# Program: 	get_utm_bounds
#
# Version:  0.1 April 29,2015       Initial Version
#
#
# This program will maximum and minimum values of uas_position_x and uas_position_y (utm) for a given flight.
# If the uas position columns are empty, the camera position columns will be used instead.
#
#
#
# OUTPUTS:  A csv file that can be used to update the uas_run table.
#
#

import local_config
import mysql.connector
from mysql.connector import errorcode
import csv
import sys
from decimal import *


getcontext().prec = 8
bufsize = 1

firstrow = 0
rownumber = 0

flights= []
utm_bounds={}

# Start of main program

outputfile = "uas_run_utm_data.csv"


# Open a new file to contain CSV output. File will be re-opened with
# append access when new data needs to be written to it.

try:
    with open(outputfile, 'wb') as csvfile:
        header = csv.writer(csvfile)
        header.writerow(['record_id','flight_id','uas_x_min', 'uas_x_max','uas_y_min', 'uas_y_max','cam_x_min', 'cam_x_max','cam_y_min', 'cam_y_max' ])
    csvfile.close
except:
    print('Error opening output file')
    sys.exit('Exiting program.')

#
#  Get the barcode information from the database
#

# Formulate the query statement

query = ("SELECT DISTINCT flight_id FROM uas_run_new")
#query = ("flight_id FROM uas_run_new")
# Connect to the wheatgenetics database

print ("Connecting to Database...")

try:
    cnx = mysql.connector.connect(user=local_config.USER, password=local_config.PASSWORD, host=local_config.HOST, database=local_config.DATABASE)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursor = cnx.cursor()

# Execute the query

print "Querying database for distinct flight ID values:", local_config.DATABASE
try:
    cursor.execute(query)
    if cursor.rowcount != 0:
        for row in cursor:
            flights+=row
except:
    print 'Unexpected error during database query:', sys.exc_info()[0]
    sys.exit()
finally:

    # Cleanup and Close Database Connection

    cursor.close

    print 'Closing database connection...'
    cnx.close()

for flight in flights:
    print flight
#
#  Get the minimum and maximum values for the x and y utm position
#
# Connect to the wheatgenetics database

print ("Connecting to Database...")

try:
    cnx = mysql.connector.connect(user=local_config.USER, password=local_config.PASSWORD, host=local_config.HOST,database=local_config.DATABASE)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursor = cnx.cursor()

# Execute the query
record_id=0
print "Querying database for the utm bounds:", local_config.DATABASE
try:
    for flight in flights:
        record_id+=1
        query2 = ("SELECT MIN(uas_position_x) as uas_min_x, MAX(uas_position_x) as uas_max_x,MIN(uas_position_y) as uas_min_y,MAX(uas_position_y) as uas_max_y, MIN(cam_position_x) as cam_min_x, MAX(cam_position_x) as cam_max_x,MIN(cam_position_y) as cam_min_y, MAX(cam_position_y) as cam_max_y FROM uas_images WHERE flight_id LIKE %s")
        flightkey=str(flight)
        print "Key: ",flightkey
        cursor.execute(query2, (flight,))
        if cursor.rowcount != 0:
            for utmrow in cursor:
                print utmrow[0],utmrow[1],utmrow[2],utmrow[3],utmrow[4],utmrow[5],utmrow[6],utmrow[7]
                #utm_bounds.append()
                #utm_bounds.append(flightkey,str(utmrow[0]),str(utmrow[1]),str(utmrow[2]),str(utmrow[3]),str(utmrow[4]),str(utmrow[5]),str(utmrow[6]),str(utmrow[7]))
                utm_bounds[record_id]=[flight,utmrow[0],utmrow[1],utmrow[2],utmrow[3],utmrow[4],utmrow[5],utmrow[6],utmrow[7]]
except:
    print 'Unexpected error during database query:', sys.exc_info()[0]
    sys.exit()
finally:

    # Cleanup and Close Database Connection

    cursor.close

    print 'Closing database connection...'
    cnx.close()

print "Writing Results File..."
#
# Write out results to a CSV file
#
with open(outputfile, 'ab') as csvfile:
    for key,value in utm_bounds.items():
        coords = csv.writer(csvfile)
        coords.writerow([key,value[0],value[1],value[2],value[3],value[4],value[5],value[6],value[7],value[8]])

# Exit the program gracefully
print ('Processing Completed. Exiting...')
sys.exit()
