__author__ = 'mlucas'
#!/usr/bin/python
#
# Program: 	populate uas_run_experiment_id
#
# Version:  0.1 May 1,2015       Initial Version
#
#
# This program will populate the experiment_ID column of the uas_run_new table
#
#
#
# OUTPUTS:  A csv file that can be used to update the uas_run table.
#


#import config
import config
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
flight_center_pts={}

# Start of main program

outputfile = "uas_run_expt_data.csv"


# Open a new file to contain CSV output. File will be re-opened with
# append access when new data needs to be written to it.

try:
    with open(outputfile, 'wb') as csvfile:
        header = csv.writer(csvfile)
        #header.writerow(['record_id','flight_id','experiment_id','location'])
        header.writerow(['record_id','experiment_id','location'])
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
    cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD, host=config.HOST, port=config.PORT,database=config.DATABASE)
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

print "Querying database for distinct flight ID values:", config.DATABASE
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

#for flight in flights:
#    print flight
#
#  Determine the center point for each flight boundary
#
# Connect to the wheatgenetics database

print ("Connecting to Database...")

try:
    cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD, host=config.HOST,port=config.PORT,database=config.DATABASE)
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
print "Querying database for the utm bounds:", config.DATABASE
try:
    for flight in flights:
        record_id+=1
        query2 = ("SELECT cam_x_min,cam_x_max,cam_y_min,cam_y_max FROM uas_run_new WHERE flight_id LIKE %s")
        flightkey=str(flight)
#        print "Key: ",flightkey
        cursor.execute(query2, (flight,))
        if cursor.rowcount != 0:
            for utmrow in cursor:
                x_min=utmrow[0]
                x_max=utmrow[1]
                y_min=utmrow[2]
                y_max=utmrow[3]
#                print "Flight Boundary (xmin,xmax,ymin,ymax)=",x_min,x_max,y_min,y_max
                flight_center_x=x_min + ((x_max-x_min)/2.0)
                flight_center_y=y_min + ((y_max-y_min)/2.0)
#                print "Flight Boundary Center Point = ",flight_center_x,flight_center_y
                flight_center_pts[record_id]=[flight,flight_center_x,flight_center_y]
except:
    print 'Unexpected error during database query:', sys.exc_info()[0]

    sys.exit()
finally:

    # Cleanup and Close Database Connection

    cursor.close

    print 'Closing database connection...'
    cnx.close()
#for flight in flights:
#    print flight
#
#  Determine the experiment ID for each flight
#
# Connect to the wheatgenetics database

print ("Connecting to Database...")

try:
    cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD, host=config.HOST,port=config.PORT,database=config.DATABASE)
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
print "Querying database for experiment_id:", config.DATABASE
try:
    for key,value in flight_center_pts.viewitems():
        query3 = ("SELECT * FROM experiment as e WHERE (e.C1_1_x < %s) AND (e.C1_2_x > %s) AND (e.C1_1_y < %s) AND (e.C2_1_y > %s)")
        #query3 = ("SELECT * FROM experiment as e WHERE ((e.C1_1_x-5.0) < %s) AND ((e.C1_2_x+5.0) > %s) AND ((e.C1_1_y-5.0) < %s) AND ((e.C2_1_y+5.0) > %s)")
#        print "Key and value: ",key,flight_center_pts[key]
        cursor.execute(query3,(value[1],value[1],value[2],value[2]))
        print '*',value
#        print "row count",cursor.rowcount
        if cursor.rowcount != 0:
            for row in cursor:
#               print "Key ",key,"Experiment ID:",row[0],row[1],row[2]
                flightYear= str(flight_center_pts[key][0][6:8])
#                print "Year",flightYear
                if row[1].startswith(flightYear):
                    expt_id = str(row[1])
                    loc= str(row[2])
                    flight_center_pts[key].append(expt_id)
                    flight_center_pts[key].append(loc)
                    print expt_id,loc
        if len(value) <= 3:
            flight_center_pts[key].append('Not Found')
            flight_center_pts[key].append('Not Found')

except:
    print 'Unexpected error during database query:', sys.exc_info()[0]

for key,value in flight_center_pts.viewitems():
    print key,value[0],value[1],value[2],value[3],value[4]

print "Writing Results File..."
#
# Write out results to a CSV file
#
with open(outputfile, 'ab') as csvfile:
    for key,value in flight_center_pts.items():
        coords = csv.writer(csvfile)
        #coords.writerow([key,value[0],value[3],value[4]])
        coords.writerow([key,value[3],value[4]])

# Exit the program gracefully
print ('Processing Completed. Exiting...')
sys.exit()
