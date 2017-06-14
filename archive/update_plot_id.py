from __future__ import print_function
__author__ = 'mlucas'

#import local_config - NB need port number when running on laptop
import config
import mysql.connector
from mysql.connector import errorcode
import argparse
import datetime
import sys

bufsize = 1  # Use line buffering, i.e. output every line to the file.

# Get table to be updated from command line

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-t', '--tbl', help='Table requiring plot_id update')
cmdline.add_argument('-y', '--yr', help='Planting Year')

args = cmdline.parse_args()

table = args.tbl
pyear = args.yr
starttime=datetime.datetime.now()
print ("Processing start time:",starttime)
print ("")

# Connect to the wheatgenetics database

print ("Connecting to Database...")

try:
    cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD, host=config.HOST,
                                  database=config.DATABASE)
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
    cursorC = cnx.cursor(buffered=True)


# Formulate the query statement

queryA = "SELECT record_id,absolute_sensor_position_x,absolute_sensor_position_y from `%s`" % (table,)
queryB ="SELECT plot_id FROM plot_map WHERE %s>=C2_1_x AND %s<=C2_2_x AND %s<=C2_1_y AND %s>=C1_1_y AND plot_id LIKE %s"
updateC = "UPDATE `%s` SET plot_id=%%s WHERE record_id=%%s" % (table,)

# Execute the query

print ("Querying database ", config.DATABASE)
try:

# cursorA.execute(queryA,(table,) )

    cursorA.execute(queryA, )
    print ("Rows returned from",table,":",cursorA.rowcount)
    if cursorA.rowcount != 0:
        for image_row in cursorA:
            recordID = image_row[0]
            if recordID%10000 == 0.0:
                completed=(100*recordID/cursorA.rowcount)
                elapsedtime=datetime.datetime.now()-starttime
                print ('Percentage of records processed: %.3f' % completed,'Elapsed Time:', elapsedtime, end='\r')
            x = image_row[1]
            y = image_row[2]
            cursorB.execute(queryB, (x, x, y, y, pyear))
            if cursorB.rowcount != 0:
                for plot_row in cursorB:
                    plotID = plot_row[0]
                    cursorC.execute(updateC, (plotID, recordID ))
                    cnx.commit()
except:
    print ('Unexpected error during database query:', sys.exc_info()[0])
    sys.exit()
finally:  # Cleanup and Close Database Connection

    cursorA.close
    cursorB.close
    cursorC.close

print ('Closing database connection...')
cnx.close()  # Exit the program gracefully
print ('Processing Completed. Exiting...')
elapsedtime=datetime.datetime.now()-starttime
print ("Total Processing Time:",elapsedtime)
sys.exit()
