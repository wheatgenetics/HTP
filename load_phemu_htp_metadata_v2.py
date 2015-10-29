from __future__ import print_function

__author__ = 'mlucas'

#import local_config
import config
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.constants import ClientFlag
import argparse
import datetime
import sys

# Get the path to the file to be imported from command line

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-p', '--path', help='Full path to the metadata file to be imported')
cmdline.add_argument('-r', '--run', help='The run ID associated with the metadata file')

args = cmdline.parse_args()

fname = args.path
runID = args.run

starttime=datetime.datetime.now()
print ("")
print ("*****************************************************************************************************")
print ("Processing start time:",starttime)


# Connect to the wheatgenetics database

print("Connecting to Database...")

try:
#    cnx = mysql.connector.connect(user=local_config.USER, password=local_config.PASSWORD, host=local_config.HOST,
#                                  port=local_config.PORT,database=local_config.DATABASE,client_flags=[ClientFlag.LOCAL_FILES])
    cnx = mysql.connector.connect(user=config.USER,password=config.PASSWORD,host=config.HOST,database=config.DATABASE, client_flags=[ClientFlag.LOCAL_FILES])

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursor  = cnx.cursor(buffered=True)
    cursorA = cnx.cursor(buffered=True)
    cursorB = cnx.cursor(buffered=True)
    cursorC = cnx.cursor(buffered=True)
    cursorD = cnx.cursor(buffered=True)


record_count = ("SELECT record_id FROM phemu_htp")

db_load ="LOAD DATA LOCAL INFILE %s INTO TABLE phemu_htp IGNORE 1 LINES (@sensor_id,@sensor_observation," \
          "@absolute_sensor_position_x,@absolute_sensor_position_y,@absolute_sensor_position_z,@sampling_time_utc," \
          "@sampling_date,@left_utc,@left_elevation,@left_long,@left_lat,@long_zone,@lat_zone,@left_utm_x,@left_utm_y," \
          "@right_utc,@right_elevation,@right_long,@right_lat,@right_utm_x,@right_utm_y,@sensor_offset_x_from_left_gps, " \
          "@sensor_offset_y_from_left_gps,@sensor_offset_z_from_left_gps)" \
          "SET sensor_id=@sensor_id,sensor_observation=@sensor_observation," \
          "absolute_sensor_position_x=@absolute_sensor_position_x," \
          "absolute_sensor_position_y=@absolute_sensor_position_y," \
          "absolute_sensor_position_z=@absolute_sensor_position_z, sampling_time_utc=@sampling_time_utc," \
          "sampling_date=@sampling_date,left_utc=@left_utc,left_elevation=@left_elevation,left_long=@left_long," \
          "left_lat=@left_lat,long_zone=@long_zone,lat_zone=@lat_zone,left_utm_x=@left_utm_x,left_utm_y=@left_utm_y," \
          "right_utc=@right_utc," \
          "right_elevation=@right_elevation,right_long=@right_long,right_lat=@right_lat,right_utm_x=@right_utm_x," \
          "right_utm_y=@right_utm_y," \
          "sensor_offset_x_from_left_gps=@sensor_offset_x_from_left_gps," \
          "sensor_offset_y_from_left_gps=@sensor_offset_y_from_left_gps," \
          "sensor_offset_z_from_left_gps=@sensor_offset_z_from_left_gps"

update_run_id = "UPDATE phemu_htp SET run_id=%s where run_id IS NULL AND record_id > %s"

db_check = "SELECT record_id FROM phemu_htp WHERE run_id LIKE %s"

fix_auto_increment = "ALTER TABLE phemu_htp AUTO_INCREMENT=1"

print('Loading file', fname, 'into Database')

cursor.execute(record_count,)
startcount = cursor.rowcount
print("Number of records in phemu_htp before insert is",startcount)

cursorA.execute(db_load,(fname, ))
cnx.commit()

cursorB.execute(update_run_id,(runID,startcount))
cnx.commit()

cursorD.execute(fix_auto_increment, )
cnx.commit()

cursor.execute(record_count,)
endcount = cursor.rowcount
print("Number of records in phemu_htp after insert is",endcount)
print("Number of records inserted = ",endcount-startcount)

cursorC.execute(db_check, (runID, ))
rcount = cursorC.rowcount

print("Number of database records found for run_id",runID,'is',rcount)


cursor.close()
cursorA.close()
cursorB.close()
cursorC.close()
cursorD.close()

print('Closing database connection...')

cnx.close()  # Exit the program gracefully

endtime=datetime.datetime.now()
elapsedtime=endtime -starttime
print ("Processing time:",elapsedtime)

print('Processing Completed. Exiting...')
print ("")

sys.exit()
