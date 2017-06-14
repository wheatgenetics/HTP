#!/usr/bin/python
#
# Program: update_uav_image_experiment.py
#
# Version: 0.1 November 24,2014
#
# This program will update the experiment ID field of all uav_images records that do not have an experiment ID.
# The program determines the experiment ID for an image by checking which experiment has coordinates that bound the
# image coordinates.
#
# If no matching experiment is found, the program returns an error message.
#
# Command Line Inputs:
#
# No inputs required.update_uav_image_experiment.py
#

__author__ = 'mlucas'

import local_config
import sys
import mysql.connector
from mysql.connector import errorcode

# Formulate query to find all uav_images records whose experiment_id field is Null.

query1 = "SELECT sequence_number,image_file_name, experiment_id,flight_id, sensor_id, absolute_sensor_position_x, " \
         "absolute_sensor_position_y from uav_images where experiment_id = ''"

print ("Connecting to Database...")
try:
    cnx = mysql.connector.connect(user=local_config.USER, password=local_config.PASSWORD, host=local_config.HOST,
                                  database=local_config.DATABASE)

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
 cursor1 = cnx.cursor(buffered=True)
 cursor2 = cnx.cursor(buffered=True)

# Execute the query
print ("Executing Query...")

# Find the experiment_id for each image file

query1 = "SELECT experiment.experiment_id,C1_1_x,C1_1_y,C1_2_x,C1_2_y,C2_1_x,C2_1_y,C2_2_x,C2_2_y,sequence_number," \
         "absolute_sensor_position_x,absolute_sensor_position_y FROM experiment INNER JOIN uav_images " \
         "ON absolute_sensor_position_x >= C2_1_x AND absolute_sensor_position_x <= C2_2_x AND " \
         "absolute_sensor_position_y >= C1_1_y AND absolute_sensor_position_y <= C2_1_y "
cursor1.execute(query1)

for (experiment_id,C1_1_x,C1_1_y,C1_2_x,C1_2_y,C2_1_x,C2_1_y,C2_2_x,C2_2_y,sequence_number,absolute_sensor_position_x,absolute_sensor_position_y) in cursor1:
    experiment_update = "UPDATE uav_images SET experiment_id = %s WHERE sequence_number=%s"
    exp= str(experiment_id)
    seq= int(sequence_number)
    print (exp,seq)
    cursor2.execute(experiment_update,(exp,seq))

# Commit changes

print ('Committing changes')
cnx.commit()

# Release the cursor

print ('Closing cursors')
cursor1.close
cursor2.close

# Release the database connection

print ("Closing database connection")
# noinspection PyUnboundLocalVariable
cnx.close()

# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()
