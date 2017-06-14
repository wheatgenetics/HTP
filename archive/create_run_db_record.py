#!/usr/bin/python
#
# Program: create_run_db_record.py
#
# Version: 0.1 February 16,2014
#
# Creates a record in the 'run' table of the wheatgenetics database based on start and end date/time of GPS records
# in the autopilot log. This table stores information about the location of the files associated with a data collection
# run for uav, phemu, phenocam and phenocorn
#
# Command Line Inputs:
#
#
# '-d' or '--db':      'Beocat database name'
#
#

__author__ = 'mlucas'

import os
import os.path

import argparse
import local_config
import subprocess
import sys
import re
import time
import datetime
import mysql.connector
from mysql.connector import errorcode

secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)

bufsize = 1 # Use line buffering, i.e. output every line to the file.


def get_GNSS_file_list(uavPath):
    # Return a list of the names and sample date & time for all GNSS files in the uav_incoming directory.

    GNSSfilelist = []

    # Get list of files in uav staging directory

    print("Fetching list of GNSS files...")

    filestocheck = subprocess.check_output(['ls', '-1', uavPath], universal_newlines=True)

    afile = ''
    filelist = []

    for char in filestocheck:
        if char != '\n':
            afile += char
        else:
            filelist.append(afile)
            afile = ''

# Get the subset of files that are the GNSS files (ideally there should be only one.)
    for f in filelist:
        k = re.search(r'GNSS', f)
        isGNSSfile = (f != '' and k != None)
        if isGNSSfile:
            filepath = uavPath+f
            GNSSfilelist.append(filepath)

    return GNSSfilelist

def UTCFromGps(gpsWeek, SOW, leapSecs=16):
    #  A Python implementation of GPS related time conversions.
    #
    # Copyright 2002 by Bud P. Bruegger, Sistema, Italy
    # mailto:bud@sistema.it
    # http://www.sistema.it
    #
    # Modifications for GPS seconds by Duncan Brown
    #
    # PyUTCFromGpsSeconds added by Ben Johnson
    # Converts gps week and seconds to UTC
    #
    # SOW = seconds of week
    # gpsWeek is the full number (not modulo 1024)
    #
    # The number of GPS leap seconds in 2014 is 16
    #

    secFract = SOW % 1
    epochTuple = gpsEpoch + (-1, -1, 0)
    t0 = time.mktime(epochTuple) - time.timezone  #mktime is localtime, correct for UTC
    tdiff = (gpsWeek * secsInWeek) + SOW - leapSecs
    t = t0 + tdiff
    (year, month, day, hh, mm, ss, dayOfWeek, julianDay, daylightsaving) = time.gmtime(t)

     #use gmtime since localtime does not allow to switch off daylight savings correction!!!

    return (year, month, day, hh, mm, ss + secFract)

def get_flightid_from_GNSS_log(fGNSSLog):
    fGNSSList = []
    with open(fGNSSLog,'rU') as logfile:
        log = logfile.readlines()
        for row in log:
            if row[0:3]=='GPS':
                rowFields  = row.split(',')
                gpsSecs    = round(float(rowFields[2])/1000,2)
                gpsWeek    = int(rowFields[3])
                (lyear, lmonth,lday,lhh,lmm,lsecs) = UTCFromGps(gpsWeek, gpsSecs, 16)
#                if round(math.modf(lsecs)[0],2) == 0.0:
#                    print lyear,lmonth,lday,lhh,lmm,lsecs
#                    key = str(lyear)+str(lmonth)+str(lday)+str(lhh)+str(lmm)+str(int(lsecs))
#                    print(key)
                timeAndDate = (str(lyear).zfill(4),str(lmonth).zfill(2),str(lday).zfill(2),str(lhh).zfill(2),
                               str(lmm).zfill(2),str(int(round(lsecs))).zfill(2))
                fGNSSList.append(timeAndDate)
    flightId =  fGNSSList[0][0]+fGNSSList[0][1]+fGNSSList[0][2] + '_' + \
                fGNSSList[0][3]+fGNSSList[0][4]+fGNSSList[0][5] + '_' + \
                fGNSSList[-1][0]+fGNSSList[-1][1]+fGNSSList[-1][2] + '_' + \
                fGNSSList[-1][3]+fGNSSList[-1][4]+fGNSSList[-1][5]
    startDate = datetime.date(int(fGNSSList[0][0]),int(fGNSSList[0][1]),int(fGNSSList[0][2]))
    startTime = datetime.time(int(fGNSSList[0][3]),int(fGNSSList[0][4]), int(fGNSSList[0][5]))
    endDate = datetime.date(int(fGNSSList[-1][0]),int(fGNSSList[-1][1]),int(fGNSSList[-1][2]))
    endTime = datetime.time(int(fGNSSList[-1][3]),int(fGNSSList[-1][4]), int(fGNSSList[-1][5]))

    return(flightId,startDate,startTime,endDate,endTime)


# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat staging directory path',
                     default='/homes/mlucas/HTP/')

args = cmdline.parse_args()

htpIncomingPath = args.dir

# Determine Flight ID based on first and last GPS measurements in the GNSS log file

GNSSFiles = get_autopilot_file_list(htpIncomingPath)
if len(GNSSFiles) == 1:
    GNSSFile = GNSSFiles[0]
    flightInfo = get_flightid_from_GNSS_log(GNSSFile)
else:
    print ("Error: More than one GNSS file found in Directory.")
    print GNSSFiles
    sys.exit()

# Create a database entry for the flight

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

# Execute the query to check that flight record does not already exist

runID = runInfo[0]

print ("Checking for existing experiment run record...")

cursor1.execute("""SELECT run_id FROM run WHERE run_id = %s""",(run_ID,))

if cursor1.rowcount==0:
    print "Adding experiment run record to database"
    insertrun = "INSERT INTO run (run_id,start_date,start_time,end_date,end_time,htp_data_source, run_filename, md5sum) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    runData = (runInfo[0],runInfo[1],runInfo[2],runInfo[3],runInfo[4],runInfo[5],runInfo[6],runInfo[7])
    try:
        cursor2.execute(insertrun,runData)
    except:
        print "Insert of experiment run record failed."
else:
    print "Experiment run record already exists."

# Commit changes

print ('Committing changes')
cnx.commit()

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
