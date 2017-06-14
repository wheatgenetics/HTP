#!/usr/bin/python
#
# Program: create_flight_db_record.py
#
# Version: 0.1 November 13,2014
#
# Creates a record in the flight table of the wheatgenetics database based on start and end date/time of GPS records
# in the autopilot log.
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

#apmString = 'apm'

def get_autopilot_file_list(uavPath):
    # Return a list of the names and sample date & time for all apm files in the uav_incoming directory.

    apmfilelist = []

    # Get list of files in uav staging directory

    print("Fetching list of autopilot (APM) files...")

    filestocheck = subprocess.check_output(['ls', '-1', uavPath], universal_newlines=True)

    afile = ''
    filelist = []

    for char in filestocheck:
        if char != '\n':
            afile += char
        else:
            filelist.append(afile)
            afile = ''

# Get the subset of files that are the apm files (ideally there should be only one.)
    for f in filelist:
        k = re.search(r'apm', f)
        isapmfile = (f != '' and k != None)
        if isapmfile:
            filepath = uavPath+f
            apmfilelist.append(filepath)

    return apmfilelist

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

def get_flightid_from_apm_log(fapmLog):
    fapmList = []
    with open(fapmLog,'rU') as logfile:
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
                fapmList.append(timeAndDate)
    flightId =  fapmList[0][0]+fapmList[0][1]+fapmList[0][2] + '_' + \
                fapmList[0][3]+fapmList[0][4]+fapmList[0][5] + '_' + \
                fapmList[-1][0]+fapmList[-1][1]+fapmList[-1][2] + '_' + \
                fapmList[-1][3]+fapmList[-1][4]+fapmList[-1][5]
    startDate = datetime.date(int(fapmList[0][0]),int(fapmList[0][1]),int(fapmList[0][2]))
    startTime = datetime.time(int(fapmList[0][3]),int(fapmList[0][4]), int(fapmList[0][5]))
    endDate = datetime.date(int(fapmList[-1][0]),int(fapmList[-1][1]),int(fapmList[-1][2]))
    endTime = datetime.time(int(fapmList[-1][3]),int(fapmList[-1][4]), int(fapmList[-1][5]))

    return(flightId,startDate,startTime,endDate,endTime)


# Get command line input.

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat UAV HTP staging directory path',
                     default='/homes/mlucas/uav_incoming/')

args = cmdline.parse_args()

uavIncomingPath = args.dir

# Determine Flight ID based on first and last GPS measurements in the APM log file

apmFiles = get_autopilot_file_list(uavIncomingPath)
if len(apmFiles) == 1:
    apmFile = apmFiles[0]
    flightInfo = get_flightid_from_apm_log(apmFile)
else:
    print ("Error: More than one APM file found in Directory.")
    print apmFiles
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

flightID = flightInfo[0]

print ("Checking for existing flight record...")

cursor1.execute("""SELECT flight_id FROM flight WHERE flight_id = %s""",(flightID,))

if cursor1.rowcount==0:
    print "Adding flight record to database"
    insertFlight = "INSERT INTO flight (flight_id,start_date,start_time,end_date,end_time) VALUES (%s,%s,%s,%s,%s)"
    flightData = (flightInfo[0],flightInfo[1],flightInfo[2],flightInfo[3],flightInfo[4])
    try:
        cursor2.execute(insertFlight,flightData)
    except:
        print "Insert of flight record failed."
else:
    print "Flight record already exists."

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
