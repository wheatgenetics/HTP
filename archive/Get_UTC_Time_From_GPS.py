#!/usr/bin/python
#



__author__ = 'mlucas'

import subprocess
import os
import time
import math
import sys
import argparse
import exifread



secsInWeek = 604800
secsInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0)  # (year, month, day, hh, mm, ss)


def UTCFromGps(gpsWeek, SOW, leapSecs=16):
    # A Python implementation of GPS related time conversions.
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

    epochTuple = gpsEpoch + (-1, -1, 0)
    t0 = time.mktime(epochTuple) - time.timezone  # mktime is localtime, correct for UTC
    tdiff = (gpsWeek * secsInWeek) + SOW - leapSecs
    t = t0 + tdiff
    (year, month, day, hh, mm, ss, dayOfWeek, julianDay, daylightsaving) = time.gmtime(t)

    # use gmtime since localtime does not allow to switch off daylight savings correction!!!

    return year, month, day, hh, mm, ss,daylightsaving

# Get command line input.

cmdline = argparse.ArgumentParser()
cmdline.add_argument('-s', '--secs', help='GPS Seconds')
cmdline.add_argument('-w', '--week', help='GPS Week')

args = cmdline.parse_args()

mgpsSeconds = args.secs
mgpsWeek = args.week

mrgpsSecs = round(float(mgpsSeconds) / 1000, 2)
print mrgpsSecs
migpsWeek = int(mgpsWeek)
print migpsWeek
myear,mmonth,mday,mhour,mmin,msec,daylightsave = UTCFromGps(migpsWeek,mrgpsSecs,16)

print myear,'-',mmonth,'-',mday,' ',mhour,':',mmin,':',msec, ' ',daylightsave


# Exit the program gracefully

print ('Processing Completed. Exiting...')
sys.exit()

