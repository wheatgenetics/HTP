#!/usr/bin/python
#
# Program: calculate_image_md5.py
#
# Version:  0.1 May 6, 2015 Initial Version - Work in Progress - Cloned from GBS md5 check program!
#
#
#
# This program will query the database for a list of image files associated with a phemu run, calculate the MD5
# checksum for each image file and generate a CSV file that can be used to update the phemu_images table with the
# MD5 sum.
#
# The default location of the files to be processed is assumed to be /home/jpoland/images/phemu/phemu_<run_id>.
# but this can be changed to support other tables.
#
# COMMAND LINE INPUTS:
#
# '-d' or '--dir':      'Beocat directory path to GBS sequence files', default='/homes/jpoland/shared/'
#
#
#import config
import local_config
import mysql.connector
from mysql.connector import errorcode
import subprocess
import hashlib
import re
import datetime
import sys
import argparse

image_md5_data = {}
image_files={}
bufsize = 1  # Use line buffering, i.e. output every line to the file.


def hashfilelist(a_file, blocksize=65536):
    hasher = hashlib.md5()
    buf = a_file.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = a_file.read(blocksize)
    return hasher.hexdigest()

# Get command line input.

cmdline = argparse.ArgumentParser()
cmdline.add_argument('-p', '--platform', help='the platform to process image data files for', default = 'phemu')
cmdline.add_argument('-r', '--runID', help='the run_id to process')
cmdline.add_argument('-d', '--dir', help='Beocat directory path to image files',
                     default='/homes/jpoland/images/')


args = cmdline.parse_args()

image_platform = str(args.platform)
print image_platform
run_identifier = str(args.runID)
print run_identifier
image_dir=str(args.dir)
image_path=image_dir+image_platform+'_'+run_identifier
print image_path
table=image_platform+'_'+'images'
print table

# Get the list of image files to calculate the MD5 sum for

# Formulate the query statement

query = "SELECT record_id,image_file_name,md5sum from phemu_images where run_id LIKE %s"

# Connect to the wheatgenetics database

print ("Connecting to Database...")
try:
    #cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD, host=config.HOST,database=config.DATABASE)

    cnx = mysql.connector.connect(user=local_config.USER, password=local_config.PASSWORD, host=local_config.HOST,
                              port= local_config.PORT, database=local_config.DATABASE)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exists")
    else:
        print(err)
else:
    cursor = cnx.cursor()

# Execute the query
print ("Executing Query...")

# noinspection PyUnboundLocalVariable
cursor.execute(query,(run_identifier,))
#cursor.execute(query)
# Store results of query in dictionary.

print("Storing md5sum in dictionary with record_id as key...")

# Filenames have a GBS ID prefix of the form:
# GBSnnnnPE or GBSnnnnL* or GBSnnnnR*

for (record_id, image_file_name, md5sum) in cursor:
    record_key = int(record_id)
    if record_key not in image_md5_data:
        image_md5_data[record_key] = [image_file_name,md5sum]

# Release the cursor

print ('Closing cursor')
# noinspection PyStatementEffect
cursor.close

# Release the database connection

print ("Closing database connection")
# noinspection PyUnboundLocalVariable
cnx.close()

for key,value in image_md5_data.iteritems():
    print key,value
sys.exit()
# Read the files in the shared directory into a list

print("Fetching list of files to compute MD5 sum for...")

files_to_check = subprocess.check_output(['ls', '-1', image_path], universal_newlines=True)

afile = ''
filelist = []
db_key = ''

for char in files_to_check:
    if char != '\n':
        afile += char
    else:
        filelist.append(afile)
        afile = ''

# Put each file in the list into a dictionary with key = gbs_id and value = filename
# Filenames have a GBS ID prefix of the form:cd
# GBSnnnnPE or GBSnnnnL* or GBSnnnnR*

for f in filelist:
    gbsfile=''
    isgbsfile = (f != "" and f[0:3] == "GBS")
    if isgbsfile:
        # Find the start position of the string 'GBS' in the filename
        k = re.search(r'GBS', f)
        key_start = k.start()
        # Find the end position of the gbs_id part of the filename which can be marked either by '_' or 'x'
        l = re.search(r'[x_]', f)
        key_end = l.start()
        pe = re.search(r'PE_', f)
        if pe is not None:
            file_key = f[key_start:key_end] + 'PE'
        else:
            file_key = f[key_start:key_end]
        gbsfile = gbs_path + f
        gbs_files[file_key]=gbsfile

#
# Search for gbs id in the filename string.
# Also handle the case of files for Paired End data (containing PE in the filename).
#

status = ''
md5failcount = 0
md5successcount = 0
gbscount = 0
gbsmissingcount=0
gbsfile = False

today = datetime.datetime.now()
logfile = "check_MD5_" + gbs_start + "_" + gbs_end + "_" + today.strftime('%Y%m%d') + ".log"
print('Opening logfile ', logfile)
with open(logfile, 'w', bufsize) as f:
    for db_key in sorted(gbs_md5_data):
        db_checksum = gbs_md5_data[db_key]
        gbsnumber = (db_key[3:7])
        if (gbs_start <= gbsnumber <= gbs_end) and (db_checksum !=''):
            gbsfilename=''
            if db_key in gbs_files:
                gbsfilename=gbs_files[db_key]
            if gbsfilename == '':
                status = 'FILE NOT FOUND FOR GBS ID'
                gbsmissingcount+=1
                logstring = str(status + ':' + db_key + '\n')
                f.write(logstring)
            else:
                gbscount += 1
                print("Processing GBS File:", gbsfilename)
                md5checksum = hashfilelist(open(gbsfilename, 'rb'))
                logstring = ''
                if md5checksum == db_checksum:
                    status = 'PASSED'
                    md5successcount += 1
                    logstring = str(status + ' ' + md5checksum + ' ' + gbsfilename + '\n')
                    f.write(logstring)
                else:
                    status = 'FAILED'
                    md5failcount += 1
                    logstring = str(status + ' ' + md5checksum + ' ' + gbsfilename + ' gbs checksum: ' + db_checksum + '\n')
                    f.write(logstring)
with open(logfile, 'a', bufsize) as f:
    logstring = ('Total Number of files with correct checksum      = ' + str(md5successcount) + '\n')
    f.write(logstring)
    logstring = ('Total Number of files with incorrect checksum    = ' + str(md5failcount) + '\n')
    f.write(logstring)
    logstring = ('Total Number of GBS files processed              = ' + str(gbscount) + '\n')
    f.write(logstring)
    logstring = ('Total Number of GBS files not found              = ' + str(gbsmissingcount) + '\n')
    f.write(logstring)
    logstring = ('Total Number of GBS Libraries checked            = ' + str(gbsmissingcount + gbscount) + '\n')
    f.write(logstring)

# Exit the program gracefully
print ('Processing Completed. Exiting...')
sys.exit()
