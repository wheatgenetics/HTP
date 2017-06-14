#!/usr/bin/python
#
import os
import csv
import hashlib
#import local_config
import config
import mysql.connector
from mysql.connector import errorcode
import sys
import argparse

image_md5_data = {}
bufsize = 1  # Use line buffering, i.e. output every line to the file.

def hashfilelist(a_file, blocksize=65536):
    hasher = hashlib.md5()
    buf = a_file.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = a_file.read(blocksize)
    return hasher.hexdigest()

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Directory within /homes/jpoland/images/phemu where metadata files are stored')

cmdline.add_argument('-t', '--tbl', help='Image data table to be updated (images or phemu_images)')

cmdline.add_argument('-o', '--out', help='Directory where image table metadata update files are stored')


args = cmdline.parse_args()

inPath=args.dir
runID=args.dir[6:]
print ("Calculating md5 checksum for images for run ID:  ",runID)
table=args.tbl
outPath=args.out

# Formulate the query statement

query = "SELECT record_id,run_id,image_file_name,md5sum from " + table +  " where run_id = %s"

# Connect to the wheatgenetics database

print ("Connecting to Database...")
try:
    cnx = mysql.connector.connect(user=config.USER, password=config.PASSWORD, host=config.HOST,database=config.DATABASE)

    #cnx = mysql.connector.connect(user=local_config.USER, password=local_config.PASSWORD, host=local_config.HOST,port= local_config.PORT, database=local_config.DATABASE)
    #cnx = mysql.connector.connect(user=local_config.USER, password=local_config.PASSWORD, host=local_config.HOST, database=local_config.DATABASE)
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

cursor.execute(query,(runID,))

# Store results of query in dictionary with image_file_name as key

print("Storing md5sum in dictionary with image_file_name as key...")

for (record_id, run_id, image_file_name, md5sum) in cursor:
    record_key = int(record_id)
    image_md5_data[image_file_name] = [record_key, run_id, md5sum]

# Release the cursor

print ('Closing cursor')
cursor.close

# Release the database connection

print ("Closing database connection")
cnx.close()

# Set working directory to the directory where images are stored

imageDir="/homes/jpoland/images/phemu/"+inPath
os.chdir(imageDir)

# Calculate md5 checksum for each image in the directory and update dictionary with md5 value for each image

imageCount=0
for file in os.listdir(imageDir):
    imageCount+=1
    if (file.endswith(".CR2") or file.endswith(".jpg")):
        md5checksum = hashfilelist(open(file, 'rb'))
        image_md5_data[file][2]=md5checksum
        print(image_md5_data[file][0],file,image_md5_data[file][2])

# Write the file containing the updates to the image metadata table.

outputPath=outPath+"phemu_image_table_update_"+ runID + ".csv"
print ("Generating Images Table Update File ",outputPath)
with open(outputPath, 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(['record_id','run_id','image_file_name','md5sum'])
csvfile.close()
with open(outputPath, 'ab') as csvfile:
    for image_file_name, values in sorted(image_md5_data.items()):
        lineitem = csv.writer(csvfile)
        lineitem.writerow([int(values[0]), values[1],image_file_name,values[2]])
csvfile.close()


sys.exit()


