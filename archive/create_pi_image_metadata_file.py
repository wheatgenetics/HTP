#!/usr/bin/python
#
# Program: create_pi_image_metadata_file
#
# Takes a p4 phemu image metadata file and generates output for the following new platform independent tables:
# 1. images
# 2. sensor_offsets
# 3. ground_vehicle_run
#
# Version: April 7, 2016
#
# Initial Version
#
#
import csv
import sys
import argparse
import datetime


cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Directory where metadata files are stored')

cmdline.add_argument('-i', '--input', help='p3 metadata input file name')

cmdline.add_argument('-g', '--gnss', help='gnss p1 input file name')

args = cmdline.parse_args()

dirPath=args.dir
inputPath=dirPath+args.input
gnssPath=dirPath+args.gnss
fref=args.input.find('p4.txt')
outputPath=dirPath+args.input[0:fref]+'p4pi.csv'
sensorOffsetsPath=dirPath + args.input[0:fref] +'sensorOffsets.csv'
updateFilePath=dirPath + args.input[0:fref] + 'p4r.txt'


print 'Input Image p4 File:',inputPath
print 'Input GNSS p1 File:',gnssPath
print ''

ref=gnssPath.find('GNSSData')
startYear=gnssPath[(ref-16):(ref-12)]
startMonth=gnssPath[(ref-11):(ref-9)]
startDay=gnssPath[(ref-8):(ref-6)]
startDate=startYear+startMonth+startDay
tempDate=datetime.date(int(startYear),int(startMonth),int(startDay))
tempDateStr=tempDate.strftime('%Y%m%d')


# Generate the run_id using the date and time of the first and last entries in the GNSS file

with open(gnssPath,'rU') as gnssFile:
    gnssRows=list(gnssFile)
    startTime=gnssRows[1].split('\t')[1][0:6]
    endTime=gnssRows[len(gnssRows)-1].split('\t')[1][0:6]
    if endTime < startTime:
        endDateD = tempDate + datetime.timedelta(days=1)
        endDate=endDateD.strftime('%Y%m%d')
    else:
        endDate=startDate
run_id='phemu_'+ startDate + '_' + startTime + '_' + endDate + '_' + endTime
gnssFile.close()

# Create the p4.csv metadata file for the sensor metadata


with open(outputPath, 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(
        ['platform','image_file_name','md5sum','run_id','sensor_id','time_utc','date_utc','position','northing','easting',
         'altitude','utm_zone','gps1_utc','gps1_altitude','gps1_longitude','gps1_latitude','gps2_utc','gps2_altitude',
         'gps2_longitude','gps2_latitude'])
csvfile.close()

with open(outputPath, 'ab') as csvfile:
    print 'Generating Platform-Independent Metadata file', outputPath

    sensorOffsets=[]

    with open(inputPath,'rU') as metadataFile:
        metadata=metadataFile.readlines()
        inputCount=0
        outputCount=0
        for row in metadata:
            inputCount+=1
            offsetlist=[]
            if inputCount > 1:
                rowFields = row.split('\t')
                platform='phemu'
                sensorID=rowFields[0]
                imageFileName=rowFields[1]
                md5sum=''
                easting=rowFields[2]
                northing=rowFields[3]
                if inputCount == 2:
                    eastingMin=easting
                    eastingMax=easting
                    northingMin=northing
                    northingMax=northing
                if easting < eastingMin:
                    eastingMin=easting
                if easting > eastingMax:
                    eastingMax=easting
                if northing < northingMin:
                    northingMin = northing
                if northing > northingMax:
                    northingMax = northing
                point_position = 'POINT(' + easting + ' ' + northing + ')'
                altitude=rowFields[4]
                utcTime=rowFields[5]
                utcDate=rowFields[6]
                gps1Utc=rowFields[7]
                gps1Altitude=rowFields[8]
                gps1Longitude=rowFields[9]
                gps1Latitude=rowFields[10]
                if inputCount == 2:
                    longMin=gps1Longitude
                    longMax=gps1Longitude
                    latMin =gps1Latitude
                    latMax =gps1Latitude
                if gps1Longitude < longMin:
                    longMin=gps1Longitude
                if gps1Longitude > longMax:
                    longMax = gps1Longitude
                if gps1Latitude < latMin:
                    latMin=gps1Latitude
                if gps1Latitude > latMax:
                    latMax = gps1Latitude
                utmZone=rowFields[11]
                lastUtmZone=utmZone
                gps2Utc = rowFields[15]
                gps2Altitude = rowFields[16]
                gps2Longitude = rowFields[17]
                gps2Latitude = rowFields[18]
                lineitem = csv.writer(csvfile)
                lineitem.writerow([platform,imageFileName,md5sum,run_id, sensorID, utcTime, utcDate,point_position,
                          northing, easting, altitude, utmZone,
                          gps1Utc, gps1Altitude,gps1Latitude, gps1Longitude, gps2Utc, gps2Altitude,
                          gps2Latitude,gps2Longitude])
                outputCount+=1
                offsetList=([sensorID,rowFields[21],rowFields[22],rowFields[23]])
                if offsetList not in sensorOffsets:
                    sensorOffsets.append((offsetList))
csvfile.close()

# Create the sensor_offsets metadata file.

sensorOffsetsPath=dirPath + args.input[0:fref] +'sensor_offsets.csv'
print "Generating Sensor Offsets File ",sensorOffsetsPath
with open(sensorOffsetsPath, 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(
        ['run_id','sensor_id','x_offset','y_offset','z_offset'])
csvfile.close()
with open(sensorOffsetsPath, 'ab') as csvfile:
    for item in sensorOffsets:
        lineitem = csv.writer(csvfile)
        lineitem.writerow([run_id, item[0], item[1], item[2], item[3]])
csvfile.close()

# Create the ground_vehicle_run metadata file


gvRunPath=dirPath+args.input[0:16] + 'gvRunData.csv'
runFolderName=run_id

print 'Generating Ground Vehicle Run file', gvRunPath

with open(gvRunPath, 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(
        ['run_id','start_date_utc','start_time_utc','end_date_utc','end_time_utc','run_folder_name','easting_min',
         'easting_max','northing_min','northing_max','utm_zone'])
    lineitem = csv.writer(csvfile)
    lineitem.writerow([run_id,startDate,startTime,endDate,endTime,runFolderName,eastingMin,eastingMax,northingMin,
                           northingMax,lastUtmZone])
csvfile.close()

# Create the legacy phemu_run metadata file

phRunPath=dirPath+args.input[0:16] + 'phRunData.csv'
runFolderName=run_id

print 'Generating Phemu Run file', phRunPath

with open(phRunPath, 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(
        ['run_id','start_date_utc','start_time_utc','end_date_utc','end_time_utc','run_folder_name','long_min',
         'long_max','lat_min','lat_max','utm_zone'])
    lineitem = csv.writer(csvfile)
    lineitem.writerow([run_id,startDate,startTime,endDate,endTime,runFolderName,longMin,longMax,latMin,
                           latMax,lastUtmZone])
csvfile.close()

# Create the updated p4 metadata file (p4r.txt) for the sensor metadata file that includes run_id
print 'Generating Updated Input Metadata file', updateFilePath

with open(inputPath,'rU') as csvinput:
    with open(updateFilePath,'wb') as csvoutput:
        writer = csv.writer(csvoutput,delimiter='\t')
        reader = csv.reader(csvinput,delimiter='\t')
# Append new column names to header row
        updatedRows = []
        row = next(reader)
        n=len(row)
        row.append('run_id')
        row.append('position')
        updatedRows.append(row)
# Append new fields to each row of the file.
        for row in reader:
            n = len(row)
            row = row[:n - 1]
            easting = row[2]
            northing = row[3]
            position = 'POINT(' + easting + ' ' + northing + ')'
            row.append(run_id)
            row.append(position)
            updatedRows.append(row)

        writer.writerows(updatedRows)
sys.exit()
