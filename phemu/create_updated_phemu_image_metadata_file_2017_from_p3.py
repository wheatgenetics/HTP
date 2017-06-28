#!/usr/bin/python
#
# Program: create_updated_phemu_image_metadata_file

# Takes a p4 phemu image metadata file and generates output for the following new platform independent tables:
# 1. Updated p3 file with run_id and position columns populated
# 2. New phemu_run table entry for the data collection run
# 3. SQL command file for loading phemu_images metadata into database/
# 4. SQL command file for loading phemu_run table
# 5. SQL Command file for populating the plot_id column in phemu_images table
#
#
# Version 0.3 June 27,2017 Changed to work from p3 file instead of p4 per Kevin's instructions.
#                          N.B. UTM Zone and Designator is fixed to 14 S since pheMU is only used in Kansas.
#
# Version 0.2 May 10,2017 Update to work with 2017 *p4.csv and *GNSS*.csv files.
#
# Version 0.1: April 27, 2016
#
# Initial Version
#
#
import csv
import sys
import argparse
import datetime
import os
import utm


cmdline = argparse.ArgumentParser()

# -dir will be of the form /homes/jpoland/images/staging/phemu_<phemu_folder_name>/ (note terminating slash)

cmdline.add_argument('-d', '--dir', help='Directory where metadata files are stored')

# -input filename will be of the form YYYY-MM-DD-hhmmss_Cam<d>_LogFile_p4.txt where d is in the range 1..5

cmdline.add_argument('-i', '--input', help='p3 metadata input file name')

# -gnss filename will be of the form YYYY-MM-DD-hhmm_GNSSData_p1.txt

cmdline.add_argument('-g', '--gnss', help='gnss p1 input file name')

args = cmdline.parse_args()

def convert_seconds_to_hhmmss(secs):
    mins, secs = divmod(secs, 60)
    hours, mins = divmod(mins, 60)
    return '%02d%02d%02d' % (hours, mins, secs)

dirPath=args.dir
inputPath=dirPath+args.input
gnssPath=dirPath+args.gnss
fref=args.input.find('p3.csv')
updateFilePath=dirPath + args.input[0:fref] + 'p3r.txt'
camSqlFilePath = dirPath + args.input[0:fref-1] + '.sql'
runSqlFilePath=dirPath + args.input[0:fref-1] + '_run.sql'
plotSqlFilePath=dirPath + args.input[0:fref-1] + '_images_plot_id_update.sql'
utmZone='14'
utmDesignator='S'
utmZoneAndDesignator = utmZone + utmDesignator


print 'Input Image p3 File:',inputPath
print 'Input GNSS p1 File:',gnssPath
print ''



# Generate the run_id using the date and time of the first and last entries in the GNSS file

with open(gnssPath,'rU') as gnssFile:
    gnssRows=list(gnssFile)
    startDate=gnssRows[1].split(',')[2]
    startYear=startDate[0:4]
    startMonth=startDate[4:6]
    startDay=startDate[6:8]
    tempDate = datetime.date(int(startYear), int(startMonth), int(startDay))
    startTimeSecs=int(gnssRows[1].split(',')[1].split('.')[0])
    startTime=convert_seconds_to_hhmmss(startTimeSecs)
    endTimeSecs=int(gnssRows[len(gnssRows)-1].split(',')[1].split('.')[0])
    endTime=convert_seconds_to_hhmmss(endTimeSecs)
    if endTime < startTime:
        endDateD = tempDate + datetime.timedelta(days=1)
        endDate=endDateD.strftime('%Y%m%d')
    else:
        endDate=startDate
run_id='phemu_'+ startDate + '_' + startTime + '_' + endDate + '_' + endTime
gnssFile.close()


# Get the additional fields required to complete the updated p3 file: position'
# Get the additional fields required to complete the run file: long_min,long_max,lat_min,lat_max

with open(inputPath,'rU') as metadataFile:
    metadata=metadataFile.readlines()
    inputCount=0
    for row in metadata:
        inputCount+=1
        if inputCount > 1:
            rowFields = row.split(',')
            platform='phemu'
            #plotID=rowFields[0]
            #sensorID=rowFields[1].split('_')[0]
            plotID='\\N'
            sensorID = rowFields[0].split('_')[1]
            imageFileName=rowFields[1]
            md5sum=''
            #easting=rowFields[4]
            #northing=rowFields[5]
            easting = rowFields[2]
            northing = rowFields[3]
            point_position = 'POINT(' + easting + ' ' + northing + ')'
            #gps1Longitude=rowFields[2]
            #gps1Latitude=rowFields[3]
            x=float(easting)
            y=float(northing)
            latLonPosition = utm.to_latlon(x, y, 14, 'S')
            gps1Latitude = str(latLonPosition[0])
            gps1Longitude = str(latLonPosition[1])
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

print 'Read ', inputCount, 'records from ', inputPath
print ''


# Formulate the date and time strings required

#s_date=startDate
#startDate=s_date[0:4]+ '-' + s_date[4:6] + '-'+ s_date[6:8]
#s_time=startTime
#startTime=s_time[0:2]+ ':' + s_time[2:4] + ':'+ s_time[4:6]
#e_date=endDate
#endDate=e_date[0:4]+ '-' + e_date[4:6] + '-'+ e_date[6:8]
#e_time=endTime
#endTime=e_time[0:2]+ ':' + e_time[2:4] + ':'+ e_time[4:6]


# Create the updated p3 metadata file (p3r.txt) for the sensor metadata file that includes run_id
print 'Generating Updated Input Metadata file', updateFilePath
outputCount=0
with open(inputPath,'rU') as csvinput:
    with open(updateFilePath,'wb') as csvoutput:
        # Create the header for the updated metadata file
        header = csv.writer(csvoutput,delimiter='\t')
        header.writerow(
            ['plot_id','image_file_name', 'absolute_sensor_longitude','absolute_sensor_latitude',
             'absolute_sensor_position_x', 'absolute_sensor_position_y', 'sampling_utc','run_id',
             'position', 'sampling_time_utc', 'sampling_date', 'lat_zone', 'long_zone','camera_sn'])
        writer = csv.writer(csvoutput,delimiter='\t')
        reader = csv.reader(csvinput,delimiter=',')

# Append new column names to header row
        updatedRows = []
        row = next(reader)
        outputCount+=1

# Append new fields to each row of the file.
        for row in reader:
            newRow=[]
            plotID='\\N'
            imageFileName=row[1]
            easting = row[2]
            northing = row[3]
            position = 'POINT(' + easting + ' ' + northing + ')'
            x = float(easting)
            y = float(northing)
            latLonPosition = utm.to_latlon(x, y, 14, 'S')
            latitude = str(latLonPosition[0])
            longitude = str(latLonPosition[1])
            sampling_utc=int(row[4].split('.')[0])
            utc_str=convert_seconds_to_hhmmss(sampling_utc)
            sampling_time_utc=utc_str[0:2]+ ':' + utc_str[2:4] + ':'+ utc_str[4:6]
            s_date = row[1].split('_')[1]
            sampling_date = s_date[0:4]+ '-' + s_date[4:6] + '-'+ s_date[6:8]
            lat_zone=utmZone
            long_zone=utmDesignator
            serial_no='CAM_'+ row[1].split('_')[0]
            newRow.extend((plotID,imageFileName,longitude,latitude,easting,northing,sampling_utc,run_id,position,
            sampling_time_utc,sampling_date,lat_zone,long_zone,serial_no))
            #writer.writerow(newRow)
            updatedRows.append(newRow)
            outputCount += 1

        # Write all rows of the file
        writer.writerows(updatedRows)

        # Kludge to get rid of blank last line in the file which causes an empty row to be loaded into the database
        # when using LOAD DATA INFILE procedure!!
        csvoutput.seek(-2, os.SEEK_END)
        csvoutput.truncate()

print 'Wrote ',outputCount, 'records to',updateFilePath
print ''


# Create the SQL command file to load the phemu_images metadata

print 'Generating SQL file',camSqlFilePath
loadCamMetCmd = """LOAD DATA LOCAL INFILE '""" + updateFilePath + """' INTO TABLE phemu_images FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\r'""" + """ IGNORE 1 LINES  
(@plot_id,image_file_name,absolute_sensor_longitude,absolute_sensor_latitude,absolute_sensor_position_x,
absolute_sensor_position_y,@sampling_utc,run_id,@position,sampling_time_utc, @sampling_date,long_zone,lat_zone,camera_sn) 
SET position=ST_PointFromText(CONCAT('POINT(',absolute_sensor_position_x,' ',absolute_sensor_position_y,')')), 
sampling_date=STR_TO_DATE(@sampling_date,'%Y-%m-%d');"""
with open(camSqlFilePath, 'w') as sqlFile:
    sqlFile.write(loadCamMetCmd)

print 'Generating SQL file',plotSqlFilePath
plotIdUpdateCmd="""UPDATE phemu_images INNER JOIN plot_map on ST_CONTAINS(plot_map.plot_polygon, phemu_images.position) SET phemu_images.plot_id = plot_map.plot_id WHERE plot_map.plot_id LIKE "17%" AND run_id LIKE '""" + run_id + """';"""
with open(plotSqlFilePath, 'w') as sqlFile:
    sqlFile.write(plotIdUpdateCmd)

sys.exit()
