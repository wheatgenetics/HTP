#!/usr/bin/python
#
# Program: create_updated_phemy_image_metadata_file
#
# Takes a p4 phemu image metadata file and generates output for the following new platform independent tables:
# 1. Updated p4 file with run_id and position columns populated
# 2. New phemu_run table entry for the data collection run
# 3. SQL command file for loading phemu_images metadata into database/
# 4. SQL command file for loading phemu_run table
# 5. SQL Command file for populating the plot_id column in phemu_images table
#
#
# May 10,2017 Update to work with 2017 *p4.csv and *GNSS*.csv files.
#
#
# Version: April 27, 2016
#
# Initial Version
#
#
import csv
import sys
import argparse
import datetime
import os


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
fref=args.input.find('p4.csv')
updateFilePath=dirPath + args.input[0:fref] + 'p4r.txt'
camSqlFilePath = dirPath + args.input[0:fref-1] + '.sql'
runSqlFilePath=dirPath + args.input[0:fref-1] + '_run.sql'
plotSqlFilePath=dirPath + args.input[0:fref-1] + '_images_plot_id_update.sql'


print 'Input Image p4 File:',inputPath
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
            plotID=rowFields[0]
            sensorID=rowFields[1].split('_')[0]
            imageFileName=rowFields[1]
            md5sum=''
            easting=rowFields[4]
            northing=rowFields[5]
            point_position = 'POINT(' + easting + ' ' + northing + ')'
            gps1Longitude=rowFields[2]
            gps1Latitude=rowFields[3]
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

# Create the legacy phemu_run metadata file

phRunPath=dirPath+args.input[0:fref-1] + 'phRunData.csv'
runFolderName=run_id
utmZone='14'
utmDesignator='S'
utmZoneAndDesignator = utmZone + utmDesignator

print 'Generating Phemu Run file', phRunPath
print ' '

s_date=startDate
startDate=s_date[0:4]+ '-' + s_date[4:6] + '-'+ s_date[6:8]
s_time=startTime
startTime=s_time[0:2]+ ':' + s_time[2:4] + ':'+ s_time[4:6]
e_date=endDate
endDate=e_date[0:4]+ '-' + e_date[4:6] + '-'+ e_date[6:8]
e_time=endTime
endTime=e_time[0:2]+ ':' + e_time[2:4] + ':'+ e_time[4:6]

with open(phRunPath, 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow(
        ['run_id','start_date_utc','start_time_utc','end_date_utc','end_time_utc','run_folder_name','long_min',
         'long_max','lat_min','lat_max','utm_zone'])
    lineitem = csv.writer(csvfile)
    lineitem.writerow([run_id,startDate,startTime,endDate,endTime,runFolderName,longMin,longMax,latMin,
                           latMax,utmZoneAndDesignator])
csvfile.close()

# Create the updated p4 metadata file (p4r.txt) for the sensor metadata file that includes run_id
print 'Generating Updated Input Metadata file', updateFilePath
outputCount=0
with open(inputPath,'rU') as csvinput:
    with open(updateFilePath,'wb') as csvoutput:
        # Create the header for the updated metadata file
        header = csv.writer(csvoutput,delimiter='\t')
        header.writerow(
            ['plot_id', 'image_file_name', 'absolute_sensor_longitude','absolute_sensor_latitude',
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
            #n = len(row)
            #row = row[:n - 1]
            easting = row[4]
            northing = row[5]
            position = 'POINT(' + easting + ' ' + northing + ')'
            row.append(run_id)
            row.append(position)
            sampling_utc=int(row[6].split('.')[0])
            utc_str=convert_seconds_to_hhmmss(sampling_utc)
            sampling_time_utc=utc_str[0:2]+ ':' + utc_str[2:4] + ':'+ utc_str[4:6]
            row.append(sampling_time_utc)
            s_date = row[1].split('_')[1]
            sampling_date = s_date[0:4]+ '-' + s_date[4:6] + '-'+ s_date[6:8]
            row.append(sampling_date)
            row.append(utmZone)
            row.append(utmDesignator)
            serial_no='CAM_'+ row[1].split('_')[0]
            row.append(serial_no)
            writer.writerow(row)
            updatedRows.append(row)
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
(plot_id,image_file_name,absolute_sensor_longitude,absolute_sensor_latitude,absolute_sensor_position_x,
absolute_sensor_position_y,@sampling_utc,run_id,@position,sampling_time_utc, @sampling_date,long_zone,lat_zone,camera_sn) 
SET position=ST_PointFromText(CONCAT('POINT(',absolute_sensor_position_x,' ',absolute_sensor_position_y,')')), 
sampling_date=STR_TO_DATE(@sampling_date,'%Y-%m-%d');"""



with open(camSqlFilePath, 'w') as sqlFile:
    sqlFile.write(loadCamMetCmd)

#print 'Generating SQL file',runSqlFilePath
#runMetCmd="""LOAD DATA LOCAL INFILE '""" + phRunPath + """' INTO TABLE phemu_run FIELDS TERMINATED BY ','""" + """ IGNORE 1 LINES (run_id,start_date_utc,start_time_utc,end_date_utc,end_time_utc,run_folder_name,long_min,long_max,lat_min,lat_max,utm_zone);"""
#with open(runSqlFilePath, 'w') as sqlFile:
#    sqlFile.write(runMetCmd)

print 'Generating SQL file',plotSqlFilePath
plotIdUpdateCmd="""UPDATE phemu_images INNER JOIN plot_map on ST_CONTAINS(plot_map.plot_polygon, phemu_images.position) SET phemu_images.plot_id = plot_map.plot_id WHERE plot_map.plot_id LIKE "17%" AND run_id LIKE '""" + run_id + """';"""
with open(plotSqlFilePath, 'w') as sqlFile:
    sqlFile.write(plotIdUpdateCmd)

sys.exit()
