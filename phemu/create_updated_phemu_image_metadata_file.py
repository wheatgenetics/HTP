#!/usr/bin/python
#
# Program: create_updated_phemy_image_metadata_file
#
# Takes a p4 phemu image metadata file and generates output for the following new platform independent tables:
# 1. Updated p4 file with run_id and position columns populated
# 2. New phemu_run table entry for the data collection run
# 3. SQL command file for loading phemu_images metadata into database/
# 4. SQL command file for loading phemu_run table
# 5. SQL Commecnt file for populating the plot_id column in phemu_images table
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


cmdline = argparse.ArgumentParser()

# -dir will be of the form /homes/jpoland/images/staging/phemu_<phemu_folder_name>/ (note terminating slash)

cmdline.add_argument('-d', '--dir', help='Directory where metadata files are stored')

# -input filename will be of the form YYYY-MM-DD-hhmmss_Cam<d>_LogFile_p4.txt where d is in the range 1..5

cmdline.add_argument('-i', '--input', help='p3 metadata input file name')

# -gnss filename will be of the form YYYY-MM-DD-hhmm_GNSSData_p1.txt

cmdline.add_argument('-g', '--gnss', help='gnss p1 input file name')

args = cmdline.parse_args()

dirPath=args.dir
inputPath=dirPath+args.input
gnssPath=dirPath+args.gnss
fref=args.input.find('p4.txt')
updateFilePath=dirPath + args.input[0:fref] + 'p4r.txt'
camSqlFilePath = dirPath + args.input[0:fref-1] + '.sql'
runSqlFilePath=dirPath + args.input[0:18] + 'run.sql'
plotSqlFilePath=dirPath + args.input[0:18] + 'images_plot_id_update.sql'



print 'Input Image p4 File:',inputPath
print 'Input GNSS p1 File:',gnssPath
print ''

ref=gnssPath.find('GNSSData')
startYear=gnssPath[(ref-16):(ref-12)]
startMonth=gnssPath[(ref-11):(ref-9)]
startDay=gnssPath[(ref-8):(ref-6)]
startDate=startYear+startMonth+startDay
tempDate=datetime.date(int(startYear),int(startMonth),int(startDay))

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

# Get the additional fields required to complete the updated p3 file: position'
# Get the additional fields required to complete the run file: long_min,long_max,lat_min,lat_max

with open(inputPath,'rU') as metadataFile:
    metadata=metadataFile.readlines()
    inputCount=0
    for row in metadata:
        inputCount+=1
        if inputCount > 1:
            rowFields = row.split('\t')
            platform='phemu'
            sensorID=rowFields[0]
            imageFileName=rowFields[1]
            md5sum=''
            easting=rowFields[2]
            northing=rowFields[3]
            point_position = 'POINT(' + easting + ' ' + northing + ')'
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
            utmDesignator = rowFields[12]
            lastUtmDesignator = utmDesignator
print 'Read ', inputCount, 'records from ', inputPath
print ''

# Create the legacy phemu_run metadata file

phRunPath=dirPath+args.input[0:18] + 'phRunData.csv'
runFolderName=run_id
utmZoneAndDesignator = lastUtmZone + lastUtmDesignator

print 'Generating Phemu Run file', phRunPath
print ' '

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
        writer = csv.writer(csvoutput,delimiter='\t')
        reader = csv.reader(csvinput,delimiter='\t')
# Append new column names to header row
        updatedRows = []
        row = next(reader)
        n=len(row)
        row.append('run_id')
        row.append('position')
        updatedRows.append(row)
        outputCount+=1
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
            outputCount += 1

        writer.writerows(updatedRows)

print 'Wrote ',outputCount, 'records to',updateFilePath
print ''


# Create the SQL command file to load the phemu_images metadata

print 'Generating SQL file',camSqlFilePath
loadCamMetCmd = """LOAD DATA LOCAL INFILE '""" + updateFilePath + """' INTO TABLE phemu_images FIELDS TERMINATED BY '\\t'""" + """ IGNORE 1 LINES  (camera_sn,image_file_name,absolute_sensor_position_x,absolute_sensor_position_y,absolute_sensor_position_z,sampling_time_utc,@sampling_date,left_utc,left_elevation,left_long,left_lat,long_zone,lat_zone,left_utm_x,left_utm_y,right_utc,right_elevation,right_long,right_lat,right_utm_x,right_utm_y,sensor_offset_x_from_left_gps,sensor_offset_y_from_left_gps,sensor_offset_z_from_left_gps,run_id,@position) SET position=ST_PointFromText(CONCAT('POINT(',absolute_sensor_position_x,' ',absolute_sensor_position_y,')')), sampling_date=STR_TO_DATE(@sampling_date,'%Y/%m/%d');"""



with open(camSqlFilePath, 'w') as sqlFile:
    sqlFile.write(loadCamMetCmd)

#print 'Generating SQL file',runSqlFilePath
#runMetCmd="""LOAD DATA LOCAL INFILE '""" + phRunPath + """' INTO TABLE phemu_run FIELDS TERMINATED BY ','""" + """ IGNORE 1 LINES (run_id,start_date_utc,start_time_utc,end_date_utc,end_time_utc,run_folder_name,long_min,long_max,lat_min,lat_max,utm_zone);"""
#with open(runSqlFilePath, 'w') as sqlFile:
#    sqlFile.write(runMetCmd)

print 'Generating SQL file',plotSqlFilePath
plotIdUpdateCmd="""UPDATE phemu_images INNER JOIN plot_map on ST_CONTAINS(plot_map.plot_polygon, phemu_images.position) SET phemu_images.plot_id = plot_map.plot_id WHERE plot_map.plot_id LIKE "16%" OR plot_map.plot_id LIKE "20%" AND run_id LIKE '""" + run_id + """';"""
with open(plotSqlFilePath, 'w') as sqlFile:
    sqlFile.write(plotIdUpdateCmd)

sys.exit()
