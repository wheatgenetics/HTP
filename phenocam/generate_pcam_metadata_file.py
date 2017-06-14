#!/usr/bin/python
#
# Program: generate_pcam_metadata_file
#
# Version: 0.2 December 16,2015
#
# Added in code to correct y position sensor offset
#
# Version: 0.1 October 14,2015
#
# Creates a database metadata file for Phenocam data to load phenocam_images table.
#
# N.B. This version does not correct for camera offset.
#
#
# Command Line Inputs:
#
# '-d', '--dir', help='Beocat directory path to phenocam staging directory',
#                      default='/homes/jpoland/images/staging/phenocam_staging'
# '-s', '--set', help='Data Set Folder Name e.g. 20150504'
# '-i', '--imagein', help='Image log input file name'
# '-c', '--corr', help='Time-corrected image log output file name'
# '-p', '--pcornin', help='Phenocorn log input file name'
# '-o', '--metout', help='Image log output folder'
#
#
__author__ = 'mlucas'
import subprocess
import csv
import collections
import datetime
import sys
import argparse
import re
import os
import utm
import hashlib
import exifread


__author__ = 'mlucas'

print "generate_pcam_metadata_file Version 0.2, December 16,2015"

serial_number_tag = 'EXIF BodySerialNumber'


def get_image_file_list(file_path, f_image_type):
    # Return a list of the names and sample date & time for all image files.

    image_file_list = []

    # Get list of files in uas staging directory

    print("Fetching list of image files for",file_path)

    files_to_check = subprocess.check_output(['ls', '-1', file_path], universal_newlines=True)

    a_file = ''
    file_list = []

    for char in files_to_check:
        if char != '\n':
            a_file += char
        else:
            file_list.append(a_file)
            a_file = ''

            # Get the subset of files that are the image files

    for f in file_list:
        start_pos = len(f) - 3
        end_pos = len(f)
        is_image_file = (f != '' and f[start_pos:end_pos] == f_image_type)
        if is_image_file:
            image_file_list.append(f)

    return image_file_list


def get_camera_body_serial_number(image_file):
    tags = exifread.process_file(image_file)
    if serial_number_tag in tags:
        sn_string = str(tags[serial_number_tag])
    else:
        sn_string = '*'
    return sn_string


def rename_image_file(image_record):
    image_file = image_record[0][5:]
    image_number = image_file[4:]
    camera = image_record[0][:4]
    camera_number = image_record[0][3]
    sample_time = image_record[1][:6]
    orig_image_path = phenocam_path + data_set + '/' + cam_folder[camera] + '/' + image_file
    try:
        with open(orig_image_path, 'rb') as image:
            cam_serial_number = get_camera_body_serial_number(image)
        renamed_image_file = cam_serial_number + '_' + data_set + '_' + sample_time + '_C0' + camera_number \
                             + '_' + image_number
        new_file_path = phenocam_path + data_set + "/" + cam_folder[camera] + "/" + renamed_image_file
        os.rename(orig_image_path, new_file_path)

        
    except IOError as fe:
        print "(rename_image_file) I/O error({0}): {1}".format(fe.errno, fe.strerror), orig_image_path
        new_file_path=''
        renamed_image_file=''
        cam_serial_number=''
    except:
        print "(rename_image_file) Unexpected error:", sys.exc_info()[0]
        sys.exit()

    return new_file_path, renamed_image_file, cam_serial_number, orig_image_path


def get_image_utm_position(f_latitude, f_longitude):
    utm_position = utm.from_latlon(f_latitude, f_longitude)
    return utm_position


def hashfilelist(a_file, blocksize=65536):
    hasher = hashlib.md5()
    buf = a_file.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = a_file.read(blocksize)
    return hasher.hexdigest()


def calculate_checksum(filename):
    checksum = hashfilelist(open(filename, 'rb'))
    return checksum


def check_time_str(time_str):
    hour = time_str[0:2]
    mins = time_str[2:4]
    secs = time_str[4:6]
    fsec = time_str[6:]

    c_hour = hour
    c_mins = mins
    c_secs = secs

    if int(secs) > 59:
        c_secs = '00'
        if int(mins) < 59:
            c_mins = str(int(mins) + 1).zfill(2)
        else:
            c_mins = '00'
            if int(hour) < 23:
                c_hour = str(int(hour) + 1).zfill(2)
            else:
                c_hour = '00'
    else:
        c_secs = secs

    if int(mins) > 59:
        c_mins = '00'
        if int(hour) < 23:
            c_hour = str(int(hour) + 1).zfill(2)
        else:
            c_hour = '00'

    if int(hour) > 23:
        c_hour = '00'

    c_time_str = c_hour + c_mins + c_secs + fsec

    return c_time_str

def slidingWindow(sequence, winSize, step=1):
    """Returns a generator that will iterate through
    the defined chunks of input sequence.  Input sequence
    must be iterable."""

    # Verify the inputs
    try:
        it = iter(sequence)
    except TypeError:
        raise Exception("**ERROR** sequence must be iterable.")
    if not ((type(winSize) == type(0)) and (type(step) == type(0))):
        raise Exception("**ERROR** type(winSize) and type(step) must be int.")
    if step > winSize:
        raise Exception("**ERROR** step must not be larger than winSize.")
    if winSize > len(sequence):
        raise Exception("**ERROR** winSize must not be larger than sequence length.")

    # Pre-compute number of chunks to emit
    numOfChunks = ((len(sequence) - winSize) / step) + 1

    # Do the work
    for i in range(0, numOfChunks * step, step):
        yield sequence[i:i + winSize]



cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat directory path to phenocam staging directory',
                     default='/homes/jpoland/images/staging/phenocam_staging')

cmdline.add_argument('-s', '--set', help='Data Set Folder Name e.g. 20150504')

cmdline.add_argument('-i', '--imagein', help='Image log input file name')

cmdline.add_argument('-c', '--corr', help='Time-corrected image log output file name')

cmdline.add_argument('-p', '--pcornin', help='Phenocorn log input file name')

cmdline.add_argument('-w', '--win', help='The window size for the sliding window that examines y position', default=5)

cmdline.add_argument('-o', '--metout', help='Image log output folder')

args = cmdline.parse_args()

phenocam_path = args.dir
data_set = args.set
date_utc = data_set[:4] + '/' + data_set[4:6] + '/' + data_set[6:8]
image_input_log_file = phenocam_path + args.set + "/" + args.imagein
corrected_image_log_file = phenocam_path + args.set + "/" + args.corr
pcorn_input_log_file = phenocam_path + args.set + "/" + args.pcornin
wsize=int(args.win) # Window size used for sliding window function
step=1  # Step size needed for sliding window function
metadata_output_path = phenocam_path + args.set + "/"
image_type = "CR2"


# CAM1_path=phenocam_path+data_set+'/Camera1/DCIM/100CANON/'
# CAM2_path=phenocam_path+data_set+'/Camera2/DCIM/100CANON/'
# CAM3_path=phenocam_path+data_set+'/Camera3/DCIM/100CANON/'

# Note that all image files must be copied up to the top level of the Camera1,Camera2 or Camera3 folders.
# The files are not consistently located in the sample location their sub-folders

CAM1_path = phenocam_path + data_set + '/Camera1/'
CAM2_path = phenocam_path + data_set + '/Camera2/'
CAM3_path = phenocam_path + data_set + '/Camera3/'

CAM1_image_file_list = []
CAM2_image_file_list = []
CAM3_image_file_list = []

# Load all of the image file names into a list per camera


CAM1_image_file_list = get_image_file_list(CAM1_path, image_type)
CAM2_image_file_list = get_image_file_list(CAM2_path, image_type)
CAM3_image_file_list = get_image_file_list(CAM3_path, image_type)

current_time_str = ''
previous_time_str = ''
corrected_image_list = []
pcorn_metadata_list = []
raw_pcam_metadata_list = []
pcam_metadata_list = []
image_dict = {}
pcorn_dict = {}
undo_list = []
cam_folder = {'CAM1': 'Camera1', 'CAM2': 'Camera2', 'CAM3': 'Camera3'}
record_id = None

# Open the Phenocam Camera Metadata file, Interpolate the timestamp if required and load corrected data
# into the list corrected_image_list.
#
# This part of the code generates a timestamp when timestamp values are missing (00000.000) in the raw data file
#
# The interpolation relies on the fact that the cameras were triggered every 0.8 seconds (800000 microseconds)
#
# The camera metadata with the corrected timestamps is stored in a new list correct_image_list.
#
# The dictionary image_dict stores the names of the 3 camera images acquired each time the cameraw were triggered
# using the timestamp as a key. This will be used later to match the phenocorn position information recorded at
# at the time represented by each timestamp.
#

try:
    with open(image_input_log_file, 'rU') as logfile:
        log = logfile.readlines()
        for row in log:
            cam1 = row.split(',')[0]
            cam2 = row.split(',')[1]
            cam3 = row.split(',')[2]
            temp_time_str = row.split(',')[3]
            current_time_str = check_time_str(temp_time_str)
            if current_time_str == '000000.000':
                previous_time = datetime.datetime.strptime(previous_time_str, '%H%M%S.%f')
                corrected_time = previous_time + datetime.timedelta(microseconds=800000)
                correct_time_str = str(corrected_time.hour).zfill(2) + str(corrected_time.minute).zfill(2) + \
                                   str(corrected_time.second).zfill(2) + '.' + \
                                   str(corrected_time.microsecond)[0:2].zfill(2)
                previous_time_str = correct_time_str
            else:
                correct_time_str = current_time_str[:-1]
                previous_time_str = correct_time_str
            correctedImageRecord = [cam1, cam2, cam3, correct_time_str]
            corrected_image_list.append(correctedImageRecord)
            image_dict[correct_time_str] = [cam1, cam2, cam3]
except IOError as e:
    print "(time correction) I/O error({0}): {1}".format(e.errno, e.strerror)
except ValueError as v:
    print "(time correction) Value error({0}): {1}".format(v.errno, v.strerror)
except:
    print "(time correction)  Unexpected error 1:", sys.exc_info()[0]
    sys.exit()

#
# Open the phenocorn data file and load the data into the list pcorn_metadata_list
#
# This file contains the accurate GPS timestamp and the position data that is used to geo-reference the images
#
# The phenocorn metadata is stored in a list called pcorn_metadata_list.
#
# The dictionary pcorn_dict stores the phenocorn position data acquired at the same time the camera were triggered
# using the timestamp as a key. This will be used later to match the phenocam position information recorded at
# at the time represented by each timestamp.
#
#

line_count = 0

try:
    with open(pcorn_input_log_file, 'rU') as pcorn_file:
        pcorn_log = pcorn_file.readlines()
        for p_row in pcorn_log:
            if line_count > 0:
                split_prow = re.split(r'\t+', p_row)
                utc_time = split_prow[2]
                elevation = split_prow[3]
                longitude = split_prow[4]
                long_ref = split_prow[5]  # W makes longitude negative, E makes longitude positive
                latitude = split_prow[6]
                lat_ref = split_prow[7]
                ndvi = split_prow[8]
                tempC = split_prow[9]
                pcornMetadataRecord = [utc_time, elevation, longitude, long_ref, latitude, lat_ref]
                pcorn_metadata_list.append(pcornMetadataRecord)
                pcorn_dict[utc_time] = [elevation, longitude, long_ref, latitude, lat_ref]
            line_count += 1
    run_start = pcorn_metadata_list[0][0][:6]
    start_time = run_start[:2] + ':' + run_start[2:4] + ':' + run_start[4:6]
    run_end = pcorn_metadata_list[len(pcorn_metadata_list) - 1][0][:6]
    end_time = run_end[:2] + ':' + run_end[2:4] + ':' + run_end[4:6]
    run_date = data_set[:8]
    run_id = run_date + '_' + run_start + '_' + run_date + '_' + run_end
    print 'Run ID = ', run_id
    undo_log = phenocam_path + args.set + "/" + run_id + '_undo_log.sh'
    metadata_output_file = metadata_output_path + 'pcam_' + run_id + '_metadata.csv'
except IOError as e:
    print "(phenocorn data input) I/O error({0}): {1}".format(e.errno, e.strerror)
except:
    print "(phenocorn data input) Unexpected error 2 :", sys.exc_info()[0]
    sys.exit()

#
# Assemble the full set of raw metadata
#
# Assemble a complete metadata record by combining raw data from image_dict and pcorn_dict using their timestamp key to
# match the two sets of data (i.e. match image names to position at a given time.)
#
sorted_image_dict = collections.OrderedDict(sorted(image_dict.items()))
for key, value in sorted_image_dict.iteritems():
    if key in pcorn_dict:
        cam1_image = value[0]
        cam2_image = value[1]
        cam3_image = value[2]
        sampleTime = key
        elevation = pcorn_dict[key][0]
        longitude = pcorn_dict[key][1]
        long_ref = pcorn_dict[key][2]
        latitude = pcorn_dict[key][3]
        lat_ref = pcorn_dict[key][4]
        pcam_metadata_record_1 = [cam1_image, sampleTime, elevation, longitude, long_ref, latitude,
                                  lat_ref]
        pcam_metadata_record_2 = [cam2_image, sampleTime, elevation, longitude, long_ref, latitude,
                                  lat_ref]
        pcam_metadata_record_3 = [cam3_image, sampleTime, elevation, longitude, long_ref, latitude,
                                  lat_ref]
        raw_pcam_metadata_list.append(pcam_metadata_record_1)
        raw_pcam_metadata_list.append(pcam_metadata_record_2)
        raw_pcam_metadata_list.append(pcam_metadata_record_3)

#
# Rename the image files according to requirements and compute a checksum for each image.
# Compute additional fields required to complete the metadata i.e. utm position and zone.
#
sampleTime = ''
posY=''         # placeholder for corrected position
heading_sign='' # placeholder for heading sign + or -

for record in raw_pcam_metadata_list:
    try:
        image_data = rename_image_file(record)
        image_file_path=image_data[0]
        image_file_name = image_data[1]
        orig_image_name = image_data[3]
        undo_list.append([image_file_path,orig_image_name])
        #N.B. Could write out undo list line by line here.
        md5 = calculate_checksum(image_file_path)
        sensorID = str(image_data[2])
        lon = record[3]
        long_ref = record[4]
        lat = record[5]
        lat_ref = record[6]
        if long_ref == "W":
            img_longitude = (float(lon)) * (-1)
        elif long_ref == "E":
            img_longitude = (float(lon))
        if lat_ref == "S":
            img_latitude = (float(lat)) * (-1)
        elif lat_ref == "N":
            img_latitude = (float(lat))
        position = get_image_utm_position(img_latitude, img_longitude)
        posX = str(position[0])
        raw_posY = str(position[1])
        posZ = record[2]
        long_zone = str(position[2])
        lat_zone = str(position[3])
        time_utc = record[1][:2] + ':' + record[1][2:4] + ':' + record[1][4:6]
        # Date is stored in date_utc
        elevation = posZ
        full_pcam_metadata_record = [record_id,run_id,image_file_name, sensorID,posX, raw_posY, posZ, time_utc, date_utc,
                                    elevation, img_longitude, img_latitude,long_zone, lat_zone,md5,posY,heading_sign]
        pcam_metadata_list.append(full_pcam_metadata_record)
        print 'Original Image File Name:',orig_image_name,'New Image File Name:',image_file_name
    except IOError as e:
        print "(metadata assembly) I/O error({0}): {1}".format(e.errno, e.strerror), image_file_name
        break
    except:
        print "(metadata assembly)Unexpected error 3:", sys.exc_info()[0]
        sys.exit()
#
# Write out the undo file log which allows us to revert to orignal image file name if there is a problem
# found with the rest of the metadata. This allows us to re-run the program again once any problems have been
# corrected.
#
with open(undo_log, 'w') as undo_file:
    print "Generating undo log file"
    for log_item in undo_list:
        mv_string = 'mv ' + log_item[0] + ' ' + log_item[1]
        undo_file.write(mv_string + '\n')
undo_file.close()

#
# Write out the corrected phenocam image metadata file
#

with open(corrected_image_log_file, 'wb') as csvfile:
    print "Initializing time-corrected image data log file"
csvfile.close()

with open(corrected_image_log_file, 'ab') as csvfile:
    print 'Generating corrected image log file', metadata_output_file
    for line_item in corrected_image_list:
        file_line = csv.writer(csvfile)
        file_line.writerow(
            [line_item[0], line_item[1], line_item[2], line_item[3]])
csvfile.close()

#
# Go through the pcam_metadata_list and calculate the heading information used to correctly apply the sensor
# offset.
#
# Process the metadata, reading only every third row, since there are 3 cameras all reporting the same y position
# Use python modulo operator % to get each unique y position.
#
# The raw y position information, the heading direction (+ or -) and the corrected y position
#
# N.B. Because of the sliding window, the heading direction for the last few rows in the data set will not be
# calculated (because there are not enough rows to fill the window.)
#
y_positions=[]
row_count=0
sensor_offset=0.24 # Sensor offset is fixed at 0.24 meters.
for row in pcam_metadata_list:
    if row_count%3==0:
        y_positions.append([float(pcam_metadata_list[row_count][5]), None, None])
    row_count +=1

windows = slidingWindow(y_positions, wsize, step)
w_count = 0
index=1
for window in windows:
    detector=0
    for i in range(0,(wsize-1)):
        if window[i][0] <= window[i+1][0]:
            detector+=1
    if detector>wsize/2.0:
        y_positions[w_count][1]='+'
        y_positions[w_count][2]=y_positions[w_count][0]+ float(sensor_offset)
    elif detector<wsize/2.0:
        y_positions[w_count][1]='-'
        y_positions[w_count][2]=y_positions[w_count][0]- float(sensor_offset)
    line='{0:^12}{1:^15}{2:^12}{3:^15}'.format(index,y_positions[w_count][0],y_positions[w_count][1],y_positions[w_count][2])
    print line
    w_count+=1
    index+=3
#
# Append the corrected absolute_sensor_position_y and heading sign to pcam_metadata_list
#
pcam_index=0
y_index=0
#print "Number of entries in pcam_metadata_list=", len(pcam_metadata_list)
while pcam_index < len(pcam_metadata_list):
    pcam_metadata_list[pcam_index][15]=y_positions[y_index][2]
    pcam_metadata_list[pcam_index][16]=y_positions[y_index][1]
    pcam_index+=1
    if pcam_index%3==0:
        y_index+=1
#
# Write out the final phenocam image metadata file that can be checked and then loaded into the database.
#

with open(metadata_output_file, 'wb') as csvfile:
    print "Initializing phenocam metadata output file"
    header = csv.writer(csvfile)
    header.writerow(
        ['record_id', 'run_id','image_file_name','camera_sn', 'absolute_sensor_position_x',
         'raw_sensor_position_y', 'absolute_sensor_position_z','sampling_time_utc','sampling_date','elevation',
         'longitude','latitude', 'long_zone','lat_zone','md5sum', 'absolute_sensor_position_y', 'heading_sign'])
csvfile.close()

with open(metadata_output_file, 'ab') as csvfile:
    print 'Generating phenocam image metadata file', metadata_output_file
    for line_item in pcam_metadata_list:
        file_line = csv.writer(csvfile)
        # Write out each line except those which do not have a valid corrected position value
        if line_item[15] !=None:
            file_line.writerow(
                [line_item[0], line_item[1], line_item[2], line_item[3], line_item[4], line_item[5],
                line_item[6], line_item[7], line_item[8], line_item[9], line_item[10], line_item[11],
                line_item[12], line_item[13],line_item[14],line_item[15],line_item[16]])
csvfile.close()

# Exit the program gracefully

print ('Processing Completed. Exiting...')

sys.exit()
