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

serial_number_tag = 'EXIF BodySerialNumber'


def get_image_file_list(file_path, f_image_type):
    # Return a list of the names and sample date & time for all image files.

    image_file_list = []

    # Get list of files in uas staging directory

    print("Fetching list of image files...")

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
        #print ''
        #print 'old file name: ', orig_image_path
        #print 'new file name: ', new_file_path
        
    except IOError as fe:
        print "I/O error({0}): {1}".format(fe.errno, fe.strerror), orig_image_path
        new_file_path=''
        renamed_image_file=''
        cam_serial_number=''
    except:
        print "Unexpected error:", sys.exc_info()[0]
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

    print 'Original Time String: ', time_str, ' Checked Time String: ', c_time_str

    return c_time_str

cmdline = argparse.ArgumentParser()

cmdline.add_argument('-d', '--dir', help='Beocat directory path to phenocam staging directory',
                     default='/homes/jpoland/images/staging/phenocam_staging')

cmdline.add_argument('-s', '--set', help='Data Set Folder Name e.g. 20150504')

cmdline.add_argument('-i', '--imagein', help='Image log input file name')

cmdline.add_argument('-c', '--corr', help='Time-corrected image log output file name')

cmdline.add_argument('-p', '--pcornin', help='Phenocorn log input file name')

cmdline.add_argument('-o', '--metout', help='Image log output folder')

args = cmdline.parse_args()

phenocam_path = args.dir
data_set = args.set
date_utc = data_set[:4] + '/' + data_set[4:6] + '/' + data_set[6:8]
image_input_log_file = phenocam_path + args.set + "/" + args.imagein
corrected_image_log_file = phenocam_path + args.set + "/" + args.corr
pcorn_input_log_file = phenocam_path + args.set + "/" + args.pcornin
metadata_output_path = phenocam_path + args.set + "/"
image_type = "CR2"


# CAM1_path=phenocam_path+data_set+'/Camera1/DCIM/100CANON/'
# CAM2_path=phenocam_path+data_set+'/Camera2/DCIM/100CANON/'
# CAM3_path=phenocam_path+data_set+'/Camera3/DCIM/100CANON/'

CAM1_path = phenocam_path + data_set + '/Camera1/'
CAM2_path = phenocam_path + data_set + '/Camera2/'
CAM3_path = phenocam_path + data_set + '/Camera3/'

CAM1_image_file_list = []
CAM2_image_file_list = []
CAM3_image_file_list = []

# noinspection PyRedeclaration
CAM1_image_file_list = get_image_file_list(CAM1_path, image_type)
# noinspection PyRedeclaration
CAM2_image_file_list = get_image_file_list(CAM2_path, image_type)
# noinspection PyRedeclaration
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
# into the list corrected_image_list

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
    print "I/O error({0}): {1}".format(e.errno, e.strerror)
except ValueError as v:
    print "Value error({0}): {1}".format(v.errno, v.strerror)
except:
    print "Unexpected error 1:", sys.exc_info()[0]
    sys.exit()

# Open the phenocorn data file and load the data into the list pcorn_metadata_list

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
    print "I/O error({0}): {1}".format(e.errno, e.strerror)
except:
    print "Unexpected error 2 :", sys.exc_info()[0]
    sys.exit()

# Assemble the full set of raw metadata
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

# Assemble the full set of finished metadata
sampleTime = ''
for record in raw_pcam_metadata_list:
    try:
        image_data = rename_image_file(record)
        image_file_path=image_data[0]
        image_file_name = image_data[1]
        orig_image_name = image_data[3]
        undo_list.append([image_file_path,orig_image_name])
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
        posY = str(position[1])
        posZ = record[2]
        long_zone = str(position[2])
        lat_zone = str(position[3])

        time_utc = record[1][:2] + ':' + record[1][2:4] + ':' + record[1][4:6]
        # Date is stored in date_utc
        elevation = posZ
        full_pcam_metadata_record = [record_id,run_id,image_file_name, sensorID,posX, posY, posZ, time_utc, date_utc,
                                    elevation, img_longitude, img_latitude,long_zone, lat_zone,md5]
        print full_pcam_metadata_record[2]
        pcam_metadata_list.append(full_pcam_metadata_record)
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror), image_file_name
    except:
        print "Unexpected error 3:", sys.exc_info()[0]
        sys.exit()

with open(undo_log, 'w') as undo_file:
    print "Generating undo log file"
    for log_item in undo_list:
        mv_string = 'mv ' + log_item[0] + ' ' + log_item[1]
        undo_file.write(mv_string + '\n')
undo_file.close()

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

with open(metadata_output_file, 'wb') as csvfile:
    print "Initializing phenocam metadata output file"
    header = csv.writer(csvfile)
    header.writerow(
        ['record_id', 'run_id','image_file_name','camera_sn', 'absolute_sensor_position_x',
         'absolute_sensor_position_y', 'absolute_sensor_position_z','sampling_time_utc','sampling_date','elevation',
         'longitude','latitude', 'long_zone','lat_zone','md5sum'])
csvfile.close()

with open(metadata_output_file, 'ab') as csvfile:
    print 'Generating phenocam image metadata file', metadata_output_file
    for line_item in pcam_metadata_list:
        file_line = csv.writer(csvfile)
        file_line.writerow(
            [line_item[0], line_item[1], line_item[2], line_item[3], line_item[4], line_item[5],
             line_item[6], line_item[7], line_item[8], line_item[9], line_item[10], line_item[11],
             line_item[12], line_item[13], line_item[14]])
csvfile.close()

# Exit the program gracefully

print ('Processing Completed. Exiting...')

sys.exit()
