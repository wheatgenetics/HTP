# HTP Tools for High-Throughput Phenotyping

## UAS Image Archiving and Pre-processing Tools

### collate_micasense_flight_data.py

This program checks Micasense flight data sets for completeness and organizes files in a standard directory structure

#### Command Line Inputs:

 -d, --dir, help=Absolute path to flight data set folder to be archived

 -o, --out, help=Output path for the validated flight folders to be archived

 Input flight data set folders should have a name in the following format:

  <dateyyyymmdd>_<location>_<experiments>_<camera_type>_<planned_elevation>_<lens_angle>_<image_type>_<flight_number>

Example:	20180404_18ASH_BYD0BYD2_Rededge_20m_-90_Still_Flight1

Output folders (ready to archive) will have a name in the following format:

 20180504_163838_MRE_20m_-90_still_0
 20180504_164811_MRE_20m_-90_still_1
 20180504_165507_MRE_20m_-90_still_2

There is one output folder produced for each SET file in the Micasense input folder. In the example above,there were 3 SETS in the original input flight data folder:

0000SET, 0001SET and 0002 SET
____________________________________________________________________________________________________________________________

### archive_micasense_images.py

This program will search for Micasense flight data folders in the specified directory, validate and rename all image files for each flight and move them to the specified output folder. It will also update the wheatgenetics uas_run table with summary information about each flight and update the wheatgenetics uas_images table with metadata about each image in each flight.

Note: It is necessary to execute the program collate_micasense_flight_data for each flight folder in order to
transform the raw data into the standard format required by archive_micasense_images.

#### Command Line Inputs:


-d or --dir:      Beocat directory path to HTP image files, default=/bulk/jpoland/images/staging/uav_incoming/

-t or --type:     Image file type, e.g. TIF, JPG,DNG,default=TIF

-o or --out:      Output file path and filename,default=/bulk/jpoland/images/staging/uav_processed/

____________________________________________________________________________________________________________________________

### preprocess_micasense_images

#### Run-time Pre-requisites

1. You must set the path to the geos run-time library before executing this program:

   Example: export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/homes/mlucas/geos/lib

2. Make sure that you have write permissions to the flight data set folder.

#### Parameters

usage: preprocess_micasense_images [-h] [-p PATH] [-c PANEL] [-e EXIFT]
                                   [-b BLACK] [-ct CONTOUR] [-r RENAME]

optional arguments:
  -h, --help              show this help message and exit
  
  -p PATH, --path         Path to Micasense flight data set directory
  
  -c PANEL, --panel       Calibration panel serial number
  
  -e EXIFT, --exift       Path to exiftool executable
  
  -b BLACK, --black       Black Threshold   (default value = 110)
  
  -ct CONTOUR, --contour  Contour Threshold (default value = 4000)
  
  -t TRUNCATE, --truncate Threshold for truncation of images with size below threshold (default=2097152)
  
  -r RENAME, --rename     Rename images (Y or N)
  
 
#### Notes:
  
This program will calibrate micasense image datasets. It can take input from either a raw flight data set 
(as produced by the UAV operators) or an archived flight data set (stored on Beocat in /bulk/jpoland/images/uas).

If processing a raw data set, the -r flag should be set to Y.
If processing an archived data set, the -r flag should be set to N.

The program will perform the following functions:

1. Verify that there are 5 and only 5 images in each image set e.g. IMG_0001_1.tif,IMG_0001_2.tif,IMG_0001_3.tif,
   IMG_0001_4.tif,IMG_0001_5.tif. Any image sets that contain less than 5 or more than 5 images will be deleted.
2. Verify that there are no truncated images (with unreadable EXIF metadata) by checking the image size is greater than
   a threshold. Any image sets containing truncated images will be deleted.
3. Copy all images to be processed into a renamed folder, leaving the original image data set intact.
4. Identify all images that are low altitude images and move them into a low_altitude folder.
5. Detect images containing the calibration panel in the image and derive calibration parameters.
6. Calibrate all images using the computed calibration parameters.

____________________________________________________________________________________________________________________________

### create_uav_dji_x_metadata_file_v07.2.py

This program is used to pre-process image data acquired by a DJI X5R camera. The program uses the DJI log file  to
assign a position to each image and renames the images in order to provide a unique name for each image. It also
creates a CSV file containing image metadata to be imported into the uas_images table in the wheatgenetics database.

The X5R flight data is segmented by range. This means that the video recording is turned on at the beginning of
a range and then turned off when the end of a range is reached. This pattern is repeated for each range that is
covered by the overall flight plan. The log parameter isTakingVideo (0 or 1) is used to determine the set of log
entries associated with each range: isTakingVideo = 1 means the UAV is flying over a range.

The images for each range are stored in a specific folder. The folder naming convention is:

DJI_<Camera Sensor ID>_<Range Sequence Number>_<Date (yyyymmdd(

Example:  DJI_A01733_C001_20170502 (First range in flight)
          DJI_A01733_C002_20170502 (Second range in flight)
          DJI_A01733_C003_20170502 (Third range in flight)

#### Notes:
1. The position data that is recorded by the DJI X5R camera (EXIF) has been found to be inaccurate. Therefore, the
   position of the UAV at a specific time is determined from the DJI log file position data.
2. The sampling frequency of the DJI log file is 10Hz.
3. The images are acquired at a frequency of 24 hz.
4. Therefore,The log file position data is interpolated at a frequency of 100 Hz in order to obtain a more accurate
   position at a specific time.
5. The image timestamp is then matched to the timestamp in the logfile (to the nearest 10ms) in order
   to identify and assign a position to the image at that time.

#### Command Line Inputs:

-d or --dir:      Directory path to folder containing UAV image and log files

-t or --type:     Image file type extension, e.g. DNG,CR2, JPG, default=DNG

-o or --out:      Output file path

-r or --rename:   Rename image files Y or N,default=N

-x or --debug:    Dump interpolated log file Y or N,default=N

-e or --expt:     Plot prefix for experiment,default=18ASH%

-y or --lonoffset:Longitude offset in degrees,default=0.0  Used to correct error in log file longitude

-z or --latoffset:Latitude offset in degrees,default=0.0   User to correct error in log file latitude

-u or --update:   Update EXIF position data,default=N

____________________________________________________________________________________________________________________________
