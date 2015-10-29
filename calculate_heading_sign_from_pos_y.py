import config
import mysql.connector
from mysql.connector import errorcode
import sys
import argparse
import csv

__author__ = 'mlucas'


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


# Get command line input.

cmdline = argparse.ArgumentParser()
cmdline.add_argument('-i', '--inf', help='The full path name of the input metadata file to process', )
cmdline.add_argument('-w', '--win', help='The window size for the sliding window that examines y position', default=5)
cmdline.add_argument('-o', '--out', help='The full path name of the output file containing heading sign for each point')
args = cmdline.parse_args()

metfile = args.inf
wsize = int(args.win)
heading_file = args.out
step = 1
y_positions = []
bufsize = 1  # Use line buffering, i.e. output every line to the file.

print "Window Size: ", wsize

try:

    with open(metfile, 'rb') as csvfile:
        reader=csv.reader(csvfile)
        metadata=list(reader)
except:
    print 'Unexpected error during database query:', sys.exc_info()[0]
    sys.exit()

metadata_hdr=metadata[0] # Save the metadata file header for writing out later
metadata.pop(0) # Remove the header line from the list
print 'Number of rows read: ', len(metadata)

row_count=0
for row in metadata:
    if row_count%3==0:
        y_positions.append([float(metadata[row_count][4]), None])
    row_count +=1

print 'Number of unique y positions: ',len(y_positions)
print ''

header ='{0:^12}{1:^15}{2:^12}'.format('DB record id', 'DB y position', 'Heading Sign')
print header



windows = slidingWindow(y_positions, wsize, step)
w_count = 0
index=6601
for window in windows:
    detector=0
    for i in range(0,(wsize-1)):
        if window[i][0] <= window[i+1][0]:
            detector+=1
    if detector>wsize/2.0:
        y_positions[w_count][1]='+'
    elif detector<wsize/2.0:
        y_positions[w_count][1]='-'
    #else:
    #     y_positions[w_count][1]='0'
    line='{0:^12}{1:^15}{2:^12}'.format(index,y_positions[w_count][0],y_positions[w_count][1])
    print line
    #print w_count,index,y_positions[w_count],detector
    w_count+=1
    index+=3


sys.exit()
with open(heading_file, 'wb') as csvfile:
    header = csv.writer(csvfile)
    header.writerow('record_id', 'absolute_sensor_position_y', 'heading_sign')
csvfile.close()

with open(heading_file, 'ab') as csvfile:
    print 'Generating heading sign file', heading_file
    for lineitem in y_positions:
        fileline = csv.writer(csvfile)
        fileline.writerow(lineitem[0], lineitem[1], lineitem[3])
csvfile.close()

sys.exit()
