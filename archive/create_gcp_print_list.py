import argparse

# Get command line input.


cmdline = argparse.ArgumentParser()


cmdline.add_argument('-y', '--year', help='The HTP data collection year (yy) e.g. 15')
cmdline.add_argument('-l', '--loc',help='The HTP data collection location e.g. 15ASH')
cmdline.add_argument('-f','--field',help = 'The HTP data collection field, e.g. 15_ASH_BYD')
cmdline.add_argument('-p', '--path', help='The file system path for the gcp output filename')
cmdline.add_argument('-n','--num', help= 'The number of new codes to generate', default=0)

htp_year=args.year
htp_location=args.loc
htp_field=args.field
htp_filepath = args.path
htp_gcps=args.num

print 'Year = ',htp_year
print 'Location = ',location
print 'Field = ',field
print 'File path = ',filepath
print 'Number of new GCPs to select = ',num_gcps

print 'Exiting...'
sys.exit()

