# Program: create_gcp_code_list.py
#
# Version: 0.1 Initial Version
#
# This program will generate a random, unique 4 letter alphabetic code which identifies a ground control point.
#
# It will check the gcp table in the wheatgenetics to make sure that the code has not already been used, and will
# generate a new code if it is already in use. This will repeat until a new unused code is found.
#
#
#
#
import string
import local_config
from string import ascii_uppercase
import random
import sys
import mysql.connector
from mysql.connector import errorcode

__author__ = 'mlucas'



#gcp_code = ''.join(random.choice(string.ascii_uppercase) for i in range(4))
#print gcp_code

print("Connecting to Database...")

try:
  cnx = mysql.connector.connect(user=local_config.USER, password=local_config.PASSWORD, host=local_config.HOST,
                                  database=local_config.DATABASE)

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursor  = cnx.cursor(buffered=True)


#Initialize an empty set which will contain the set of gcps already allocated.
old_set=set()

query='SELECT id from gcp'

cursor.execute(query)

for value in cursor:
   old_set.add(value)

print 'Existing Codes in Database: ',old_set

# Release the cursor

print ('Closing cursor')
# noinspection PyStatementEffect
cursor.close

# Release the database connection

print ("Closing database connection")
# noinspection PyUnboundLocalVariable
cnx.close()

valid_string=False

while valid_string==False:
   gcp_code = ''.join(random.choice(string.ascii_uppercase) for i in range(4))
   if gcp_code not in old_set:
     valid_string=True
print gcp_code


sys.exit()
