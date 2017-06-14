__author__ = 'mlucas'
import sys


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

    print c_time_str

    return c_time_str


current_time_str = raw_input('Enter Time String: ')
checked_time_str = check_time_str(current_time_str)

sys.exit()
