import utm

lati=raw_input('Enter Latitude:')
longi= raw_input('Enter Longitude:')
latitude=float(lati)
longitude=float(longi)
p = utm.from_latlon(latitude,longitude)
print p
q=utm.to_latlon(p[0],p[1],p[2],p[3])
print q
