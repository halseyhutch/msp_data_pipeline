UPDATE hospitals
SET hospital_name = %(hospital_name)s,
address = %(address)s,
city = %(city)s,
state = %(state)s,
zip = %(zip)s,
fips_code = %(fips_code)s,
lat = %(lat)s,
long = %(long)s
WHERE hospital_pk = %(hospital_pk)s;