UPDATE hospitals
SET hospital_name = %(hospital_name)s,
address = %(address)s,
city = %(city)s,
state = %(state)s,
zip = %(zip)s,
county= %(county)s,
hospital_owner=%(hospital_owner)s,
hospital_type=%(hospital_type)s,
ems_provided= %(ems_provided)s
WHERE hospital_pk = %(hospital_pk)s;