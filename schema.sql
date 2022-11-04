#Edit comment test
CREATE TABLE hospitals (
	id string PRIMARY KEY,
	state text,
	name text,
	address text,
	city text,
	zip integer,
	fips_code text,
	lat integer,
	long integer,
	hospital_type text,
	ems_provided boolean
);


CREATE TABLE hospital_beds (
	hospital_id string REFERENCES hospitals (id),
	record_date date,
	adult_beds_available integer,
	pediatric_beds_available integer,
	adult_beds_used integer,
	pediatric_beds_used integer,
	icu_beds_used integer,
	inpatients_beds_used_covid integer,
	staffed_adult_icu_patients_confirmed integer,
	PRIMARY KEY (hospital_id, record_date)
);


CREATE TABLE hospital_quality (
	hospital_id string REFERENCES hospitals (id),
	record_date date,
	quality_rating integer,
	PRIMARY KEY (hospital_id, record_date)
);


