/* This table provides the overall information for a hospital,
such as the state it is located in, name, address,
and other descriptive features.

Though zip codes uniquely identify states, and it is possible for
state codes to change, it has only happened once in history,
and it is not likely to occur again. */
CREATE TABLE hospitals (
	id text PRIMARY KEY,
	name text,
	address text,
	city text,
	state text,
	zip integer,
	fips_code text,
	lat numeric,
	long numeric,
	hospital_type text,
	ems_provided boolean
);


/* This table summarizes the number of available beds
for each hospital by date. */
CREATE TABLE hospital_beds (
	hospital_id text REFERENCES hospitals (id),
	record_date date CHECK (record_date <= CURRENT_DATE),
	adult_beds_available integer CHECK (adult_beds_available >= 0),
	pediatric_beds_available integer CHECK (pediatric_beds_available >= 0),
	adult_beds_used integer CHECK (adult_beds_used >= 0),
	pediatric_beds_used integer CHECK (pediatric_beds_used >= 0),
	icu_beds_used integer (icu_beds_used >= 0),
	inpatients_beds_used_covid integer (inpatients_beds_used_covid >= 0),
	staffed_adult_icu_patients_confirmed integer (staffed_adult_icu_patients_confirmed >= 0),
	PRIMARY KEY (hospital_id, record_date)
);


/* This table provides an overall quality rating
for a hospital which can be tracked at various dates.

Note that we put this data in a separate table from hospital_beds
since it is highly unlikely to be contemporaneous. A single temporal table
would have many more nulls than this alternative. */
CREATE TABLE hospital_quality (
	hospital_id text REFERENCES hospitals (id),
	record_date date CHECK (record_date <= CURRENT_DATE),
	quality_rating integer,
	PRIMARY KEY (hospital_id, record_date)
);