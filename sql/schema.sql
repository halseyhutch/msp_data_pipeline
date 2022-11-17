/* This table provides the overall information for a hospital,
such as the state it is located in, name, address,
and other descriptive features.

Though zip codes uniquely identify states, and it is possible for
state codes to change, it has only happened once in history,
and it is not likely to occur again. */
CREATE TABLE hospitals (
	hospital_pk text PRIMARY KEY,
	hospital_name text,
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
	hospital_pk text REFERENCES hospitals,
	collection_week date CHECK (collection_week <= CURRENT_DATE),
	all_adult_hospital_beds_7_day_avg numeric CHECK (all_adult_hospital_beds_7_day_avg >= 0),
	all_pediatric_inpatient_beds_7_day_avg numeric CHECK (all_pediatric_inpatient_beds_7_day_avg >= 0),
	all_adult_hospital_inpatient_bed_occupied_7_day_coverage numeric CHECK (all_adult_hospital_inpatient_bed_occupied_7_day_coverage >= 0),
	all_pediatric_inpatient_bed_occupied_7_day_avg numeric CHECK (all_pediatric_inpatient_bed_occupied_7_day_avg >= 0),
	total_icu_beds_7_day_avg numeric CHECK (total_icu_beds_7_day_avg >= 0),
	icu_beds_used_7_day_avg numeric CHECK (icu_beds_used_7_day_avg >= 0),
	inpatient_beds_used_covid_7_day_avg numeric CHECK (inpatient_beds_used_covid_7_day_avg >= 0),
	staffed_icu_adult_patients_confirmed_covid_7_day_avg numeric CHECK (staffed_icu_adult_patients_confirmed_covid_7_day_avg >= 0),
	PRIMARY KEY (hospital_pk, collection_week)
);


/* This table provides an overall quality rating
for a hospital which can be tracked at various dates.

Note that we put this data in a separate table from hospital_beds
since it is highly unlikely to be contemporaneous. A single temporal table
would have many more nulls than this alternative. */
CREATE TABLE hospital_quality (
	hospital_pk text REFERENCES hospitals,
	record_date date CHECK (record_date <= CURRENT_DATE),
	quality_rating integer,
	PRIMARY KEY (hospital_pk, record_date)
);