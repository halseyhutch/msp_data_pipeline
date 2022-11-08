-- This table provides the overall information for a hospital, such as the state it is located in, name, address, and other descriptive features
CREATE TABLE hospitals (
	id text PRIMARY KEY,
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


-- This table summarizes the number of available beds for each hospital by date
CREATE TABLE hospital_beds (
	hospital_id text REFERENCES hospitals (id),
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


-- This table provides an overall quality rating for a hospital which can be tracked at various dates
CREATE TABLE hospital_quality (
	hospital_id text REFERENCES hospitals (id),
	record_date date,
	quality_rating integer,
	PRIMARY KEY (hospital_id, record_date)
);


/*Entities: hospitals, hostipal_bed, hospital_quality*/

/*we choose them by checking how each variables are related to each other and see whether they can be connect by the same variable. */

/*In order to avoid redundency, we design our databse with the help of heritage to insure that there are no repeated information stored.*/
