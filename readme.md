This project contains files that read, load, and update information for the HHS weekly data files and Hospital quality files. For the Hospital quality, the files can be ran with 'python load_quality.py date datafile.csv', and for the HHS weekly data files using 'python load-hhs.py datafile.csv'.


Folder Sql: Store insert and update sql commands for all three tables (hospitals, hospital_beds, hospital_quality).

Schema.sql: Store the tables structure: hospitals, hospital_beds, hospital_quality

load-hhs.py: input with the name of the HHS weekly file as argument. Load the HHS weekly data, select and process columns, and call functions to insert and update hospitals table and hospital_beds table. 

load_quality.py: input with the 1st argument as the date of this quality data, and 2nd argument as the CMS quality file name. Load the quality data, select and process useful columns, and call functions to insert and update hospitals table and hospital_quality table.

misc-helpers.py: store three functions: nan_to_null(), get_insert_rows(), get_update_rows() that we can call and help in other files. 

load_hhs_hospitals.py: with the data from HHS weekly file, insert and update it with the hospitals table. 

load_hhs_hospital_beds.py: with the data from HHS weekly file, insert and update it with the hospital_bed table. 

load_cms_hospital.py: with the data from CMS quality file, insert and update it with the hospitals table. 

load_cms_quality.py: with the data from CMS quality file, insert and update it with the hospital_quality table. 
