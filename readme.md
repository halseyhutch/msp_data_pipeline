# Loading Data

This project contains files that read, load, and update information for the HHS weekly data files and Hospital quality files. For the Hospital quality, the files can be ran with `python load_cms.py <date> <datafile.csv>`, and for the HHS weekly data files using `python load_hhs.py <datafile.csv>`.

Sql Folder: Contains commands to insert and update data for all three tables (`hospitals`, `hospital_beds`, `hospital_quality`), as well as the schema defintion in `schema.sql`.

`load_hhs_.py`: Main code to run HHS data with the name of the HHS weekly file as an argument. Loads the HHS weekly data, selects and processes columns, and calls functions to insert and update hospitals and hospital_beds tables. 

`load_cms.py`: Main code to run CMS quality data with the 1st argument as the date of this quality data and 2nd argument as the CMS quality file name. Loads the quality data, selects and processes columns, and calls functions to insert and update hospitals and hospital_quality tables.

`misc_helpers.py`: Store three helper functions `nan_to_null()`, `get_insert_rows()`, and `get_update_rows()` which are called in other files. 

`load_hhs_hospitals.py`: Uses the data from the HHS weekly file, and inserts and updates the hospitals table. 

`load_hhs_hospital_beds.py`: Uses the data from the HHS weekly file, and inserts and updates the hospital_bed table. 

`load_cms_hospital.py`: Uses the data from the CMS quality file, and inserts and updates the hospitals table. 

`load_cms_quality.py`: Uses the data from the CMS quality file, and inserts and updates the hospital_quality table. 

# Dashboard

We created a streamlit dashboard on top of the data stored in SQL. This can be run by installing streamlit and running the following in the main folder:

`streamlit run weekly_report.py`

we mainly include 7 tables and plots below:

1. A summary of how many hospital records were loaded in the most recent week, and how that compares to previous weeks.
2. A table summarizing the number of adult and pediatric beds available this week, the number used, and the number used by patients with COVID, compared to the 4 most recent weeks
3. A graph or table summarizing the fraction of beds currently in use by hospital quality rating, so we can compare high-quality and low-quality hospitals
4. A plot of the total number of hospital beds used per week, over all time, split into all cases and COVID cases
5. Heatmap of COVID cases as of current date
6. A table of hospitals that did not report any data in the past week, their names, and the date they most recently reported data
7. Graphs of hospital utilization (the percent of available beds being used) by state over time
