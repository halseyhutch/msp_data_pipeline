"""main file to read cms data, get raw data from it, and call helper functions
   to insert and update them into table hospitals and hospital_quality."""


import psycopg as pc
import pandas as pd
import sys
from load_cms_hospitals import load_cms_hospitals
from load_cms_quality import load_cms_quality
from credentials import DB_USER, DB_PW


file_to_load = sys.argv[2]
record_date = sys.argv[1]
cn = pc.connect(
    host="sculptor.stat.cmu.edu", dbname=DB_USER,
    user=DB_USER, password=DB_PW
)

to_load = pd.read_csv(
    file_to_load,
    usecols=[
        'Facility ID',
        'Facility Name',
        'Address',
        'City',
        'State',
        'ZIP Code',
        'County Name',
        'Hospital Ownership',
        'Hospital Type',
        'Emergency Services',
        'Hospital overall rating'],
    dtype={
        'Facility ID': 'str',
        'Facility Name': 'str',
        'Address': 'str',
        'City': 'str',
        'State': 'str',
        'ZIP Code': 'int',
        'County Name': 'str',
        'Hospital Ownership': 'str',
        'Hospital Type': 'str',
        'Emergency Services': 'str',
        'Hospital overall rating': 'float'},
    na_values=['Not Available']
).rename(
    {
        'Facility ID': 'hospital_pk',
        'Facility Name': 'hospital_name',
        'Address': 'address',
        'State': 'state',
        'County Name': 'county',
        'Hospital Ownership': 'hospital_owner',
        'Hospital Type': 'hospital_type',
        'Emergency Services': 'ems_provided',
        'Hospital overall rating': 'quality_rating',
        'ZIP Code': 'zip',
        'City': 'city'
    },
    axis='columns'
).assign(
    record_date=record_date
)

load_cms_hospitals(cn, to_load)
load_cms_quality(cn, to_load)

cn.close()
