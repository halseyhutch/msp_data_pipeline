import psycopg as pc
import pandas as pd
import sys
from load_cms_hospital import load_cms_hospitals
from load_cms_quality import load_cms_quality
from credentials import DB_USER, DB_PW


file_to_load = sys.argv[2]

cn = pc.connect(
    host="sculptor.stat.cmu.edu", dbname=DB_USER,
    user=DB_USER, password=DB_PW
)
cur = cn.cursor()

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
        'ZIP Code': 'str',
        'County Name': 'str',
        'Hospital Ownership': 'str',
        'Hospital Type': 'str',
        'Emergency Services': 'str',
        'Hospital overall rating': 'float'},
    na_values=['Not Available'])


to_load_1 = to_load.rename({'Facility ID': 'hospital_pk',
                            'Facility Name': 'hospital_name',
                            'Address': 'address',
                            'State': 'state',
                            'Country Name': 'county',
                            'Hospital Ownership': 'hospital_owner',
                            'Hospital Type': 'hospital_type',
                            'Emergency Services': 'ems_provided',
                            'Hospital overall rating': 'quality_rating',
                            'ZIP Code': 'zip',
                            'City': 'city'}, axis='columns')

record_date = str(sys.argv[1])
to_load_1['record_date'] = [record_date for i in range(len(to_load_1))]

load_cms_hospitals(cn, to_load_1)
load_cms_quality(cn, to_load_1)
