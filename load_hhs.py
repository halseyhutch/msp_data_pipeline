import psycopg as pc
import pandas as pd
import sys
from load_hhs_hospitals import load_hhs_hospitals
from load_hhs_hospital_beds import load_hhs_hospital_beds
from credentials import DB_USER, DB_PW


file_to_load = sys.argv[1]
cn = pc.connect(
    host="sculptor.stat.cmu.edu", dbname=DB_USER,
    user=DB_USER, password=DB_PW
)


to_load = pd.read_csv(
    file_to_load,
    usecols=[
        'hospital_pk',
        'state',
        'hospital_name',
        'address',
        'city',
        'zip',
        'fips_code',
        'geocoded_hospital_address',
        'collection_week',
        'all_adult_hospital_beds_7_day_avg',
        'all_pediatric_inpatient_beds_7_day_avg',
        'all_adult_hospital_inpatient_bed_occupied_7_day_coverage',
        'all_pediatric_inpatient_bed_occupied_7_day_avg',
        'total_icu_beds_7_day_avg',
        'icu_beds_used_7_day_avg',
        'inpatient_beds_used_covid_7_day_avg',
        'staffed_icu_adult_patients_confirmed_covid_7_day_avg'
    ],
    dtype={
        'hospital_pk': 'str',
        'state': 'str',
        'hospital_name': 'str',
        'address': 'str',
        'city': 'str',
        'zip': 'int',
        'fips_code': 'str',
        'geocoded_hospital_address': 'str',
        'collection_week': 'str',
        'all_adult_hospital_beds_7_day_avg': 'float',
        'all_pediatric_inpatient_beds_7_day_avg': 'float',
        'all_adult_hospital_inpatient_bed_occupied_7_day_coverage': 'float',
        'all_pediatric_inpatient_bed_occupied_7_day_avg': 'float',
        'total_icu_beds_7_day_avg': 'float',
        'icu_beds_used_7_day_avg': 'float',
        'inpatient_beds_used_covid_7_day_avg': 'float',
        'staffed_icu_adult_patients_confirmed_covid_7_day_avg': 'float'
    },
    parse_dates=['collection_week'],
    na_values=['-999999']
)

assert len(to_load.collection_week.unique()) == 1, 'More than one date in file.'

load_hhs_hospitals(cn, to_load)
load_hhs_hospital_beds(cn, to_load)

cn.close()
