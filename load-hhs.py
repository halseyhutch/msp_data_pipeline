import psycopg as pc
import pandas as pd
from re import sub
from credentials import DB_USER, DB_PW


cn = pc.connect(
    host="sculptor.stat.cmu.edu", dbname=DB_USER,
    user=DB_USER, password=DB_PW
)
cur = cn.cursor()


to_load = pd.read_csv(
    'data/2022-09-23-hhs-data.csv',
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
    dtype = {
        'hospital_pk': 'str',
        'state': 'str',
        'hospital_name': 'str',
        'address': 'str',
        'city': 'str',
        'zip': 'int64',
        'fips_code': 'str',
        'geocoded_hospital_address': 'str',
        'collection_week': 'str',
        'all_adult_hospital_beds_7_day_avg': 'float64',
        'all_pediatric_inpatient_beds_7_day_avg': 'float64',
        'all_adult_hospital_inpatient_bed_occupied_7_day_coverage': 'float64',
        'all_pediatric_inpatient_bed_occupied_7_day_avg': 'float64',
        'total_icu_beds_7_day_avg': 'float64',
        'icu_beds_used_7_day_avg': 'float64',
        'inpatient_beds_used_covid_7_day_avg': 'float64',
        'staffed_icu_adult_patients_confirmed_covid_7_day_avg': 'float64'
    },
    parse_dates=['collection_week'],
    na_values=['-999999']
)

new_hospitals_data = to_load.filter(items=[
    'hospital_pk',
    'state',
    'hospital_name',
    'address',
    'city',
    'zip',
    'fips_code',
    'geocoded_hospital_address'
])

# transform geocoded address into lat/long columns
new_hospitals_data['long'] = [float(sub(r'POINT \((.*) .*', r'\g<1>', str(x)))
    for x in new_hospitals_data['geocoded_hospital_address']]
new_hospitals_data['lat'] = [float(sub(r'POINT \(.* (.*)\)', r'\g<1>', str(x))) 
    for x in new_hospitals_data['geocoded_hospital_address']]
new_hospitals_data.drop('geocoded_hospital_address', axis=1, inplace=True)

# determine which hospitals need to be inserted
existing_data = pd.read_sql_query('SELECT * FROM hospitals;', cn)
hp_to_insert = new_hospitals_data.merge(
    existing_data.hospital_pk,
    how='outer',
    on='hospital_pk',
    indicator=True
).query(
    "_merge == 'left_only'"
).drop(
    '_merge', axis=1
)

# determine which hospitals need to be updated
ed_long = existing_data.melt(id_vars='hospital_pk', value_name='old_val')
nhd_long = new_hospitals_data.melt(id_vars='hospital_pk', value_name='new_val')
hp_cmp = ed_long.merge(nhd_long, on=['hospital_pk', 'variable'])
pks_to_update = hp_cmp.query("old_val != new_val").hospital_pk.unique()
hp_to_update = new_hospitals_data.merge(pks_to_update, on='hospital_pk')

rows_inserted = 0
with cn.transaction():
    # TODO: find a better way to iterate through rows
    for row in new_hospitals_data:
        try:
            with cn.transaction():
                # placeholder query
                cur.execute('SELECT * FROM hospitals;')
        except Exception as e:
            print(e)
        else:
            rows_inserted += 1

cn.commit()