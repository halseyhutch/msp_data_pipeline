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
hp_to_update = new_hospitals_data.merge(
    existing_data.melt(
        id_vars='hospital_pk',
        value_name='old_val'
    ).merge(
        new_hospitals_data.melt(
            id_vars='hospital_pk',
            value_name='new_val'
        ),
        on=['hospital_pk', 'variable']
    ).query(
        "old_val != new_val"
    ).hospital_pk.drop_duplicates(),
    on='hospital_pk'
)

# insert new rows
rows_inserted = 0
with open('insert_hospitals_hhs.sql') as f:
    insert_query = f.read()
with cn.transaction():
    for i in range(hp_to_insert.shape[0]):
        row = hp_to_insert.iloc[i, :]
        try:
            with cn.transaction():
                # TODO: fix NaN to be NULL in the database
                cur.execute(
                    insert_query,
                    {
                        'hospital_pk': row.hospital_pk,
                        'hospital_name': row.hospital_name,
                        'address': row.address,
                        'city': row.city,
                        'state': row.state,
                        'zip': int(row.zip),
                        'fips_code': row.fips_code,
                        'lat': row.lat,
                        'long': row.long
                    }
                )
        except Exception as e:
            print(e)
            # TODO: make this better
            # row.to_csv('insert_errors.csv', mode='a')
        else:
            rows_inserted += 1

cn.commit()


# TODO: update changed rows