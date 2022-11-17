import psycopg as pc
import pandas as pd
from re import sub
from credentials import DB_USER, DB_PW


cn = pc.connect(
    host="sculptor.stat.cmu.edu", dbname=DB_USER,
    user=DB_USER, password=DB_PW
)
cur = cn.cursor()

to_load = pd.read_csv("data/Hospital_General_Information-2021-07.csv",
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
        dtype = {
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

new_update_hospitals_data=to_load.rename({'Facility ID':'hospital_pk',
                                                        'Facility Name':'hospital_name',
                                                        'Address':'address',
                                                        'State':'state',
                                                        'Country Name':'country',
                                                        'Hospital Ownership':'hospital_owner',
                                                        'Hospital Type':'hospital_type',
                                                        'Emergency Services':'ems_provided',
                                                        'Hospital overall rating':'quality_rating',
                                                        "ZIP Code":'zip',
                                                        'City':'city',
                                                        'County Name':'country'
                                                        
                    },axis='columns')


update_hospital_data = new_update_hospitals_data[[
            'hospital_pk',    
            'hospital_name',
            'address',
            'city',
            'state',
            'country',
            'zip',
            'hospital_owner',
            'hospital_type',
            'ems_provided'        
]]

update_hospital_data['ems_provided']=update_hospital_data['ems_provided'].map({'Yes': 1, 'No': 0})

existing_data = pd.read_sql_query('SELECT * FROM hospitals;', cn)
hp_to_insert =update_hospital_data.merge(
    existing_data.hospital_pk,
    how='outer',
    on='hospital_pk',
    indicator=True
).query(
    "_merge == 'left_only'"
).drop(
    '_merge', axis=1
)
    

hp_to_update = update_hospital_data.merge(
    existing_data.melt(
        id_vars='hospital_pk',
        value_name='old_val'
    ).merge(
        update_hospital_data.melt(
            id_vars='hospital_pk',
            value_name='new_val'
        ),
        on=['hospital_pk', 'variable']
    ).query(
        "old_val != new_val"
    ).hospital_pk.drop_duplicates(),
    on='hospital_pk'
)

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
                        'country':row.country,
                        'zip': int(row.zip),
                        'hospital_owner':row.hospital_owner,
                        'hospital_type':row.hospital_type,
                        'ems_provided':row.ems_provided
                    }
                )
        except Exception as e:
            print(e)
            # TODO: make this better
            # row.to_csv('insert_errors.csv', mode='a')
        else:
            rows_inserted += 1

cn.commit()
