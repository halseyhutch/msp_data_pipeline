# load_cms_quality.py

import pandas as pd
from misc_helpers import nan_to_null, get_insert_rows, get_update_rows

def hospitals_to_sql(cn, to_insert, to_update, orig_to_load):

    cur = cn.cursor()

    rows_inserted = 0
    rows_updated = 0
    insert_error_pks = []
    update_error_pks = []

    with open('sql/insert_cms_quality.sql') as f:
        insert_query = f.read()

    with open('sql/update_cms_quality.sql') as f:
        update_query = f.read()

    with cn.transaction():

        rows_inserted = 0
        rows_updated = 0
        with open('insert_hospital_quality.sql') as f:
            insert_query = f.read()
        with open('update_hospital_quality.sql') as f:
            update_query = f.read()
        with cn.transaction():
            #insert rows
            for i in range(to_insert.shape[0]):
                row = to_insert.iloc[i, :]
                try:
                    with cn.transaction():
                        # TODO: fix NaN to be NULL in the database
                        cur.execute(
                            insert_query,
                            {
                                'hospital_pk': row.hospital_pk,
                                'record_date': row.record_date,
                                'quality_rating': row.quality_rating,
                            }
                        )
                except Exception as e:
                    print(e)
                    insert_error_pks.append(row.hospital_pk)
                else:
                    rows_inserted += 1
        
            #update rows
            for i in range(to_update.shape[0]):
                row = to_update.iloc[i, :]
                try:
                    with cn.transaction():
                        cur.execute(
                            update_query,
                            {
                                'hospital_pk': row.hospital_pk,
                                'record_date': row.record_date,
                                'quality_rating': row.quality_rating,
                            }
                        )
                except Exception as e:
                    print(e)
                    update_error_pks.append(row.hospital_pk)
                else:
                    rows_updated += 1


    orig_to_load.merge(
        pd.DataFrame(
            {'hospital_pk': insert_error_pks}
        ),
        on='hospital_pk'
    ).to_csv(
        'hhs_hospital_insert_errors.csv',
        index=False
    )

    orig_to_load.merge(
        pd.DataFrame(
            {'hospital_pk': update_error_pks}
        ),
        on='hospital_pk'
    ).to_csv(
        'hhs_hospital_update_errors.csv',
        index=False
    )

    cn.commit()

    print(f'Inserted {rows_inserted} rows in the hospitals table.')
    print(f'Updated {rows_updated} rows in the hospitals table.')


# hospitals data = hd
def load_hhs_hospitals(cn, to_load):

    new_hd = to_load.filter(items=[
        'hospital_pk',
        'state',
        'hospital_name',
        'address',
        'city',
        'zip',
        'fips_code',
        'geocoded_hospital_address'
    ])

    # preprocessing
    geocode_to_lat_long(new_hd)
    new_hd = nan_to_null(new_hd)

    # divide into insert / update subsets
    existing_hospitals = pd.read_sql_query('SELECT * FROM hospitals;', cn)
    join_keys = ['hospital_pk']
    to_insert = get_insert_rows(new_hd, existing_hospitals, join_keys)
    to_update = get_update_rows(new_hd, existing_hospitals, join_keys)

    # push the data to sql
    hospitals_to_sql(cn, to_insert, to_update, to_load)
