import warnings
import pandas as pd
from misc_helpers import nan_to_null, get_insert_rows, get_update_rows


def hospitals_to_sql(cn, to_insert, to_update, orig_to_load):
    cur = cn.cursor()

    rows_inserted = 0
    rows_updated = 0
    insert_error_pks = []
    update_error_pks = []

    with open('sql/insert_hospitals_cms.sql') as f:
        insert_query = f.read()

    with open('sql/update_hospitals_cms.sql') as f:
        update_query = f.read()

    with cn.transaction():

        # insert rows
        for i in range(to_insert.shape[0]):
            row = to_insert.iloc[i, :]
            try:
                with cn.transaction():
                    cur.execute(
                        insert_query,
                        {
                            'hospital_pk': row.hospital_pk,
                            'hospital_name': row.hospital_name,
                            'address': row.address,
                            'city': row.city,
                            'state': row.state,
                            'county': row.county,
                            'zip': int(row.zip),
                            'hospital_owner': row.hospital_owner,
                            'hospital_type': row.hospital_type,
                            'ems_provided': row.ems_provided
                        }
                    )
            except Exception as e:
                print(e)
                insert_error_pks.append(row.hospital_pk)
            else:
                rows_inserted += 1
            # progress bar
            j = (i + 1)/to_insert.shape[0]
            print("[%-20s] %d%%" % ('='*int(20*j), 100*j), end = '\r')

        # update rows
        for i in range(to_update.shape[0]):
            row = to_update.iloc[i, :]
            try:
                with cn.transaction():
                    cur.execute(
                        update_query,
                        {
                            'hospital_pk': row.hospital_pk,
                            'hospital_name': row.hospital_name,
                            'address': row.address,
                            'city': row.city,
                            'state': row.state,
                            'county': row.county,
                            'zip': int(row.zip),
                            'hospital_owner': row.hospital_owner,
                            'hospital_type': row.hospital_type,
                            'ems_provided': row.ems_provided
                        }
                    )
            except Exception as e:
                print(e)
                update_error_pks.append(row.hospital_pk)
            else:
                rows_updated += 1
            # progress bar
            j = (i + 1)/to_update.shape[0]
            print("[%-20s] %d%%" % ('='*int(20*j), 100*j), end = '\r')

    orig_to_load.merge(
        pd.DataFrame(
            {'hospital_pk': insert_error_pks}
        ),
        on='hospital_pk'
    ).to_csv(
        'cms_hospital_insert_errors.csv',
        index=False
    )

    orig_to_load.merge(
        pd.DataFrame(
            {'hospital_pk': update_error_pks}
        ),
        on='hospital_pk'
    ).to_csv(
        'cms_hospital_update_errors.csv',
        index=False
    )

    cn.commit()

    print(f'Inserted {rows_inserted} rows in the hospital table for CMS data.')
    print(f'Updated {rows_updated} rows in the hospitals table for CMS data.')


def load_cms_hospitals(cn, to_load):
    new_hd = to_load[[
            'hospital_pk',
            'hospital_name',
            'address',
            'city',
            'state',
            'county',
            'zip',
            'hospital_owner',
            'hospital_type',
            'ems_provided']]
    # preprocessing
    new_hd = nan_to_null(new_hd)

    # divide into insert / update subsets
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        existing_hospitals = pd.read_sql_query('SELECT * FROM hospitals;', cn)
    existing_hospitals['ems_provided'] = existing_hospitals.ems_provided.map({True: 'Yes', False: 'No'})
    join_keys = ['hospital_pk']
    to_insert = get_insert_rows(new_hd, existing_hospitals, join_keys)
    to_update = get_update_rows(new_hd, existing_hospitals, join_keys)

    # push the data to sql
    hospitals_to_sql(cn, to_insert, to_update, to_load)
