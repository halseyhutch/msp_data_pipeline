import pandas as pd
import warnings
from re import sub
from misc_helpers import nan_to_null, get_insert_rows, get_update_rows, \
    progress_bar


def geocode_to_lat_long(df):
    df['long'] = [float(sub(r'POINT \((.*) .*', r'\g<1>', str(x)))
                  for x in df['geocoded_hospital_address']]
    df['lat'] = [float(sub(r'POINT \(.* (.*)\)', r'\g<1>', str(x)))
                 for x in df['geocoded_hospital_address']]
    df.drop('geocoded_hospital_address', axis=1, inplace=True)


def lat_long_to_geocode(df):
    df['geocoded_hospital_address'] = 'POINT (' + \
        df['long'].astype(str) + ' ' + df['lat'].astype(str) + ')'
    df.loc[df['long'].isnull(), 'geocoded_hospital_address'] = 'NA'
    df.drop(['lat', 'long'], axis=1, inplace=True)


def hospitals_to_sql(cn, to_insert, to_update, orig_to_load):

    cur = cn.cursor()

    rows_inserted = 0
    rows_updated = 0
    insert_error_pks = []
    update_error_pks = []

    with open('sql/insert_hospitals_hhs.sql') as f:
        insert_query = f.read()

    with open('sql/update_hospitals_hhs.sql') as f:
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
                            'zip': int(row.zip),
                            'fips_code': row.fips_code,
                            'lat': row.lat,
                            'long': row.long
                        }
                    )
            except Exception as e:
                print(e)
                insert_error_pks.append(row.hospital_pk)
            else:
                rows_inserted += 1
            progress_bar(i, to_insert.shape[0], 'Inserting HHS hospitals...')


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
                            'zip': int(row.zip),
                            'fips_code': row.fips_code,
                            'lat': row.lat,
                            'long': row.long
                        }
                    )
            except Exception as e:
                print(e)
                update_error_pks.append(row.hospital_pk)
            else:
                rows_updated += 1
            progress_bar(i, to_update.shape[0], 'Updating HHS hospitals...')


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

    print(f'Inserted {rows_inserted} rows in the hospitals table.'+' '*20)
    print(f'Updated {rows_updated} rows in the hospitals table.'+' '*20)


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
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        existing_hd = pd.read_sql_query('SELECT * FROM hospitals;', cn)
    join_keys = ['hospital_pk']
    to_insert = get_insert_rows(new_hd, nan_to_null(existing_hd), join_keys)
    to_update = get_update_rows(new_hd, nan_to_null(existing_hd), join_keys)

    # push the data to sql
    hospitals_to_sql(cn, to_insert, to_update, to_load)
