# load_cms_quality.py

import warnings
import pandas as pd
from misc_helpers import nan_to_null, get_insert_rows, get_update_rows, \
    progress_bar


def quality_to_sql(cn, to_insert, to_update, orig_to_load):

    cur = cn.cursor()

    rows_inserted = 0
    rows_updated = 0
    insert_error_pks = []
    update_error_pks = []

    with open('sql/insert_quality_cms.sql') as f:
        insert_query = f.read()

    with open('sql/update_quality_cms.sql') as f:
        update_query = f.read()

    with cn.transaction():

        # Insert rows
        for i in range(to_insert.shape[0]):
            row = to_insert.iloc[i, :]
            try:
                with cn.transaction():
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
            progress_bar(i, to_insert.shape[0], 'Inserting hospital_quality...')


        # Update rows
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
            progress_bar(i, to_update.shape[0], 'Updating hospital_quality...')

    orig_to_load.merge(
        pd.DataFrame(
            {'hospital_pk': insert_error_pks}
        ),
        on='hospital_pk'
    ).to_csv(
        'cms_quality_insert_errors.csv',
        index=False
    )

    orig_to_load.merge(
        pd.DataFrame(
            {'hospital_pk': update_error_pks}
        ),
        on='hospital_pk'
    ).to_csv(
        'cms_quality_update_errors.csv',
        index=False
    )

    cn.commit()

    print(f'Inserted {rows_inserted} rows in the hospital_quality table.'+' '*20)
    print(f'Updated {rows_updated} rows in the hospital_quality table.'+' '*20)


# quality data = hd
def load_cms_quality(cn, to_load):

    new_qd = to_load.filter(items=[
        'hospital_pk',
        'record_date',
        'quality_rating'
    ]).query(
        'not quality_rating.isnull()'
    )

    # Preprocessing
    new_qd = nan_to_null(new_qd)

    # Divide into insert / update subsets
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        existing_qd = pd.read_sql_query(
            'SELECT * FROM hospital_quality;', cn)
    existing_qd['record_date'] = existing_qd['record_date'].astype(str)
    join_keys = ['hospital_pk', 'record_date']
    to_insert = get_insert_rows(new_qd, existing_qd, join_keys)
    to_update = get_update_rows(new_qd, existing_qd, join_keys)

    # Push the data to sql
    quality_to_sql(cn, to_insert, to_update, to_load)
