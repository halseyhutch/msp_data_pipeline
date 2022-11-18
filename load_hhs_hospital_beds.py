import pandas as pd
import warnings
from misc_helpers import nan_to_null, get_insert_rows, get_update_rows, \
    progress_bar


def hospital_beds_to_sql(cn, to_insert, to_update, orig_to_load):

    cur = cn.cursor()

    rows_inserted = 0
    rows_updated = 0
    # ASSUMPTION: each file has only one date.
    # (this is an assert in the main script)
    insert_error_pks = []
    update_error_pks = []

    with open('sql/insert_hospital_beds.sql') as f:
        insert_query = f.read()

    with open('sql/update_hospital_beds.sql') as f:
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
                            'collection_week': row.collection_week,
                            'all_adult_hospital_beds_7_day_avg':
                                row.all_adult_hospital_beds_7_day_avg,
                            'all_pediatric_inpatient_beds_7_day_avg':
                                row.all_pediatric_inpatient_beds_7_day_avg,
                            'all_adult_hospital_inpatient_bed_occupied_7_day_coverage':
                                row.all_adult_hospital_inpatient_bed_occupied_7_day_coverage,
                            'all_pediatric_inpatient_bed_occupied_7_day_avg':
                            row.all_pediatric_inpatient_bed_occupied_7_day_avg,
                            'total_icu_beds_7_day_avg':
                                row.total_icu_beds_7_day_avg,
                            'icu_beds_used_7_day_avg':
                                row.icu_beds_used_7_day_avg,
                            'inpatient_beds_used_covid_7_day_avg':
                                row.inpatient_beds_used_covid_7_day_avg,
                            'staffed_icu_adult_patients_confirmed_covid_7_day_avg':
                                row.staffed_icu_adult_patients_confirmed_covid_7_day_avg
                        }
                    )
            except Exception as e:
                print(e)
                insert_error_pks.append(row.hospital_pk)
            else:
                rows_inserted += 1
            progress_bar(i, to_insert.shape[0], 'Inserting hospital_beds...')

        # update rows
        for i in range(to_update.shape[0]):
            row = to_update.iloc[i, :]
            try:
                with cn.transaction():
                    cur.execute(
                        update_query,
                        {
                            'hospital_pk': row.hospital_pk,
                            'collection_week': row.collection_week,
                            'all_adult_hospital_beds_7_day_avg':
                                row.all_adult_hospital_beds_7_day_avg,
                            'all_pediatric_inpatient_beds_7_day_avg':
                                row.all_pediatric_inpatient_beds_7_day_avg,
                            'all_adult_hospital_inpatient_bed_occupied_7_day_coverage':
                                row.all_adult_hospital_inpatient_bed_occupied_7_day_coverage,
                            'all_pediatric_inpatient_bed_occupied_7_day_avg':
                                row.all_pediatric_inpatient_bed_occupied_7_day_avg,
                            'total_icu_beds_7_day_avg':
                                row.total_icu_beds_7_day_avg,
                            'icu_beds_used_7_day_avg':
                                row.icu_beds_used_7_day_avg,
                            'inpatient_beds_used_covid_7_day_avg':
                                row.inpatient_beds_used_covid_7_day_avg,
                            'staffed_icu_adult_patients_confirmed_covid_7_day_avg':
                                row.staffed_icu_adult_patients_confirmed_covid_7_day_avg
                        }
                    )
            except Exception as e:
                print(e)
                update_error_pks.append(row.hospital_pk)
            else:
                rows_updated += 1
            progress_bar(i, to_update.shape[0], 'Updating hospital_beds...')


    orig_to_load.merge(
        pd.DataFrame(
            {'hospital_pk': insert_error_pks}
        ),
        on='hospital_pk'
    ).to_csv(
        'hospital_beds_insert_errors.csv',
        index=False
    )

    orig_to_load.merge(
        pd.DataFrame(
            {'hospital_pk': update_error_pks}
        ),
        on='hospital_pk'
    ).to_csv(
        'hospital_beds_update_errors.csv',
        index=False
    )

    cn.commit()
    print(f'Inserted {rows_inserted} rows in the hospital beds table.'+' '*20)
    print(f'Updated {rows_updated} rows in the hospital beds table.'+' '*20)


# abbreviate hospital beds as hb
def load_hhs_hospital_beds(cn, to_load):

    new_hb = to_load.filter(items=[
        'hospital_pk',
        'collection_week',
        'all_adult_hospital_beds_7_day_avg',
        'all_pediatric_inpatient_beds_7_day_avg',
        'all_adult_hospital_inpatient_bed_occupied_7_day_coverage',
        'all_pediatric_inpatient_bed_occupied_7_day_avg',
        'total_icu_beds_7_day_avg',
        'icu_beds_used_7_day_avg',
        'inpatient_beds_used_covid_7_day_avg',
        'staffed_icu_adult_patients_confirmed_covid_7_day_avg'
    ])

    # preprocessing
    new_hb = nan_to_null(new_hb)

    # divide into insert / update subsets
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        existing_hb = pd.read_sql_query(
            'SELECT * FROM hospital_beds;',
            cn,
            parse_dates=['collection_week']
        )
    join_keys = ['hospital_pk', 'collection_week']
    to_insert = get_insert_rows(new_hb, nan_to_null(existing_hb), join_keys)
    to_update = get_update_rows(new_hb, nan_to_null(existing_hb), join_keys)

    # push the data to sql
    hospital_beds_to_sql(cn, to_insert, to_update, to_load)
