import numpy as np


def nan_to_null(df):
    return df.replace({np.nan: None})


def get_insert_rows(new, existing, join_keys):
    to_insert = new.merge(
        existing[join_keys],
        how='outer',
        on=join_keys,
        indicator=True
    ).query(
        "_merge == 'left_only'"
    ).drop(
        '_merge', axis=1
    )
    return to_insert


def get_update_rows(new, existing, join_keys):
    to_update = new.merge(
        existing.melt(
            id_vars=join_keys,
            value_name='old_val'
        ).merge(
            new.melt(
                id_vars=join_keys,
                value_name='new_val'
            ),
            on=join_keys.copy().append(['variable'])
        ).query(
            "old_val != new_val and not (old_val.isnull() and new_val.isnull())"
        )[join_keys].drop_duplicates(),
        on=join_keys
    )
    return to_update
