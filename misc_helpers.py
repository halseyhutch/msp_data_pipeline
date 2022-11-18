import numpy as np


def nan_to_null(df):
    """helpful function that turn nan into None in dataframe."""
    return df.replace({np.nan: None})


def progress_bar(i, n, label):
    """helper function that shows the percentage completed as progress bar."""
    j = (i + 1)/n
    print("%s  [%-20s] %d%%       " % (label, '='*int(20*j), 100*j), end='\r')


def get_insert_rows(new, existing, join_keys):
    """decide data that showed first time that needed to be inserted,
        new is new data that needed to be checked,
        existing is data originally in the table,
        join_keys are the primary keys for both for them to join tables."""
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
    """decide data that already showed before and needed to be updated,
        new is new data that needed to be checked,
        existing is data originally in the table,
        join_keys are the primary keys for both for them to join tables."""
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
