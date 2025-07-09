 <set aggregations where
        dbms = nov and
        table=rand_data and
        intervals = 30 and
        time = 1 minute and
        time_column = timestamp and
        value_column = column_1
>

<set aggregations  where
    dbms = nov and
    table = rand_data and
    intervals = 10 and
    time = 1 minute and
    time_column = timestamp and
    value_column = column_1 and
    target_dbms = nov and
    target_table = rand_data_column_1>