 <set aggregations where
        dbms = nov and
        table=rand_data and
        intervals = 10 and
        time = 1 minute and
        time_column = timestamp and
        value_column = column_10>



<set aggregations  where
    dbms = nov and
    table = column_1 and
    intervals = 10 and
    time = 1 minute and
    time_column = timestamp and
    value_column = value and
    target_dbms = nov and
    target_table = rand_data_column_1a>