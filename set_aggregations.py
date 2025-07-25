import argparse
import requests



def get_tables(conn:str, db_name:str)->dict:
    """
    get list of tables getting data
    :args:
        conn:str - REST connection
        db_name:str - logical database name
    :params:
        tables:dict - list of tables
    :return:
        tables
    """
    tables = {}

    headers = {
        'command': 'get streaming where format=json',
        "User-Agent": "AnyLog/1.23"
    }

    try:
        response = requests.get(url=f"http://{conn}", headers=headers)
        response.raise_for_status()
    except Exception as error:
        raise error
    for row in response.json():
        if 'table' in row and db_name in row['table']:
            tables[row['table'].split('.')[-1]] = {}
    return tables


def get_columns(conn:str, db_name:str, tables:dict):
    timestamp_column = 'insert_timestamp'
    command = f'get columns where dbms={db_name} and table=%s and format=json'
    headers = {
        'command': None,
        'User-Agent': 'AnyLog/1.23'
    }

    for table in tables:
        headers['command'] = command % table
        try:
            response = requests.get(url=f"http://{conn}", headers=headers)
            response.raise_for_status()
        except Exception as error:
            raise error
        data = response.json()

        for column in data:
            if column not in ['row_id', 'insert_timestamp', 'tsd_name', 'tsd_id']:
                if data[column].strip().split(' ', 1)[0] == 'timestamp':
                    timestamp_column = column
                elif data[column] in ['numeric', 'double', 'decimal', 'integer', 'float']:
                    tables[table][column] = data[column]
        tables[table][timestamp_column] = 'timestamp'

    return tables


def build_command(db_name:str, table_name:str, interval:int, time_frame:str, time_column:str, value_column:str):
    command = f"""set aggregations where 
        dbms={db_name} and 
        table={table_name} and 
        intervals={interval} and 
        time={time_frame}  and
        time_column={time_column} and
        value_column={value_column}""".replace("\n"," ")
    while "  " in command:
        command = command.replace("  ", " ")

    return command

def post_command(conn:str, command:str):
    try:
        response = requests.post(url=f'http://{conn}', headers={'command': command, 'User-Agent': 'AnyLog/1.23'})
        response.raise_for_status()
    except Exception as error:
        raise requests.exceptions.RequestException(f'Failed to execute GET against {conn} (Error: {error})')

def main():
    """
    Set aggregations based on logical database name
    :args:

    :return:
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('conn', type=str, help='Comma-separated operator or publisher connections')
    parser.add_argument('dbms', type=str, default='test', help='Database name')
    parser.add_argument('--table', type=str, default=None, help='Table name')
    parser.add_argument('--interval', type=int, default=10)
    parser.add_argument('--time-frame', type=str, default='1 minute')
    parser.add_argument('--column-as-table', type=bool, nargs='?', const=True, default=False, help='convert columns into timestamp/value tables')
    args = parser.parse_args()

    tables =  get_tables(conn=args.conn, db_name=args.dbms)
    if not args.table:
        tables = get_columns(conn=args.conn, db_name=args.dbms, tables=tables)

    for table in tables:
        timestamp_column = next((k for k, v in tables[table].items() if v == 'timestamp'), None)
        for column in tables[table]:
            if column != timestamp_column:
                command = build_command(db_name=args.dbms, table_name=table, interval=args.interval,
                                        time_frame=args.time_frame, time_column=timestamp_column, value_column=column)
                print(command)
                post_command(conn=args.conn, command=command)

    # for i in range(args.num_columns):
    #     column_name = f'column_{i+1}' if args.column_as_table is False else 'value'
    #     table_name = f'{args.table}_column_{i+1}' if args.column_as_table is True else args.table
    #
    #     for conn in args.conn.split(','):
    #


if __name__ == '__main__':
    main()