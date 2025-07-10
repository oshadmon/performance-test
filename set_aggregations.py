import argparse
import requests

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
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('conn', type=str, help='Comma-separated operator or publisher connections')
    parser.add_argument('--num-columns', type=int, default=1, help='Number of columns')
    parser.add_argument('--dbms', type=str, default='test', help='Database name')
    parser.add_argument('--table', type=str, default='rand_data', help='Table name')
    parser.add_argument('--interval', type=int, default=10)
    parser.add_argument('--time-frame', type=str, default='1 minute')
    parser.add_argument('--column-as-table', type=bool, nargs='?', const=True, default=False, help='convert columns into timestamp/value tables')
    args = parser.parse_args()

    for i in range(args.num_columns):
        column_name = f'column_{i+1}' if args.column_as_table is False else 'value'
        table_name = f'{args.table}_column_{i+1}' if args.column_as_table is True else args.table
        command = build_command(db_name=args.dbms, table_name=table_name, interval=args.interval,
                                time_frame=args.time_frame, time_column='timestamp', value_column=column_name)
        for conn in args.conn.split(','):
            post_command(conn=conn, command=command)


if __name__ == '__main__':
    main()