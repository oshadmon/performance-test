import argparse
import datetime
import random
import re
import time
import json
import urllib3
# import math

# from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Union, List, Tuple
from collections   import defaultdict

CONNS = {}

def __calculate_row_count(num_columns: int, size_str: Union[str, int]) -> Tuple[List[str], int]:
    """

    :param num_columns:
    :param size_str:
    :return:
    """
    column_names = [f'column_{i + 1}' for i in range(num_columns)]

    if isinstance(size_str, int) or (isinstance(size_str, str) and size_str.isdigit()):
        return column_names, int(size_str)

    match = re.fullmatch(r"(\d+(?:\.\d+)?)([A-Za-z]+)", size_str.strip())
    if not match:
        raise ValueError("Invalid size format. Use digits or suffix with B/KB/MB/GB/TB.")

    size_num = float(match.group(1))
    size_unit = match.group(2).upper()

    size_multiplier = {
        "B": 1,
        "KB": 1024,
        "MB": 1024 ** 2,
        "GB": 1024 ** 3,
        "TB": 1024 ** 4
    }

    if size_unit not in size_multiplier:
        raise ValueError(f"Unsupported unit: {size_unit}. Use B, KB, MB, GB, or TB.")

    row_size_bytes = num_columns * 8 + 8
    total_bytes = size_num * size_multiplier[size_unit]

    return column_names, int(total_bytes // row_size_bytes)


def generate_row(columns: list) -> dict:
    return {
        'timestamp': datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        **{column: round(random.random() * random.randint(1, 999), random.randint(0, 2)) for column in columns}
    }


def insert_data(dbms: str, table: str, payload:(list or dict)):
    global CONNS
    headers = {
        'type': 'json',
        'dbms': dbms,
        'table': table,
        'mode': 'streaming',
        'Content-Type': 'text/plain'
    }
    conn = None
    while conn is None:
        conn = random.choice(list(CONNS.keys()))
        if CONNS[conn] is False:
            CONNS[conn] = True
        else:
            conn = None
    try:
        if isinstance(payload, dict):
            for table in payload:
                headers['table'] = table
                response = urllib3.request(method='PUT', url=f'http://{conn}', headers=headers, body=json.dumps(payload[table]))
        else:
            response = urllib3.request(method='PUT', url=f'http://{conn}', headers=headers, body=json.dumps(payload))
    except Exception as error:
        raise Exception(f'❌ Failed insert to {conn}: {error}')
    else:
        if 200 <= response.status < 300:
            CONNS[conn] = False
        else:
            raise urllib3.exceptions.ConnectionError(response.status)


def main():
    global CONNS
    parser = argparse.ArgumentParser()
    parser.add_argument('--conn', required=True, type=str, help='Comma-separated operator or publisher connections')
    parser.add_argument('--num-columns', type=int, default=1, help='Number of columns')
    parser.add_argument('--batch-size', type=int, default=10, help='Rows per insert (0 = auto)')
    parser.add_argument('--size', type=str, default="10MB", help='Target table size (e.g. 10MB, 1GB) (0 = ignore)')
    parser.add_argument('--hz', type=int, default=0, help='Inserts per second (0 = no rate limit)')
    # parser.add_argument('--threads', type=int, default=1, help='Number of threads to run in parallel')
    parser.add_argument('--run-time', type=float, default=0, help='Run for X seconds (0 = ignore)')
    parser.add_argument('--dbms', type=str, default='test', help='Database name')
    parser.add_argument('--table', type=str, default='rand_data', help='Table name')
    parser.add_argument('--column-as-table', type=bool, nargs='?', const=True, default=False, help='convert columns into timestamp/value tables')
    args = parser.parse_args()

    for conn in args.conn.split(','):
        CONNS[conn] = False

    columns, total_rows = (
        __calculate_row_count(args.num_columns, args.size)
        if args.size != "0"
        else ([f'column_{i+1}' for i in range(args.num_columns)], float('inf'))
    )

    # args.batch_size = 1000 if args.batch_size < 1 else args.batch_size
    sleep_time = args.batch_size / args.hz if args.hz > 0 else 0

    print(f"🎯 Target row count: {total_rows if total_rows != float('inf') else '∞'} | Batch size: {args.batch_size:,} |  Sleep: {sleep_time:.2f}s")

    start_time = time.time()
    total_inserted = 0


    while True:
        if 0 < args.run_time <= time.time() - start_time:
            break

        if args.run_time == 0 and total_inserted >= total_rows:
            break

        batch_size = min(args.batch_size, total_rows - total_inserted) if total_rows != float('inf') else args.batch_size
        batch = [generate_row(columns) for _ in range(batch_size)]
        if args.column_as_table:
            data = defaultdict(list)
            for row in batch:
                timestamp = row['timestamp']
                for key, value in row.items():
                    if key == 'timestamp':
                        continue
                    data[f"{args.table}_{key}"].append({'timestamp': timestamp, 'value': value})
            batch = data

        # Submit insert job to executor
        insert_data(args.dbms, args.table, batch)

        total_inserted += batch_size

        if sleep_time:
            time.sleep(sleep_time)

    elapsed = round(time.time() - start_time, 2)
    print(f"✅ Done. Inserted {total_inserted:,} rows in {elapsed} seconds")


if __name__ == "__main__":
    main()
