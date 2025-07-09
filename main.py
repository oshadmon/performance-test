import argparse
import datetime
import random
import re
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor



def __calculate_row_count(num_columns:int, size_str:(str or int))->(list, int):
    """
    Generate column names and generate row count based on number of columns if byte based
    :args:
        num_columns:int - number of column
        size_str:(str or int) - number of row (int) or quantity of data (str)
    :params:
        match - re match checker
        size_calc:dict - power params
        row_size_bytes:int - Estimated row size
        total_bytes:int - calculate bytes
        column_names:list - list of columns
        size_num, size_str - calculate the quantity of data
        total_bytes:float - size_str in bytes

    :return:
        column names and number of rows
    """
    column_names = [f'column_{i + 1}' for i in range(num_columns)]

    # If it's already a row count (int or numeric string)
    if isinstance(size_str, int) or (isinstance(size_str, str) and size_str.isdigit()):
        return column_names, int(size_str)

    # Try to parse size string like "10MB", "1.5GB", etc.
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([A-Za-z]+)", size_str.strip())
    if not match:
        raise ValueError("Invalid size string format. Use digits, or suffix with B/MB/GB/TB.")

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

    # Estimate row size in bytes: each numeric column = 8 bytes, timestamp = 8 bytes
    row_size_bytes = num_columns * 8 + 8
    total_bytes = size_num * size_multiplier[size_unit]

    return column_names, int(total_bytes // row_size_bytes)


def generate_row(columns: list) -> dict:
    return {
        'timestamp': datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        **{column: round(random.random() * random.randint(1, 999), random.randint(0, 2)) for column in columns}
    }

def insert_data(conns:list, dbms:str, table:str, payload:list):
    headers = {
        'type': 'json',
        'dbms': dbms,
        'table': table,
        'mode': 'streaming',
        'Content-Type': 'text/plain'
    }
    conn = random.choice(conns)
    try:
        response = requests.put(url=f'http:{conn}', headers=headers, data=json.dumps(payload))
        response.raise_for_status()
    except Exception as error:
        raise Exception(f'Failed to execute PUT against {conn} (Error: {error})')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--conn', required=True, type=str, help='Comma-separated operator or publisher connections')
    # parser.add_argument('--query-node', required=False, type=str, help='Query node IP:Port')
    parser.add_argument('--num-columns', type=int, default=1, help='Number of columns')
    parser.add_argument('--batch-size', type=int, default=10, help='Rows per insert')
    parser.add_argument('--size', type=str, default="10MB", help='Target table size (e.g. 10MB, 1GB)')
    parser.add_argument('--hz', type=int, default=0, help='inserts per second (if 0 then ignored)')
    parser.add_argument('--threads', type=int, default=5, help='Number of threads to use')
    parser.add_argument('--dbms', type=str, default='test', help='database name')
    parser.add_argument('--table', type=str, default='rand_data', help='table name')

    args = parser.parse_args()

    columns, row_count = __calculate_row_count(num_columns=args.num_columns, size_str=args.size)
    sleep_time = (args.batch_size / args.hz) if args.hz > 0 else 0
    print(f"🎯 Target row count: {row_count:,} rows | Columns: {len(columns)}")

    total_rows = 0
    t0 = time.time()
    while total_rows < row_count:
        rows = []
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            rows.append(generate_row(columns=columns))
            if len(rows) % args.batch_size == 0:
                # 🔄 Simulate batch insert (replace with PUT request)
                insert_data(conns=args.conn.split(','), dbms=args.dbms, table=args.table, payload=rows)
                print(f"📤 Inserted batch of {len(rows)} rows...")
                total_rows += len(rows)
                rows = []
                elapsed_time = time.time() - start_time
                if sleep_time > elapsed_time:
                    time.sleep(sleep_time - elapsed_time)

    # Insert remaining rows
    if rows:
        print(f"📤 Inserted final batch of {len(rows)} rows.")
        total_rows += len(rows)

    print(f"✅ Done. Total inserted: {total_rows:,} rows | Total Time: {round(time.time() - t0, 2)}")


if __name__ == "__main__":
    main()
