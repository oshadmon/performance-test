import argparse
import datetime
import random
import re
import time
import json
import urllib3
import math

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Union, List, Tuple
from collections   import defaultdict

CONNS = {}
http = urllib3.PoolManager()


def __seconds_to_hhmmss_f(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:04.1f}"


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

def insert_data(dbms: str, table: str, payload: (list or dict)):
    global CONNS
    headers_base = {
        'type': 'json',
        'dbms': dbms,
        'mode': 'streaming',
        'Content-Type': 'text/plain'
    }

    def get_conn():
        conn = None
        while conn is None:
            candidate = random.choice(list(CONNS.keys()))
            if not CONNS[candidate]:
                CONNS[candidate] = True
                conn = candidate
        return conn

    def release_conn(conn):
        CONNS[conn] = False

    def send_request(conn, table_name, data, retries=3):
        headers = headers_base.copy()
        headers['table'] = table_name
        url = f'http://{conn}'
        for attempt in range(1, retries + 1):
            try:
                response = http.request(
                    method='PUT',
                    url=url,
                    headers=headers,
                    body=json.dumps(data)
                )

                if 500 <= response.status < 600:
                    # print(f"⚠️ Server error (500+) on attempt {attempt} to {url}")
                    time.sleep(2 * attempt)
                    continue  # Retry
                if not (200 <= response.status < 300):
                    raise Exception(f"HTTP {response.status}: {response.data.decode('utf-8')}")

                return  # Success, exit the loop

            except Exception as e:
                if attempt == retries:
                    preview = json.dumps(data[:1], indent=2) if isinstance(data, list) else str(data)[:500]
                    raise Exception(
                        f"❌ Final failure inserting to {conn} (table: {table_name}) after {retries} attempts. Error: {e}\nPayload (truncated): {preview}")
                time.sleep(2 * attempt)

        raise Exception(f"❌ Unreachable: failed all {retries} attempts to {conn}")

    conn = get_conn()
    try:
        if isinstance(payload, dict):
            for tbl, rows in payload.items():
                send_request(conn, tbl, rows)
        else:
            send_request(conn, table, payload)
    finally:
        release_conn(conn)

    if isinstance(payload, dict):
        # Send all tables in parallel, each on its own connection
        with ThreadPoolExecutor(max_workers=len(payload)) as executor:
            futures = []
            for tbl, data in payload.items():
                conn = get_conn()
                futures.append(executor.submit(send_request, conn, tbl, data))
            for future in as_completed(futures):
                # Will raise exceptions here if any request failed
                future.result()
    else:
        # Single payload, single connection
        conn = get_conn()
        headers = headers_base.copy()
        headers['table'] = table
        try:
            response = http.request(
                method='PUT',
                url=f'http://{conn}',
                headers=headers,
                body=json.dumps(payload)
            )
            if not (200 <= response.status < 300):
                raise urllib3.exceptions.ConnectionError(f"Status: {response.status}")
        except Exception as e:
            raise Exception(f"❌ Failed insert to {conn}: {e}")
        finally:
            release_conn(conn)


def main():
    """
    The following is a tool to insert data (via PUT) in order to test performance for AnyLog/EdgeLake

    The application allows for insertion to run either based on time interval or quantity of data.

    Users may also choose the number of columns (not including timestamp) and whether to store the columns in a single table
    or each column in its own table.
    :positional arguments:
        conn                  Comma-separated operator or publisher connections
    :options:
        -h, --help                              show this help message and exit
        --num-columns       NUM_COLUMNS         Number of columns (default: 1)
        --batch-size        BATCH_SIZE          Rows per insert (0 = auto) (default: 10)
        --size              SIZE                Target table size (e.g. 10MB, 1GB) (0 = ignore) (default: 10MB)
        --hz                HZ                  Inserts per column per second (0 = no rate limit) (default: 0)
        --max-threads       MAX_THREADS         Max number of threads to run in parallel (0 = ignore) (default: 0)
        --run-time          RUN_TIME            Run for X seconds (0 = ignore) (default: 0)
        --dbms              DBMS                Database name (default: test)
        --table             TABLE               Table name (default: rand_data)
        --column-as-table   [COLUMN_AS_TABLE]   convert columns into timestamp/value tables (default: False)
    :global:
        CONNs:dict - Connectors to AnyLog nodes.
    :return:
    """
    global CONNS
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('conn', type=str, help='Comma-separated operator or publisher connections')
    parser.add_argument('--num-columns', type=int, default=1, help='Number of columns')
    parser.add_argument('--batch-size', type=int, default=10, help='Rows per insert (0 = auto)')
    parser.add_argument('--size', type=str, default="10MB", help='Target table size (e.g. 10MB, 1GB) (0 = ignore)')
    parser.add_argument('--hz', type=int, default=0, help='Inserts per column per second (0 = no rate limit)')
    parser.add_argument('--max-threads', type=int, default=0, help='Max number of threads to run in parallel  (0 = ignore)')
    parser.add_argument('--run-time', type=float, default=0, help='Run for X seconds (0 = ignore)')
    parser.add_argument('--dbms', type=str, default='test', help='Database name')
    parser.add_argument('--table', type=str, default='rand_data', help='Table name')
    parser.add_argument('--column-as-table', type=bool, nargs='?', const=True, default=False, help='convert columns into timestamp/value tables')
    args = parser.parse_args()

    for conn in args.conn.split(','):
        if conn not in CONNS:
            CONNS[conn] = False

    threads = args.max_threads if len(list(CONNS.keys())) > args.max_threads > 0 else len(list(CONNS.keys()))

    columns, total_rows = (
        __calculate_row_count(args.num_columns, args.size)
        if args.size != "0"
        else ([f'column_{i+1}' for i in range(args.num_columns)], float('inf'))
    )

    # args.batch_size = 1000 if args.batch_size < 1 else args.batch_size
    # if args.hz > 0:
    #     args.batch_size = math.ceil(args.hz * args.num_columns)
    # elif args.batch_size < 1:
    #     args.batch_size = math.ceil(total_rows / threads)
    #

    output = f"🎯 Target - Batch size: {args.batch_size:,} | Number of Columns: {args.num_columns:,} | Number of Threads: {threads}"
    if args.run_time > 0:
        output += f" | Expected Run Time: {__seconds_to_hhmmss_f(args.run_time)}"
    elif args.size:
        output += f" | Expected Size: {args.size} | Expected Row Count: {total_rows:,}"
    print(output)

    start_time = time.time()
    total_inserted = 0

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = []

        while True:
            iteration_start = time.time()
            if (0 < args.run_time <= time.time() - start_time) or (args.run_time == 0 and total_inserted >= total_rows):
                break

            batch_size = min(args.batch_size, total_rows - total_inserted) if total_rows != float('inf') else args.batch_size
            batch = [generate_row(columns) for _ in range(batch_size)]
            if args.column_as_table is True:
                data = defaultdict(list)
                for row in batch:
                    timestamp = row['timestamp']
                    for key, value in row.items():
                        if key == 'timestamp':
                            continue
                        data[f"{args.table}_{key}"].append({'timestamp': timestamp, 'value': value})
                future = executor.submit(insert_data, args.dbms, args.table, data)
                futures.append(future)
                total_inserted += batch_size
            else:
                future = executor.submit(insert_data, args.dbms, args.table, batch)
                futures.append(future)
                total_inserted += batch_size
            # print(f"{total_inserted:,}")

            # Wait for all tasks to complete
            for future in as_completed(futures):
                future.result()
                # try:
                #     future.result()
                # except Exception as e:
                #     print(f"❌ Thread error: {e}")

            if args.hz > 0 and (time.time() - iteration_start) < 1:
                time.sleep(1-(time.time() - iteration_start))

    elapsed = round(time.time() - start_time, 2)
    print(f"✅ Done. Inserted {total_inserted:,} rows in {elapsed} seconds")



if __name__ == "__main__":
    main()
