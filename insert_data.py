import argparse
import datetime
import random
import re
import time
import json
import urllib3

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Union, List, Tuple
from collections import defaultdict

CONNS = []
TOTAL_ROWS = 0
http = urllib3.PoolManager()


def __seconds_to_hhmmss_f(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:04.1f}"


def __calculate_row_count(num_columns: int, size_str: Union[str, int]) -> Tuple[List[str], int]:
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


def insert_data(dbms: str, table: str, payload: Union[list, dict]):
    global CONNS
    global TOTAL_ROWS

    headers_base = {
        'type': 'json',
        'dbms': dbms,
        'mode': 'streaming',
        'Content-Type': 'text/plain'
    }

    def get_conn():
        candidate = random.choice(CONNS)
        return candidate

    def release_conn(conn):
        CONNS[conn] = False

    def send_request(conn, table_name, data):
        global  TOTAL_ROWS
        headers = headers_base.copy()
        headers['table'] = table_name
        url = f'http://{conn}'

        try:
            response = http.request(
                method='PUT',
                url=url,
                headers=headers,
                body=json.dumps(data)
            )

            if 200 <= response.status < 300:
                TOTAL_ROWS += len(data) if isinstance(data, list) else 1

        except Exception as e:
            pass
            # print(f"❌ Failed attempt {attempt} to insert into {table_name} at {conn}: {e}")
            # time.sleep(min(10, 2 * attempt))
            # attempt += 1

        # If we exit loop, all retries failed
        # raise Exception(f"Max retries exceeded for table {table_name} on {conn}")

    if isinstance(payload, dict):
        with ThreadPoolExecutor(max_workers=len(payload)) as executor:
            futures = []
            for tbl, rows in payload.items():
                conn = get_conn()
                future = executor.submit(send_request, conn, tbl, rows)
                futures.append((future, conn, tbl))
            for future, conn, tbl in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Insert failed for table {tbl} on {conn}: {e}")
                finally:
                    release_conn(conn)
    else:
        conn = get_conn()
        try:
            send_request(conn, table, payload)
        except Exception as e:
            print(f"❌ Insert failed for table {table} on {conn}: {e}")
        finally:
            release_conn(conn)



def main():
    global CONNS
    global TOTAL_ROWS
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
            CONNS.append(conn)

    if args.column_as_table is True and ((args.num_columns <= 10 and args.max_threads <= 1) or args.max_threads == 0):
        threads = args.num_columns
    else:
        threads = args.max_threads if len(CONNS) > args.max_threads > 0 else len(CONNS)

    columns, total_rows = (
        __calculate_row_count(args.num_columns, args.size)
        if args.size != "0"
        else ([f'column_{i + 1}' for i in range(args.num_columns)], float('inf'))
    )

    output = f"🎯 Target - Batch size: {args.batch_size:,} | Number of Columns: {args.num_columns:,} | Number of Threads: {threads}"
    if args.run_time > 0:
        output += f" | Expected Run Time: {__seconds_to_hhmmss_f(args.run_time)}"
    elif args.size:
        output += f" | Expected Size: {args.size} | Expected Row Count: {total_rows:,}"
    output += f" | Convert Columns to Tables: {str(args.column_as_table).capitalize()}"
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

            if args.column_as_table:
                data = defaultdict(list)
                for row in batch:
                    timestamp = row['timestamp']
                    for key, value in row.items():
                        if key == 'timestamp':
                            continue
                        data[f"{args.table}_{key}"].append({'timestamp': timestamp, 'value': value})
                future = executor.submit(insert_data, args.dbms, args.table, data)
            else:
                future = executor.submit(insert_data, args.dbms, args.table, batch)

            futures.append(future)
            total_inserted += batch_size

            for future in as_completed(futures):
                try:
                    future.result()
                except TypeError:
                    pass
                except Exception as e:
                    print(f"❌ Thread insert failed: {e}")

            if args.hz > 0 and (time.time() - iteration_start) < 1:
                time.sleep(1 - (time.time() - iteration_start))

    elapsed = round(time.time() - start_time, 2)
    print(f"✅ Done. Inserted {TOTAL_ROWS:,} rows in {__seconds_to_hhmmss_f(elapsed)}")


if __name__ == "__main__":
    main()
