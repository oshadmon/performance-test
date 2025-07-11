import argparse
import datetime
import random
import re
import time
import json
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Union, List, Tuple

CONNS = []
TOTAL_ROWS = 0
http = urllib3.PoolManager()


def seconds_to_hhmmss(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:04.1f}"


def calculate_row_count(num_columns: int, size_str: Union[str, int]) -> Tuple[List[str], int]:
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


def get_conn():
    return random.choice(CONNS)


def release_conn(conn):
    pass  # No-op


def send_request(conn, dbms, table, data):
    global TOTAL_ROWS
    headers = {
        'type': 'json',
        'dbms': dbms,
        'mode': 'streaming',
        'Content-Type': 'text/plain',
        'table': table
    }
    url = f'http://{conn}'
    try:
        response = http.request(
            method='PUT',
            url=url,
            headers=headers,
            body=json.dumps(data)
        )
        if 200 <= response.status < 300:
            TOTAL_ROWS += len(data)
    except Exception:
        pass


def insert_data(dbms: str, table: str, payload: list):
    conn = get_conn()
    try:
        send_request(conn, dbms, table, payload)
    finally:
        release_conn(conn)


def worker(stop_time: float, total_rows: int, rps: float, columns: List[str], dbms: str, table: str):
    interval = 1.0  # seconds
    rows_per_interval = int(rps * interval)

    while time.time() < stop_time and TOTAL_ROWS < total_rows:
        start = time.time()

        batch = [generate_row(columns) for _ in range(rows_per_interval)]
        insert_data(dbms, table, batch)

        elapsed = time.time() - start
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)


def main():
    global CONNS
    global TOTAL_ROWS

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('conn', type=str, help='Comma-separated connections (host:port)')
    parser.add_argument('--num-columns', type=int, default=10, help='Number of columns')
    parser.add_argument('--hz', type=int, default=0, help='Target Hz (data points per column per second)')
    parser.add_argument('--size', type=str, default="100MB", help='Target table size (e.g. 10MB, 1GB)')
    parser.add_argument('--run-time', type=float, default=0, help='Run for X seconds (overrides size)')
    parser.add_argument('--max-threads', type=int, default=8, help='Max number of threads')
    parser.add_argument('--dbms', type=str, default='test', help='Database name')
    parser.add_argument('--table', type=str, default='rand_data', help='Table name')
    args = parser.parse_args()

    CONNS = args.conn.split(',')
    threads = min(32, len(CONNS) * 2)
    columns, total_rows = (
        calculate_row_count(args.num_columns, args.size)
        if args.run_time == 0 else ([f'column_{i + 1}' for i in range(args.num_columns)], float('inf'))
    )

    rps = args.hz * args.num_columns if args.hz > 0 else float('inf')
    if rps == float('inf'):
        print("⚠️  --hz not set or 0: sending as fast as possible\n")

    print(f"\n🚀 Starting performance test with {threads} threads")
    print(f"📈 Target Hz per column: {args.hz} | ⚡ Rows/sec: {'∞' if rps == float('inf') else int(rps)}")
    print(f"🧱 Columns: {args.num_columns}")
    print(f"🎯 Target: {args.size if args.run_time == 0 else f'{args.run_time}s duration'}\n")

    start_time = time.time()
    stop_time = start_time + args.run_time if args.run_time > 0 else float('inf')

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [
            executor.submit(worker, stop_time, total_rows, rps / threads, columns, args.dbms, args.table)
            for _ in range(threads)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"❌ Thread error: {e}")

    elapsed = round(time.time() - start_time, 2)
    print(f"\n✅ Done. Inserted {TOTAL_ROWS:,} rows in {seconds_to_hhmmss(elapsed)}")
    print(f"⚡ Throughput: {int(TOTAL_ROWS / elapsed):,} rows/sec\n")


if __name__ == "__main__":
    main()
