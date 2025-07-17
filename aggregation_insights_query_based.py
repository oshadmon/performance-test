import datetime
import json
import matplotlib.pyplot as plt
import requests
from tabulate import tabulate
import matplotlib.ticker as ticker


def get_data(conn: str, db_name: str, table_name: str):
    headers = {
        'command': f'sql {db_name} format=json:list and stat=false "select increments(minute, 1, timestamp), min(timestamp) as min_ts, max(timestamp) as max_ts, count(*) as row_count from {table_name};"',
        'User-Agent': 'AnyLog/1.23',
        "destination": "network"
    }

    try:
        response = requests.get(url=f"http://{conn}", headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as error:
        raise requests.exceptions.RequestException(f'Failed to GET from {conn} (Error: {error})')


def extract_data(raw_data: list, db_name: str, table_name: str):
    data = []
    interval = 0

    for row in raw_data:
        interval += 1
        time_diff = datetime.datetime.strptime(row['max_ts'], '%Y-%m-%d %H:%M:%S.%f') - datetime.datetime.strptime(
            row['min_ts'], '%Y-%m-%d %H:%M:%S.%f')
        seconds = float(time_diff.total_seconds())
        eps = round(row['row_count'] / seconds) if seconds > 0 else 0
        data.append({
            'interval': interval,
            'time_diff': seconds,
            'row_count': row['row_count'],
            'event_per_second': eps
        })

    return data


def print_column_stats(stats: list):
    headers = ["Interval", "Time Period (s)", "Row Count", "Events/sec"]

    rows = [
        [s['interval'], s['time_diff'], f"{s['row_count']:,.2f}", f"{s['event_per_second']:,.2f}"]
        for s in stats
    ]

    total_intervals = len(stats)
    total_row_count = sum(s['row_count'] for s in stats)
    avg_events_per_sec = sum(s['event_per_second'] for s in stats) / total_intervals

    # Total row count | average events per second
    rows.append(["Summary", "", f"{total_row_count:,.2f}", f"{avg_events_per_sec:,.2f}"])

    print(tabulate(rows, headers=headers, floatfmt=".2f"))


def plot_stats(structured: list, db: str, table: str):
    intervals = [s['interval'] for s in structured]
    row_counts = [s['row_count'] for s in structured]
    events_per_sec = [s['event_per_second'] for s in structured]

    fig, ax1 = plt.subplots(figsize=(12, 6))

    ax1.set_title(f"Row Count & Events/sec per Interval - {db}.{table}")
    ax1.set_xlabel("Interval")

    color1 = "tab:blue"
    ax1.set_ylabel("Row Count", color=color1)
    ax1.plot(intervals, row_counts, marker='o', linestyle='-', color=color1, label="Row Count")
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))

    ax2 = ax1.twinx()
    color2 = "tab:green"
    ax2.set_ylabel("Events/sec", color=color2)
    ax2.plot(intervals, events_per_sec, marker='x', linestyle='--', color=color2, label="Events/sec")
    ax2.tick_params(axis='y', labelcolor=color2)

    fig.tight_layout()
    plt.grid(True)
    plt.show()


def main():
    conn = '10.0.0.86:32349'
    db = 'nov'
    table = 'rand_data'

    raw = get_data(conn, db, table)
    structured = extract_data(raw, db, table)

    print_column_stats(structured)
    plot_stats(structured, db, table)


if __name__ == '__main__':
    main()
