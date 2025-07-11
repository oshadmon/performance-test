import matplotlib.pyplot as plt
import requests
from collections import defaultdict
from tabulate import tabulate


def get_data(conn: str):
    headers = {
        'command': 'get aggregations where format=json',
        'User-Agent': 'AnyLog/1.23'
    }

    try:
        response = requests.get(url=f"http://{conn}", headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as error:
        raise requests.exceptions.RequestException(f'Failed to GET from {conn} (Error: {error})')


def extract_data(raw_data: list):
    data = {}
    for row in raw_data:
        table = f"{row['dbms']}.{row['table']}"
        column = row['value_column']
        interval = row['interval_id']

        if table not in data:
            data[table] = {}
        if interval not in data[table]:
            data[table][interval] = []

        data[table][interval].append({
            'column': column,
            'events_per_second': row['events_sec'],
            'count': row['count']
        })

    return data


def aggregate_stats(data: dict):
    table_column_stats = defaultdict(lambda: defaultdict(lambda: {'events': [], 'counts': []}))

    for table, intervals in data.items():
        for interval, entries in intervals.items():
            for entry in entries:
                column = entry['column']
                eps = entry['events_per_second']
                cnt = entry['count']

                table_column_stats[table][column]['events'].append(float(eps))
                table_column_stats[table][column]['counts'].append(float(cnt))

    result = {}

    for table, columns in table_column_stats.items():
        result[table] = {}
        for column, metrics in columns.items():
            e = metrics['events']
            c = metrics['counts']

            if e and c:
                result[table][column] = {
                    'events_per_second': {
                        'min': min(e),
                        'max': max(e),
                        'avg': round(sum(e) / len(e), 3)
                    },
                    'count': {
                        'min': min(c),
                        'max': max(c),
                        'avg': round(sum(c) / len(c), 3)
                    },
                    'intervals': len(e)  # assume 1 data point per interval
                }

    return result


def print_column_stats(stats: dict):
    rows = []
    eps_avg_list, count_avg_list, interval_counts = [], [], []

    for column, metrics in stats.items():
        eps = metrics['events_per_second']
        count = metrics['count']
        intervals = metrics['intervals']

        rows.append([
            column, intervals,
            eps['min'], eps['max'], eps['avg'],
            count['min'], count['max'], count['avg']

        ])

        eps_avg_list.append(eps['avg'])
        count_avg_list.append(count['avg'])
        interval_counts.append(intervals)

    summary_row = [
        "Summary", "",
        "", "", f"{round(sum(eps_avg_list) / len(eps_avg_list), 2):,}",
        "", "", f"{round(sum(count_avg_list) / len(count_avg_list), 2):,}"

    ]
    rows.append(summary_row)

    headers = [
        "Column", "Intervals",
        "EPS Min", "EPS Max", "EPS Avg",
        "Count Min", "Count Max", "Count Avg"

    ]

    print(tabulate(rows, headers=headers, floatfmt=".2f"))

def plot_table_interval_stats(table: str, data: dict):
    if table not in data:
        print(f"[ERROR] Table '{table}' not found in data.")
        return

    interval_stats = defaultdict(lambda: {'eps': [], 'count': 0})

    for interval, entries in data[table].items():
        for entry in entries:
            interval_stats[interval]['eps'].append(entry['events_per_second'])
        interval_stats[interval]['count'] = len(entries)

    # Prepare data for plotting
    intervals = sorted(interval_stats.keys())
    eps_min, eps_max, eps_avg = [], [], []
    avg_points = []

    for interval in intervals:
        eps_list = interval_stats[interval]['eps']
        if not eps_list:
            continue
        eps_min.append(min(eps_list))
        eps_max.append(max(eps_list))
        eps_avg.append(sum(eps_list) / len(eps_list))
        avg_points.append(interval_stats[interval]['count'])

    x = range(len(intervals))
    width = 0.25

    # Use colorblind-friendly colors (Okabe-Ito palette)
    colors = {
        'EPS Min': '#E69F00',  # orange
        'EPS Avg': '#56B4E9',  # sky blue
        'EPS Max': '#009E73',  # bluish green
    }

    # --- Bar Chart ---

    fig, ax1 = plt.subplots(figsize=(12, 6))

    ax1.bar([i - width for i in x], eps_min, width=width, label='EPS Min', color=colors['EPS Min'])
    ax1.bar(x, eps_avg, width=width, label='EPS Avg', color=colors['EPS Avg'])
    ax1.bar([i + width for i in x], eps_max, width=width, label='EPS Max', color=colors['EPS Max'])

    ax1.set_title(f"Events Per Second over Intervals - {table}")
    ax1.set_xlabel("Interval")
    ax1.set_ylabel("Events Per Second")
    ax1.set_xticks(x)
    ax1.set_xticklabels(intervals, rotation=45)
    ax1.legend()
    ax1.grid(True)

    plt.tight_layout()
    plt.show()

    # # --- Line Chart ---
    # fig, ax2 = plt.subplots(figsize=(12, 4))
    # ax2.plot(intervals, avg_points, marker='o', linestyle='-', color='purple')
    # ax2.set_title(f"Avg Number of Data Points per Interval - {table}")
    # ax2.set_xlabel("Interval")
    # ax2.set_ylabel("Data Points")
    # ax2.grid(True)
    # plt.xticks(rotation=45)
    #
    # plt.tight_layout()
    # plt.show()


def main():
    conn = '10.0.0.220:32149'
    raw = get_data(conn)
    structured = extract_data(raw)
    stats = aggregate_stats(structured)

    print("\n=== Per Table Per Column Stats ===")
    for table, column_stats in stats.items():
        print(f"\nTable: {table}")
        print_column_stats(column_stats)
        plot_table_interval_stats(table, structured)  # Plot per table


if __name__ == '__main__':
    main()
