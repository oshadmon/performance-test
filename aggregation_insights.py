import json
import requests
import  numpy as np
from tabulate import tabulate

from collections import defaultdict


def get_data(conn:str):
    headers = {
        'command': 'get aggregations where format=json',
        'User-Agent': 'AnyLog/1.23'
    }

    try:
        response = requests.get(url=f"http://{conn}", headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as error:
        raise requests.exceptions.RequestException(f'Failed to execute GET against {conn} (Error: {error})')

def extract_data(raw_data:list, column_as_tables:bool=False):
    data = {}
    for row in raw_data:
        table = f"{row['dbms']}.{row['table']}"
        value_column = row['value_column']
        interval = row['interval_id']

        if table not in data:
            data[table] = {}
        if interval not in data[table]:
            data[table][interval] = []

        data[table][interval].append({
            'column': value_column,
            'events_per_second': row['events_sec'],
            'count': row['count']
        })

    return data

def aggregate_stats(data: dict):
    table_stats = defaultdict(lambda: {'events': [], 'counts': []})
    table_column_stats = defaultdict(lambda: defaultdict(lambda: {'events': [], 'counts': []}))
    table_column_interval_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {'events': [], 'counts': []})))

    for table, intervals in data.items():
        for interval, entries in intervals.items():
            for entry in entries:
                column = entry['column']
                eps = entry['events_per_second']
                cnt = entry['count']

                # Table level
                table_stats[table]['events'].append(eps)
                table_stats[table]['counts'].append(cnt)

                # Table + column level
                table_column_stats[table][column]['events'].append(eps)
                table_column_stats[table][column]['counts'].append(cnt)

                # Table + column + interval level
                table_column_interval_stats[table][column][interval]['events'].append(eps)
                table_column_interval_stats[table][column][interval]['counts'].append(cnt)

    def compute_stats(stats_dict):
        def safe_cast_list(values):
            cleaned = []
            for v in values:
                try:
                    cleaned.append(float(v))
                except (ValueError, TypeError):
                    continue
            return cleaned

        result = {}
        for key, metrics in stats_dict.items():
            result[key] = {}
            # Detect nested vs flat structure
            if isinstance(metrics, dict) and all(isinstance(v, dict) for v in metrics.values()):
                for subkey, values in metrics.items():
                    e = safe_cast_list(values['events'])
                    c = safe_cast_list(values['counts'])
                    if e and c:
                        result[key][subkey] = {
                            'events_per_second': {
                                'min': min(e),
                                'max': max(e),
                                'avg': round(sum(e) / len(e),3)
                            },
                            'count': {
                                'min': min(c),
                                'max': max(c),
                                'avg': round(sum(c) / len(c), 3)
                            }
                        }
            else:
                e = safe_cast_list(metrics['events'])
                c = safe_cast_list(metrics['counts'])
                if e and c:
                    result[key] = {
                        'events_per_second': {
                            'min': min(e),
                            'max': max(e),
                            'avg': round(sum(e) / len(e), 3)
                        },
                        'count': {
                            'min': min(c),
                            'max': max(c),
                            'avg': round(sum(c) / len(c), 3)
                        }
                    }

        return result

    return {
        'table': compute_stats(table_stats),
        'table_column': compute_stats(table_column_stats),
        'table_column_interval': compute_stats(table_column_interval_stats)
    }

def json_to_numpy_table(stats_dict):
    rows = []

    for table, stat in stats_dict.items():
        if isinstance(stat, dict) and all(isinstance(v, dict) for v in stat.values()):
            for column_or_interval, values in stat.items():
                if 'events_per_second' not in values or 'count' not in values:
                    print(f"[WARN] Missing keys in stats for {table} -> {column_or_interval}: {values}")
                    continue
                row = (
                    table,
                    column_or_interval,
                    values['events_per_second'].get('min', 0.0),
                    values['events_per_second'].get('max', 0.0),
                    values['events_per_second'].get('avg', 0.0),
                    values['count'].get('min', 0.0),
                    values['count'].get('max', 0.0),
                    values['count'].get('avg', 0.0)
                )
                rows.append(row)
        elif isinstance(stat, dict):  # Top-level
            if 'events_per_second' not in stat or 'count' not in stat:
                print(f"[WARN] Missing top-level keys in {table}: {stat}")
                continue
            row = (
                table,
                '',
                stat['events_per_second'].get('min', 0.0),
                stat['events_per_second'].get('max', 0.0),
                stat['events_per_second'].get('avg', 0.0),
                stat['count'].get('min', 0.0),
                stat['count'].get('max', 0.0),
                stat['count'].get('avg', 0.0)
            )
            rows.append(row)
        else:
            print(f"[WARN] Skipping unexpected structure for {table}: {stat}")

    dtype = [
        ('table', 'U50'),
        ('column_or_interval', 'U50'),
        ('eps_min', 'f8'),
        ('eps_max', 'f8'),
        ('eps_avg', 'f8'),
        ('count_min', 'f8'),
        ('count_max', 'f8'),
        ('count_avg', 'f8')
    ]

    return np.array(rows, dtype=dtype)


def print_column_stats(stats):
    rows = []

    for column, metrics in stats.items():
        eps = metrics.get('events_per_second', {})
        count = metrics.get('count', {})
        rows.append([
            column,
            eps.get('min', 0),
            eps.get('max', 0),
            eps.get('avg', 0),
            count.get('min', 0),
            count.get('max', 0),
            count.get('avg', 0),
        ])
    headers = [
        "Column",
        "EPS Min", "EPS Max", "EPS Avg",
        "Count Min", "Count Max", "Count Avg"
    ]

    print(tabulate(rows, headers=headers, floatfmt=".2f"))



def main():
    output = get_data(conn='10.0.0.220:32149')
    data = extract_data(raw_data=output)
    stats = aggregate_stats(data)

    print("\n=== Per Table Stats ===")
    for table in stats['table']:
        print(f"\nTable: {table}")
        print_column_stats(stats['table'][table])

    print("\n=== Per Table Per Column Stats ===")
    for table in stats['table_column']:
        print(f"\nTable: {table}")
        print_column_stats(stats['table_column'][table])

    print("\n=== Per Table Per Column Per Interval Stats ===")
    for table in stats['table_column_interval']:
        print(f"\nTable: {table}")
        for column in stats['table_column_interval'][table]:
            print(f"  Column: {column}")
            rows = []
            for interval, metrics in stats['table_column_interval'][table][column].items():
                eps = metrics.get('events_per_second', {})
                count = metrics.get('count', {})
                rows.append([
                    interval,
                    eps.get('min', 0),
                    eps.get('max', 0),
                    eps.get('avg', 0),
                    count.get('min', 0),
                    count.get('max', 0),
                    count.get('avg', 0),
                ])
            headers = ["Interval", "EPS Min", "EPS Max", "EPS Avg", "Count Min", "Count Max", "Count Avg"]
            print(tabulate(rows, headers=headers, floatfmt=".2f", tablefmt="grid"))
if  __name__ == '__main__':
    main()