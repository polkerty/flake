#!/usr/bin/env python3
import argparse
import psycopg2
import pandas as pd
from scipy.stats import chi2, binomtest

# Mapping for allowed since periods.
ALLOWED_PERIODS = {
    "day": "1 day",
    "week": "1 week",
    "month": "1 month",
    "year": "1 year"
}

def get_aggregated_data(conn, animal=None, since=None, granularity="month"):
    """
    Retrieve aggregated failure data from the run table.
    Optionally filter by a specific animal and/or only include events since a given period.
    Only buckets with more than 25 total events are returned.
    """
    clauses = []
    params = []
    if animal:
        clauses.append("animal = %s")
        params.append(animal)
    if since:
        if since not in ALLOWED_PERIODS:
            raise ValueError("Invalid 'since' parameter. Must be one of: day, week, month, year.")
        clauses.append(f"snapshot >= now() - interval '{ALLOWED_PERIODS[since]}'")
    
    where_clause = ""
    if clauses:
        where_clause = "WHERE " + " AND ".join(clauses)
    
    # Use the provided granularity (day, week, month, or year) in the date_trunc function.
    query = f"""
    SELECT
        animal,
        date_trunc('{granularity}', snapshot) AS bucket,
        SUM(CASE WHEN fail_stage IS NULL THEN 0 ELSE 1 END) AS failures,
        COUNT(*) AS total
    FROM run
    {where_clause}
    GROUP BY animal, date_trunc('{granularity}', snapshot)
    ORDER BY animal, bucket;
    """
    df = pd.read_sql(query, conn, params=params)
    # Only consider buckets with more than 25 events.
    df = df[df['total'] > 25]
    return df

def get_overall_dates(conn, animal=None, since=None):
    """
    Retrieve overall first and last event dates from the run table.
    Optionally filter by a specific animal and/or limit data to a given time period.
    """
    clauses = []
    params = []
    if animal:
        clauses.append("animal = %s")
        params.append(animal)
    if since:
        if since not in ALLOWED_PERIODS:
            raise ValueError("Invalid 'since' parameter. Must be one of: day, week, month, year.")
        clauses.append(f"snapshot >= now() - interval '{ALLOWED_PERIODS[since]}'")
    
    where_clause = ""
    if clauses:
        where_clause = "WHERE " + " AND ".join(clauses)
    
    query = f"""
    SELECT
        animal,
        MIN(snapshot) AS first_event,
        MAX(snapshot) AS last_event
    FROM run
    {where_clause}
    GROUP BY animal
    ORDER BY animal;
    """
    df = pd.read_sql(query, conn, params=params)
    return df

def analyze_animal(df_agg, overall_dates, animal):
    # Filter data for the specific animal.
    data = df_agg[df_agg['animal'] == animal].copy()
    if data.empty or len(data) < 2:
        print(f"Not enough buckets with >25 events for animal: {animal}")
        return None, None

    # Overall totals.
    total_failures = data['failures'].sum()
    total_events = data['total'].sum()
    total_successes = total_events - total_failures
    overall_failure_rate = total_failures / total_events if total_events > 0 else None
    total_buckets = data['bucket'].nunique()

    # Get first and last event dates.
    overall = overall_dates[overall_dates['animal'] == animal]
    first_event = overall['first_event'].iloc[0] if not overall.empty else None
    last_event = overall['last_event'].iloc[0] if not overall.empty else None

    # Calculate chi-square statistic.
    chi_square_stat = 0.0
    for _, row in data.iterrows():
        bucket_total = row['total']
        observed_failures = row['failures']
        observed_successes = bucket_total - observed_failures
        
        expected_failures = total_failures * (bucket_total / total_events)
        expected_successes = total_successes * (bucket_total / total_events)
        
        if expected_failures > 0:
            chi_square_stat += (observed_failures - expected_failures) ** 2 / expected_failures
        if expected_successes > 0:
            chi_square_stat += (observed_successes - expected_successes) ** 2 / expected_successes

    dof = len(data) - 1
    if dof <= 0:
        p_value = None
        test_result = "Not enough buckets to test"
    else:
        p_value = chi2.sf(chi_square_stat, dof)
        test_result = ("Reject Null Hypothesis: Rates are different across periods"
                       if p_value < 0.05 else
                       "Fail to Reject Null Hypothesis: No significant difference in rates")

    # Detailed per-bucket failure rate.
    data['failure_rate'] = data['failures'] / data['total']

    summary = {
        "animal": animal,
        "total_failures": total_failures,
        "total_events": total_events,
        "failure_rate": overall_failure_rate,
        "total_buckets": total_buckets,
        "first_event": first_event,
        "last_event": last_event,
        "chi_square_stat": chi_square_stat,
        "degrees_of_freedom": dof,
        "p_value": p_value,
        "result": test_result
    }
    return summary, data.sort_values("bucket")  # Order individual animal view by date ascending

def analyze_all_animals(df_agg, overall_dates):
    summaries = []
    for animal, group in df_agg.groupby('animal'):
        if len(group) < 2:
            continue  # Skip animals with insufficient buckets.
        total_failures = group['failures'].sum()
        total_events = group['total'].sum()
        total_successes = total_events - total_failures
        overall_failure_rate = total_failures / total_events if total_events > 0 else None
        total_buckets = group['bucket'].nunique()

        overall = overall_dates[overall_dates['animal'] == animal]
        first_event = overall['first_event'].iloc[0] if not overall.empty else None
        last_event = overall['last_event'].iloc[0] if not overall.empty else None

        chi_square_stat = 0.0
        for _, row in group.iterrows():
            bucket_total = row['total']
            observed_failures = row['failures']
            observed_successes = bucket_total - observed_failures
            expected_failures = total_failures * (bucket_total / total_events)
            expected_successes = total_successes * (bucket_total / total_events)
            if expected_failures > 0:
                chi_square_stat += (observed_failures - expected_failures) ** 2 / expected_failures
            if expected_successes > 0:
                chi_square_stat += (observed_successes - expected_successes) ** 2 / expected_successes

        dof = len(group) - 1
        if dof <= 0:
            continue  # Skip animals with insufficient data for a chi-square test.
        p_value = chi2.sf(chi_square_stat, dof)
        test_result = ("Reject Null Hypothesis: Rates are different across periods"
                       if p_value < 0.05 else
                       "Fail to Reject Null Hypothesis: No significant difference in rates")
        
        # Compute spike: difference between worst bucket failure rate and overall rate.
        bucket_rates = group['failures'] / group['total']
        max_rate = bucket_rates.max()
        spike = max_rate - overall_failure_rate if overall_failure_rate is not None else None

        summaries.append({
            "animal": animal,
            "total_failures": total_failures,
            "total_events": total_events,
            "failure_rate": overall_failure_rate,
            "total_buckets": total_buckets,
            "first_event": first_event,
            "last_event": last_event,
            "chi_square_stat": chi_square_stat,
            "degrees_of_freedom": dof,
            "p_value": p_value,
            "spike": spike,
            "result": test_result
        })
    if summaries:
        df_summary = pd.DataFrame(summaries)
        # Sorting will be handled in main based on the --spikes flag.
        return df_summary
    else:
        return pd.DataFrame()

def generate_grid_html(summary_df, df_agg, top_n, output_file="grid.html"):
    """
    Generate an HTML grid showing the top_n animals (rows) and buckets (columns).
    Each cell shows the bucket failure rate and total count.
    If the bucket failure rate is significantly higher (one-tailed binomial test, p < 0.05)
    than the animal's overall failure rate, the cell is colored red.
    """
    # Filter to top_n animals.
    top_animals = summary_df.head(top_n)
    animal_list = top_animals["animal"].tolist()

    # Filter aggregated data for these animals.
    df = df_agg[df_agg["animal"].isin(animal_list)].copy()
    # Ensure 'bucket' is a datetime; then format as YYYY-MM-DD (or YYYY-MM if monthly).
    df["bucket_str"] = pd.to_datetime(df["bucket"]).dt.strftime("%Y-%m-%d")
    
    # Get the union of all buckets for these animals, sorted ascending.
    all_buckets = sorted(df["bucket_str"].unique())

    # Build grid data: a dict of dict: grid[animal][bucket] = cell content
    grid = {animal: {bucket: "" for bucket in all_buckets} for animal in animal_list}
    # Also, get overall failure rates for each animal from summary.
    overall_rates = top_animals.set_index("animal")["failure_rate"].to_dict()

    # For each record, fill the grid.
    for _, row in df.iterrows():
        animal = row["animal"]
        bucket = row["bucket_str"]
        failures = row["failures"]
        total = row["total"]
        rate = failures / total if total > 0 else 0
        # Do a one-tailed binomial test comparing this cell to overall rate.
        overall_rate = overall_rates.get(animal, 0)
        cell_significant = False
        if total > 0 and rate > overall_rate:
            bt = binomtest(failures, n=total, p=overall_rate, alternative="greater")
            if bt.pvalue < 0.05:
                cell_significant = True
        content = f"{rate:.1%} (n={total})"
        grid[animal][bucket] = (content, cell_significant)

    # Build HTML.
    html = []
    html.append("<html><head><meta charset='UTF-8'><title>Failure Rate Grid</title>")
    html.append("""
    <style>
    table { border-collapse: collapse; }
    th, td { border: 1px solid #ddd; padding: 8px; text-align: center; }
    th { background-color: #f2f2f2; }
    .significant { background-color: #ffcccc; }
    </style>
    """)
    html.append("</head><body>")
    html.append("<h2>Top {} Animals Failure Rate Grid</h2>".format(top_n))
    html.append("<table>")
    header = "<tr><th>Animal</th>"
    for bucket in all_buckets:
        header += f"<th>{bucket}</th>"
    header += "</tr>"
    html.append(header)
    for animal in animal_list:
        row_html = f"<tr><td>{animal}</td>"
        for bucket in all_buckets:
            cell = grid[animal][bucket]
            if cell:
                content, sig = cell
                td_class = 'significant' if sig else ''
                row_html += f"<td class='{td_class}'>{content}</td>"
            else:
                row_html += "<td></td>"
        row_html += "</tr>"
        html.append(row_html)
    html.append("</table>")
    html.append("</body></html>")
    html_str = "\n".join(html)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_str)
    print(f"Grid HTML file written to {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Chi-square analysis of run table failure rates with variable granularity.")
    parser.add_argument("--animal", type=str, help="Analyze a specific animal (optional)")
    parser.add_argument("--since", type=str, choices=["day", "week", "month", "year"],
                        help="Limit data to events since this time period (e.g., 'day', 'week', 'month', 'year')")
    parser.add_argument("--granularity", type=str, choices=["day", "week", "month", "year"],
                        default="month",
                        help="Granularity for analysis buckets (e.g., 'day', 'week', 'month', 'year')")
    parser.add_argument("--grid", type=int, help="Generate an HTML grid for the top x animals")
    parser.add_argument("--spikes", action="store_true", help="Sort grid by spike (difference between worst bucket rate and overall rate)")
    args = parser.parse_args()

    conn = psycopg2.connect(
        dbname="flake",
        host="localhost",
        port=6565,
        user="jbrazeal",   # your user
        password=""        # adjust if needed
    )

    df_agg = get_aggregated_data(conn, animal=args.animal, since=args.since, granularity=args.granularity)
    overall_dates = get_overall_dates(conn, animal=args.animal, since=args.since)
    conn.close()

    # If grid option is provided, generate the HTML grid.
    if args.grid:
        summary_df = analyze_all_animals(df_agg, overall_dates)
        if summary_df.empty:
            print("No animals with sufficient data to generate grid.")
        else:
            # If --spikes flag is provided, sort by spike difference; otherwise by overall failure rate.
            if args.spikes:
                summary_df = summary_df.sort_values("spike", ascending=False, na_position="last")
            else:
                summary_df = summary_df.sort_values("failure_rate", ascending=False, na_position="last")
            generate_grid_html(summary_df, df_agg, top_n=args.grid)
        return

    if args.animal:
        summary, detailed = analyze_animal(df_agg, overall_dates, args.animal)
        if summary is None:
            print(f"No sufficient data for animal '{args.animal}' to make a decision.")
        else:
            print("Summary:")
            for key, value in summary.items():
                print(f"{key:20s}: {value}")
            print("\nDetailed data (ordered by date ascending):")
            print(detailed.to_string(index=False))
    else:
        summary_df = analyze_all_animals(df_agg, overall_dates)
        if summary_df.empty:
            print("No animals with sufficient data to make a decision.")
        else:
            # Default sorting: overall failure rate descending.
            summary_df = summary_df.sort_values("failure_rate", ascending=False, na_position="last")
            print("Summary for all animals (ordered by highest failure rate descending):")
            print(summary_df.to_string(index=False))

if __name__ == "__main__":
    main()
