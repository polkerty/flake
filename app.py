#!/usr/bin/env python3
from flask import Flask, render_template, request, jsonify
import psycopg2
import pandas as pd
from scipy.stats import chi2, binomtest
import datetime

app = Flask(__name__)

# Allowed periods for the "since" filter.
ALLOWED_PERIODS = {
    "day": "1 day",
    "week": "1 week",
    "month": "1 month",
    "year": "1 year"
}

def get_db_connection():
    """Create and return a new database connection."""
    return psycopg2.connect(
        dbname="flake",
        host="localhost",
        port=6565,
        user="jbrazeal",   # adjust as needed
        password=""        # adjust as needed
    )

def get_aggregated_data(conn, animal=None, since=None, granularity="month"):
    """
    Retrieve aggregated failure data from the run table.
    The grouping is based on the given granularity (day, week, month, or year).
    Only buckets with more than 25 events are returned.
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
        date_trunc('{granularity}', snapshot) AS bucket,
        SUM(CASE WHEN fail_stage IS NULL THEN 0 ELSE 1 END) AS failures,
        COUNT(*) AS total
    FROM run
    {where_clause}
    GROUP BY animal, date_trunc('{granularity}', snapshot)
    ORDER BY animal, bucket;
    """
    df = pd.read_sql(query, conn, params=params)
    df = df[df['total'] > 10]
    return df

def get_overall_dates(conn, animal=None, since=None):
    """
    Retrieve the overall first and last event dates for each animal from the run table.
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

def analyze_all_animals(df_agg, overall_dates):
    """
    Compute the overall failure rate and chi-square statistic for each animal,
    and compute a "spike" (difference between worst bucket failure rate and overall rate).
    """
    summaries = []
    for animal, group in df_agg.groupby('animal'):
        if len(group) < 2:
            continue  # Skip if not enough buckets.
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
            continue  # Skip if degrees of freedom not sufficient.
        p_value = chi2.sf(chi_square_stat, dof)
        test_result = ("Reject Null Hypothesis: Rates are different across periods"
                       if p_value < 0.05 else
                       "Fail to Reject Null Hypothesis: No significant difference in rates")
        
        # Compute spike: the difference between the highest bucket failure rate and overall rate.
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
        return pd.DataFrame(summaries)
    else:
        return pd.DataFrame()

def generate_grid_data(summary_df, df_agg, top_n):
    """
    Build a data structure for the grid.
    Returns a tuple: (animal_list, bucket_list, grid)
    where grid[animal][bucket] = (cell content string, significant flag)
    """
    if summary_df.empty:
        return ([], [], {})
    # Choose the top_n animals.
    top_animals = summary_df.head(top_n)
    animal_list = top_animals["animal"].tolist()
    df = df_agg[df_agg["animal"].isin(animal_list)].copy()
    # Format the bucket (time period) as a string (YYYY-MM-DD).
    df["bucket_str"] = pd.to_datetime(df["bucket"]).dt.strftime("%Y-%m-%d")
    
    # Get the union of buckets.
    bucket_list = sorted(df["bucket_str"].unique())
    
    grid = {animal: {bucket: None for bucket in bucket_list} for animal in animal_list}
    overall_rates = top_animals.set_index("animal")["failure_rate"].to_dict()
    
    for _, row in df.iterrows():
        animal = row["animal"]
        bucket = row["bucket_str"]
        failures = row["failures"]
        total = row["total"]
        rate = failures / total if total > 0 else 0
        overall_rate = overall_rates.get(animal, 0)
        cell_significant = False
        if total > 0 and rate > overall_rate:
            bt = binomtest(failures, n=total, p=overall_rate, alternative="greater")
            if bt.pvalue < 0.05:
                cell_significant = True
        content = f"{rate:.1%} (n={total})"
        grid[animal][bucket] = (content, cell_significant)
    return (animal_list, bucket_list, grid)

@app.route("/", methods=["GET"])
def index():
    # Read parameters from the query string.
    animal = request.args.get("animal", "")
    since = request.args.get("since", "month")
    granularity = request.args.get("granularity", "month")
    try:
        top_n = int(request.args.get("top_n", 10))
    except ValueError:
        top_n = 10
    spikes = request.args.get("spikes", "false").lower() in ["true", "1", "on"]
    
    conn = get_db_connection()
    df_agg = get_aggregated_data(conn, animal=animal if animal else None, since=since, granularity=granularity)
    overall_dates = get_overall_dates(conn, animal=animal if animal else None, since=since)
    conn.close()
    
    summary_df = analyze_all_animals(df_agg, overall_dates)
    if not summary_df.empty:
        if spikes:
            summary_df = summary_df.sort_values("spike", ascending=False, na_position="last")
        else:
            summary_df = summary_df.sort_values("failure_rate", ascending=False, na_position="last")
    grid_data = generate_grid_data(summary_df, df_agg, top_n) if not summary_df.empty else ([], [], {})
    
    return render_template("index.html", 
                           animal=animal, 
                           since=since, 
                           granularity=granularity, 
                           top_n=top_n, 
                           spikes=spikes,
                           animals=grid_data[0],
                           buckets=grid_data[1],
                           grid=grid_data[2])

@app.route("/snapshots", methods=["GET"])
def snapshots():
    """
    Given an animal and a bucket (a date string in YYYY-MM-DD format) along with the current granularity,
    query the database for all snapshots in that bucket. For each snapshot, return the timestamp,
    result (success/failure), branch, commit, fail_stage and a log URL.
    """
    animal = request.args.get("animal")
    bucket = request.args.get("bucket")  # expected format: YYYY-MM-DD
    granularity = request.args.get("granularity", "month")
    if not animal or not bucket:
        return jsonify({"error": "Missing parameters"}), 400

    try:
        bucket_ts = pd.to_datetime(bucket)
    except Exception:
        return jsonify({"error": "Invalid bucket format"}), 400

    conn = get_db_connection()
    query = f"""
        SELECT snapshot, result, branch, commit, fail_stage
        FROM run
        WHERE animal = %s AND date_trunc('{granularity}', snapshot) = %s
        ORDER BY snapshot;
    """
    cur = conn.cursor()
    cur.execute(query, (animal, bucket_ts))
    rows = cur.fetchall()
    conn.close()
    snapshots_list = []
    for row in rows:
        snap_ts, result, branch, commit, fail_stage = row
        snap_str = snap_ts.strftime("%Y-%m-%d %H:%M:%S")
        log_link = f"https://buildfarm.postgresql.org/cgi-bin/show_log.pl?nm={animal}&dt={snap_ts.strftime('%Y-%m-%d%%20%H:%M:%S')}"
        snapshots_list.append({
            "snapshot": snap_str,
            "status": result,
            "branch": branch,
            "commit": commit,
            "fail_stage": fail_stage,
            "log_link": log_link
        })
    return jsonify(snapshots_list)

if __name__ == "__main__":
    app.run(debug=True)
