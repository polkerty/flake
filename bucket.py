#!/usr/bin/env python3
import argparse
import psycopg2
import pandas as pd
from scipy.stats import chi2

# Mapping for allowed since periods.
ALLOWED_PERIODS = {
    "day": "1 day",
    "week": "1 week",
    "month": "1 month",
    "year": "1 year"
}

def get_monthly_data(conn, animal=None, since=None):
    """
    Retrieve monthly aggregated failure data from the run table.
    Optionally filter by a specific animal and/or only include events since a given period.
    Only monthly buckets with more than 25 total events are returned.
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
        date_trunc('month', snapshot) AS month,
        SUM(CASE WHEN fail_stage IS NULL THEN 0 ELSE 1 END) AS failures,
        COUNT(*) AS total
    FROM run
    {where_clause}
    GROUP BY animal, date_trunc('month', snapshot)
    ORDER BY animal, month;
    """
    df = pd.read_sql(query, conn, params=params)
    # Only consider monthly buckets with more than 25 events.
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

def analyze_animal(df_monthly, overall_dates, animal):
    # Filter data for the specific animal.
    data = df_monthly[df_monthly['animal'] == animal].copy()
    if data.empty or len(data) < 2:
        print(f"Not enough monthly buckets with >25 events for animal: {animal}")
        return None, None

    # Overall totals.
    total_failures = data['failures'].sum()
    total_events = data['total'].sum()
    total_successes = total_events - total_failures
    overall_failure_rate = total_failures / total_events if total_events > 0 else None
    total_months = data['month'].nunique()

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
        test_result = "Not enough monthly buckets to test"
    else:
        p_value = chi2.sf(chi_square_stat, dof)
        test_result = ("Reject Null Hypothesis: Rates are different across months"
                       if p_value < 0.05 else
                       "Fail to Reject Null Hypothesis: No significant difference in rates")

    # Detailed per-month failure rate.
    data['failure_rate'] = data['failures'] / data['total']

    summary = {
        "animal": animal,
        "total_failures": total_failures,
        "total_events": total_events,
        "failure_rate": overall_failure_rate,
        "total_months": total_months,
        "first_event": first_event,
        "last_event": last_event,
        "chi_square_stat": chi_square_stat,
        "degrees_of_freedom": dof,
        "p_value": p_value,
        "result": test_result
    }
    return summary, data.sort_values("month")  # Order individual animal view by date ascending

def analyze_all_animals(df_monthly, overall_dates):
    summaries = []
    for animal, group in df_monthly.groupby('animal'):
        if len(group) < 2:
            continue  # Skip animals with insufficient monthly buckets.
        total_failures = group['failures'].sum()
        total_events = group['total'].sum()
        total_successes = total_events - total_failures
        overall_failure_rate = total_failures / total_events if total_events > 0 else None
        total_months = group['month'].nunique()

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
        test_result = ("Reject Null Hypothesis: Rates are different across months"
                       if p_value < 0.05 else
                       "Fail to Reject Null Hypothesis: No significant difference in rates")

        summaries.append({
            "animal": animal,
            "total_failures": total_failures,
            "total_events": total_events,
            "failure_rate": overall_failure_rate,
            "total_months": total_months,
            "first_event": first_event,
            "last_event": last_event,
            "chi_square_stat": chi_square_stat,
            "degrees_of_freedom": dof,
            "p_value": p_value,
            "result": test_result
        })
    if summaries:
        # Order by highest failure rate descending.
        return pd.DataFrame(summaries).sort_values("failure_rate", ascending=False, na_position="last")
    else:
        return pd.DataFrame()

def main():
    parser = argparse.ArgumentParser(
        description="Chi-square analysis of run table monthly failure rates.")
    parser.add_argument("--animal", type=str, help="Analyze a specific animal (optional)")
    parser.add_argument("--since", type=str, choices=["day", "week", "month", "year"],
                        help="Limit data to events since this time period (e.g., 'day', 'week', 'month', 'year')")
    args = parser.parse_args()

    # Connect to the database using user jbrazeal.
    conn = psycopg2.connect(
        dbname="flake",
        host="localhost",
        port=5258,
        user="jbrazeal",   # your user
        password=""        # adjust if needed
    )

    df_monthly = get_monthly_data(conn, animal=args.animal, since=args.since)
    overall_dates = get_overall_dates(conn, animal=args.animal, since=args.since)
    conn.close()

    if args.animal:
        summary, detailed = analyze_animal(df_monthly, overall_dates, args.animal)
        if summary is None:
            print(f"No sufficient data for animal '{args.animal}' to make a decision.")
        else:
            print("Summary:")
            for key, value in summary.items():
                print(f"{key:20s}: {value}")
            print("\nDetailed monthly data (ordered by date ascending):")
            print(detailed.to_string(index=False))
    else:
        summary_df = analyze_all_animals(df_monthly, overall_dates)
        if summary_df.empty:
            print("No animals with sufficient data to make a decision.")
        else:
            print("Summary for all animals (ordered by highest failure rate descending):")
            print(summary_df.to_string(index=False))

if __name__ == "__main__":
    main()
