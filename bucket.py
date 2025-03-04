#!/usr/bin/env python3
import argparse
import psycopg2
import pandas as pd
from scipy.stats import chi2

def get_monthly_data(conn, animal=None):
    # Query to get monthly aggregates (only buckets with > 25 events)
    base_query = """
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
    where_clause = ""
    if animal:
        where_clause = "WHERE animal = %s"
    query = base_query.format(where_clause=where_clause)
    if animal:
        df = pd.read_sql(query, conn, params=(animal,))
    else:
        df = pd.read_sql(query, conn)
    # Only consider monthly buckets with more than 25 events
    df = df[df['total'] > 25]
    return df

def get_overall_dates(conn, animal=None):
    # Query to get overall first and last event dates per animal
    base_query = """
    SELECT
        animal,
        MIN(snapshot) AS first_event,
        MAX(snapshot) AS last_event
    FROM run
    {where_clause}
    GROUP BY animal
    ORDER BY animal;
    """
    where_clause = ""
    if animal:
        where_clause = "WHERE animal = %s"
    query = base_query.format(where_clause=where_clause)
    if animal:
        df = pd.read_sql(query, conn, params=(animal,))
    else:
        df = pd.read_sql(query, conn)
    return df

def analyze_animal(df_monthly, overall_dates, animal):
    # Filter data for the specific animal.
    data = df_monthly[df_monthly['animal'] == animal].copy()
    if data.empty:
        print(f"No monthly buckets with >25 events for animal: {animal}")
        return

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
        # Expected counts in this bucket (proportional to bucket size).
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
    return summary, data

def analyze_all_animals(df_monthly, overall_dates):
    summaries = []
    for animal, group in df_monthly.groupby('animal'):
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
            p_value = None
            test_result = "Not enough monthly buckets to test"
        else:
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
    return pd.DataFrame(summaries).sort_values("p_value", na_position="last")

def main():
    parser = argparse.ArgumentParser(description="Chi-square analysis of run table monthly failure rates.")
    parser.add_argument("--animal", type=str, help="Analyze a specific animal (optional)")
    args = parser.parse_args()

    # Connect to database using user jbrazeal.
    conn = psycopg2.connect(
        dbname="flake",
        host="localhost",
        port=5258,
        user="jbrazeal",   # your user
        password=""        # adjust if needed
    )

    df_monthly = get_monthly_data(conn, animal=args.animal)
    overall_dates = get_overall_dates(conn, animal=args.animal)
    conn.close()

    if args.animal:
        # Analyze a specific animal.
        summary, detailed = analyze_animal(df_monthly, overall_dates, args.animal)
        if summary is None:
            print(f"No sufficient data for animal '{args.animal}'.")
        else:
            print("Summary:")
            for key, value in summary.items():
                print(f"{key:20s}: {value}")
            print("\nDetailed monthly data:")
            # Format the detailed data.
            detailed = detailed.sort_values("month")
            print(detailed.to_string(index=False))
    else:
        # Analyze all animals.
        summary_df = analyze_all_animals(df_monthly, overall_dates)
        print("Summary for all animals:")
        print(summary_df.to_string(index=False))

if __name__ == "__main__":
    main()
