CREATE EXTENSION plpython3u;

CREATE OR REPLACE FUNCTION chi2_p_value(input_stat numeric, input_df integer)
RETURNS numeric AS $$
    import math
    # Return None if any input is missing.
    if input_stat is None or input_df is None:
        return None
    # If degrees of freedom are not positive, return 1.0 (no variation to test).
    if input_df <= 0:
        return 1.0

    # Convert inputs to float to avoid Decimal issues.
    stat_val = float(input_stat)
    df_val = float(input_df)
    
    ITMAX = 100
    EPS = 3e-7

    def gammaincc(a, x):
        # Validate parameters.
        if x < 0 or a <= 0:
            raise ValueError("Invalid arguments in gammaincc")
        # Use series expansion if x < a + 1.
        if x < a + 1:
            ap = a
            sum_val = 1.0 / a
            delta = sum_val
            for n in range(1, ITMAX+1):
                ap += 1
                delta *= x / ap
                sum_val += delta
                if abs(delta) < abs(sum_val)*EPS:
                    break
            glna = math.lgamma(a)
            P = sum_val * math.exp(-x + a*math.log(x) - glna)
            return 1 - P  # Q = 1 - P.
        else:
            # Continued fraction representation.
            glna = math.lgamma(a)
            b = x + 1 - a
            c = 1e-30  # Prevent division by zero.
            d = 1.0 / b
            h = d
            for i in range(1, ITMAX+1):
                an = -i * (i - a)
                b += 2
                d = an * d + b
                if abs(d) < 1e-30:
                    d = 1e-30
                c = b + an / c
                if abs(c) < 1e-30:
                    c = 1e-30
                d = 1.0 / d
                delta = d * c
                h *= delta
                if abs(delta - 1.0) < EPS:
                    break
            return math.exp(-x + a*math.log(x) - glna) * h

    # The survival function for chi-square is Q(df/2, stat/2).
    return float(gammaincc(df_val/2.0, stat_val/2.0))
$$ LANGUAGE plpython3u IMMUTABLE;

create materialized view by_month as
WITH rates AS (
    SELECT 
        animal, 
        date_trunc('month', snapshot) AS month, 
        SUM(CASE WHEN fail_stage IS NULL THEN 0 ELSE 1 END) AS failures, 
        COUNT(*) AS total 
    FROM run  
    GROUP BY animal, date_trunc('month', snapshot)
),
observed AS (
    SELECT 
        animal,
        month,
        failures,
        total - failures AS successes,
        total
    FROM rates
    WHERE total > 25
),
totals AS (
    SELECT 
        animal,
        SUM(failures) AS total_failures,
        SUM(successes) AS total_successes,
        SUM(total) AS grand_total
    FROM observed
    GROUP BY animal
),
expected AS (
    SELECT 
        o.animal,
        o.month,
        -- Expected failures in a bucket:
        (t.total_failures * o.total::NUMERIC / t.grand_total) AS expected_failures,
        -- Expected successes:
        (t.total_successes * o.total::NUMERIC / t.grand_total) AS expected_successes,
        o.failures,
        o.successes
    FROM observed o
    JOIN totals t ON o.animal = t.animal
),
chi_square AS (
    SELECT 
        animal,
        -- Chi-square: Sum over buckets of (observed - expected)^2/expected for both failures and successes.
        SUM(POWER(failures - expected_failures, 2) / NULLIF(expected_failures, 0)) +
        SUM(POWER(successes - expected_successes, 2) / NULLIF(expected_successes, 0)) AS chi_square_stat,
        COUNT(DISTINCT month) - 1 AS degrees_of_freedom  -- one less than the number of months
    FROM expected
    GROUP BY animal
)
SELECT 
    animal,
    chi_square_stat,
    degrees_of_freedom,
    chi2_p_value(chi_square_stat, degrees_of_freedom::integer) AS p_value,
    CASE 
        WHEN chi2_p_value(chi_square_stat, degrees_of_freedom::integer) < 0.05 
            THEN 'Reject Null Hypothesis: Rates are different across months'
        ELSE 'Fail to Reject Null Hypothesis: No significant difference in rates'
    END AS result
FROM chi_square
ORDER BY p_value;
