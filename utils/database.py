"""
Database connection and query functions for Sentry customer data.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_db_engine():
    """Create and return a SQLAlchemy engine for Supabase/PostgreSQL."""
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_port = os.getenv('DB_PORT', '5432')
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)
    return engine


def query_to_dataframe(query):
    """Execute a SQL query and return results as a pandas DataFrame."""
    engine = get_db_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    return df


def get_monthly_metrics():
    """Get monthly summary metrics: active customers, ARR, events."""
    query = """
    SELECT 
        month_date,
        COUNT(DISTINCT CASE WHEN is_active THEN id END) as active_customers,
        COUNT(DISTINCT id) as total_customers,
        ROUND(SUM(CASE WHEN is_active THEN annual_recurring_revenue ELSE 0 END), 2) as total_arr,
        ROUND(AVG(CASE WHEN is_active THEN annual_recurring_revenue END), 2) as avg_arr,
        ROUND(SUM(CASE WHEN is_active THEN events_28d ELSE 0 END), 0) as total_events_28d,
        ROUND(AVG(CASE WHEN is_active THEN events_28d END), 0) as avg_events_28d
    FROM customer_monthly_metrics
    GROUP BY month_date
    ORDER BY month_date;
    """
    return query_to_dataframe(query)


def get_arr_by_plan():
    """Get monthly ARR breakdown by plan tier."""
    query = """
    SELECT 
        month_date,
        plan,
        COUNT(DISTINCT id) as customers,
        ROUND(SUM(annual_recurring_revenue), 2) as total_arr,
        ROUND(AVG(annual_recurring_revenue), 2) as avg_arr
    FROM customer_monthly_metrics
    WHERE is_active
    GROUP BY month_date, plan
    ORDER BY month_date, 
        CASE plan 
            WHEN 'Free' THEN 1 
            WHEN 'Trial' THEN 2 
            WHEN 'Low' THEN 3 
            WHEN 'Middle' THEN 4 
            WHEN 'Top' THEN 5 
        END;
    """
    return query_to_dataframe(query)


def get_plan_distribution():
    """Get customer distribution by plan over time."""
    query = """
    SELECT 
        month_date,
        plan,
        COUNT(DISTINCT id) as customer_count
    FROM customer_monthly_metrics
    WHERE is_active
    GROUP BY month_date, plan
    ORDER BY month_date, 
        CASE plan 
            WHEN 'Free' THEN 1 
            WHEN 'Trial' THEN 2 
            WHEN 'Low' THEN 3 
            WHEN 'Middle' THEN 4 
            WHEN 'Top' THEN 5 
        END;
    """
    return query_to_dataframe(query)


def get_churn_metrics():
    """Calculate monthly churn rate."""
    query = """
    WITH monthly_status AS (
        SELECT 
            id,
            month_date,
            is_active,
            LAG(is_active) OVER (PARTITION BY id ORDER BY month_date) as prev_month_active
        FROM customer_monthly_metrics
    )
    SELECT 
        month_date,
        COUNT(CASE WHEN prev_month_active THEN 1 END) as prev_month_active_count,
        COUNT(CASE WHEN prev_month_active AND NOT is_active THEN 1 END) as churned_count,
        ROUND(100.0 * 
            COUNT(CASE WHEN prev_month_active AND NOT is_active THEN 1 END) / 
            NULLIF(COUNT(CASE WHEN prev_month_active THEN 1 END), 0), 2
        ) as churn_rate_pct
    FROM monthly_status
    WHERE prev_month_active IS NOT NULL
    GROUP BY month_date
    ORDER BY month_date;
    """
    return query_to_dataframe(query)


def get_new_customers():
    """Get monthly new customer acquisition."""
    query = """
    SELECT 
        DATE_TRUNC('month', account_created_date)::DATE as signup_month,
        COUNT(DISTINCT id) as new_customers,
        COUNT(DISTINCT CASE WHEN first_paid_date IS NOT NULL THEN id END) as new_paying_customers
    FROM (
        SELECT DISTINCT ON (id) 
            id, 
            account_created_date,
            first_paid_date
        FROM customer_monthly_metrics
        WHERE account_created_date IS NOT NULL
    ) customer_first_seen
    GROUP BY DATE_TRUNC('month', account_created_date)
    ORDER BY signup_month;
    """
    return query_to_dataframe(query)


def get_usage_by_plan():
    """Get event usage statistics by plan tier."""
    query = """
    SELECT 
        plan,
        COUNT(DISTINCT id) as unique_customers,
        COUNT(*) as customer_months,
        ROUND(AVG(events_daily)::numeric, 0) as avg_events_daily,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY events_daily)::numeric, 0) as median_events_daily,
        ROUND(AVG(events_28d)::numeric, 0) as avg_events_28d,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY events_28d)::numeric, 0) as median_events_28d,
        COUNT(CASE WHEN events_28d = 0 THEN 1 END) as inactive_months,
        ROUND(100.0 * COUNT(CASE WHEN events_28d = 0 THEN 1 END) / COUNT(*), 2) as pct_inactive
    FROM customer_monthly_metrics
    WHERE is_active
    GROUP BY plan
    ORDER BY 
        CASE plan 
            WHEN 'Free' THEN 1 
            WHEN 'Trial' THEN 2
            WHEN 'Low' THEN 3 
            WHEN 'Middle' THEN 4
            WHEN 'Top' THEN 5 
        END;
    """
    return query_to_dataframe(query)


def get_revenue_by_geography():
    """Get revenue breakdown by country for the most recent month."""
    query = """
    WITH latest_month AS (
        SELECT MAX(month_date) as max_date 
        FROM customer_monthly_metrics
    )
    SELECT 
        country,
        COUNT(DISTINCT id) as unique_customers,
        ROUND(SUM(annual_recurring_revenue), 2) as total_arr
    FROM customer_monthly_metrics
    WHERE country IS NOT NULL 
        AND country != ''
        AND month_date = (SELECT max_date FROM latest_month)
        AND is_active = TRUE
    GROUP BY country
    ORDER BY total_arr DESC;
    """
    return query_to_dataframe(query)


def get_industry_breakdown():
    """Get revenue breakdown by industry for the most recent month."""
    query = """
    WITH latest_month AS (
        SELECT MAX(month_date) as max_date 
        FROM customer_monthly_metrics
    )
    SELECT 
        industry,
        COUNT(DISTINCT id) as unique_customers,
        ROUND(SUM(annual_recurring_revenue), 2) as total_arr
    FROM customer_monthly_metrics
    WHERE industry IS NOT NULL 
        AND industry != ''
        AND month_date = (SELECT max_date FROM latest_month)
        AND is_active = TRUE
    GROUP BY industry
    ORDER BY total_arr DESC;
    """
    return query_to_dataframe(query)



def get_conversion_funnel():
    """Get free to paid conversion statistics."""
    query = """
    WITH customer_journey AS (
        SELECT 
            id,
            MIN(month_date) as first_month,
            MIN(CASE WHEN annual_recurring_revenue > 0 THEN month_date END) as first_paid_month
        FROM customer_monthly_metrics
        GROUP BY id
    )
    SELECT 
        COUNT(*) as total_users,
        COUNT(first_paid_month) as converted_users,
        ROUND(100.0 * COUNT(first_paid_month) / COUNT(*), 2) as conversion_rate_pct,
        ROUND(AVG(first_paid_month - first_month), 1) as avg_days_to_convert
    FROM customer_journey;
    """
    return query_to_dataframe(query)


def get_latest_snapshot():
    """Get most recent month's key metrics for KPI cards."""
    query = """
    WITH latest_month AS (
        SELECT MAX(month_date) as max_date FROM customer_monthly_metrics
    ),
    prev_month AS (
        SELECT MAX(month_date) as prev_date 
        FROM customer_monthly_metrics 
        WHERE month_date < (SELECT max_date FROM latest_month)
    )
    SELECT 
        (SELECT COUNT(DISTINCT id) FROM customer_monthly_metrics 
         WHERE month_date = (SELECT max_date FROM latest_month) AND is_active) as active_customers,
        (SELECT ROUND(SUM(annual_recurring_revenue), 2) FROM customer_monthly_metrics 
         WHERE month_date = (SELECT max_date FROM latest_month) AND is_active) as total_arr,
        (SELECT COUNT(DISTINCT id) FROM customer_monthly_metrics 
         WHERE month_date = (SELECT prev_date FROM prev_month) AND is_active) as prev_active_customers,
        (SELECT ROUND(SUM(annual_recurring_revenue), 2) FROM customer_monthly_metrics 
         WHERE month_date = (SELECT prev_date FROM prev_month) AND is_active) as prev_arr;
    """
    return query_to_dataframe(query)

def get_mau_metrics():
    """Get Monthly Active Users (customers with events) over time."""
    query = """
    SELECT 
        month_date,
        COUNT(DISTINCT CASE WHEN events_28d > 0 THEN id END) as monthly_active_users,
        COUNT(DISTINCT CASE WHEN is_active THEN id END) as active_customers,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN events_28d > 0 THEN id END) / 
              NULLIF(COUNT(DISTINCT CASE WHEN is_active THEN id END), 0), 2) as engagement_rate
    FROM customer_monthly_metrics
    GROUP BY month_date
    ORDER BY month_date;
    """
    return query_to_dataframe(query)

def get_inactive_rate_current():
    """Get inactive rate by plan for the most recent month."""
    query = """
    WITH latest_month AS (
        SELECT MAX(month_date) as max_date 
        FROM customer_monthly_metrics
    )
    SELECT 
        plan,
        COUNT(*) as total_customers,
        COUNT(CASE WHEN events_28d = 0 THEN 1 END) as inactive_count,
        ROUND(100.0 * COUNT(CASE WHEN events_28d = 0 THEN 1 END) / COUNT(*), 2) as pct_inactive
    FROM customer_monthly_metrics
    WHERE month_date = (SELECT max_date FROM latest_month)
        AND is_active = TRUE
    GROUP BY plan
    ORDER BY 
        CASE plan 
            WHEN 'Free' THEN 1 
            WHEN 'Trial' THEN 2
            WHEN 'Low' THEN 3 
            WHEN 'Middle' THEN 4
            WHEN 'Top' THEN 5 
        END;
    """
    return query_to_dataframe(query)

def get_top_customers():
    """Get top 10 customers by ARR in the most recent month."""
    query = """
    WITH latest_month AS (
        SELECT MAX(month_date) as max_date 
        FROM customer_monthly_metrics
    )
    SELECT 
        id as customer_id,
        annual_recurring_revenue as total_arr,
        ROUND(EXTRACT(YEAR FROM AGE((SELECT max_date FROM latest_month), account_created_date)) + 
              EXTRACT(MONTH FROM AGE((SELECT max_date FROM latest_month), account_created_date)) / 12.0, 1) as years_as_client,
        plan
    FROM customer_monthly_metrics
    WHERE month_date = (SELECT max_date FROM latest_month)
        AND is_active = TRUE
        AND annual_recurring_revenue > 0
    ORDER BY annual_recurring_revenue DESC
    LIMIT 10;
    """
    return query_to_dataframe(query)


def get_new_customers_of_note():
    """Get top 10 largest new customers from the last 3 months."""
    query = """
    WITH recent_months AS (
        SELECT DISTINCT month_date
        FROM customer_monthly_metrics
        ORDER BY month_date DESC
        LIMIT 3
    ),
    new_customers AS (
        SELECT 
            id,
            MIN(month_date) as first_seen
        FROM customer_monthly_metrics
        GROUP BY id
        HAVING MIN(month_date) IN (SELECT month_date FROM recent_months)
    ),
    customer_metrics AS (
        SELECT 
            nc.id,
            MAX(c.plan) as current_plan,
            SUM(c.annual_recurring_revenue) as total_arr,
            nc.first_seen,
            ROUND(EXTRACT(YEAR FROM AGE(MAX(c.month_date), nc.first_seen)) + 
                  EXTRACT(MONTH FROM AGE(MAX(c.month_date), nc.first_seen)) / 12.0, 1) as years_as_client
        FROM new_customers nc
        JOIN customer_monthly_metrics c ON nc.id = c.id
        WHERE c.is_active
        GROUP BY nc.id, nc.first_seen
    )
    SELECT 
        id as customer_id,
        ROUND(total_arr, 2) as total_arr,
        years_as_client,
        current_plan as plan,
        first_seen as joined_date
    FROM customer_metrics
    ORDER BY total_arr DESC
    LIMIT 10;
    """
    return query_to_dataframe(query)

def get_usage_trends_by_plan():
    """Get average event usage trends over time by plan tier."""
    query = """
    SELECT 
        month_date,
        plan,
        ROUND(AVG(events_28d)::numeric, 0) as avg_events
    FROM customer_monthly_metrics
    WHERE is_active
    GROUP BY month_date, plan
    ORDER BY month_date, 
        CASE plan 
            WHEN 'Free' THEN 1 
            WHEN 'Trial' THEN 2
            WHEN 'Low' THEN 3 
            WHEN 'Middle' THEN 4
            WHEN 'Top' THEN 5 
        END;
    """
    return query_to_dataframe(query)


def get_power_users():
    """Get top 10 customers by event volume with ARR and plan (most recent month)."""
    query = """
    WITH latest_month AS (
        SELECT MAX(month_date) as max_date 
        FROM customer_monthly_metrics
    )
    SELECT 
        id as customer_id,
        plan,
        annual_recurring_revenue as arr,
        events_28d as total_events,
        ROUND(events_28d / 28.0, 1) as avg_events_per_day
    FROM customer_monthly_metrics
    WHERE month_date = (SELECT max_date FROM latest_month)
        AND is_active = TRUE
        AND events_28d > 0
    ORDER BY events_28d DESC
    LIMIT 10;
    """
    return query_to_dataframe(query)


def get_at_risk_accounts():
    """Get high-ARR customers with low usage (churn risk indicators)."""
    query = """
    WITH latest_month AS (
        SELECT MAX(month_date) as max_date 
        FROM customer_monthly_metrics
    ),
    customer_usage AS (
        SELECT 
            id,
            annual_recurring_revenue as arr,
            plan,
            events_28d,
            ROUND(events_28d / 28.0, 1) as avg_events_per_day
        FROM customer_monthly_metrics
        WHERE month_date = (SELECT max_date FROM latest_month)
            AND is_active = TRUE
            AND annual_recurring_revenue > 0
    )
    SELECT 
        id as customer_id,
        plan,
        arr,
        events_28d as total_events,
        avg_events_per_day
    FROM customer_usage
    WHERE events_28d < 100  -- Low usage threshold
        AND arr > 1000  -- Only customers paying meaningful amounts
    ORDER BY arr DESC
    LIMIT 10;
    """
    return query_to_dataframe(query)

def get_high_opportunity_clients():
    """Get Free/Trial tier customers with high usage (upsell opportunities) from last 6 months."""
    query = """
    WITH recent_6_months AS (
        SELECT DISTINCT month_date
        FROM customer_monthly_metrics
        ORDER BY month_date DESC
        LIMIT 6
    ),
    high_usage_free AS (
        SELECT 
            c.id as customer_id,
            c.plan,
            c.account_created_date,
            AVG(c.events_28d) as avg_events,
            MAX(c.events_28d) as max_events,
            COUNT(*) as active_months
        FROM customer_monthly_metrics c
        WHERE c.month_date IN (SELECT month_date FROM recent_6_months)
            AND c.is_active = TRUE
            AND c.plan IN ('Free', 'Trial')
            AND c.events_28d > 0
        GROUP BY c.id, c.plan, c.account_created_date
        HAVING AVG(c.events_28d) > 500  -- High usage threshold
    )
    SELECT 
        customer_id,
        plan,
        ROUND(avg_events, 0) as avg_events_28d,
        max_events as peak_events_28d,
        active_months as months_active,
        account_created_date as joined_date
    FROM high_usage_free
    ORDER BY avg_events DESC
    LIMIT 15;
    """
    return query_to_dataframe(query)

def get_potential_referrals():
    """Get customers who upgraded in first 6 months with high usage (referral opportunities)."""
    query = """
    WITH customer_plan_history AS (
        SELECT 
            id,
            month_date,
            plan,
            account_created_date,
            events_28d,
            annual_recurring_revenue,
            CASE plan 
                WHEN 'Free' THEN 1 
                WHEN 'Trial' THEN 2
                WHEN 'Low' THEN 3 
                WHEN 'Middle' THEN 4
                WHEN 'Top' THEN 5 
            END as plan_rank,
            EXTRACT(YEAR FROM AGE(month_date, account_created_date)) * 12 + 
            EXTRACT(MONTH FROM AGE(month_date, account_created_date)) as months_since_joining
        FROM customer_monthly_metrics
        WHERE account_created_date IS NOT NULL
    ),
    first_six_months AS (
        SELECT *
        FROM customer_plan_history
        WHERE months_since_joining <= 6
    ),
    plan_changes AS (
        SELECT 
            id,
            MIN(plan_rank) as starting_plan_rank,
            MAX(plan_rank) as ending_plan_rank,
            MIN(CASE WHEN plan_rank = 1 OR plan_rank = 2 THEN plan END) as starting_plan_name,
            MAX(plan) as ending_plan_name,
            ROUND(AVG(events_28d)::numeric, 0) as avg_events_first_6mo,
            MAX(events_28d) as peak_events_first_6mo,
            COUNT(DISTINCT month_date) as months_observed,
            MIN(account_created_date) as joined_date
        FROM first_six_months
        GROUP BY id
        HAVING MAX(plan_rank) > MIN(plan_rank)
    )
    SELECT 
        id as customer_id,
        starting_plan_name,
        ending_plan_name,
        avg_events_first_6mo as avg_events,
        peak_events_first_6mo as peak_events,
        months_observed,
        joined_date,
        CASE 
            WHEN starting_plan_rank IN (1, 2) AND ending_plan_rank >= 3 THEN 'Free/Trial → Paid'
            WHEN starting_plan_rank >= 3 AND ending_plan_rank > starting_plan_rank THEN 'Paid Tier Upgrade'
        END as upgrade_type
    FROM plan_changes
    WHERE avg_events_first_6mo > 100
        AND (
            (starting_plan_rank IN (1, 2) AND ending_plan_rank >= 3) OR
            (starting_plan_rank >= 3 AND ending_plan_rank > starting_plan_rank)
        )
    ORDER BY 
        CASE 
            WHEN starting_plan_rank IN (1, 2) AND ending_plan_rank >= 3 THEN 1
            WHEN starting_plan_rank >= 3 AND ending_plan_rank > starting_plan_rank THEN 2
        END,
        avg_events_first_6mo DESC;
    """
    return query_to_dataframe(query)


