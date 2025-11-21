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
    
    # Force IPv4 connection
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?options=-c%20address_family=ipv4"
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
    """Get ARR breakdown by country."""
    query = """
    SELECT 
        COALESCE(country, 'Unknown') as country,
        COUNT(DISTINCT id) as unique_customers,
        ROUND(SUM(annual_recurring_revenue), 2) as total_arr,
        ROUND(AVG(annual_recurring_revenue), 2) as avg_arr
    FROM customer_monthly_metrics
    WHERE is_active
    GROUP BY country
    HAVING SUM(annual_recurring_revenue) > 0
    ORDER BY total_arr DESC
    LIMIT 20;
    """
    return query_to_dataframe(query)


def get_industry_breakdown():
    """Get customer and revenue metrics by industry."""
    query = """
    SELECT 
        COALESCE(industry, 'Unknown') as industry,
        COUNT(DISTINCT id) as unique_customers,
        ROUND(SUM(annual_recurring_revenue), 2) as total_arr,
        ROUND(AVG(CASE WHEN annual_recurring_revenue > 0 THEN annual_recurring_revenue END), 2) as avg_arr_paying,
        ROUND(AVG(events_28d), 0) as avg_events_28d
    FROM customer_monthly_metrics
    WHERE is_active
    GROUP BY industry
    HAVING COUNT(DISTINCT id) >= 10
    ORDER BY total_arr DESC
    LIMIT 15;
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