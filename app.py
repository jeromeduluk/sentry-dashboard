"""
Sentry Customer Analytics Dashboard
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from utils import database as db

# Page config
st.set_page_config(
    page_title="Sentry Customer Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar
st.sidebar.title("Sentry Analytics")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["Executive Summary", "Revenue Analysis", "Customer Growth", "Product Usage", "Segmentation"]
)

st.sidebar.markdown("---")
st.sidebar.info("""
**Data Range:** Sept 2021 - Sept 2023  
**Customers:** 11,766 unique accounts  
**Records:** 221,079 customer-months
""")
st.sidebar.markdown("---")
with st.sidebar.expander("📖 Metric Definitions"):
    st.markdown("""
    **Active Customers**: Customers with `is_active = TRUE` in the database
    
    **Monthly Active Users (MAU)**: Customers who generated events (events_28d > 0)
    
    **Annual Recurring Revenue (ARR)**: Yearly subscription revenue from active customers
    
    **Churn Rate**: % of previously active customers who became inactive in a given month
    
    **Free → Paid Rate**: % of all customers who converted from free tier to paid plan
    
    **Events**: Error/exception tracking events reported by customer applications
    - `events_daily`: Events on the last day of the month
    - `events_28d`: Total events in the 28-day rolling window
    
    **Inactive**: Active account with 0 events in the 28-day period
    """)

# Helper functions
@st.cache_data(ttl=600)
def load_monthly_metrics():
    return db.get_monthly_metrics()

@st.cache_data(ttl=600)
def load_arr_by_plan():
    return db.get_arr_by_plan()

@st.cache_data(ttl=600)
def load_plan_distribution():
    return db.get_plan_distribution()

@st.cache_data(ttl=600)
def load_churn_metrics():
    return db.get_churn_metrics()

@st.cache_data(ttl=600)
def load_new_customers():
    return db.get_new_customers()

@st.cache_data(ttl=600)
def load_usage_by_plan():
    return db.get_usage_by_plan()

@st.cache_data(ttl=600)
def load_geography():
    return db.get_revenue_by_geography()

@st.cache_data(ttl=600)
def load_industry():
    return db.get_industry_breakdown()

@st.cache_data(ttl=600)
def load_conversion():
    return db.get_conversion_funnel()

@st.cache_data(ttl=600)
def load_latest_snapshot():
    return db.get_latest_snapshot()

@st.cache_data(ttl=600)
def load_mau_metrics():
    return db.get_mau_metrics()

@st.cache_data(ttl=600)
def load_inactive_rate_current():
    return db.get_inactive_rate_current()

@st.cache_data(ttl=600)
def load_top_customers():
    return db.get_top_customers()

@st.cache_data(ttl=600)
def load_new_customers_of_note():
    return db.get_new_customers_of_note()

@st.cache_data(ttl=600)
def load_usage_trends_by_plan():
    return db.get_usage_trends_by_plan()

@st.cache_data(ttl=600)
def load_power_users():
    return db.get_power_users()

@st.cache_data(ttl=600)
def load_at_risk_accounts():
    return db.get_at_risk_accounts()

@st.cache_data(ttl=600)
def load_high_opportunity_clients():
    return db.get_high_opportunity_clients()

@st.cache_data(ttl=600)
def load_potential_referrals():
    return db.get_potential_referrals()



# PAGE 1: EXECUTIVE SUMMARY
if page == "Executive Summary":
    st.title("Executive Summary")
    
    snapshot = load_latest_snapshot()
    monthly_metrics = load_monthly_metrics()
    arr_by_plan = load_arr_by_plan()
    
    if not snapshot.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        active_customers = snapshot['active_customers'].iloc[0]
        total_arr = snapshot['total_arr'].iloc[0]
        prev_active = snapshot['prev_active_customers'].iloc[0]
        prev_arr = snapshot['prev_arr'].iloc[0]
        
        customer_growth = ((active_customers - prev_active) / prev_active * 100) if prev_active > 0 else 0
        arr_growth = ((total_arr - prev_arr) / prev_arr * 100) if prev_arr > 0 else 0
        
        with col1:
            st.metric("Active Customers", f"{active_customers:,}", f"{customer_growth:+.1f}% MoM")
        
        with col2:
            st.metric("Total ARR", f"${total_arr:,.0f}", f"{arr_growth:+.1f}% MoM")
        
        with col3:
            avg_arr = total_arr / active_customers if active_customers > 0 else 0
            st.metric("Avg ARR per Customer", f"${avg_arr:,.0f}")
        
        with col4:
            conversion = load_conversion()
            if not conversion.empty:
                st.metric("Free to Paid Rate", f"{conversion['conversion_rate_pct'].iloc[0]:.1f}%")
    

    st.markdown("---")
    
    st.subheader("Total ARR Trend")
    if not monthly_metrics.empty:
        # Calculate 3-month moving average
        monthly_metrics_ma = monthly_metrics.copy()
        monthly_metrics_ma['arr_3mo_ma'] = monthly_metrics_ma['total_arr'].rolling(window=3, min_periods=1).mean()
        
        # Create figure with both actual ARR and moving average
        fig = go.Figure()
        
        # Add actual ARR line (solid, prominent)
        fig.add_trace(go.Scatter(
            x=monthly_metrics_ma['month_date'],
            y=monthly_metrics_ma['total_arr'],
            name='Actual ARR',
            line=dict(color='#1f77b4', width=3),
            mode='lines'
        ))
        
        # Add 3-month moving average (dashed, muted)
        fig.add_trace(go.Scatter(
            x=monthly_metrics_ma['month_date'],
            y=monthly_metrics_ma['arr_3mo_ma'],
            name='3-Month Moving Average',
            line=dict(color='#ff7f0e', width=2, dash='dash'),
            mode='lines'
        ))
        
        fig.update_layout(
            xaxis_title='Month',
            yaxis_title='Total ARR ($)',
            hovermode='x unified',
            height=400,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")

    st.subheader("ARR by Plan Tier")
    st.caption("Annual Recurring Revenue from all active customers over time")
    if not arr_by_plan.empty:
        pivot_arr = arr_by_plan.pivot(index='month_date', columns='plan', values='total_arr').fillna(0)
        fig = go.Figure()
        for plan in ['Top', 'Middle', 'Low', 'Trial', 'Free']:
            if plan in pivot_arr.columns:
                fig.add_trace(go.Scatter(x=pivot_arr.index, y=pivot_arr[plan],
                                        name=plan, stackgroup='one'))
        fig.update_layout(hovermode='x unified', height=350)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Monthly Active Users vs Active Customers")
    st.caption("Active Customers (accounts) vs MAU (product usage). Gap indicates low engagement.")
    
    mau_data = load_mau_metrics()
    if not mau_data.empty:
        fig = go.Figure()
        
        # Add MAU line
        fig.add_trace(go.Scatter(
            x=mau_data['month_date'],
            y=mau_data['monthly_active_users'],
            name='Monthly Active Users',
            line=dict(color='#9467bd', width=3),
            mode='lines'
        ))
        
        # Add Active Customers line
        fig.add_trace(go.Scatter(
            x=mau_data['month_date'],
            y=mau_data['active_customers'],
            name='Active Customers',
            line=dict(color='#2ca02c', width=3),
            mode='lines'
        ))
        
        fig.update_layout(
            xaxis_title='Month',
            yaxis_title='Count',
            hovermode='x unified',
            height=400,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Add explanation
        st.info("""
        **Monthly Active Users (MAU)** = Customers who generated events (used the product)  
        **Active Customers** = Customers with active accounts
        
        The gap between these lines shows customers with active accounts who aren't using the product.
        """)


# PAGE 2: REVENUE ANALYSIS
elif page == "Revenue Analysis":
    st.title("Revenue Analysis")
    
    arr_by_plan = load_arr_by_plan()
    plan_dist = load_plan_distribution()
    geography = load_geography()
    
    st.subheader("Revenue by Plan Tier")
    if not arr_by_plan.empty:
        fig = px.line(arr_by_plan, x='month_date', y='total_arr', color='plan')
        fig.update_layout(hovermode='x unified', height=400)
        st.plotly_chart(fig, use_container_width=True)
    st.warning("""
        **Data Quality Note**: Customer ID 1003419 shows ARR in the Free tier for March 2022, 
        while all other months are marked as Top tier. This appears to be a data classification error. 
        The Free tier spike in that month should likely be attributed to Top tier instead.
        
        *This will be flagged for data engineering review before using in production reporting.*
        """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Plan Distribution")
        if not plan_dist.empty:
            latest = plan_dist[plan_dist['month_date'] == plan_dist['month_date'].max()]
            fig = px.pie(latest, values='customer_count', names='plan', hole=0.4)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Top Countries by ARR")
        if not geography.empty:
            fig = px.bar(geography.head(10), x='total_arr', y='country', orientation='h')
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    
    # Top Customers and New Customers sections
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top 10 Customers by ARR")
        top_customers = load_top_customers()
        if not top_customers.empty:
            # Format the dataframe for display
            display_df = top_customers.copy()
            # Handle NULL values before formatting
            display_df['total_arr'] = display_df['total_arr'].fillna(0).apply(lambda x: f"${x:,.2f}")
            # Reorder columns to match desired display order
            display_df = display_df[['customer_id', 'total_arr', 'years_as_client', 'plan']]
            display_df.columns = ['Customer ID', 'Total ARR', 'Years as Client', 'Plan']
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No customer data available")

    
    with col2:
        st.subheader("New Customers of Note (Last 3 Months)")
        new_customers = load_new_customers_of_note()
        if not new_customers.empty:
            # Format the dataframe for display
            display_df = new_customers.copy()
            # Handle NULL values before formatting
            display_df['total_arr'] = display_df['total_arr'].fillna(0).apply(lambda x: f"${x:,.0f}")
            display_df['joined_date'] = pd.to_datetime(display_df['joined_date']).dt.strftime('%b %Y')
            display_df = display_df[['customer_id', 'total_arr', 'joined_date', 'plan']]
            display_df.columns = ['Customer ID', 'Total ARR', 'Joined', 'Plan']
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No new customers in the last 3 months")


# PAGE 3: CUSTOMER GROWTH
elif page == "Customer Growth":
    st.title("Customer Growth & Retention")
    
    new_customers = load_new_customers()
    churn = load_churn_metrics()

    st.subheader("Free to Paid Conversion")
    conversion = load_conversion()
    if not conversion.empty:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Users", f"{conversion['total_users'].iloc[0]:,}")
        with col2:
            st.metric("Converted to Paid", f"{conversion['converted_users'].iloc[0]:,}")
        with col3:
            st.metric("Conversion Rate", f"{conversion['conversion_rate_pct'].iloc[0]:.1f}%")
  
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("New Customer Acquisition")
        if not new_customers.empty:
            fig = px.bar(new_customers, x='signup_month', y='new_customers')
            fig.update_traces(marker_color='#2ca02c')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Churn Rate Trend")
        if not churn.empty:
            fig = px.line(churn, x='month_date', y='churn_rate_pct')
            fig.update_traces(line_color='#d62728', line_width=3)
            st.plotly_chart(fig, use_container_width=True)
    

    st.markdown("""
    **Churn Rate** is calculated as: (Customers active last month who became inactive this month) / 
    (Total customers active last month). Note: This only tracks account status changes, not product usage.
    """)

    st.markdown("---")
    
    # High Opportunity Clients (Upsell Targets)
    st.subheader("High Opportunity Clients (Upsell Targets)")
    high_opp = load_high_opportunity_clients()
    if not high_opp.empty:
        display_opp = high_opp.copy()
        display_opp['joined_date'] = pd.to_datetime(display_opp['joined_date']).dt.strftime('%b %Y')
        display_opp.columns = ['Customer ID', 'Current Plan', 'Avg Events (28d)', 'Peak Events', 'Months Active', 'Joined']
        st.dataframe(display_opp, use_container_width=True, hide_index=True)
        st.success("""
        **Sales Opportunity:** Free or Trial tier customers with high engagement (>500 avg events in last 6 months). 
        These users are actively using the product and are prime candidates for conversion to paid plans.
        """)
    else:
        st.info("No high-opportunity free/trial users identified in the last 6 months")

        st.markdown("---")
    
    # Potential Referrals Section
    st.subheader("Potential Referrals - Moments of Delight")
    
    st.info("""
    **What is a "Moment of Delight"?**  
    Customers who demonstrate high product engagement (>100 avg events) AND upgrade their plan 
    within the first 6 months show strong product-market fit. This combination indicates the product 
    is meeting or exceeding their expectations - a perfect moment to ask for referrals.
    
    The sales team should prioritize outreach to these satisfied customers while their positive 
    experience is fresh.
    """)
    
    referrals = load_potential_referrals()
    
    if not referrals.empty:
        # Split into two groups for separate tables
        free_to_paid = referrals[referrals['upgrade_type'] == 'Free/Trial → Paid'].copy()
        paid_upgrades = referrals[referrals['upgrade_type'] == 'Paid Tier Upgrade'].copy()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Free/Trial → Paid Conversions**")
            if not free_to_paid.empty:
                display_f2p = free_to_paid[['customer_id', 'starting_plan_name', 'ending_plan_name', 
                                            'avg_events', 'peak_events', 'joined_date']].copy()
                display_f2p['joined_date'] = pd.to_datetime(display_f2p['joined_date']).dt.strftime('%b %Y')
                display_f2p.columns = ['Customer ID', 'From', 'To', 'Avg Events', 'Peak Events', 'Joined']
                st.dataframe(display_f2p, use_container_width=True, hide_index=True)
                st.caption(f"{len(free_to_paid)} customers converted from free to paid with high engagement")
            else:
                st.info("No free-to-paid conversions with high usage")
        
        with col2:
            st.markdown("**Paid Tier Upgrades**")
            if not paid_upgrades.empty:
                display_upgrades = paid_upgrades[['customer_id', 'starting_plan_name', 'ending_plan_name', 
                                                   'avg_events', 'peak_events', 'joined_date']].copy()
                display_upgrades['joined_date'] = pd.to_datetime(display_upgrades['joined_date']).dt.strftime('%b %Y')
                display_upgrades.columns = ['Customer ID', 'From', 'To', 'Avg Events', 'Peak Events', 'Joined']
                st.dataframe(display_upgrades, use_container_width=True, hide_index=True)
                st.caption(f"{len(paid_upgrades)} customers upgraded between paid tiers with high engagement")
            else:
                st.info("No paid tier upgrades with high usage")
    else:
        st.info("No referral opportunities identified based on current criteria")



# PAGE 4: PRODUCT USAGE
elif page == "Product Usage":
    st.title("Product Usage & Engagement")
    
    usage_by_plan = load_usage_by_plan()
    usage_trends = load_usage_trends_by_plan()
    power_users = load_power_users()
    at_risk = load_at_risk_accounts()
    
    # 1. Usage Trends Over Time by Plan
    st.subheader("Average Event Usage Trends by Plan")
    if not usage_trends.empty:
        fig = px.line(
            usage_trends, 
            x='month_date', 
            y='avg_events', 
            color='plan',
            labels={'month_date': 'Month', 'avg_events': 'Avg Events (28d)', 'plan': 'Plan'},
            category_orders={'plan': ['Free', 'Trial', 'Low', 'Middle', 'Top']}
        )
        fig.update_layout(hovermode='x unified', height=400)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Average number of events per customer over a 28-day rolling window, grouped by plan tier.")
    
    st.markdown("---")
    
    # 2. Summary Stats Table
    st.subheader("Usage Summary by Plan")
    if not usage_by_plan.empty:
        display_usage = usage_by_plan.copy()
        display_usage.columns = ['Plan', 'Customers', 'Customer-Months', 'Avg Daily Events', 
                                  'Median Daily Events', 'Avg 28d Events', 'Median 28d Events',
                                  'Inactive Months', '% Inactive']
        st.dataframe(display_usage, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # 3. Power Users and At-Risk Accounts side-by-side
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Power Users (Top 10 by Events)")
        if not power_users.empty:
            display_power = power_users.copy()
            display_power['arr'] = display_power['arr'].fillna(0).apply(lambda x: f"${x:,.0f}")
            display_power.columns = ['Customer ID', 'Plan', 'ARR', 'Total Events (28d)', 'Avg Events/Day']
            st.dataframe(display_power, use_container_width=True, hide_index=True)
            st.caption("Customers generating the most events. Heavy users on lower tiers may be expansion opportunities.")
        else:
            st.info("No power user data available")
    
    with col2:
        st.subheader("At-Risk Accounts")
        if not at_risk.empty:
            display_risk = at_risk.copy()
            display_risk['arr'] = display_risk['arr'].fillna(0).apply(lambda x: f"${x:,.0f}")
            display_risk.columns = ['Customer ID', 'Plan', 'ARR', 'Total Events (28d)', 'Avg Events/Day']
            st.dataframe(display_risk, use_container_width=True, hide_index=True)
            st.info("""
            **At-Risk Criteria:** ARR > $1,000 AND total events < 100 in the last 28 days. 
            These customers are paying but not actively using the product, indicating potential churn risk.
            """)
        else:
            st.info("No at-risk accounts identified with current criteria (ARR > $1,000, Events < 100)")


# PAGE 5: SEGMENTATION
elif page == "Segmentation":
    st.title("Customer Segmentation")
    
    industry = load_industry()
    geography = load_geography()

        # Add data quality note below the charts
    st.warning("""
    **Data Quality Note:** Approximately 76% of customer records are missing country and industry data. 
    This is expected for free-tier accounts where demographic information is not collected. 
    The charts above represent only customers with complete geographic and industry information.
    """)
    
    st.subheader("Top Industries by ARR")
    if not industry.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(industry.head(10), x='total_arr', y='industry', orientation='h')
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Format ARR as currency
            display_ind = industry[['industry', 'unique_customers', 'total_arr']].head(10).copy()
            display_ind['total_arr'] = display_ind['total_arr'].apply(lambda x: f"${x:,.0f}")
            st.dataframe(display_ind, use_container_width=True, hide_index=True)
    


    st.subheader("Top Countries by ARR")
    col1, col2 = st.columns(2)
    with col1:
        if not geography.empty:
            top_10_geo = geography.head(10)
            fig = px.bar(
                top_10_geo, 
                x='total_arr', 
                y='country', 
                orientation='h',
                labels={'total_arr': 'Total ARR ($)', 'country': 'Country'}
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
            fig.update_traces(marker_color='#2ca02c')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if not geography.empty:
            # Format ARR as currency
            display_geo = geography[['country', 'unique_customers', 'total_arr']].head(10).copy()
            display_geo['total_arr'] = display_geo['total_arr'].apply(lambda x: f"${x:,.0f}")
            st.dataframe(display_geo, use_container_width=True, hide_index=True)
    


st.sidebar.markdown("---")
st.sidebar.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*")
