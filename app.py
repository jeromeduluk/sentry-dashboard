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
    st.caption("Annual Recurring Revenue from all active customers over time")
    if not monthly_metrics.empty:
        fig = px.line(monthly_metrics, x='month_date', y='total_arr',
                     labels={'month_date': 'Month', 'total_arr': 'Total ARR ($)'})
        fig.update_traces(line_color='#1f77b4', line_width=3)
        fig.update_layout(hovermode='x unified', height=400)
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

# PAGE 4: PRODUCT USAGE
elif page == "Product Usage":
    st.title("Product Usage & Engagement")
    
    usage_by_plan = load_usage_by_plan()
    monthly_metrics = load_monthly_metrics()
    
    st.subheader("Event Usage by Plan Tier")
    if not usage_by_plan.empty:
        st.dataframe(usage_by_plan, use_container_width=True, hide_index=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Average Events by Plan")
        if not usage_by_plan.empty:
            fig = px.bar(usage_by_plan, x='plan', y='avg_events_28d')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Inactive Rate by Plan (Current Month)")
        inactive_current = load_inactive_rate_current()
        if not inactive_current.empty:
            # Add latest month to title
            latest_month = inactive_current.iloc[0] if len(inactive_current) > 0 else None
            
            fig = px.bar(
                inactive_current, 
                x='plan', 
                y='pct_inactive',
                text='pct_inactive',
                labels={'pct_inactive': 'Inactive Rate (%)', 'plan': 'Plan Tier'},
                category_orders={'plan': ['Free', 'Trial', 'Low', 'Middle', 'Top']}
            )
            fig.update_traces(
                marker_color='#d62728',
                texttemplate='%{text:.1f}%',
                textposition='outside'
            )
            fig.update_layout(height=350, yaxis_range=[0, max(inactive_current['pct_inactive']) * 1.1])
            st.plotly_chart(fig, use_container_width=True)
            
            st.caption("Inactive = Active customers with 0 events in 28-day period")
        else:
            st.warning("No inactive rate data available")

# PAGE 5: SEGMENTATION
elif page == "Segmentation":
    st.title("Customer Segmentation")
    
    industry = load_industry()
    geography = load_geography()
    
    st.subheader("Top Industries by Revenue")
    if not industry.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig = px.bar(industry.head(10), x='total_arr', y='industry', orientation='h')
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.dataframe(industry[['industry', 'unique_customers', 'total_arr']].head(10),
                        use_container_width=True, hide_index=True)

st.sidebar.markdown("---")
st.sidebar.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*")
