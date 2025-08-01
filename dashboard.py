"""Tesla Competitive Intelligence Dashboard - Streamlit Application."""
import logging
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import date, datetime, timedelta
from streamlit_autorefresh import st_autorefresh

from dashboard_service import DashboardDataService
from config import (
    settings, COMPANY_NAMES, COMPANY_COLORS, DASHBOARD_METRICS
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title=settings.dashboard_page_title if settings else "Tesla Dashboard",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize dashboard service
@st.cache_resource
def get_dashboard_service():
    """Initialize and cache dashboard service."""
    return DashboardDataService()

# Cache data with TTL
@st.cache_data(ttl=settings.dashboard_cache_ttl if settings else 300)
def load_financial_data(tickers, start_date, end_date, metrics):
    """Load financial data with caching."""
    service = get_dashboard_service()
    return service.get_quarterly_financials(tickers, start_date, end_date, metrics)

@st.cache_data(ttl=settings.dashboard_cache_ttl if settings else 300)
def load_performance_metrics(ticker):
    """Load performance metrics with caching."""
    service = get_dashboard_service()
    return service.get_performance_metrics(ticker)

@st.cache_data(ttl=settings.dashboard_cache_ttl if settings else 300)
def load_comparison_data(metric, quarters):
    """Load comparison data with caching."""
    service = get_dashboard_service()
    return service.get_comparison_data(metric, quarters)

def create_revenue_chart(df):
    """Create quarterly revenue comparison chart."""
    if df.empty:
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    fig = px.line(
        df, 
        x='quarter_date', 
        y='revenue',
        color='ticker',
        title='Quarterly Revenue Comparison',
        labels={'revenue': 'Revenue ($B)', 'quarter_date': 'Quarter'},
        color_discrete_map=COMPANY_COLORS
    )
    
    # Format y-axis to show billions
    fig.update_yaxes(tickformat='.1f')
    
    # Update layout
    fig.update_layout(
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400
    )
    
    return fig

def create_eps_chart(df):
    """Create EPS performance trends chart."""
    if df.empty:
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    fig = px.line(
        df,
        x='quarter_date',
        y='eps',
        color='ticker',
        title='Earnings Per Share (EPS) Trends',
        labels={'eps': 'EPS ($)', 'quarter_date': 'Quarter'},
        color_discrete_map=COMPANY_COLORS
    )
    
    # Add horizontal line at zero
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    
    fig.update_layout(
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400
    )
    
    return fig

def create_gross_profit_chart(df):
    """Create gross profit analysis chart."""
    if df.empty:
        return go.Figure().add_annotation(
            text="No data available", xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    fig = px.bar(
        df,
        x='quarter_label',
        y='gross_profit',
        color='ticker',
        title='Gross Profit by Quarter',
        labels={'gross_profit': 'Gross Profit ($B)', 'quarter_label': 'Quarter'},
        color_discrete_map=COMPANY_COLORS,
        barmode='group'
    )
    
    fig.update_layout(
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400
    )
    
    return fig

def display_kpi_cards():
    """Display KPI cards for Tesla performance."""
    tesla_metrics = load_performance_metrics('TSLA')
    
    if not tesla_metrics:
        st.warning("No Tesla performance data available")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        revenue = tesla_metrics.get('latest_revenue', 0)
        revenue_growth = tesla_metrics.get('revenue_growth', 0)
        st.metric(
            "Latest Revenue",
            f"${revenue/1e9:.1f}B" if revenue else "N/A",
            f"{revenue_growth:+.1f}%" if revenue_growth else None
        )
    
    with col2:
        eps = tesla_metrics.get('latest_eps', 0)
        eps_growth = tesla_metrics.get('eps_growth', 0)
        st.metric(
            "Latest EPS",
            f"${eps:.2f}" if eps else "N/A",
            f"{eps_growth:+.1f}%" if eps_growth else None
        )
    
    with col3:
        gross_profit = tesla_metrics.get('latest_gross_profit', 0)
        gp_growth = tesla_metrics.get('gross_profit_growth', 0)
        st.metric(
            "Latest Gross Profit",
            f"${gross_profit/1e9:.1f}B" if gross_profit else "N/A",
            f"{gp_growth:+.1f}%" if gp_growth else None
        )
    
    with col4:
        quarter = tesla_metrics.get('latest_quarter', 'N/A')
        st.metric(
            "Latest Quarter",
            quarter,
            None
        )

def main():
    """Main dashboard application."""
    # Auto-refresh functionality
    if settings and hasattr(settings, 'dashboard_auto_refresh'):
        st_autorefresh(interval=settings.dashboard_auto_refresh * 1000, key="data_refresh")
    
    # Header
    st.title("🚗 Tesla Competitive Intelligence Dashboard")
    st.markdown("Real-time analysis of Tesla vs EV competitors performance")
    
    # Sidebar filters
    st.sidebar.header("Filters & Settings")
    
    # Company selection
    available_companies = list(COMPANY_NAMES.keys())
    selected_companies = st.sidebar.multiselect(
        "Select Companies",
        options=available_companies,
        default=available_companies,
        format_func=lambda x: f"{x} - {COMPANY_NAMES[x]}"
    )
    
    # Date range selection
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=date.today() - timedelta(days=730),  # 2 years ago
            max_value=date.today()
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            value=date.today(),
            max_value=date.today()
        )
    
    # Quarters for comparison
    quarters_to_show = st.sidebar.slider(
        "Quarters to Display",
        min_value=4,
        max_value=16,
        value=8,
        step=1
    )
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Data freshness indicator
    try:
        service = get_dashboard_service()
        freshness = service.get_data_freshness()
        if freshness:
            latest_update = max(freshness.values())
            st.sidebar.info(f"Data last updated: {latest_update.strftime('%Y-%m-%d %H:%M')}")
    except Exception as e:
        st.sidebar.error(f"Could not check data freshness: {e}")
    
    # Main content area
    if not selected_companies:
        st.warning("Please select at least one company to display data.")
        return
    
    # KPI Cards (Tesla-focused)
    if 'TSLA' in selected_companies:
        st.header("📊 Tesla Key Performance Indicators")
        display_kpi_cards()
        st.divider()
    
    # Load financial data
    try:
        financial_data = load_financial_data(
            selected_companies, start_date, end_date, None
        )
        
        if financial_data.empty:
            st.warning("No financial data available for the selected criteria.")
            return
        
        # Convert revenue and gross_profit to billions for display
        financial_data['revenue'] = financial_data['revenue'] / 1e9
        financial_data['gross_profit'] = financial_data['gross_profit'] / 1e9
        
    except Exception as e:
        st.error(f"Error loading financial data: {e}")
        return
    
    # Charts section
    st.header("📈 Financial Performance Charts")
    
    # Three-column layout for charts
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.plotly_chart(
            create_revenue_chart(financial_data),
            use_container_width=True
        )
    
    with col2:
        st.plotly_chart(
            create_eps_chart(financial_data),
            use_container_width=True
        )
    
    with col3:
        st.plotly_chart(
            create_gross_profit_chart(financial_data),
            use_container_width=True
        )
    
    # Detailed data table
    st.header("📋 Detailed Financial Data")
    
    # Format data for display
    display_data = financial_data.copy()
    display_data['revenue'] = display_data['revenue'].apply(lambda x: f"${x:.1f}B" if pd.notna(x) else "N/A")
    display_data['eps'] = display_data['eps'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
    display_data['gross_profit'] = display_data['gross_profit'].apply(lambda x: f"${x:.1f}B" if pd.notna(x) else "N/A")
    
    st.dataframe(
        display_data[['ticker', 'company_name', 'quarter_label', 'revenue', 'eps', 'gross_profit']].rename(
            columns={
                'ticker': 'Ticker',
                'company_name': 'Company',
                'quarter_label': 'Quarter',
                'revenue': 'Revenue',
                'eps': 'EPS',
                'gross_profit': 'Gross Profit'
            }
        ),
        use_container_width=True,
        hide_index=True
    )
    
    # Footer with system info
    st.divider()
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.caption("🔄 Auto-refresh: 5 minutes")
    
    with col2:
        st.caption("📊 Data source: PostgreSQL ETL Pipeline")
    
    with col3:
        try:
            health = service.health_check()
            status_emoji = "🟢" if health.get('status') == 'healthy' else "🔴"
            st.caption(f"{status_emoji} System: {health.get('status', 'unknown')}")
        except:
            st.caption("🔴 System: status unknown")

if __name__ == "__main__":
    main()