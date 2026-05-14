"""百事微促达 - 门店360°投资决策看板 · Linear风格"""
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

st.set_page_config(page_title="门店360°看板", page_icon="▦", layout="wide")

DB_PATH = os.path.join(os.path.dirname(__file__), "store360.duckdb")

# ============ GLOBAL CSS ============
st.markdown("""
<style>
/* --- Import Inter font --- */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* --- Root overrides --- */
html, body, [class*="st-"], .stApp {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Segoe UI', sans-serif;
    color: #e6e6e6;
}

/* --- Main app background --- */
.stApp {
    background-color: #0d0d0d;
}

/* --- Header area --- */
[data-testid="stHeader"] {
    background-color: #0d0d0d;
    border-bottom: 1px solid #1f1f1f;
}

/* --- Sidebar --- */
[data-testid="stSidebar"] {
    background-color: #0a0a0a;
    border-right: 1px solid #1f1f1f;
}

/* --- Titles & text --- */
h1, h2, h3, h4 {
    color: #f0f0f0 !important;
    font-weight: 600 !important;
    letter-spacing: -0.02em;
}
h1 { font-size: 1.75rem !important; }
h2 { font-size: 1.25rem !important; }
h3 { font-size: 1.05rem !important; color: #999 !important; }
p, li, label, .stMarkdown { color: #b0b0b0; }

/* --- Main container --- */
.block-container {
    padding-top: 2rem;
    padding-bottom: 2rem;
    max-width: 1400px;
}

/* --- Cards / containers --- */
[data-testid="stVerticalBlockBorderWrapper"], .stExpander {
    background-color: #141414 !important;
    border: 1px solid #1f1f1f !important;
    border-radius: 8px !important;
    padding: 20px !important;
    box-shadow: none !important;
}

/* --- Dividers --- */
hr {
    border-color: #1f1f1f !important;
    margin: 1.5rem 0 !important;
}

/* --- Tabs --- */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    border-bottom: 1px solid #1f1f1f;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: #808080 !important;
    font-family: 'Inter', sans-serif;
    font-size: 0.875rem;
    font-weight: 500;
    padding: 8px 20px;
    border-radius: 0 !important;
    border: none !important;
    border-bottom: 2px solid transparent !important;
}
.stTabs [data-baseweb="tab"][aria-selected="true"] {
    color: #e6e6e6 !important;
    border-bottom: 2px solid #5E6AD2 !important;
}
.stTabs [data-baseweb="tab"]:hover {
    color: #c0c0c0 !important;
    background: rgba(255,255,255,0.03) !important;
}

/* --- Input widgets --- */
.stTextInput input, .stSelectbox select, [data-baseweb="input"], [data-baseweb="select"] {
    background-color: #141414 !important;
    border: 1px solid #1f1f1f !important;
    border-radius: 6px !important;
    color: #e6e6e6 !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 0.875rem !important;
}
.stTextInput input:focus, [data-baseweb="input"]:focus {
    border-color: #5E6AD2 !important;
    box-shadow: 0 0 0 2px rgba(94,106,210,0.15) !important;
}
.stTextInput input::placeholder {
    color: #555 !important;
}

/* --- Select dropdown --- */
[data-baseweb="popover"] {
    background-color: #1a1a1a !important;
    border: 1px solid #2a2a2a !important;
    border-radius: 8px !important;
}
[data-baseweb="option"] {
    color: #b0b0b0 !important;
    font-size: 0.875rem !important;
}
[data-baseweb="option"]:hover {
    background-color: #222 !important;
    color: #f0f0f0 !important;
}

/* --- Buttons --- */
.stButton button, [data-testid="stBaseButton-secondary"] {
    background-color: #1a1a1a !important;
    border: 1px solid #2a2a2a !important;
    border-radius: 6px !important;
    color: #d0d0d0 !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 0.8125rem !important;
    font-weight: 500 !important;
    padding: 6px 14px !important;
    transition: all 0.15s ease;
}
.stButton button:hover {
    background-color: #222 !important;
    border-color: #3a3a3a !important;
    color: #f0f0f0 !important;
}

/* --- Download button --- */
[data-testid="stDownloadButton"] button {
    background-color: #1a1a1a !important;
    border: 1px solid #2a2a2a !important;
    color: #999 !important;
}
[data-testid="stDownloadButton"] button:hover {
    background-color: #222 !important;
    border-color: #5E6AD2 !important;
    color: #e6e6e6 !important;
}

/* --- Dataframes / Tables --- */
[data-testid="stTable"], .stDataFrame {
    background-color: #141414 !important;
    border: 1px solid #1f1f1f !important;
    border-radius: 8px !important;
    overflow: hidden;
}
[data-testid="stTable"] th, .stDataFrame th {
    background-color: #1a1a1a !important;
    color: #808080 !important;
    font-weight: 500 !important;
    font-size: 0.75rem !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    border-bottom: 1px solid #1f1f1f !important;
    padding: 8px 12px !important;
}
[data-testid="stTable"] td, .stDataFrame td {
    background-color: #141414 !important;
    color: #c0c0c0 !important;
    font-size: 0.8125rem !important;
    border-bottom: 1px solid #141414 !important;
    padding: 6px 12px !important;
}
[data-testid="stTable"] tr:hover td, .stDataFrame tr:hover td {
    background-color: #1c1c1c !important;
}

/* --- Metric --- */
[data-testid="stMetric"] {
    background-color: #141414 !important;
    border: 1px solid #1f1f1f !important;
    border-radius: 8px !important;
    padding: 16px 20px !important;
}
[data-testid="stMetric"] label {
    color: #808080 !important;
    font-size: 0.75rem !important;
}
[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #f0f0f0 !important;
    font-size: 1.5rem !important;
    font-weight: 600 !important;
}

/* --- Caption --- */
.stCaption { color: #666 !important; font-size: 0.8rem !important; }

/* --- Slider --- */
[data-baseweb="slider"] [role="slider"] {
    background-color: #5E6AD2 !important;
}
[data-baseweb="slider"] div:first-child {
    background-color: #5E6AD2 !important;
}

/* --- Scrollbar --- */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #0d0d0d; }
::-webkit-scrollbar-thumb { background: #2a2a2a; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #3a3a3a; }

/* --- Alert / Info boxes --- */
[data-testid="stAlert"] {
    background-color: #141414 !important;
    border: 1px solid #2a2a2a !important;
    border-radius: 8px !important;
    color: #808080 !important;
}

/* --- Expander --- */
.streamlit-expanderHeader {
    background-color: #141414 !important;
    border: 1px solid #1f1f1f !important;
    border-radius: 8px !important;
    color: #c0c0c0 !important;
}

</style>
""", unsafe_allow_html=True)

# ============ PLOTLY LINEAR DARK TEMPLATE ============
import plotly.io as pio

LINEAR_COLORS = ['#5E6AD2', '#8B5CF6', '#3B82F6', '#06B6D4', '#10B981',
                 '#F59E0B', '#EF4444', '#EC4899', '#6366F1', '#14B8A6']

pio.templates["linear_dark"] = go.layout.Template(
    layout={
        "paper_bgcolor": "#0d0d0d",
        "plot_bgcolor": "#141414",
        "font": {"family": "Inter, -apple-system, sans-serif", "color": "#b0b0b0", "size": 12},
        "title": {"font": {"family": "Inter, -apple-system, sans-serif", "color": "#e6e6e6", "size": 15, "weight": "bold"}, "x": 0},
        "xaxis": {"gridcolor": "#1f1f1f", "linecolor": "#2a2a2a", "zerolinecolor": "#1f1f1f",
                  "title": {"font": {"color": "#808080", "size": 11}}},
        "yaxis": {"gridcolor": "#1f1f1f", "linecolor": "#2a2a2a", "zerolinecolor": "#1f1f1f",
                  "title": {"font": {"color": "#808080", "size": 11}}},
        "colorway": LINEAR_COLORS,
        "legend": {"bgcolor": "rgba(0,0,0,0)", "font": {"color": "#808080", "size": 11},
                   "bordercolor": "#1f1f1f", "borderwidth": 1},
        "margin": {"l": 40, "r": 20, "t": 40, "b": 40},
        "hovermode": "x unified",
        "hoverlabel": {"bgcolor": "#1a1a1a", "font": {"color": "#e6e6e6", "family": "Inter, sans-serif"},
                       "bordercolor": "#2a2a2a"},
        "bargap": 0.25,
        "bargroupgap": 0.1,
    }
)
pio.templates.default = "linear_dark"

# ============ DB CONNECTION ============
@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

con = get_connection()

# ============ DATA LOADING FUNCTIONS ============
@st.cache_data(ttl=300)
def get_store_list():
    q = """SELECT store_unicode, shop_name, shop_sn, city_name, province_name,
           COALESCE(city_level_label, '—') AS city_level,
           COALESCE(shop_group_label, '—') AS shop_group,
           COALESCE(trade_channel_label, '—') AS trade_channel,
           COALESCE(store_roi, 0) AS roi,
           COALESCE(total_expenses, 0) AS total_expenses,
           COALESCE(detail_total_money, 0) AS total_sales,
           COALESCE(aligned_sales, 0) AS aligned_sales,
           COALESCE(aligned_roi, 0) AS aligned_roi
           FROM v_store_metrics ORDER BY total_expenses DESC"""
    return con.execute(q).df()

@st.cache_data(ttl=300)
def get_store_overview(store_unicode):
    q = """SELECT * FROM v_store_metrics WHERE store_unicode = ?"""
    return con.execute(q, [store_unicode]).df().iloc[0] if con.execute("SELECT COUNT(*) FROM v_store_metrics WHERE store_unicode = ?", [store_unicode]).fetchone()[0] > 0 else None

@st.cache_data(ttl=300)
def get_store_brand_metrics(store_unicode):
    q = """SELECT *, COALESCE(activity_type_label, activity_type) AS activity_display
           FROM v_store_brand_metrics WHERE store_unicode = ? AND brand_name IS NOT NULL ORDER BY roi DESC NULLS LAST"""
    return con.execute(q, [store_unicode]).df()

@st.cache_data(ttl=300)
def get_store_expense_detail(store_unicode):
    q = """SELECT project_name, efficacious_session_days, brand_sales_total, single_store_sales,
           wages, incentive_bonuses, three_salary, total_short_term_expenses,
           actual_sales_achieved_amount, promoter_name, mu_name
           FROM fact_store_cost WHERE dim_store_unicode = ? ORDER BY total_short_term_expenses DESC"""
    return con.execute(q, [store_unicode]).df()

@st.cache_data(ttl=300)
def get_store_exec_quality(store_unicode):
    q = """SELECT arrange_date, start_status, end_status, start_is_late, end_is_leave_early,
           start_position_deviation, end_position_deviation, end_pos_audit_status,
           work_status,
           brand_sales, taste_info, buy_num, promoter_name, project_name, activity_type,
           COALESCE(activity_type_label, activity_type) AS activity_type_label
           FROM fact_store_execution WHERE dim_store_unicode = ? ORDER BY arrange_date DESC"""
    return con.execute(q, [store_unicode]).df()

@st.cache_data(ttl=300)
def get_store_tasks(store_unicode):
    q = """SELECT task_name, task_type, COALESCE(task_type_label, task_type) AS task_type_label,
           exe_time, status, promoter_name,
           CASE WHEN status='PASS' THEN 1 ELSE 0 END AS is_pass
           FROM fact_store_task WHERE dim_store_unicode = ?"""
    return con.execute(q, [store_unicode]).df()

@st.cache_data(ttl=300)
def get_store_shop_plans(store_unicode):
    q = """SELECT sp.start_time, sp.end_time, sp.display_type, sp.display_area,
           sp.have_posm, sp.have_dm, sp.dm_price, sp.project_brand, sp.activity_type,
           sp.brand_sales_total, sp.promoter_number, sp.auth_status
           FROM fact_shop_plan sp WHERE sp.dim_store_unicode = ? ORDER BY sp.start_time DESC"""
    return con.execute(q, [store_unicode]).df()

@st.cache_data(ttl=300)
def get_peer_stores_for_benchmark(store_unicode):
    q = """SELECT city_level, shop_group, trade_channel FROM dim_store WHERE store_unicode = ?"""
    info = con.execute(q, [store_unicode]).fetchone()
    if not info:
        return pd.DataFrame()
    cl, sg, tc = info
    q2 = """SELECT store_unicode, shop_name, city_name, store_roi, total_expenses,
            total_achieved_amount, detail_total_money, total_session_days,
            checkin_rate, task_pass_rate, avg_display_area
            FROM v_store_metrics
            WHERE city_level = ? AND shop_group = ? AND store_unicode != ?
            AND total_expenses > 0 LIMIT 100"""
    return con.execute(q2, [cl, sg, store_unicode]).df()

@st.cache_data(ttl=300)
def get_all_stores_brand_ranking():
    q = """SELECT brand_name, activity_type,
           AVG(roi) AS avg_roi, SUM(total_expenses) AS total_investment,
           SUM(cost_sales) AS total_sales, COUNT(DISTINCT store_unicode) AS store_count,
           AVG(achievement_rate) AS avg_achievement
           FROM v_store_brand_metrics WHERE roi IS NOT NULL AND brand_name IS NOT NULL
           GROUP BY brand_name, activity_type HAVING COUNT(*) >= 5
           ORDER BY avg_roi DESC"""
    return con.execute(q).df()

@st.cache_data(ttl=300)
def get_store_kpi_by_year(store_unicode, year=None):
    """Get store cost/sales/execution KPIs filtered by year."""
    if year is None:
        return None
    yr = int(year)
    cost = con.execute("""
        SELECT SUM(total_short_term_expenses) AS cost,
               SUM(wages) AS wages, SUM(incentive_bonuses) AS bonuses,
               SUM(three_salary) AS three_salary
        FROM fact_store_cost
        WHERE dim_store_unicode = ? AND EXTRACT(YEAR FROM create_time) = ?
    """, [store_unicode, yr]).fetchone()
    sales = con.execute("""
        SELECT SUM(totalMoney) AS sales, SUM(total_sales) AS qty
        FROM fact_store_sales
        WHERE dim_store_unicode = ? AND EXTRACT(YEAR FROM sales_date) = ?
    """, [store_unicode, yr]).fetchone()
    exec_r = con.execute("""
        SELECT COUNT(*) AS arranges, SUM(CASE WHEN start_status=1 THEN 1 ELSE 0 END) AS checkins
        FROM fact_store_execution
        WHERE dim_store_unicode = ? AND EXTRACT(YEAR FROM arrange_date) = ?
    """, [store_unicode, yr]).fetchone()
    pts = con.execute("""
        SELECT SUM(points_value) AS points_val
        FROM fact_points
        WHERE dim_store_unicode = ? AND EXTRACT(YEAR FROM points_date) = ?
    """, [store_unicode, yr]).fetchone()
    c0, s0, p0 = float(cost[0] or 0), float(sales[0] or 0), float(pts[0] or 0)
    exp = c0 + p0
    return {
        'cost': c0, 'wages': float(cost[1] or 0), 'bonuses': float(cost[2] or 0), 'three_salary': float(cost[3] or 0),
        'sales': s0, 'qty': float(sales[1] or 0),
        'arranges': int(exec_r[0] or 0), 'checkins': int(exec_r[1] or 0),
        'points': p0,
        'expenses': exp,
        'roi': round(s0 / exp, 3) if exp > 0 else None
    }

def get_store_project_count_by_year(store_unicode, year):
    """Count distinct projects active in a given year for a store."""
    yr = int(year)
    r = con.execute("""
        SELECT COUNT(DISTINCT proj) FROM (
            SELECT project_reconciliation_unicode AS proj FROM fact_store_cost WHERE dim_store_unicode=? AND EXTRACT(YEAR FROM create_time)=?
            UNION
            SELECT project_unicode FROM fact_store_sales WHERE dim_store_unicode=? AND EXTRACT(YEAR FROM sales_date)=?
            UNION
            SELECT project_unicode FROM fact_store_execution WHERE dim_store_unicode=? AND EXTRACT(YEAR FROM arrange_date)=?
            UNION
            SELECT project_unicode FROM fact_shop_plan WHERE dim_store_unicode=? AND EXTRACT(YEAR FROM start_time)=?
        )
    """, [store_unicode, yr, store_unicode, yr, store_unicode, yr, store_unicode, yr]).fetchone()
    return r[0] if r else 0

# ============ PROMOTER DATA FUNCTIONS ============
@st.cache_data(ttl=300)
def get_promoter_list():
    q = """SELECT promoter_sn, promoter_unicode, promoter_name, promoter_mobile,
           supervisor_name, gender, age, membership_name,
           profile_city, profile_au, profile_ru, profile_mu,
           total_arranges, total_session_days,
           detail_total_money, total_expenses, sales_roi,
           attendance_rate, task_pass_rate, taste_conversion, labor_efficiency
           FROM v_promoter_metrics ORDER BY total_arranges DESC"""
    return con.execute(q).df()

@st.cache_data(ttl=300)
def get_promoter_overview(promoter_unicode):
    q = """SELECT * FROM v_promoter_metrics WHERE promoter_unicode = ?"""
    return con.execute(q, [promoter_unicode]).df().iloc[0] if con.execute("SELECT COUNT(*) FROM v_promoter_metrics WHERE promoter_unicode = ?", [promoter_unicode]).fetchone()[0] > 0 else None

@st.cache_data(ttl=300)
def get_promoter_execution_detail(promoter_unicode):
    q = """SELECT arrange_date, start_status, end_status, start_is_late, end_is_leave_early,
           project_name, activity_type, COALESCE(activity_type_label, activity_type) AS activity_type_label,
           brand_sales, taste_info, buy_num,
           start_position_deviation, end_position_deviation
           FROM fact_store_execution WHERE promoter_unicode = ? ORDER BY arrange_date DESC"""
    return con.execute(q, [promoter_unicode]).df()

@st.cache_data(ttl=300)
def get_promoter_sales_detail(promoter_unicode):
    q = """SELECT fss.project_name, fss.product_name, fss.product_brand,
           fss.total_sales, fss.total_volumes, fss.totalMoney,
           dp.brand_name AS project_brand
           FROM fact_store_sales fss
           LEFT JOIN dim_project dp ON fss.project_unicode = dp.project_unicode
           WHERE fss.promoter_unicode = ?
           ORDER BY fss.totalMoney DESC"""
    return con.execute(q, [promoter_unicode]).df()

@st.cache_data(ttl=300)
def get_promoter_tasks_detail(promoter_unicode):
    q = """SELECT task_name, task_type, COALESCE(task_type_label, task_type) AS task_type_label,
           exe_time, status, reward_point
           FROM fact_store_task WHERE promoter_unicode = ? ORDER BY exe_time DESC"""
    return con.execute(q, [promoter_unicode]).df()

@st.cache_data(ttl=300)
def get_promoter_cost_detail(promoter_unicode):
    q = """SELECT fsc.project_name, fsc.efficacious_session_days, fsc.wages,
           fsc.incentive_bonuses, fsc.three_salary, fsc.total_short_term_expenses,
           fsc.actual_sales_achieved_amount, fsc.shop_name
           FROM fact_store_cost fsc
           INNER JOIN dim_promoter_clean dpm ON fsc.promoter_sn = dpm.promoter_sn
           WHERE dpm.promoter_unicode = ?
           ORDER BY fsc.total_short_term_expenses DESC"""
    return con.execute(q, [promoter_unicode]).df()

@st.cache_data(ttl=300)
def get_promoter_brand_sales(promoter_unicode):
    q = """SELECT product_brand AS brand_name, SUM(totalMoney) AS total_sales,
           SUM(total_sales) AS total_qty
           FROM fact_store_sales
           WHERE promoter_unicode = ? AND product_brand IS NOT NULL
           GROUP BY product_brand ORDER BY total_sales DESC"""
    return con.execute(q, [promoter_unicode]).df()

@st.cache_data(ttl=300)
def get_promoter_kpi_by_year(promoter_unicode, year=None):
    """Get promoter cost/sales/execution KPIs filtered by year."""
    if year is None:
        return None
    yr = int(year)
    cost = con.execute("""
        SELECT SUM(fsc.total_short_term_expenses) AS cost,
               SUM(fsc.wages) AS wages, SUM(fsc.incentive_bonuses) AS bonuses
        FROM fact_store_cost fsc
        INNER JOIN dim_promoter_clean dpm ON fsc.promoter_sn = dpm.promoter_sn
        WHERE dpm.promoter_unicode = ? AND EXTRACT(YEAR FROM fsc.create_time) = ?
    """, [promoter_unicode, yr]).fetchone()
    sales = con.execute("""
        SELECT SUM(totalMoney) AS sales, SUM(total_sales) AS qty
        FROM fact_store_sales
        WHERE promoter_unicode = ? AND EXTRACT(YEAR FROM sales_date) = ?
    """, [promoter_unicode, yr]).fetchone()
    exec_r = con.execute("""
        SELECT COUNT(*) AS arranges,
               SUM(CASE WHEN start_status=1 THEN 1 ELSE 0 END) AS checkins
        FROM fact_store_execution
        WHERE promoter_unicode = ? AND EXTRACT(YEAR FROM arrange_date) = ?
    """, [promoter_unicode, yr]).fetchone()
    pts = con.execute("""
        SELECT SUM(fp.points_value) AS points_val
        FROM fact_points fp
        INNER JOIN dim_promoter_clean dpm ON fp.promoter_sn = dpm.promoter_sn
        WHERE dpm.promoter_unicode = ? AND EXTRACT(YEAR FROM fp.points_date) = ?
    """, [promoter_unicode, yr]).fetchone()
    c0, s0, p0 = float(cost[0] or 0), float(sales[0] or 0), float(pts[0] or 0)
    exp = c0 + p0
    return {
        'cost': c0, 'wages': float(cost[1] or 0), 'bonuses': float(cost[2] or 0),
        'sales': s0, 'qty': float(sales[1] or 0),
        'arranges': int(exec_r[0] or 0), 'checkins': int(exec_r[1] or 0),
        'points': p0,
        'expenses': exp,
        'roi': round(s0 / exp, 3) if exp > 0 else None
    }

# ============ PROJECT DATA FUNCTIONS ============
@st.cache_data(ttl=300)
def get_project_list():
    q = """SELECT project_unicode, project_name, status, activity_type,
           activity_type_label, project_type, project_type_label,
           brand_name, start_time, end_time,
           total_expenses, detail_total_money, detail_roi, cost_roi,
           total_session_days, achievement_rate, taste_conversion,
           cost_store_count, cost_promoter_count
           FROM v_project_metrics ORDER BY total_expenses DESC"""
    return con.execute(q).df()

@st.cache_data(ttl=300)
def get_project_overview(project_unicode):
    q = """SELECT * FROM v_project_metrics WHERE project_unicode = ?"""
    return con.execute(q, [project_unicode]).df().iloc[0] if con.execute("SELECT COUNT(*) FROM v_project_metrics WHERE project_unicode = ?", [project_unicode]).fetchone()[0] > 0 else None

@st.cache_data(ttl=300)
def get_project_activity_type_summary():
    q = """SELECT * FROM v_project_activity_type_metrics ORDER BY total_expenses DESC"""
    return con.execute(q).df()

@st.cache_data(ttl=300)
def get_project_brand_activity_matrix():
    q = """SELECT brand_name, activity_type,
           COUNT(*) AS project_count,
           AVG(detail_roi) AS avg_roi,
           SUM(total_expenses) AS total_investment,
           SUM(detail_total_money) AS total_sales
           FROM v_project_metrics
           WHERE brand_name IS NOT NULL AND activity_type IS NOT NULL
           GROUP BY brand_name, activity_type
           HAVING COUNT(*) >= 1
           ORDER BY avg_roi DESC"""
    return con.execute(q).df()

def _yr_stats():
    """Helper: query 2026 global stats with fallback for missing columns."""
    yr = 2026
    f = lambda v: float(v) if v is not None else 0.0
    i = lambda v: int(v) if v is not None else 0
    yr1 = str(yr + 1)
    yr_s = str(yr)

    def _q(sql, default=(0,0)):
        try:
            return con.execute(sql).fetchone()
        except Exception:
            return default

    # Use date-range filter which works regardless of column type (DATE/TIMESTAMP/VARCHAR)
    cost = _q(f"SELECT SUM(total_short_term_expenses), COUNT(DISTINCT dim_store_unicode) FROM fact_store_cost WHERE create_time >= '{yr_s}-01-01' AND create_time < '{yr1}-01-01'")
    sales = _q(f"SELECT SUM(totalMoney), COUNT(DISTINCT dim_store_unicode) FROM fact_store_sales WHERE project_unicode IN (SELECT project_unicode FROM dim_project WHERE start_time >= '{yr_s}-01-01' AND start_time < '{yr1}-01-01')")
    exec_r = _q(f"SELECT COUNT(*), COUNT(DISTINCT dim_store_unicode), COUNT(DISTINCT promoter_unicode), AVG(CASE WHEN start_status=1 THEN 1.0 ELSE 0.0 END)*100 FROM fact_store_execution WHERE arrange_date >= '{yr_s}-01-01' AND arrange_date < '{yr1}-01-01'")
    pts = _q(f"SELECT SUM(points_value), COUNT(DISTINCT dim_store_unicode) FROM fact_points WHERE points_date >= '{yr_s}-01-01' AND points_date < '{yr1}-01-01'")
    task = _q(f"SELECT COUNT(*), SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) FROM fact_store_task WHERE exe_time >= '{yr_s}-01-01' AND exe_time < '{yr1}-01-01'")
    proj_exec = _q(f"SELECT COUNT(DISTINCT project_unicode) FROM fact_store_execution WHERE arrange_date >= '{yr_s}-01-01' AND arrange_date < '{yr1}-01-01'")
    proj_cost = _q(f"SELECT COUNT(DISTINCT project_name) FROM fact_store_cost WHERE create_time >= '{yr_s}-01-01' AND create_time < '{yr1}-01-01'")
    all_exec_stores = i(_q(f"SELECT COUNT(DISTINCT dim_store_unicode) FROM fact_store_execution WHERE arrange_date >= '{yr_s}-01-01' AND arrange_date < '{yr1}-01-01'")[0])
    # Top stores via v_store_metrics (stable view)
    try:
        top = con.execute(f"SELECT shop_name, aligned_sales FROM v_store_metrics WHERE aligned_sales > 0 ORDER BY aligned_sales DESC LIMIT 3").fetchall()
        top_str = ', '.join(f'{t[0][:12]} ¥{f(t[1]):,.0f}' for t in top)
    except Exception:
        top_str = '数据暂不可用'
    exp_total = f(cost[0]) + f(pts[0])
    s_total = f(sales[0])
    return {
        'cost_total': f(cost[0]), 'cost_stores': i(cost[1]),
        'sales_total': s_total, 'sales_stores': i(sales[1]),
        'exec_count': i(exec_r[0]), 'exec_stores': i(exec_r[1]), 'exec_promoters': i(exec_r[2]), 'exec_att': f(exec_r[3]),
        'pts_total': f(pts[0]), 'pts_stores': i(pts[1]),
        'task_total': i(task[0]), 'task_pass': i(task[1]), 'task_pass_rate': round(f(task[1])/max(f(task[0]),1)*100,1) if task[0] else 0,
        'proj_exec': i(proj_exec[0]), 'proj_cost': i(proj_cost[0]),
        'all_exec_stores': all_exec_stores,
        'zero_sales_pct': round((1-i(sales[1])/all_exec_stores)*100,1) if all_exec_stores>0 else 0,
        'top_stores': top_str,
        'exp_total': exp_total,
        'roi': round(s_total/exp_total,3) if exp_total>0 else 0,
    }

def ai_summary_store(s):
    return f"""
<div style="background:linear-gradient(135deg, #1a1400, #141000); border:1px solid #F59E0B; border-radius:10px; padding:18px 24px; margin:8px 0 16px 0;">
<div style="font-size:1rem; font-weight:700; color:#F59E0B; margin-bottom:12px;">⚡ AI 决策摘要 · 门店360 · 基于2026年数据</div>
<div style="font-size:0.9rem; color:#e0e0e0; line-height:1.8;">
<b style="color:#F59E0B;">▸ 门店覆盖</b><br>
2026年 <b>{s['all_exec_stores']:,}</b> 个门店有执行记录，<b>{s['sales_stores']}</b> 个产生销售（<b>{s['zero_sales_pct']}%</b>零销售）。总投入 <b>{format_money(s['exp_total'])}</b>，总销售 <b>{format_money(s['sales_total'])}</b>，ROI <b>{s['roi']}×</b>。销售Top3：{s['top_stores']}。<br>
<b style="color:#F59E0B;">▸ 决策建议</b><br>
① {s['zero_sales_pct']}%门店零销售——建议核查试饮/陈列类门店是否应独立评估，不纳入销售考核。
② 将执行资源向 {s['sales_stores']} 个有销售门店集中，重点关注高投入低产出门店。
③ 选中具体门店后，下方决策卡片会给出该门店的个性化预警和建议。
</div></div>"""

def ai_summary_promoter(s):
    return f"""
<div style="background:linear-gradient(135deg, #1a1400, #141000); border:1px solid #F59E0B; border-radius:10px; padding:18px 24px; margin:8px 0 16px 0;">
<div style="font-size:1rem; font-weight:700; color:#F59E0B; margin-bottom:12px;">⚡ AI 决策摘要 · 促销员360 · 基于2026年数据</div>
<div style="font-size:0.9rem; color:#e0e0e0; line-height:1.8;">
<b style="color:#F59E0B;">▸ 执行概况</b><br>
2026年 <b>{s['exec_promoters']:,}</b> 名促销员执行 <b>{s['exec_count']:,}</b> 场次，覆盖 <b>{s['exec_stores']:,}</b> 个门店。平均出勤率 <b>{s['exec_att']:.1f}%</b>，任务通过率 <b>{s['task_pass_rate']}%</b>。<br>
<b style="color:#F59E0B;">▸ 决策建议</b><br>
① 出勤率{s['exec_att']:.1f}%表观正常，建议交叉验证GPS签到偏差，识别虚假考勤。
② 任务通过率{s['task_pass_rate']}%，关注未通过任务的类型分布，针对性培训。
③ 选中具体促销员后，下方会显示三维评分、最弱维度及提升建议。
</div></div>"""

def ai_summary_project(s):
    top_proj = con.execute("SELECT project_name, detail_roi FROM v_project_metrics WHERE total_expenses > 0 ORDER BY detail_roi DESC LIMIT 3").fetchall()
    top_proj_str = ', '.join(f'{p[0][:15]} {float(p[1]):.3f}×' for p in top_proj)
    return f"""
<div style="background:linear-gradient(135deg, #1a1400, #141000); border:1px solid #F59E0B; border-radius:10px; padding:18px 24px; margin:8px 0 16px 0;">
<div style="font-size:1rem; font-weight:700; color:#F59E0B; margin-bottom:12px;">⚡ AI 决策摘要 · 项目360 · 基于2026年数据</div>
<div style="font-size:0.9rem; color:#e0e0e0; line-height:1.8;">
<b style="color:#F59E0B;">▸ 项目概况</b><br>
2026年 <b>{s['proj_exec']}</b> 个项目有执行记录，<b>{s['proj_cost']}</b> 个有费用。总投入 <b>{format_money(s['exp_total'])}</b>，积分投入 <b>{format_money(s['pts_total'])}</b>。<br>
<b style="color:#F59E0B;">▸ ROI Top3项目</b><br>{top_proj_str}<br>
<b style="color:#F59E0B;">▸ 决策建议</b><br>
① 优先参考ROI Top项目，分析其高回报驱动因素（品牌/活动类型/门店覆盖）。
② 关注ROI倒挂项目，核查费用是否集中在低效门店或促销员。
③ 选中具体项目后查看品牌×活动类型热力图，辅助资源配置决策。
</div></div>"""


def get_monthly_trend():
    q = """SELECT year_month, sales_amount, cost_amount, points_value, labor_cost,
           arranges, exec_stores, taste_total, monthly_roi
           FROM v_monthly_trend ORDER BY year_month"""
    return con.execute(q).df()

# ============ PROMOTER SCORING ============
def score_promoter(row):
    """3-dimension weighted scoring: 产出20% + 质量45% + 效率35%"""
    scores = {}

    # Dimension 1: Output (20%) — sales volume, with baseline for non-sales roles
    sales = row.get('detail_total_money', 0) or 0
    if sales >= 500000: scores['output'] = 100
    elif sales >= 100000: scores['output'] = 85
    elif sales >= 20000: scores['output'] = 65
    elif sales >= 5000: scores['output'] = 45
    elif sales >= 1000: scores['output'] = 30
    elif sales > 0: scores['output'] = 20
    else: scores['output'] = 10

    # Dimension 2: Quality (45%) — attendance 40% + task pass 30% + taste conv 30%
    attendance = row.get('attendance_rate', 0) or 0
    task_pass = row.get('task_pass_rate', 0) or 0
    taste_conv = row.get('taste_conversion', 0) or 0
    quality_sub = (min(attendance, 100) / 100 * 40 +
                   min(task_pass, 100) / 100 * 30 +
                   min(taste_conv, 100) / 100 * 30)
    scores['quality'] = round(quality_sub, 1)

    # Dimension 3: Efficiency (35%) — labor efficiency 60% + sales ROI 40%
    labor_eff = row.get('labor_efficiency', 0) or 0
    sales_roi_val = row.get('sales_roi', 0) or 0
    # Use softer caps so low-but-nonzero contributors still score
    labor_score = min(labor_eff / 30 * 100, 100) if labor_eff > 0 else 0
    roi_score = min(sales_roi_val / 60 * 100, 100) if sales_roi_val > 0 else 0
    eff_score = labor_score * 0.6 + roi_score * 0.4
    scores['efficiency'] = round(eff_score, 1)

    total = scores['output'] * 0.20 + scores['quality'] * 0.45 + scores['efficiency'] * 0.35
    scores['total'] = round(total, 1)
    scores['tier'] = '卓越' if total >= 80 else ('优秀' if total >= 60 else ('良好' if total >= 40 else '待提升'))
    return scores

# ============ UI HELPERS ============
def kpi_card(label, value, sub=None, accent="#5E6AD2"):
    """Linear-style KPI card: fixed height for row uniformity."""
    st.markdown(f"""
    <div style="background:#141414;border:1px solid #1f1f1f;border-radius:8px;padding:16px 20px;margin:2px 0;
                height:110px;display:flex;flex-direction:column;justify-content:space-between;
                transition:border-color 0.2s;">
        <div>
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
                <div style="width:6px;height:6px;border-radius:50%;background:{accent};flex-shrink:0;"></div>
                <span style="font-size:0.72rem;color:#808080;font-weight:500;letter-spacing:0.03em;text-transform:uppercase;">{label}</span>
            </div>
            <div style="font-size:1.6rem;font-weight:700;color:#f0f0f0;letter-spacing:-0.02em;line-height:1.15;">{value}</div>
        </div>
        <div style="font-size:0.75rem;color:#555;min-height:1.1em;line-height:1.2;">{sub if sub else '&nbsp;'}</div>
    </div>
    """, unsafe_allow_html=True)

def format_money(v):
    if v is None or pd.isna(v): return "—"
    if abs(v) >= 1e8: return f"¥{v/1e8:.2f}亿"
    if abs(v) >= 1e4: return f"¥{v/1e4:.1f}万"
    return f"¥{v:,.0f}"

def format_pct(v):
    if v is None or pd.isna(v): return "—"
    return f"{v:.1f}%"

DIM_COLORS = {
    "expense": "#EF4444", "sales": "#10B981", "roi": "#5E6AD2",
    "project": "#8B5CF6", "quality": "#F59E0B", "default": "#5E6AD2"
}

# ============ MAIN APP ============
st.markdown("# 百事微促达投资决策分析")
st.caption("销量 2025-01 → 2026-04 | 执行 2025-01 → 2026-06 | 费用仅 2026-04(8天) | 积分 2025-01 → 2026-05")
st.caption("⚠ 费用缺失2025年数据，全局ROI分母偏小、数值虚高，请用年份筛选切换至2026年查看真实ROI")

section = st.radio("", ["门店360°", "促销员360°", "项目360°"], horizontal=True, label_visibility="collapsed")

# Pre-compute 2026 stats for AI summaries
yr_stats = _yr_stats()

if section == "门店360°":

    show_store_ai = st.checkbox("⚡ AI 决策摘要 · 门店360", value=True, key="show_store_ai")
    if show_store_ai:
        st.markdown(ai_summary_store(yr_stats), unsafe_allow_html=True)

    # ---- Global Store Selector ----
    store_df = get_store_list()

    col_s1, col_s2, col_s3 = st.columns([2.5, 1, 1])
    with col_s1:
        search = st.text_input("搜索门店", placeholder="输入门店名称、编码或城市…", label_visibility="collapsed")
    with col_s2:
        city_filter = st.selectbox("城市", ["全部城市"] + sorted(store_df['city_name'].dropna().unique().tolist()), label_visibility="collapsed")
    with col_s3:
        level_filter = st.selectbox("城市等级", ["全部等级"] + sorted(store_df['city_level'].dropna().unique().tolist()), label_visibility="collapsed")

    filtered = store_df.copy()
    if search:
        mask = (filtered['shop_name'].str.contains(search, na=False, case=False) |
                filtered['shop_sn'].str.contains(search, na=False, case=False) |
                filtered['city_name'].str.contains(search, na=False, case=False))
        filtered = filtered[mask]
    if city_filter != "全部城市":
        filtered = filtered[filtered['city_name'] == city_filter]
    if level_filter != "全部等级":
        filtered = filtered[filtered['city_level'] == level_filter]

    filtered = filtered.head(200)

    if len(filtered) == 0:
        st.warning("未找到匹配门店")
        st.stop()

    def _fmt_store(i):
        shop = filtered.loc[i, 'shop_name']
        city = filtered.loc[i, 'city_name'] or '—'
        exp = filtered.loc[i, 'total_expenses']
        if exp > 0:
            return f"{shop}  ·  {city}  ·  投入 {format_money(exp)}  ·  ROI {filtered.loc[i,'roi']:.3f}"
        return f"{shop}  ·  {city}  ·  暂无费用数据"

    selected_idx = st.selectbox("选择门店", filtered.index.tolist(),
        format_func=_fmt_store, label_visibility="collapsed")

    selected_store = filtered.loc[selected_idx, 'store_unicode']

    # Load all data for selected store
    overview = get_store_overview(selected_store)
    if overview is None:
        st.error("门店数据不存在")
        st.stop()

    brand_metrics = get_store_brand_metrics(selected_store)
    expense_detail = get_store_expense_detail(selected_store)
    exec_quality = get_store_exec_quality(selected_store)
    tasks = get_store_tasks(selected_store)
    shop_plans = get_store_shop_plans(selected_store)
    peer_stores = get_peer_stores_for_benchmark(selected_store)

    st.markdown(f"""
    <div style="padding:8px 0 4px 0;">
        <span style="font-size:1.05rem;font-weight:600;color:#e6e6e6;">{overview['shop_name']}</span>
        <span style="color:#555;margin:0 8px;">·</span>
        <span style="font-size:0.85rem;color:#808080;">{overview.get('city_name','') or ''} {overview.get('province_name','') or ''}</span>
    </div>
    """, unsafe_allow_html=True)

    # Year filter — applies to all tabs
    store_year = st.radio("统计年份", ["全部", "2025年", "2026年"], horizontal=True, key="store_year",
                          label_visibility="collapsed")
    yr_kpi = None
    yr_label = ""
    yr = None
    if store_year != "全部":
        yr = int(store_year.replace("年", ""))
        yr_kpi = get_store_kpi_by_year(selected_store, yr)
        yr_label = f"({store_year})"
        # Filter expense_detail by year
        if len(expense_detail) > 0 and 'create_time' in expense_detail.columns:
            expense_detail = expense_detail[pd.to_datetime(expense_detail['create_time']).dt.year == yr].copy()
        # Filter exec_quality by year
        if len(exec_quality) > 0:
            mask = pd.to_datetime(exec_quality['arrange_date']).dt.year == yr
            exec_quality = exec_quality[mask].copy()
        # Filter tasks by year
        if len(tasks) > 0:
            mask = pd.to_datetime(tasks['exe_time']).dt.year == yr
            tasks = tasks[mask].copy()

    # ---- Store Decision Card ----
    store_warnings = []
    store_suggestions = []
    if yr_kpi:
        if yr_kpi.get('roi') is not None and yr_kpi['roi'] < 0.5:
            store_warnings.append(f"ROI仅{yr_kpi['roi']:.3f}×，投入产出严重倒挂")
        if yr_kpi.get('sales', 0) == 0:
            store_warnings.append(f"{yr}年零销售")
    else:
        d_roi = overview['detail_roi'] or overview['store_roi'] or 0
        if d_roi < 0.5:
            store_warnings.append(f"ROI仅{d_roi:.3f}×，投入产出严重倒挂")
    checkin = overview['checkin_rate'] or 0
    if checkin < 80:
        store_warnings.append(f"出勤率仅{checkin:.1f}%，低于80%警戒线")
    if overview['total_taste'] > 0 and overview['total_buy_num'] > 0:
        conv = overview['total_buy_num'] / overview['total_taste'] * 100
        if conv < 5:
            store_warnings.append(f"试饮转化率仅{conv:.1f}%，需优化试饮策略")
    if len(peer_stores) > 0 and overview['total_expenses'] > 0:
        peer_avg_roi = peer_stores['store_roi'].dropna().mean()
        my_roi = overview['store_roi'] or 0
        if my_roi < peer_avg_roi * 0.5:
            store_suggestions.append(f"ROI低于同行均值{peer_avg_roi:.3f}×的一半，建议审视门店投入有效性")
        if len(expense_detail) > 0:
            top_expense_proj = expense_detail.groupby('project_name')['total_short_term_expenses'].sum().nlargest(1)
            if len(top_expense_proj) > 0:
                store_suggestions.append(f"最大投入项目「{top_expense_proj.index[0][:20]}」，建议核查回报")
    if not store_suggestions:
        store_suggestions.append("当前数据有限，建议积累更多费用数据后再做深度判断")

    st.markdown(f"""
    <div style="background:linear-gradient(135deg, #1a1000, #140e00); border:1px solid #F59E0B; border-radius:10px; padding:14px 18px; margin:6px 0 10px 0;">
    <div style="font-size:0.82rem; font-weight:700; color:#F59E0B; margin-bottom:8px;">📋 门店决策辅助 · {overview['shop_name'][:20]}</div>
    <div style="font-size:0.76rem; color:#d0d0d0; line-height:1.6;">
    {'<br>'.join(f'<span style="color:#EF4444;">⚠ {w}</span>' for w in store_warnings) if store_warnings else '✅ 暂无预警'}
    <br><span style="color:#F59E0B;">💡 建议：</span>{'；'.join(store_suggestions)}
    </div></div>""", unsafe_allow_html=True)

    # ---- TABS ----
    tabs = st.tabs(["概览", "投入", "产出", "ROI", "建议", "质量", "查询"])

    # ===================== TAB 1: OVERVIEW =====================
    with tabs[0]:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if yr_kpi:
                kpi_card(f"投入费用 {yr_label}", format_money(yr_kpi['expenses']),
                         sub=f"工资 {format_money(yr_kpi['wages'])} · 积分 {format_money(yr_kpi['points'])}",
                         accent=DIM_COLORS["expense"])
            else:
                kpi_card("累计投入费用", format_money(overview['total_expenses']),
                         sub=f"工资 {format_money(overview['total_wages'])} · 奖金 {format_money(overview['total_bonuses'])}",
                         accent=DIM_COLORS["expense"])
        with col2:
            if yr_kpi:
                kpi_card(f"销售额 {yr_label}", format_money(yr_kpi['sales']),
                         sub=f"销量 {yr_kpi['qty']:,} 件 · 执行 {yr_kpi['arranges']:,} 次",
                         accent=DIM_COLORS["sales"])
            else:
                kpi_card("累计销售额", format_money(overview['detail_total_money']),
                         sub=f"2025年对齐: {format_money(overview.get('aligned_sales', 0) or 0)}",
                         accent=DIM_COLORS["sales"])
        with col3:
            if yr_kpi and yr_kpi['roi'] is not None:
                kpi_card(f"ROI {yr_label}", f"{yr_kpi['roi']:.3f}×",
                         sub=f"销售额 / 投入费用",
                         accent=DIM_COLORS["roi"])
            else:
                detail_roi = overview['detail_roi'] or overview['store_roi']
                aligned_roi = overview.get('aligned_roi', 0) or 0
                kpi_card("整体 ROI", f"{detail_roi:.3f}×" if detail_roi and detail_roi > 0 else "—",
                         sub=f"2025年对齐: {aligned_roi:.3f}×" if aligned_roi > 0 else "时间维度未对齐",
                         accent=DIM_COLORS["roi"])
        with col4:
            if yr_kpi:
                yr_projects = get_store_project_count_by_year(selected_store, yr)
                kpi_card(f"参与项目 {yr_label}", f"{yr_projects}",
                         sub=f"费用/销售/执行/计划覆盖",
                         accent=DIM_COLORS["project"])
            else:
                kpi_card("参与项目 / 计划数", f"{overview['plan_project_count']}",
                         sub=f"{overview['cost_project_count']} 个有费用 · {overview['total_shop_plans']} 个门店计划",
                         accent=DIM_COLORS["project"])

        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
        if yr_kpi:
            st.caption(f"⚠ 当前显示{yr}年KPI，下方图表为全时段数据供参考")
        st.markdown("#### 品牌投入产出")
        if len(brand_metrics) > 0 and brand_metrics['total_expenses'].sum() > 0:
            fig = px.scatter(brand_metrics, x='total_expenses', y='cost_sales', size='session_days',
                             color='brand_name', hover_name='brand_name',
                             labels={'total_expenses': '投入费用', 'cost_sales': '销售额', 'session_days': '场次天数'})
            fig.update_traces(marker=dict(line=dict(width=0)), selector=dict(mode='markers'))
            max_v = max(brand_metrics['total_expenses'].max(), brand_metrics['cost_sales'].max()) * 1.15
            fig.add_trace(go.Scatter(x=[0, max_v], y=[0, max_v], mode='lines',
                                     line=dict(dash='dash', color='#3a3a3a', width=1), name='ROI=1'))
            fig.update_layout(height=380, showlegend=True, legend=dict(orientation='h', y=-0.15))
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("暂无品牌投入产出数据")

        st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)
        st.markdown("#### 门店档案")
        info_items = [
            ("MU", overview.get('mu', '—')), ("RU", overview.get('ru', '—')),
            ("AU", overview.get('au', '—')), ("城市等级", overview.get('city_level_label', '—')),
            ("店群", overview.get('shop_group_label', '—')), ("贸易渠道", overview.get('trade_channel_label', '—')),
            ("客户集群", overview.get('customer_cluster_label', '—')),
            ("Account", overview.get('account_label', '—')),
            ("Top 店", "是" if overview.get('is_top') == 1 else "否"),
            ("GPS 范围", f"{overview.get('check_range','—')}m"),
        ]
        # 5-column grid for full-width display
        info_cols = st.columns(5)
        for i, (k, v) in enumerate(info_items):
            with info_cols[i % 5]:
                st.markdown(f"""<div style="background:#141414;border:1px solid #1f1f1f;border-radius:6px;padding:10px 14px;margin:2px 0;">
                    <div style="font-size:0.7rem;color:#808080;text-transform:uppercase;letter-spacing:0.03em;">{k}</div>
                    <div style="font-size:0.95rem;font-weight:600;color:#e0e0e0;">{v}</div>
                </div>""", unsafe_allow_html=True)

        st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)
        st.markdown("#### 历史项目")
        if len(shop_plans) > 0:
            plans_summary = shop_plans[['start_time', 'project_brand', 'activity_type', 'brand_sales_total']].copy()
            plans_summary.columns = ['开始时间', '品牌', '活动类型', '目标销量']
            plans_summary = plans_summary.drop_duplicates().head(8)
            st.dataframe(plans_summary, use_container_width=True, hide_index=True)
        else:
            st.caption("暂无项目记录")

        st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)
        st.markdown("#### 同行对标")
        if len(peer_stores) > 0:
            cm1, cm2, cm3, cm4 = st.columns(4)
            with cm1:
                peer_avg_roi = peer_stores['store_roi'].dropna().mean()
                my_roi = overview['store_roi'] or 0
                delta = f"{'↑' if my_roi > peer_avg_roi else '↓'} {abs(my_roi - peer_avg_roi):.1f} vs 同行均 {peer_avg_roi:.3f}"
                kpi_card("ROI 对比", f"{my_roi:.3f}×", sub=delta, accent=DIM_COLORS["roi"])
            with cm2:
                peer_avg_expense = peer_stores['total_expenses'].dropna().mean()
                kpi_card("投入对比", format_money(overview['total_expenses']),
                         sub=f"同行均 {format_money(peer_avg_expense)}", accent=DIM_COLORS["expense"])
            with cm3:
                my_sales = overview['detail_total_money'] or overview['total_achieved_amount']
                peer_avg_sales = peer_stores['detail_total_money'].dropna().mean()
                kpi_card("销售对比", format_money(my_sales),
                         sub=f"同行均 {format_money(peer_avg_sales)}", accent=DIM_COLORS["sales"])
            with cm4:
                peer_avg_checkin = peer_stores['checkin_rate'].dropna().mean()
                my_checkin = overview['checkin_rate'] or 0
                kpi_card("出勤率对比", format_pct(my_checkin),
                         sub=f"同行均 {format_pct(peer_avg_checkin)}", accent=DIM_COLORS["quality"])

        st.markdown('<div style="height:16px;"></div>', unsafe_allow_html=True)
        st.markdown("#### 时间趋势")
        trend_df = get_monthly_trend()
        if len(trend_df) > 0:
            # Time coverage notice
            cost_months = (trend_df['cost_amount'] > 0).sum()
            points_months = (trend_df['points_value'] > 0).sum()
            sales_months = (trend_df['sales_amount'] > 0).sum()
            total_cost_months = ((trend_df['cost_amount'] + trend_df['points_value']) > 0).sum()
            st.markdown(f"""<div style="background:#141414;border:1px solid #1f1f1f;border-radius:6px;padding:8px 14px;margin-bottom:8px;font-size:0.78rem;color:#808080;">
                销量覆盖 <b style="color:#10B981;">{sales_months}</b> 个月 · 费用覆盖 <b style="color:#EF4444;">{cost_months}</b> 个月 · 积分覆盖 <b style="color:#06B6D4;">{points_months}</b> 个月 · 投入合计覆盖 <b style="color:#F59E0B;">{total_cost_months}</b> 个月
            </div>""", unsafe_allow_html=True)

            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=trend_df['year_month'], y=trend_df['sales_amount'], name='月销售额',
                                marker_color='#10B981', marker=dict(opacity=0.7), yaxis='y'),
                         secondary_y=False)
            fig.add_trace(go.Bar(x=trend_df['year_month'], y=trend_df['cost_amount'], name='月费用',
                                marker_color='#EF4444', marker=dict(opacity=0.9), yaxis='y'),
                         secondary_y=False)
            fig.add_trace(go.Bar(x=trend_df['year_month'], y=trend_df['points_value'], name='月积分',
                                marker_color='#06B6D4', marker=dict(opacity=0.8), yaxis='y'),
                         secondary_y=False)
            fig.add_trace(go.Scatter(x=trend_df['year_month'], y=trend_df['monthly_roi'], name='月ROI',
                                    line=dict(color='#5E6AD2', width=2.5), mode='lines+markers',
                                    marker=dict(size=4), yaxis='y2'),
                         secondary_y=True)
            fig.update_layout(height=380, barmode='overlay',
                             legend=dict(orientation='h', y=-0.1),
                             yaxis=dict(title='金额', gridcolor='#1f1f1f'),
                             yaxis2=dict(title='ROI', gridcolor='rgba(0,0,0,0)', side='right'),
                             hovermode='x unified')
            st.plotly_chart(fig, width='stretch')

    # ===================== TAB 2: INVESTMENT =====================
    with tabs[1]:
        # Use year-filtered values if available
        if yr_kpi:
            exp_total = yr_kpi['expenses']
            wages_val = yr_kpi['wages']
            bonuses_val = yr_kpi['bonuses']
            three_sal_val = yr_kpi['three_salary']
            pts_val = yr_kpi['points']
        else:
            exp_total = overview['total_expenses']
            wages_val = overview['total_wages']
            bonuses_val = overview['total_bonuses']
            three_sal_val = overview['total_three_salary']
            pts_val = overview.get('total_points_value', 0) or 0

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            kpi_card(f"总投入费用 {yr_label}", format_money(exp_total), accent=DIM_COLORS["expense"])
        with col2:
            pct_wages = wages_val/exp_total*100 if exp_total > 0 else 0
            kpi_card(f"工资支出 {yr_label}", format_money(wages_val), sub=f"占比 {pct_wages:.1f}%", accent="#F59E0B")
        with col3:
            kpi_card("激励奖金", format_money(bonuses_val), accent=DIM_COLORS["roi"])
        with col4:
            kpi_card("三薪", format_money(three_sal_val), accent=DIM_COLORS["project"])
        with col5:
            kpi_card("积分投入", format_money(pts_val), sub="100分=¥10", accent="#06B6D4")

        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown(f"#### 费用结构 {yr_label}")
            if exp_total > 0:
                fig = go.Figure(data=[go.Pie(labels=['工资', '激励奖金', '三薪', '积分奖励'],
                        values=[wages_val, bonuses_val, three_sal_val, pts_val],
                        hole=0.45, marker_colors=['#EF4444', '#5E6AD2', '#8B5CF6', '#06B6D4'],
                        textinfo='label+percent', textfont=dict(color='#b0b0b0'))])
                fig.update_layout(height=350, showlegend=False, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig, width='stretch')

        with col_b:
            st.markdown(f"#### 费用按项目趋势 {yr_label}")
            if len(expense_detail) > 0:
                expense_detail['project_name_short'] = expense_detail['project_name'].str[:25]
                proj_bar = expense_detail.groupby('project_name_short').agg(总费用=('total_short_term_expenses', 'sum')).reset_index().sort_values('总费用', ascending=True)
                fig = px.bar(proj_bar, x='总费用', y='project_name_short', orientation='h',
                             color_discrete_sequence=['#5E6AD2'])
                fig.update_layout(height=350, showlegend=False, xaxis_title=None, yaxis_title=None)
                st.plotly_chart(fig, width='stretch')

        st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)
        st.markdown("#### 项目投入明细")
        if len(expense_detail) > 0:
            proj_exp = expense_detail.groupby('project_name').agg(
                总费用=('total_short_term_expenses', 'sum'),
                有效天数=('efficacious_session_days', 'sum'),
                达成金额=('actual_sales_achieved_amount', 'sum')
            ).reset_index().sort_values('总费用', ascending=False)
            proj_exp['ROI'] = (proj_exp['达成金额'] / proj_exp['总费用']).round(1)
            st.dataframe(proj_exp, use_container_width=True, hide_index=True,
                        column_config={'总费用': st.column_config.NumberColumn(format="¥%.0f"),
                                      '达成金额': st.column_config.NumberColumn(format="¥%.0f")})

    # ===================== TAB 3: OUTPUT =====================
    with tabs[2]:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if yr_kpi:
                kpi_card(f"销售额 {yr_label}", format_money(yr_kpi['sales']),
                         sub=f"销量 {yr_kpi['qty']:,.0f} 件", accent=DIM_COLORS["sales"])
            else:
                kpi_card("累计销售额", format_money(overview['detail_total_money']), accent=DIM_COLORS["sales"])
        with col2:
            kpi_card("销量(件数)", f"{overview['detail_total_qty']:,}",
                     sub=f"{overview['detail_total_volumes']:,} 体积单位", accent="#10B981")
        with col3:
            if yr_kpi:
                kpi_card(f"执行次数 {yr_label}", f"{yr_kpi['arranges']:,} 次",
                         sub=f"正常签到 {yr_kpi['checkins']:,}", accent="#06B6D4")
            else:
                daily = overview['daily_avg_sales']
                kpi_card("有效场次天数", f"{overview['total_session_days']:,} 天",
                         sub=f"日均 {format_money(daily)}" if daily else "", accent="#06B6D4")
        with col4:
            kpi_card("试饮总量", f"{overview['total_taste']:,}",
                     sub=f"购买转化 {overview['total_buy_num']:,} 单", accent="#F59E0B")

        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("#### 品牌销售贡献")
            if len(brand_metrics) > 0 and brand_metrics['cost_sales'].sum() > 0:
                brand_sales = brand_metrics[brand_metrics['cost_sales'] > 0].groupby('brand_name')['cost_sales'].sum().sort_values(ascending=True)
                fig = px.bar(x=brand_sales.values, y=brand_sales.index, orientation='h',
                             color_discrete_sequence=['#10B981'])
                fig.update_layout(height=350, showlegend=False, xaxis_title=None, yaxis_title=None)
                st.plotly_chart(fig, width='stretch')

        with col_b:
            st.markdown("#### 活动类型产出")
            if len(brand_metrics) > 0:
                atype = brand_metrics.groupby('activity_display').agg(
                    销售额=('cost_sales', 'sum'), ROI=('roi', 'mean')
                ).reset_index().dropna(subset=['销售额'])
                if len(atype) > 0:
                    fig = px.bar(atype, x='activity_display', y='销售额', color='activity_display',
                                text=atype['ROI'].apply(lambda x: f'ROI {x:.3f}×'))
                    fig.update_layout(height=350, showlegend=False, xaxis_title=None, yaxis_title=None)
                    fig.update_traces(textposition='outside', textfont=dict(color='#999', size=11))
                    st.plotly_chart(fig, width='stretch')

        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
        st.markdown("#### 月度销售趋势")
        if len(exec_quality) > 0:
            exec_quality['month'] = pd.to_datetime(exec_quality['arrange_date']).dt.to_period('M').astype(str)
            monthly = exec_quality.groupby('month').agg(排班次数=('arrange_date', 'count'), 品牌销量=('brand_sales', 'sum')).reset_index()
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=monthly['month'], y=monthly['排班次数'], name='排班次数', marker_color='#5E6AD2',
                                 marker=dict(opacity=0.8)), secondary_y=False)
            fig.add_trace(go.Scatter(x=monthly['month'], y=monthly['品牌销量'], name='品牌销量', mode='lines+markers',
                                    line=dict(color='#10B981', width=2), marker=dict(size=5)), secondary_y=True)
            fig.update_layout(height=350)
            st.plotly_chart(fig, width='stretch')

    # ===================== TAB 4: ROI ANALYSIS =====================
    with tabs[3]:
        col1, col2, col3 = st.columns(3)
        with col1:
            if yr_kpi and yr_kpi['roi'] is not None:
                kpi_card(f"门店 ROI {yr_label}", f"{yr_kpi['roi']:.3f}×",
                         sub=f"销售 {format_money(yr_kpi['sales'])} / 投入 {format_money(yr_kpi['expenses'])}", accent=DIM_COLORS["roi"])
            else:
                d_roi = overview['detail_roi'] or overview['store_roi']
                kpi_card("门店 ROI", f"{d_roi:.3f}×" if d_roi and d_roi > 0 else "—",
                         sub="销售额 / 投入费用", accent=DIM_COLORS["roi"])
        with col2:
            if yr_kpi and yr_kpi['expenses'] > 0:
                daily_yr = yr_kpi['sales'] / yr_kpi['arranges'] if yr_kpi['arranges'] > 0 else 0
                kpi_card(f"场均产出 {yr_label}", format_money(daily_yr), accent="#06B6D4")
            else:
                cost_eff = overview['detail_total_money'] / overview['total_session_days'] if overview['total_session_days'] > 0 else 0
                kpi_card("场次日均产出", format_money(cost_eff), accent="#06B6D4")
        with col3:
            if yr_kpi and yr_kpi['wages'] > 0:
                labor_yr = yr_kpi['sales'] / yr_kpi['wages']
                kpi_card(f"工资投产比 {yr_label}", f"{labor_yr:.1f}×", sub="销售额 / 工资", accent=DIM_COLORS["project"])
            else:
                labor_eff = overview['detail_total_money'] / overview['total_wages'] if overview['total_wages'] > 0 else 0
                kpi_card("工资投产比", f"{labor_eff:.1f}×", sub="销售额 / 工资", accent=DIM_COLORS["project"])

        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("#### 品牌 ROI 排名")
            if len(brand_metrics) > 0 and brand_metrics['roi'].notna().sum() > 0:
                br = brand_metrics.dropna(subset=['roi']).nlargest(8, 'roi')
                colors = ['#5E6AD2' if i == 0 else '#3a3a5c' for i in range(len(br))]
                fig = px.bar(br.sort_values('roi'), x='roi', y='brand_name', orientation='h',
                             color_discrete_sequence=['#5E6AD2'])
                fig.add_vline(x=1, line_dash='dash', line_color='#3a3a3a', annotation_text='ROI=1', annotation_font_color='#666')
                fig.update_layout(height=360, showlegend=False, xaxis_title='ROI', yaxis_title=None)
                st.plotly_chart(fig, width='stretch')

        with col_b:
            st.markdown("#### 品牌 × 活动类型 ROI")
            if len(brand_metrics) > 0 and brand_metrics['roi'].notna().sum() > 0:
                heatmap_data = brand_metrics.pivot_table(values='roi', index='brand_name', columns='activity_display', aggfunc='mean')
                if not heatmap_data.empty:
                    fig = px.imshow(heatmap_data, text_auto='.1f', aspect='auto',
                                   color_continuous_scale=[[0, '#141414'], [0.5, '#2a2040'], [1, '#5E6AD2']])
                    fig.update_layout(height=360)
                    st.plotly_chart(fig, width='stretch')

        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
        st.markdown("#### ROI × 投入 矩阵")
        if len(brand_metrics) > 0 and brand_metrics['total_expenses'].notna().sum() > 0:
            bm = brand_metrics.dropna(subset=['total_expenses', 'roi', 'cost_sales'])
            fig = px.scatter(bm, x='total_expenses', y='roi', size='session_days', color='brand_name',
                            labels={'total_expenses': '投入费用', 'roi': 'ROI'})
            fig.add_hline(y=1, line_dash='dash', line_color='#3a3a3a')
            avg_roi = bm['roi'].mean()
            fig.add_hline(y=avg_roi, line_dash='dot', line_color='#5E6AD2', annotation_text=f'均值 {avg_roi:.3f}', annotation_font_color='#5E6AD2')
            fig.update_traces(marker=dict(line=dict(width=0)))
            fig.update_layout(height=380)
            st.plotly_chart(fig, width='stretch')

    # ===================== TAB 5: RECOMMENDATIONS =====================
    with tabs[4]:
        st.markdown("#### 投资建议引擎")
        st.caption("综合评分：ROI(40%) + 销售绝对值(25%) + 执行质量(20%) + 稳定性(10%) + 同行参照(5%)")

        if len(brand_metrics) > 0:
            # Use detail_roi and detail_sales for richer scoring
            brand_metrics['_roi'] = brand_metrics['detail_roi'].fillna(brand_metrics['roi'])
            brand_metrics['_sales'] = brand_metrics['detail_sales'].fillna(brand_metrics['cost_sales'])
            valid = brand_metrics.dropna(subset=['_roi', '_sales']).copy()
            if len(valid) > 0:
                for col_name in ['_roi', '_sales', 'session_days']:
                    std = valid[col_name].std()
                    valid[f'{col_name}_norm'] = (valid[col_name] - valid[col_name].min()) / (valid[col_name].max() - valid[col_name].min()) if std > 0 else 0.5
                valid['consistency_norm'] = valid['session_days_norm']
                exec_score = overview['checkin_rate'] / 100 if overview['checkin_rate'] else 0.5
                task_score = overview['task_pass_rate'] / 100 if overview['task_pass_rate'] else 0.5
                valid['综合得分'] = (0.40 * valid['_roi_norm'] + 0.25 * valid['_sales_norm'] +
                                   0.20 * (exec_score + task_score) / 2 + 0.10 * valid['consistency_norm'] + 0.05 * 0.5)
                valid['综合得分'] = (valid['综合得分'] * 100).round(1)
                valid = valid.sort_values('综合得分', ascending=False)

                st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
                col_r1, col_r2 = st.columns([2, 1])
                with col_r1:
                    st.markdown("##### 品牌投资推荐排名")
                    rec = valid[['brand_name', 'activity_type', '_roi', '_sales', 'total_expenses', '综合得分']].head(8)
                    rec.columns = ['品牌', '活动类型', 'ROI', '销售额', '投入费用', '得分']
                    st.dataframe(rec, use_container_width=True, hide_index=True,
                               column_config={'ROI': st.column_config.NumberColumn(format="%.1f×"),
                                             '销售额': st.column_config.NumberColumn(format="¥%.0f"),
                                             '投入费用': st.column_config.NumberColumn(format="¥%.0f")})

                with col_r2:
                    best = valid.iloc[0]
                    kpi_card("🏆 首选品牌", best['brand_name'], sub=f"综合得分 {best['综合得分']:.0f} 分", accent="#F59E0B")
                    kpi_card("推荐活动类型", best['activity_type'] if pd.notna(best['activity_type']) else "综合", accent=DIM_COLORS["roi"])
                    kpi_card("历史 ROI", f"{best['_roi']:.3f}×", accent=DIM_COLORS["sales"])
                    kpi_card("历史销售额", format_money(best['_sales']), accent=DIM_COLORS["project"])

                st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
                st.markdown("##### 陈列与促销员建议")
                col_s1, col_s2 = st.columns(2)
                with col_s1:
                    if len(shop_plans) > 0:
                        display_analysis = shop_plans[shop_plans['display_type'].notna()].groupby('display_type').agg(
                            计划数=('brand_sales_total', 'count'), 平均目标销量=('brand_sales_total', 'mean'),
                            POSM率=('have_posm', 'mean'), DM率=('have_dm', 'mean')
                        ).reset_index().round(2)
                        st.dataframe(display_analysis, use_container_width=True, hide_index=True)
                with col_s2:
                    promo_eff = overview['total_achieved_amount'] / overview['arr_promoter_count'] if overview['arr_promoter_count'] > 0 else 0
                    st.markdown(f"""
                    <div style='color:#b0b0b0;font-size:0.85rem;line-height:1.8;'>
                    历史促销员 <b style='color:#e6e6e6;'>{overview['arr_promoter_count']}</b> 人<br>
                    人均产出 <b style='color:#e6e6e6;'>{format_money(promo_eff)}</b><br>
                    场次日均销售 <b style='color:#e6e6e6;'>{format_money(overview['daily_avg_sales'])}</b>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.warning("数据不足以生成投资建议")
        else:
            st.warning("该门店暂无品牌维度数据")

    # ===================== TAB 6: QUALITY =====================
    with tabs[5]:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            kpi_card("正常出勤率", format_pct(overview['checkin_rate']), accent=DIM_COLORS["sales"])
        with col2:
            kpi_card("迟到次数", f"{overview['late_count']:,}", accent=DIM_COLORS["expense"])
        with col3:
            kpi_card("任务通过率", format_pct(overview['task_pass_rate']), accent=DIM_COLORS["roi"])
        with col4:
            gps_val = f"{overview['avg_start_gps_dev']:.0f}m" if overview['avg_start_gps_dev'] and overview['avg_start_gps_dev'] > 0 else "—"
            kpi_card("平均 GPS 偏差", gps_val, accent=DIM_COLORS["project"])

        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("#### 出勤质量趋势")
            if len(exec_quality) > 0:
                exec_quality['month'] = pd.to_datetime(exec_quality['arrange_date']).dt.to_period('M').astype(str)
                monthly_q = exec_quality.groupby('month').agg(
                    总排班=('arrange_date', 'count'), 迟到=('start_is_late', 'sum'),
                    早退=('end_is_leave_early', 'sum'), 正常=('start_status', lambda x: (x == 1).sum())
                ).reset_index()
                monthly_q['出勤率'] = (monthly_q['正常'] / monthly_q['总排班'] * 100).round(1)
                fig = go.Figure()
                fig.add_trace(go.Bar(x=monthly_q['month'], y=monthly_q['总排班'], name='总排班', marker_color='#5E6AD2', opacity=0.7))
                fig.add_trace(go.Bar(x=monthly_q['month'], y=monthly_q['迟到'], name='迟到', marker_color='#EF4444', opacity=0.8))
                fig.add_trace(go.Bar(x=monthly_q['month'], y=monthly_q['早退'], name='早退', marker_color='#F59E0B', opacity=0.8))
                fig.add_trace(go.Scatter(x=monthly_q['month'], y=monthly_q['出勤率'], name='出勤率 %', yaxis='y2',
                                        line=dict(color='#10B981', width=2), mode='lines+markers', marker=dict(size=4)))
                fig.update_layout(barmode='stack', height=350, yaxis2=dict(overlaying='y', side='right', range=[0, 105],
                                   gridcolor='rgba(0,0,0,0)'), hovermode='x')
                st.plotly_chart(fig, width='stretch')

        with col_b:
            st.markdown("#### 任务执行")
            if len(tasks) > 0:
                task_summary = tasks.groupby('task_type_label').agg(总数=('is_pass', 'count'), 通过率=('is_pass', 'mean')).reset_index()
                task_summary['通过率'] = (task_summary['通过率'] * 100).round(1)
                fig = px.bar(task_summary, x='task_type_label', y='通过率', color='task_type_label',
                            text=task_summary['通过率'].apply(lambda x: f'{x:.0f}%'))
                fig.update_layout(height=350, showlegend=False, yaxis_range=[0, 110], xaxis_title=None, yaxis_title='通过率 %')
                fig.update_traces(textposition='outside', textfont=dict(color='#999', size=11))
                st.plotly_chart(fig, width='stretch')
            else:
                st.info("暂无任务数据")

        st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
        st.markdown("#### 审批与工作状态")
        if len(exec_quality) > 0:
            col_s1, col_s2 = st.columns(2)
            with col_s1:
                pos_audit = exec_quality['end_pos_audit_status'].value_counts().reset_index()
                pos_audit.columns = ['审批状态', '次数']
                st.dataframe(pos_audit, use_container_width=True, hide_index=True)
            with col_s2:
                leave_stats = exec_quality['work_status'].value_counts().reset_index()
                leave_stats.columns = ['工作状态', '次数']
                work_map = {1: '正常', 2: '请假', 3: '其他'}
                leave_stats['工作状态'] = leave_stats['工作状态'].map(work_map)
                st.dataframe(leave_stats, use_container_width=True, hide_index=True)

    # ===================== TAB 7: SELF-SERVICE =====================
    with tabs[6]:
        st.markdown("#### 自助查询")
        query_type = st.selectbox("查询维度", ["门店关键指标", "品牌 × 门店分析", "费用明细", "排班明细", "任务明细", "门店计划详情"], label_visibility="collapsed")

        st.markdown('<div style="height:4px;"></div>', unsafe_allow_html=True)
        data = None
        if query_type == "门店关键指标":
            limit = st.slider("显示条数", 100, 5000, 500, 100)
            data = con.execute(f"""
                SELECT shop_name AS 门店, city_name AS 城市, province_name AS 省份,
                       COALESCE(city_level_label, '—') AS 城市等级,
                       total_expenses AS 总投入, detail_total_money AS 总销售, store_roi AS ROI,
                       total_session_days AS 有效天数, checkin_rate AS 出勤率, task_pass_rate AS 任务通过率
                FROM v_store_metrics WHERE total_expenses > 0 ORDER BY total_expenses DESC LIMIT {limit}
            """).df()
        elif query_type == "品牌 × 门店分析":
            data = con.execute("""SELECT shop_name AS 门店, city_name AS 城市, brand_name AS 品牌,
                activity_type AS 活动类型, roi AS ROI, cost_sales AS 销售额, total_expenses AS 投入
                FROM v_store_brand_metrics WHERE roi IS NOT NULL ORDER BY roi DESC LIMIT 1000""").df()
        elif query_type == "费用明细":
            data = con.execute("""SELECT shop_name AS 门店, project_name AS 项目, promoter_name AS 促销员,
                efficacious_session_days AS 有效天数, wages AS 工资, incentive_bonuses AS 奖金,
                total_short_term_expenses AS 总费用, actual_sales_achieved_amount AS 达成金额
                FROM fact_store_cost ORDER BY total_short_term_expenses DESC LIMIT 1000""").df()
        elif query_type == "排班明细":
            data = con.execute("""SELECT arrange_date AS 日期, promoter_name AS 促销员, project_name AS 项目,
                start_status, end_status, start_is_late AS 迟到, brand_sales AS 销量
                FROM fact_store_execution ORDER BY arrange_date DESC LIMIT 1000""").df()
        elif query_type == "任务明细":
            data = con.execute("""SELECT task_name AS 任务名, COALESCE(task_type_label, task_type) AS 任务类型, exe_time AS 执行时间,
                status AS 状态, promoter_name AS 促销员 FROM fact_store_task ORDER BY exe_time DESC LIMIT 1000""").df()
        elif query_type == "门店计划详情":
            data = con.execute("""SELECT plan_shop_name AS 门店, project_brand AS 品牌, activity_type AS 活动类型,
                display_type AS 陈列, display_area AS 面积, have_posm AS POSM, dm_price AS DM价
                FROM fact_shop_plan WHERE dim_store_unicode IS NOT NULL ORDER BY start_time DESC LIMIT 1000""").df()

        if data is not None:
            st.dataframe(data, use_container_width=True, hide_index=True)
            st.download_button("导出 CSV", data.to_csv(index=False).encode('utf-8'), f"{query_type}.csv", "text/csv")

    # ===================== TAB 8: PROMOTER =====================
elif section == "促销员360°":
    show_promoter_ai = st.checkbox("⚡ AI 决策摘要 · 促销员360", value=True, key="show_promoter_ai")
    if show_promoter_ai:
        st.markdown(ai_summary_promoter(yr_stats), unsafe_allow_html=True)
    promoter_df = get_promoter_list()

    # --- Filters ---
    col_f1, col_f2, col_f3, col_f4 = st.columns([2, 1, 1, 1])
    with col_f1:
        promo_search = st.text_input("搜索促销员", placeholder="姓名、手机号、主管...", key="promo_search", label_visibility="collapsed")
    with col_f2:
        min_arranges = st.number_input("最低场次", 0, 10000, 0, 100, key="min_arranges", label_visibility="collapsed")
    with col_f3:
        gender_filter = st.selectbox("性别", ["全部", "男", "女"], key="gender_filter", label_visibility="collapsed")
    with col_f4:
        score_tier_filter = st.selectbox("评级", ["全部", "卓越", "优秀", "良好", "待提升"], key="tier_filter", label_visibility="collapsed")

    pfiltered = promoter_df.copy()
    if promo_search:
        mask = (pfiltered['promoter_name'].str.contains(promo_search, na=False, case=False) |
                pfiltered['promoter_mobile'].str.contains(promo_search, na=False, case=False) |
                pfiltered['supervisor_name'].str.contains(promo_search, na=False, case=False))
        pfiltered = pfiltered[mask]
    if min_arranges > 0:
        pfiltered = pfiltered[pfiltered['total_arranges'] >= min_arranges]
    if gender_filter == "男":
        pfiltered = pfiltered[pfiltered['gender'] == 1]
    elif gender_filter == "女":
        pfiltered = pfiltered[pfiltered['gender'] == 2]

    # Score all filtered promoters
    pfiltered_scores = pfiltered.copy()
    # Pre-init score columns to avoid KeyError on empty dataframes
    for col in ['综合得分', '产出得分', '质量得分', '效率得分', '等级']:
        pfiltered_scores[col] = 0
        pfiltered_scores[col] = pfiltered_scores[col].astype(object)
    for idx, row in pfiltered_scores.iterrows():
        s = score_promoter(row)
        pfiltered_scores.at[idx, '综合得分'] = s['total']
        pfiltered_scores.at[idx, '产出得分'] = s['output']
        pfiltered_scores.at[idx, '质量得分'] = s['quality']
        pfiltered_scores.at[idx, '效率得分'] = s['efficiency']
        pfiltered_scores.at[idx, '等级'] = s['tier']
    if score_tier_filter != "全部":
        pfiltered_scores = pfiltered_scores[pfiltered_scores['等级'] == score_tier_filter]

    pfiltered = pfiltered[pfiltered['promoter_unicode'].isin(pfiltered_scores['promoter_unicode'])]
    # Full scored list for KPI stats
    total_p = len(pfiltered_scores)
    if total_p == 0:
        st.warning("没有符合条件的促销员")
        st.stop()
    # Top-N for selectbox display (keeps UI responsive)
    promoter_display = pfiltered_scores.sort_values('综合得分', ascending=False).head(500)

    # --- Overview KPIs ---
    total_all = con.execute("SELECT COUNT(*) FROM stg_promoter_profile").fetchone()[0]
    active_all = con.execute("SELECT COUNT(*) FROM v_promoter_metrics").fetchone()[0]
    st.markdown(f"<span style='color:#808080;font-size:0.8rem;'>促销员总数 {total_all:,} · 活跃 {active_all:,}（有执行/销售/费用任一记录）· 当前筛选 {total_p:,} 人（展示 Top 500）</span>", unsafe_allow_html=True)
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        kpi_card("促销员总数", f"{total_all:,}", sub=f"活跃 {active_all:,} 人", accent="#5E6AD2")
    with col2:
        avg_att = pfiltered_scores['attendance_rate'].dropna().mean()
        kpi_card("平均出勤率", format_pct(avg_att), accent=DIM_COLORS["sales"])
    with col3:
        avg_score = pfiltered_scores['综合得分'].mean()
        kpi_card("平均综合得分", f"{avg_score:.1f}" if pd.notna(avg_score) else "—", accent=DIM_COLORS["roi"])
    with col4:
        top_tier = (pfiltered_scores['等级'] == '卓越').sum()
        kpi_card("卓越人数", f"{top_tier}", accent="#F59E0B")
    with col5:
        total_sales_all = pfiltered_scores['detail_total_money'].sum()
        kpi_card("总销售额", format_money(total_sales_all), accent=DIM_COLORS["sales"])

    st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)

    # --- Promoter Selector ---
    if len(promoter_display) == 0:
        st.warning("没有符合条件的促销员")
        st.stop()

    def _fmt_promoter(i):
        row = promoter_display.iloc[i]
        tier = row.get('等级', '')
        return f"{row['promoter_name']}  ·  {row.get('profile_city', '—') or '—'}  ·  场次{int(row['total_arranges'])}  ·  {tier}"

    promo_idx = st.selectbox("选择促销员", range(len(promoter_display)),
        format_func=_fmt_promoter, label_visibility="collapsed")

    selected_promoter = promoter_display.iloc[promo_idx]
    pu = selected_promoter['promoter_unicode']

    # Year filter for promoter KPIs
    promo_year = st.radio("统计年份", ["全部", "2025年", "2026年"], horizontal=True, key="promo_year",
                          label_visibility="collapsed")

    poverview = get_promoter_overview(pu)

    # --- Personal KPI cards ---
    st.markdown('<div style="height:4px;"></div>', unsafe_allow_html=True)

    # Year-filtered promoter KPIs
    p_yr_kpi = None
    p_yr_label = ""
    if promo_year != "全部":
        p_yr = int(promo_year.replace("年", ""))
        p_yr_kpi = get_promoter_kpi_by_year(pu, p_yr)
        p_yr_label = f"({promo_year})"

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        kpi_card("综合得分", f"{selected_promoter.get('综合得分', 0):.0f}",
                 sub=f"{selected_promoter.get('等级', '—')}", accent="#5E6AD2")
    with col2:
        if p_yr_kpi:
            kpi_card(f"销售额 {p_yr_label}", format_money(p_yr_kpi['sales']),
                     sub=f"投入 {format_money(p_yr_kpi['expenses'])} · ROI {p_yr_kpi['roi']:.3f}×" if p_yr_kpi['roi'] else "",
                     accent=DIM_COLORS["sales"])
        else:
            kpi_card("总销售额", format_money(poverview['detail_total_money']), accent=DIM_COLORS["sales"])
    with col3:
        kpi_card("出勤率", format_pct(poverview['attendance_rate']), accent=DIM_COLORS["quality"])
    with col4:
        kpi_card("任务通过率", format_pct(poverview['task_pass_rate']), accent=DIM_COLORS["roi"])
    with col5:
        if p_yr_kpi and p_yr_kpi['wages'] > 0:
            p_labor_yr = p_yr_kpi['sales'] / p_yr_kpi['wages']
            kpi_card(f"工资投产比 {p_yr_label}", f"{p_labor_yr:.1f}×",
                     sub=f"销售 {format_money(p_yr_kpi['sales'])} / 工资 {format_money(p_yr_kpi['wages'])}", accent=DIM_COLORS["project"])
        else:
            kpi_card("工资投产比", f"{poverview['labor_efficiency']:.1f}×" if poverview['labor_efficiency'] and poverview['labor_efficiency'] > 0 else "—",
                     accent=DIM_COLORS["project"])
    with col6:
        kpi_card("试饮转化率", format_pct(poverview['taste_conversion']), accent="#06B6D4")

    # --- Profile info ---
    gender_label = {1: '男', 2: '女'}.get(poverview['gender'], '—')
    st.markdown(f"""<div style='display:flex;gap:24px;flex-wrap:wrap;padding:6px 0;color:#808080;font-size:0.8rem;'>
        <span>性别 <b style='color:#d0d0d0;'>{gender_label}</b></span>
        <span>年龄 <b style='color:#d0d0d0;'>{poverview.get('age','—') or '—'}岁</b></span>
        <span>会员等级 <b style='color:#d0d0d0;'>{poverview.get('membership_name','—') or '—'}</b></span>
        <span>所属城市 <b style='color:#d0d0d0;'>{poverview.get('profile_city','—') or '—'}</b></span>
        <span>AU <b style='color:#d0d0d0;'>{poverview.get('profile_au','—') or '—'}</b></span>
        <span>主管 <b style='color:#d0d0d0;'>{poverview.get('supervisor_name','—') or '—'}</b></span>
    </div>""", unsafe_allow_html=True)

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

    # --- Promoter Decision Card ---
    p_warnings = []
    p_suggestions = []
    if p_yr_kpi:
        if p_yr_kpi.get('roi') is not None and p_yr_kpi['roi'] < 0.5:
            p_warnings.append(f"{promo_year}年ROI仅{p_yr_kpi['roi']:.3f}×，投入产出倒挂")
        if p_yr_kpi.get('sales', 0) == 0:
            p_warnings.append(f"{promo_year}年零销售")
    score = selected_promoter.get('综合得分', 0)
    dims = {'产出': selected_promoter.get('产出得分', 0), '质量': selected_promoter.get('质量得分', 0), '效率': selected_promoter.get('效率得分', 0)}
    weakest = min(dims, key=dims.get)
    p_suggestions.append(f"最弱维度：{weakest}({dims[weakest]:.0f}分)，建议针对性提升")
    if score < 40:
        p_warnings.append(f"综合得分仅{score:.0f}分，处于待提升区间")
    att = poverview['attendance_rate'] or 0
    if att < 80:
        p_warnings.append(f"出勤率仅{att:.1f}%，低于警戒线")
    if not p_warnings:
        p_warnings.append("✅ 关键指标正常")

    st.markdown(f"""
    <div style="background:linear-gradient(135deg, #1a1000, #140e00); border:1px solid #F59E0B; border-radius:10px; padding:14px 18px; margin:6px 0 10px 0;">
    <div style="font-size:0.82rem; font-weight:700; color:#F59E0B; margin-bottom:8px;">📋 促销员决策辅助 · {selected_promoter['promoter_name']} · {selected_promoter.get('等级','—')}</div>
    <div style="font-size:0.76rem; color:#d0d0d0; line-height:1.6;">
    {'<br>'.join(f'<span style="color:#EF4444;">⚠ {w}</span>' for w in p_warnings) if isinstance(p_warnings[0], str) and p_warnings[0].startswith('⚠') else '<br>'.join(f'<span style="color:#EF4444;">⚠ {w}</span>' if w.startswith('⚠') or w.startswith('ROI') or w.startswith('综合') or w.startswith('出勤') else f'<span style="color:#10B981;">{w}</span>' for w in p_warnings)}
    <br><span style="color:#F59E0B;">💡 建议：</span>{'；'.join(p_suggestions)}
    </div></div>""", unsafe_allow_html=True)

    # --- Charts row: radar + brand bar ---
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### 能力指纹")
        radar_fields = [
            ('出勤率', poverview['attendance_rate'] or 0),
            ('任务通过率', poverview['task_pass_rate'] or 0),
            ('试饮转化率', min(poverview['taste_conversion'] or 0, 100)),
            ('产出得分', selected_promoter.get('产出得分', 0)),
            ('质量得分', selected_promoter.get('质量得分', 0)),
            ('效率得分', selected_promoter.get('效率得分', 0)),
        ]
        fig = go.Figure(data=go.Scatterpolar(
            r=[v for _, v in radar_fields],
            theta=[k for k, _ in radar_fields],
            fill='toself',
            marker=dict(color='#5E6AD2', size=4),
            line=dict(color='#5E6AD2', width=2),
            fillcolor='rgba(94,106,210,0.2)'
        ))
        fig.update_layout(
            polar=dict(
                radialaxis=dict(range=[0, 100], gridcolor='#1f1f1f', linecolor='#2a2a2a',
                              tickfont=dict(color='#666', size=9)),
                angularaxis=dict(gridcolor='#1f1f1f', linecolor='#2a2a2a',
                               tickfont=dict(color='#b0b0b0', size=10))
            ),
            height=340, showlegend=False, margin=dict(l=30, r=30, t=10, b=10)
        )
        st.plotly_chart(fig, width='stretch')

    with col_b:
        st.markdown("#### 品牌销售贡献")
        brand_sales_df = get_promoter_brand_sales(pu)
        if len(brand_sales_df) > 0:
            fig = px.bar(brand_sales_df.head(8).sort_values('total_sales'), x='total_sales', y='brand_name',
                        orientation='h', color_discrete_sequence=['#10B981'])
            fig.update_layout(height=340, showlegend=False, xaxis_title=None, yaxis_title=None)
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("暂无品牌销售数据")

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

    # --- Ranking Table ---
    st.markdown("#### 促销员排名")
    rank_df = pfiltered_scores.sort_values('综合得分', ascending=False).head(200)
    tier_colors = {'卓越': '#10B981', '优秀': '#5E6AD2', '良好': '#F59E0B', '待提升': '#EF4444'}

    def highlight_tier(val):
        return f'background-color:{tier_colors.get(val,"#333")};color:#fff;padding:2px 8px;border-radius:4px;font-weight:600;'

    display_cols = {
        'promoter_name': '姓名', 'profile_city': '城市', '综合得分': '综合得分',
        '产出得分': '产出', '质量得分': '质量', '效率得分': '效率',
        '等级': '等级', 'total_arranges': '场次', 'detail_total_money': '销售额',
        'attendance_rate': '出勤率', 'task_pass_rate': '任务通过率'
    }
    display_df = rank_df[list(display_cols.keys())].rename(columns=display_cols).copy()
    display_df['销售额'] = display_df['销售额'].apply(format_money)
    display_df['出勤率'] = display_df['出勤率'].apply(format_pct)
    display_df['任务通过率'] = display_df['任务通过率'].apply(format_pct)
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=400,
                column_config={
                    '综合得分': st.column_config.NumberColumn(format="%.0f"),
                    '产出': st.column_config.NumberColumn(format="%.0f"),
                    '质量': st.column_config.NumberColumn(format="%.0f"),
                    '效率': st.column_config.NumberColumn(format="%.0f"),
                })

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

    # --- Multi-select comparison mode ---
    with st.expander("多选对比模式"):
        compare_promoters = st.multiselect("选择促销员进行对比", promoter_display['promoter_name'].tolist(),
                                           max_selections=5, key="compare_promoters")
        if len(compare_promoters) >= 2:
            compare_df = promoter_display[promoter_display['promoter_name'].isin(compare_promoters)]
            fig = go.Figure()
            for _, row in compare_df.iterrows():
                radar_fields_c = [
                    ('出勤率', row['attendance_rate'] or 0),
                    ('任务通过率', row['task_pass_rate'] or 0),
                    ('试饮转化率', min(row['taste_conversion'] or 0, 100)),
                    ('产出得分', row.get('产出得分', 0)),
                    ('质量得分', row.get('质量得分', 0)),
                    ('效率得分', row.get('效率得分', 0)),
                ]
                fig.add_trace(go.Scatterpolar(
                    r=[v for _, v in radar_fields_c],
                    theta=[k for k, _ in radar_fields_c],
                    name=row['promoter_name'],
                    fill='toself', opacity=0.35
                ))
            fig.update_layout(
                polar=dict(radialaxis=dict(range=[0, 100], gridcolor='#1f1f1f'),
                          angularaxis=dict(gridcolor='#1f1f1f')),
                height=400, legend=dict(orientation='h', y=-0.05)
            )
            st.plotly_chart(fig, width='stretch')

            compare_tbl = compare_df[['promoter_name', '综合得分', '产出得分', '质量得分', '效率得分', '等级']].copy()
            compare_tbl.columns = ['姓名', '综合得分', '产出', '质量', '效率', '等级']
            st.dataframe(compare_tbl.sort_values('综合得分', ascending=False), use_container_width=True, hide_index=True)

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

    # --- Detail drilling ---
    with st.expander("明细下钻"):
        detail_tab = st.radio("选择数据类型", ["执行记录", "销售明细", "任务明细", "费用明细"],
                              horizontal=True, key="promoter_detail_tab")

        if detail_tab == "执行记录":
            exec_df = get_promoter_execution_detail(pu)
            if len(exec_df) > 0:
                exec_df['考勤'] = exec_df['start_status'].map({1: '✓', 0: '✗'}).fillna('?')
                show = exec_df[['arrange_date', 'project_name', 'activity_type_label', '考勤',
                               'brand_sales', 'taste_info', 'buy_num', 'start_position_deviation']].head(200)
                show.columns = ['日期', '项目', '活动类型', '考勤', '品牌销量', '试饮', '购买', 'GPS偏差']
                st.dataframe(show, use_container_width=True, hide_index=True)
            else:
                st.caption("暂无执行记录")

        elif detail_tab == "销售明细":
            sales_df = get_promoter_sales_detail(pu)
            if len(sales_df) > 0:
                show = sales_df[['project_name', 'product_name', 'product_brand', 'total_sales', 'totalMoney']].head(200)
                show.columns = ['项目', '商品', '品牌', '销量', '销售额']
                st.dataframe(show, use_container_width=True, hide_index=True,
                           column_config={'销售额': st.column_config.NumberColumn(format="¥%.0f")})
            else:
                st.caption("暂无销售明细")

        elif detail_tab == "任务明细":
            task_df = get_promoter_tasks_detail(pu)
            if len(task_df) > 0:
                show = task_df[['task_name', 'task_type_label', 'exe_time', 'status', 'reward_point']].head(200)
                show.columns = ['任务名', '任务类型', '执行时间', '状态', '积分']
                st.dataframe(show, use_container_width=True, hide_index=True)
            else:
                st.caption("暂无任务明细")

        elif detail_tab == "费用明细":
            cost_df = get_promoter_cost_detail(pu)
            if len(cost_df) > 0:
                show = cost_df[['project_name', 'shop_name', 'efficacious_session_days',
                               'wages', 'incentive_bonuses', 'total_short_term_expenses']].head(200)
                show.columns = ['项目', '门店', '有效天数', '工资', '奖金', '总费用']
                st.dataframe(show, use_container_width=True, hide_index=True,
                           column_config={'工资': st.column_config.NumberColumn(format="¥%.0f"),
                                         '奖金': st.column_config.NumberColumn(format="¥%.0f"),
                                         '总费用': st.column_config.NumberColumn(format="¥%.0f")})
            else:
                st.caption("暂无费用明细")

# ===================== TAB 9: PROJECT =====================
elif section == "项目360°":
    show_proj_ai = st.checkbox("⚡ AI 决策摘要 · 项目360", value=True, key="show_proj_ai")
    if show_proj_ai:
        st.markdown(ai_summary_project(yr_stats), unsafe_allow_html=True)
    project_df = get_project_list()

    # --- Overview KPIs ---
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        kpi_card("有数据项目数", f"{len(project_df)}",
                 sub=f"进行中 {project_df[project_df['status']=='PROGRESS'].shape[0]} 个", accent="#5E6AD2")
    with col2:
        total_inv = project_df['total_expenses'].sum()
        kpi_card("总投入", format_money(total_inv), accent=DIM_COLORS["expense"])
    with col3:
        avg_roi = project_df['detail_roi'].dropna().mean()
        kpi_card("平均 ROI", f"{avg_roi:.3f}×" if pd.notna(avg_roi) else "—", accent=DIM_COLORS["roi"])
    with col4:
        total_detail = project_df['detail_total_money'].sum()
        kpi_card("总销售额(明细)", format_money(total_detail), accent=DIM_COLORS["sales"])

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

    # --- Project Selector ---
    if len(project_df) == 0:
        st.warning("暂无项目数据")
        st.stop()

    def _fmt_project(i):
        row = project_df.iloc[i]
        roi = row['detail_roi'] or row['cost_roi']
        roi_str = f"ROI {roi:.3f}×" if roi and roi > 0 else "—"
        at_label = row.get('activity_type_label', '') or row.get('activity_type', '—') or '—'
        return f"{row['project_name'][:40]}  ·  {row.get('brand_name','—') or '—'}  ·  {at_label}  ·  {roi_str}"

    proj_idx = int(st.selectbox("选择项目", range(len(project_df)),
        format_func=_fmt_project, label_visibility="collapsed", key="project_selector"))

    selected_project = project_df.iloc[proj_idx]
    pu_proj = selected_project['project_unicode']
    pj_overview = get_project_overview(pu_proj)

    # --- Project Detail KPIs ---
    at_display = pj_overview.get('activity_type_label', '') or pj_overview.get('activity_type', '—') or '—'
    pt_display = pj_overview.get('project_type_label', '') or pj_overview.get('project_type', '—') or '—'
    st.markdown(f"<span style='font-size:1rem;font-weight:600;color:#e6e6e6;'>{pj_overview['project_name'][:60]}</span>", unsafe_allow_html=True)
    st.markdown(f"<span style='font-size:0.75rem;color:#666;'>品牌 {pj_overview.get('brand_name','—') or '—'}  ·  活动 {at_display}  ·  类型 {pt_display}</span>", unsafe_allow_html=True)
    st.markdown('<div style="height:4px;"></div>', unsafe_allow_html=True)

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        d_roi = pj_overview['detail_roi'] or pj_overview['cost_roi']
        kpi_card("ROI", f"{d_roi:.3f}×" if d_roi and d_roi > 0 else "—",
                 sub=f"费用ROI {pj_overview['cost_roi']:.3f}×" if pj_overview['cost_roi'] else "", accent=DIM_COLORS["roi"])
    with col2:
        kpi_card("总投入", format_money(pj_overview['total_expenses']), accent=DIM_COLORS["expense"])
    with col3:
        kpi_card("总销售额", format_money(pj_overview['detail_total_money']), accent=DIM_COLORS["sales"])
    with col4:
        kpi_card("达成率", format_pct(pj_overview['achievement_rate']), accent=DIM_COLORS["sales"])
    with col5:
        kpi_card("试饮转化", format_pct(pj_overview['taste_conversion']), accent="#F59E0B")
    with col6:
        kpi_card("覆盖门店", f"{int(pj_overview['cost_store_count'])}",
                 sub=f"{int(pj_overview['cost_promoter_count'])} 名促销员", accent=DIM_COLORS["project"])

    st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)

    # --- Charts row 1: Scatter + Bar ---
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### ROI × 投入 矩阵 (全部项目)")
        if len(project_df) > 0:
            plot_df = project_df.dropna(subset=['total_expenses', 'detail_roi']).copy()
            plot_df['display_roi'] = plot_df['detail_roi'].clip(-5, 50)
            fig = px.scatter(plot_df, x='total_expenses', y='display_roi', size='cost_store_count',
                            color='activity_type', hover_name='project_name',
                            labels={'total_expenses': '投入费用', 'display_roi': 'ROI', 'activity_type': '活动类型'})
            fig.add_hline(y=1, line_dash='dash', line_color='#3a3a3a', annotation_text='ROI=1', annotation_font_color='#666')
            fig.update_traces(marker=dict(line=dict(width=0)))
            fig.update_layout(height=360, legend=dict(orientation='h', y=-0.15))
            st.plotly_chart(fig, width='stretch')

    with col_b:
        st.markdown("#### Top 10 ROI 项目")
        top10 = project_df.dropna(subset=['detail_roi']).nlargest(10, 'detail_roi')
        if len(top10) > 0:
            top10['short_name'] = top10['project_name'].str[:30]
            fig = px.bar(top10.sort_values('detail_roi'), x='detail_roi', y='short_name',
                        orientation='h', color_discrete_sequence=['#5E6AD2'])
            fig.update_layout(height=360, showlegend=False, xaxis_title='ROI', yaxis_title=None)
            st.plotly_chart(fig, width='stretch')

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

    # --- Activity Type Comparison ---
    st.markdown("#### 活动类型对比")
    at_summary = get_project_activity_type_summary()
    if len(at_summary) > 0:
        col_c1, col_c2, col_c3 = st.columns(3)
        for i, (_, row) in enumerate(at_summary.iterrows()):
            c = [col_c1, col_c2, col_c3][i]
            with c:
                kpi_card(
                    row['activity_type'],
                    f"{row['avg_detail_roi']:.3f}×",
                    sub=f"{int(row['project_count'])} 个项目 · 投入 {format_money(row['total_expenses'])}",
                    accent=LINEAR_COLORS[i % len(LINEAR_COLORS)]
                )

        st.markdown('<div style="height:4px;"></div>', unsafe_allow_html=True)
        col_a2, col_b2 = st.columns(2)
        with col_a2:
            fig = px.bar(at_summary, x='activity_type', y='avg_detail_roi', color='activity_type',
                        text=at_summary['avg_detail_roi'].apply(lambda x: f'{x:.3f}×'))
            fig.update_layout(height=300, showlegend=False, xaxis_title=None, yaxis_title='平均ROI')
            fig.update_traces(textposition='outside', textfont=dict(color='#999', size=11))
            st.plotly_chart(fig, width='stretch')
        with col_b2:
            fig = px.bar(at_summary, x='activity_type', y='total_expenses', color='activity_type',
                        text=at_summary['total_expenses'].apply(lambda x: format_money(x)))
            fig.update_layout(height=300, showlegend=False, xaxis_title=None, yaxis_title='总投入')
            fig.update_traces(textposition='outside', textfont=dict(color='#999', size=10))
            st.plotly_chart(fig, width='stretch')

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

    # --- Brand × Activity Type ROI Heatmap ---
    st.markdown("#### 品牌 × 活动类型 ROI 热力图")
    matrix_df = get_project_brand_activity_matrix()
    if len(matrix_df) > 0:
        try:
            pivot = matrix_df.pivot_table(values='avg_roi', index='brand_name', columns='activity_type', aggfunc='mean')
            if not pivot.empty:
                fig = px.imshow(pivot, text_auto='.1f', aspect='auto',
                               color_continuous_scale=[[0, '#141414'], [0.5, '#2a2040'], [1, '#5E6AD2']])
                fig.update_layout(height=max(300, len(pivot) * 25))
                st.plotly_chart(fig, width='stretch')
            else:
                st.caption("数据不足以生成热力图")
        except Exception:
            st.caption("数据不足以生成热力图")

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

    # --- Project comparison table + investment recommendations ---
    st.markdown("#### 项目综合对比 & 投资建议")
    compare_cols_r = project_df.dropna(subset=['detail_roi']).copy()
    compare_cols_r['ROI得分'] = compare_cols_r['detail_roi'].clip(0, 20) / 20 * 100
    compare_cols_r['规模得分'] = (compare_cols_r['cost_store_count'] / compare_cols_r['cost_store_count'].max() * 100).fillna(0)
    compare_cols_r['覆盖得分'] = (compare_cols_r['cost_promoter_count'] / compare_cols_r['cost_promoter_count'].max() * 100).fillna(0)
    compare_cols_r['投资评分'] = (compare_cols_r['ROI得分'] * 0.5 + compare_cols_r['规模得分'] * 0.25 + compare_cols_r['覆盖得分'] * 0.25).round(0)
    compare_cols_r['投资建议'] = compare_cols_r['投资评分'].apply(
        lambda x: '强烈推荐' if x >= 70 else ('推荐' if x >= 50 else ('谨慎' if x >= 30 else '不推荐')))

    tbl = compare_cols_r.nlargest(50, '投资评分')[
        ['project_name', 'brand_name', 'activity_type', 'detail_roi', 'total_expenses',
         'cost_store_count', 'cost_promoter_count', '投资评分', '投资建议']
    ].copy()
    tbl.columns = ['项目名', '品牌', '活动类型', 'ROI', '投入', '门店数', '促销员数', '投资评分', '投资建议']
    tbl['ROI'] = tbl['ROI'].apply(lambda x: f"{x:.3f}×")
    tbl['投入'] = tbl['投入'].apply(format_money)
    st.dataframe(tbl, use_container_width=True, hide_index=True, height=400)

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)

    # --- Detail drilling ---
    with st.expander("明细下钻"):
        proj_detail_tab = st.radio("选择数据类型", ["费用明细", "执行明细", "销售概览", "场次概览"],
                                   horizontal=True, key="project_detail_tab")

        if proj_detail_tab == "费用明细":
            cost_q = """SELECT fsc.project_name, fsc.shop_name, fsc.promoter_name,
                       fsc.efficacious_session_days, fsc.wages, fsc.incentive_bonuses,
                       fsc.total_short_term_expenses, fsc.actual_sales_achieved_amount
                       FROM fact_store_cost fsc
                       WHERE fsc.project_name = ? ORDER BY fsc.total_short_term_expenses DESC LIMIT 200"""
            cost_d = con.execute(cost_q, [pj_overview['project_name']]).df()
            if len(cost_d) > 0:
                cost_d.columns = ['项目', '门店', '促销员', '有效天数', '工资', '奖金', '总费用', '达成金额']
                st.dataframe(cost_d, use_container_width=True, hide_index=True,
                           column_config={'工资': st.column_config.NumberColumn(format="¥%.0f"),
                                         '奖金': st.column_config.NumberColumn(format="¥%.0f"),
                                         '总费用': st.column_config.NumberColumn(format="¥%.0f"),
                                         '达成金额': st.column_config.NumberColumn(format="¥%.0f")})
            else:
                st.caption("暂无费用明细")

        elif proj_detail_tab == "执行明细":
            exec_q = """SELECT arrange_date, promoter_name, shop_name AS store, project_name,
                       brand_sales, taste_info, buy_num, start_status
                       FROM fact_store_execution WHERE project_unicode = ? ORDER BY arrange_date DESC LIMIT 200"""
            exec_d = con.execute(exec_q, [pu_proj]).df()
            if len(exec_d) > 0:
                exec_d.columns = ['日期', '促销员', '门店', '项目', '销量', '试饮', '购买', '考勤']
                st.dataframe(exec_d, use_container_width=True, hide_index=True)
            else:
                st.caption("暂无执行明细")

        elif proj_detail_tab == "销售概览":
            sales_q = """SELECT product_brand, product_name, SUM(total_sales) AS qty,
                        SUM(totalMoney) AS money, COUNT(DISTINCT dim_store_unicode) AS stores
                        FROM fact_store_sales WHERE project_unicode = ?
                        GROUP BY product_brand, product_name ORDER BY money DESC LIMIT 200"""
            sales_d = con.execute(sales_q, [pu_proj]).df()
            if len(sales_d) > 0:
                sales_d.columns = ['品牌', '商品', '销量', '销售额', '门店数']
                st.dataframe(sales_d, use_container_width=True, hide_index=True,
                           column_config={'销售额': st.column_config.NumberColumn(format="¥%.0f")})
            else:
                st.caption("暂无销售数据")

        elif proj_detail_tab == "场次概览":
            sess_q = """SELECT project_name, shop_name, promoter_name, arrange_date,
                       brand_sales_total, single_store_sales
                       FROM fact_store_session WHERE project_name = ? ORDER BY arrange_date DESC LIMIT 200"""
            sess_d = con.execute(sess_q, [pj_overview['project_name']]).df()
            if len(sess_d) > 0:
                sess_d.columns = ['项目', '门店', '促销员', '日期', '品牌销量', '单店销量']
                st.dataframe(sess_d, use_container_width=True, hide_index=True)
            else:
                st.caption("暂无场次数据")

st.divider()
st.caption(f"数据底座 store360.duckdb · 百事微促达临促管理系统")
