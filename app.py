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
           city_level, shop_group, trade_channel, COALESCE(store_roi, 0) AS roi,
           COALESCE(total_expenses, 0) AS total_expenses,
           COALESCE(total_achieved_amount, 0) AS total_sales
           FROM v_store_metrics ORDER BY total_expenses DESC"""
    return con.execute(q).df()

@st.cache_data(ttl=300)
def get_store_overview(store_unicode):
    q = """SELECT * FROM v_store_metrics WHERE store_unicode = ?"""
    return con.execute(q, [store_unicode]).df().iloc[0] if con.execute("SELECT COUNT(*) FROM v_store_metrics WHERE store_unicode = ?", [store_unicode]).fetchone()[0] > 0 else None

@st.cache_data(ttl=300)
def get_store_brand_metrics(store_unicode):
    q = """SELECT * FROM v_store_brand_metrics WHERE store_unicode = ? AND brand_name IS NOT NULL ORDER BY roi DESC NULLS LAST"""
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
           brand_sales, taste_info, buy_num, promoter_name, project_name, activity_type
           FROM fact_store_execution WHERE dim_store_unicode = ? ORDER BY arrange_date DESC"""
    return con.execute(q, [store_unicode]).df()

@st.cache_data(ttl=300)
def get_store_tasks(store_unicode):
    q = """SELECT task_name, task_type, exe_time, status, promoter_name,
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
            total_achieved_amount, total_session_days, checkin_rate, task_pass_rate,
            avg_display_area
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

# ============ UI HELPERS ============
def kpi_card(label, value, sub=None, accent="#5E6AD2"):
    """Linear-style KPI card: dark surface, subtle border, accent indicator."""
    st.markdown(f"""
    <div style="background:#141414;border:1px solid #1f1f1f;border-radius:8px;padding:18px 20px;margin:4px 0;
                transition:border-color 0.2s;">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
            <div style="width:6px;height:6px;border-radius:50%;background:{accent};flex-shrink:0;"></div>
            <span style="font-size:0.75rem;color:#808080;font-weight:500;letter-spacing:0.03em;text-transform:uppercase;">{label}</span>
        </div>
        <div style="font-size:1.75rem;font-weight:700;color:#f0f0f0;letter-spacing:-0.02em;line-height:1.2;">{value}</div>
        {f'<div style="font-size:0.8rem;color:#666;margin-top:4px;">{sub}</div>' if sub else ''}
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
st.markdown("""
<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
    <span style="font-size:1.5rem;font-weight:700;color:#f0f0f0;letter-spacing:-0.03em;">门店360°</span>
    <span style="background:#5E6AD2;color:#fff;font-size:0.65rem;font-weight:600;padding:2px 8px;border-radius:4px;letter-spacing:0.05em;">BETA</span>
</div>
""", unsafe_allow_html=True)
st.caption("基于 10 张业务表 · 17,367 门店 · 全链路投资决策分析")

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
        return f"{shop}  ·  {city}  ·  投入 {format_money(exp)}  ·  ROI {filtered.loc[i,'roi']:.1f}"
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

# ---- TABS ----
tabs = st.tabs(["概览", "投入", "产出", "ROI", "建议", "质量", "查询"])

# ===================== TAB 1: OVERVIEW =====================
with tabs[0]:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        kpi_card("累计投入费用", format_money(overview['total_expenses']),
                 sub=f"工资 {format_money(overview['total_wages'])} · 奖金 {format_money(overview['total_bonuses'])}",
                 accent=DIM_COLORS["expense"])
    with col2:
        kpi_card("累计达成销售额", format_money(overview['total_achieved_amount']),
                 sub=f"排班执行销量 {format_money(overview['exec_brand_sales'])}",
                 accent=DIM_COLORS["sales"])
    with col3:
        roi_val = overview['store_roi']
        kpi_card("整体 ROI", f"{roi_val:.1f}×" if roi_val and roi_val > 0 else "—",
                 sub="每投入 ¥1 的销售额产出",
                 accent=DIM_COLORS["roi"])
    with col4:
        kpi_card("参与项目 / 计划数", f"{overview['plan_project_count']}",
                 sub=f"{overview['cost_project_count']} 个有费用 · {overview['total_shop_plans']} 个门店计划",
                 accent=DIM_COLORS["project"])

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.markdown("#### 品牌投入产出")
        if len(brand_metrics) > 0 and brand_metrics['total_expenses'].sum() > 0:
            fig = px.scatter(brand_metrics, x='total_expenses', y='cost_sales', size='session_days',
                             color='brand_name', hover_name='brand_name',
                             labels={'total_expenses': '投入费用', 'cost_sales': '销售额', 'session_days': '场次天数'})
            fig.update_traces(marker=dict(line=dict(width=0)), selector=dict(mode='markers'))
            max_v = max(brand_metrics['total_expenses'].max(), brand_metrics['cost_sales'].max()) * 1.15
            fig.add_trace(go.Scatter(x=[0, max_v], y=[0, max_v], mode='lines',
                                     line=dict(dash='dash', color='#3a3a3a', width=1), name='ROI=1'))
            fig.update_layout(height=400, showlegend=True, legend=dict(orientation='h', y=-0.15))
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("暂无品牌投入产出数据")

    with col_r:
        st.markdown("#### 门店档案")
        info_items = [
            ("MU", overview.get('mu', '—')), ("RU", overview.get('ru', '—')),
            ("AU", overview.get('au', '—')), ("城市等级", overview.get('city_level', '—')),
            ("店群", overview.get('shop_group', '—')), ("贸易渠道", overview.get('trade_channel', '—')),
            ("客户集群", overview.get('customer_cluster', '—')),
            ("Top 店", "是" if overview.get('is_top') == 1 else "否"),
            ("GPS 范围", f"{overview.get('check_range','—')}m"),
        ]
        for k, v in info_items:
            st.markdown(f"<div style='display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #1a1a1a;'><span style='color:#808080;font-size:0.8rem;'>{k}</span><span style='color:#d0d0d0;font-size:0.8rem;'>{v}</span></div>", unsafe_allow_html=True)

        st.markdown("")
        st.markdown("#### 历史项目")
        if len(shop_plans) > 0:
            plans_summary = shop_plans[['start_time', 'project_brand', 'activity_type', 'brand_sales_total']].copy()
            plans_summary.columns = ['开始时间', '品牌', '活动类型', '目标销量']
            plans_summary = plans_summary.drop_duplicates().head(8)
            st.dataframe(plans_summary, use_container_width=True, hide_index=True)
        else:
            st.caption("暂无项目记录")

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
    st.markdown("#### 同行对标")
    if len(peer_stores) > 0:
        cm1, cm2, cm3, cm4 = st.columns(4)
        with cm1:
            peer_avg_roi = peer_stores['store_roi'].dropna().mean()
            my_roi = overview['store_roi'] or 0
            delta = f"{'↑' if my_roi > peer_avg_roi else '↓'} {abs(my_roi - peer_avg_roi):.1f} vs 同行均 {peer_avg_roi:.1f}"
            kpi_card("ROI 对比", f"{my_roi:.1f}×", sub=delta, accent=DIM_COLORS["roi"])
        with cm2:
            peer_avg_expense = peer_stores['total_expenses'].dropna().mean()
            kpi_card("投入对比", format_money(overview['total_expenses']),
                     sub=f"同行均 {format_money(peer_avg_expense)}", accent=DIM_COLORS["expense"])
        with cm3:
            peer_avg_sales = peer_stores['total_achieved_amount'].dropna().mean()
            kpi_card("销售对比", format_money(overview['total_achieved_amount']),
                     sub=f"同行均 {format_money(peer_avg_sales)}", accent=DIM_COLORS["sales"])
        with cm4:
            peer_avg_checkin = peer_stores['checkin_rate'].dropna().mean()
            my_checkin = overview['checkin_rate'] or 0
            kpi_card("出勤率对比", format_pct(my_checkin),
                     sub=f"同行均 {format_pct(peer_avg_checkin)}", accent=DIM_COLORS["quality"])

# ===================== TAB 2: INVESTMENT =====================
with tabs[1]:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        kpi_card("总投入费用", format_money(overview['total_expenses']), accent=DIM_COLORS["expense"])
    with col2:
        pct_wages = overview['total_wages']/overview['total_expenses']*100 if overview['total_expenses'] > 0 else 0
        kpi_card("工资支出", format_money(overview['total_wages']), sub=f"占比 {pct_wages:.1f}%", accent="#F59E0B")
    with col3:
        kpi_card("激励奖金", format_money(overview['total_bonuses']), accent=DIM_COLORS["roi"])
    with col4:
        kpi_card("三薪", format_money(overview['total_three_salary']), accent=DIM_COLORS["project"])

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### 费用结构")
        if overview['total_expenses'] > 0:
            fig = go.Figure(data=[go.Pie(labels=['工资', '激励奖金', '三薪'],
                    values=[overview['total_wages'], overview['total_bonuses'], overview['total_three_salary']],
                    hole=0.45, marker_colors=['#EF4444', '#5E6AD2', '#8B5CF6'],
                    textinfo='label+percent', textfont=dict(color='#b0b0b0'))])
            fig.update_layout(height=350, showlegend=False, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig, width='stretch')

    with col_b:
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

    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
    if len(expense_detail) > 0:
        st.markdown("#### 费用按项目趋势")
        expense_detail['project_name_short'] = expense_detail['project_name'].str[:25]
        proj_bar = expense_detail.groupby('project_name_short').agg(总费用=('total_short_term_expenses', 'sum')).reset_index().sort_values('总费用', ascending=True)
        fig = px.bar(proj_bar, x='总费用', y='project_name_short', orientation='h',
                     color_discrete_sequence=['#5E6AD2'])
        fig.update_layout(height=350, showlegend=False, xaxis_title=None, yaxis_title=None)
        st.plotly_chart(fig, width='stretch')

# ===================== TAB 3: OUTPUT =====================
with tabs[2]:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        kpi_card("累计达成销售额", format_money(overview['total_achieved_amount']), accent=DIM_COLORS["sales"])
    with col2:
        kpi_card("排班执行销量", format_money(overview['exec_brand_sales']), accent="#10B981")
    with col3:
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
            atype = brand_metrics.groupby('activity_type').agg(
                销售额=('cost_sales', 'sum'), ROI=('roi', 'mean')
            ).reset_index().dropna(subset=['销售额'])
            if len(atype) > 0:
                fig = px.bar(atype, x='activity_type', y='销售额', color='activity_type',
                            text=atype['ROI'].apply(lambda x: f'ROI {x:.1f}×'))
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
        kpi_card("门店 ROI", f"{overview['store_roi']:.1f}×" if overview['store_roi'] and overview['store_roi'] > 0 else "—",
                 sub="销售额 / 投入费用", accent=DIM_COLORS["roi"])
    with col2:
        cost_eff = overview['total_achieved_amount'] / overview['total_session_days'] if overview['total_session_days'] > 0 else 0
        kpi_card("场次日均产出", format_money(cost_eff), accent="#06B6D4")
    with col3:
        labor_eff = overview['total_achieved_amount'] / overview['total_wages'] if overview['total_wages'] > 0 else 0
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
            heatmap_data = brand_metrics.pivot_table(values='roi', index='brand_name', columns='activity_type', aggfunc='mean')
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
        fig.add_hline(y=avg_roi, line_dash='dot', line_color='#5E6AD2', annotation_text=f'均值 {avg_roi:.1f}', annotation_font_color='#5E6AD2')
        fig.update_traces(marker=dict(line=dict(width=0)))
        fig.update_layout(height=380)
        st.plotly_chart(fig, width='stretch')

# ===================== TAB 5: RECOMMENDATIONS =====================
with tabs[4]:
    st.markdown("#### 投资建议引擎")
    st.caption("综合评分：ROI(40%) + 销售绝对值(25%) + 执行质量(20%) + 稳定性(10%) + 同行参照(5%)")

    if len(brand_metrics) > 0:
        valid = brand_metrics.dropna(subset=['roi', 'cost_sales']).copy()
        if len(valid) > 0:
            for col_name in ['roi', 'cost_sales', 'session_days']:
                std = valid[col_name].std()
                valid[f'{col_name}_norm'] = (valid[col_name] - valid[col_name].min()) / (valid[col_name].max() - valid[col_name].min()) if std > 0 else 0.5
            valid['consistency_norm'] = valid['session_days_norm']
            exec_score = overview['checkin_rate'] / 100 if overview['checkin_rate'] else 0.5
            task_score = overview['task_pass_rate'] / 100 if overview['task_pass_rate'] else 0.5
            valid['综合得分'] = (0.40 * valid['roi_norm'] + 0.25 * valid['cost_sales_norm'] +
                               0.20 * (exec_score + task_score) / 2 + 0.10 * valid['consistency_norm'] + 0.05 * 0.5)
            valid['综合得分'] = (valid['综合得分'] * 100).round(1)
            valid = valid.sort_values('综合得分', ascending=False)

            st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
            col_r1, col_r2 = st.columns([2, 1])
            with col_r1:
                st.markdown("##### 品牌投资推荐排名")
                rec = valid[['brand_name', 'activity_type', 'roi', 'cost_sales', 'total_expenses', '综合得分']].head(8)
                rec.columns = ['品牌', '活动类型', 'ROI', '销售额', '投入费用', '得分']
                st.dataframe(rec, use_container_width=True, hide_index=True,
                           column_config={'ROI': st.column_config.NumberColumn(format="%.1f×"),
                                         '销售额': st.column_config.NumberColumn(format="¥%.0f"),
                                         '投入费用': st.column_config.NumberColumn(format="¥%.0f")})

            with col_r2:
                best = valid.iloc[0]
                kpi_card("🏆 首选品牌", best['brand_name'], sub=f"综合得分 {best['综合得分']:.0f} 分", accent="#F59E0B")
                kpi_card("推荐活动类型", best['activity_type'] if pd.notna(best['activity_type']) else "综合", accent=DIM_COLORS["roi"])
                kpi_card("历史 ROI", f"{best['roi']:.1f}×", accent=DIM_COLORS["sales"])
                kpi_card("历史销售额", format_money(best['cost_sales']), accent=DIM_COLORS["project"])

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
            task_summary = tasks.groupby('task_type').agg(总数=('is_pass', 'count'), 通过率=('is_pass', 'mean')).reset_index()
            task_summary['通过率'] = (task_summary['通过率'] * 100).round(1)
            fig = px.bar(task_summary, x='task_type', y='通过率', color='task_type',
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
            SELECT shop_name AS 门店, city_name AS 城市, province_name AS 省份, city_level AS 城市等级,
                   total_expenses AS 总投入, total_achieved_amount AS 总销售, store_roi AS ROI,
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
        data = con.execute("""SELECT task_name AS 任务名, task_type AS 任务类型, exe_time AS 执行时间,
            status AS 状态, promoter_name AS 促销员 FROM fact_store_task ORDER BY exe_time DESC LIMIT 1000""").df()
    elif query_type == "门店计划详情":
        data = con.execute("""SELECT plan_shop_name AS 门店, project_brand AS 品牌, activity_type AS 活动类型,
            display_type AS 陈列, display_area AS 面积, have_posm AS POSM, dm_price AS DM价
            FROM fact_shop_plan WHERE dim_store_unicode IS NOT NULL ORDER BY start_time DESC LIMIT 1000""").df()

    if data is not None:
        st.dataframe(data, use_container_width=True, hide_index=True)
        st.download_button("导出 CSV", data.to_csv(index=False).encode('utf-8'), f"{query_type}.csv", "text/csv")

st.divider()
st.caption(f"数据底座 store360.duckdb · 百事微促达临促管理系统")
