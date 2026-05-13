"""Phase 2: ETL - Replace sales source + enum mapping tables."""
import duckdb
import pandas as pd
import os
import time
import re

BASE = "/Users/jayden/Desktop/百事微促达临促项目"
DB_PATH = os.path.join(BASE, "store360.duckdb")

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

con = duckdb.connect(DB_PATH)
con.execute("SET memory_limit='4GB'")
con.execute("SET threads=4")

def load_excel(table_name, file_name, sheet=0):
    path = os.path.join(BASE, file_name)
    t0 = time.time()
    print(f"  Reading {file_name}...", end=" ", flush=True)
    df = pd.read_excel(path, sheet_name=sheet)
    print(f"{len(df):,} rows, ", end=" ", flush=True)
    con.execute(f"CREATE TABLE stg_{table_name} AS SELECT * FROM df")
    print(f"done ({time.time()-t0:.1f}s)")
    return len(df)

# ============================================================
print("=" * 60)
print("STEP 0: Parse enum mapping files")
print("=" * 60)

def parse_account_enum(filepath):
    """Parse account枚举值.txt: CODE(10021xxx,"LABEL"),"""
    mappings = []
    with open(filepath, 'r') as f:
        for line in f:
            m = re.search(r'\((\d+),\s*"([^"]*)"\)', line)
            if m:
                mappings.append((int(m.group(1)), m.group(2)))
    return mappings

def parse_shop_group_enum(filepath):
    """Parse shop_group枚举值.txt: NAME(10011xxx,"Label",level),"""
    mappings = []
    with open(filepath, 'r') as f:
        for line in f:
            m = re.search(r'\((\d+),\s*"([^"]*)"\s*,\s*(\d+)\)', line)
            if m:
                mappings.append((int(m.group(1)), m.group(2), int(m.group(3))))
    return mappings

def parse_city_level_enum(filepath):
    """Parse city_level枚举值.txt: NAME(10071xxx, "CGx"),"""
    mappings = []
    with open(filepath, 'r') as f:
        for line in f:
            m = re.search(r'\((\d+),\s*"([^"]*)"\)', line)
            if m:
                mappings.append((int(m.group(1)), m.group(2)))
    return mappings

def parse_cluster_enum(filepath):
    """Parse Customer Cluster枚举值.txt: LABEL CODE，LABEL CODE，..."""
    mappings = []
    with open(filepath, 'r') as f:
        text = f.read()
    for m in re.finditer(r'(\S+)\s+(\d{8})', text):
        mappings.append((int(m.group(2)), m.group(1)))
    return mappings

def parse_channel_enum(filepath):
    """Parse Trade Channel枚举值.txt: Trade Channel（LABEL CODE，...）"""
    mappings = []
    with open(filepath, 'r') as f:
        text = f.read()
    for m in re.finditer(r'(\S+)\s+(\d{8})', text):
        mappings.append((int(m.group(2)), m.group(1)))
    return mappings

# Parse all enums
acct_map = parse_account_enum(os.path.join(BASE, "account枚举值.txt"))
sg_map = parse_shop_group_enum(os.path.join(BASE, "shop_group枚举值.txt"))
cl_map = parse_city_level_enum(os.path.join(BASE, "city_level枚举值.txt"))
cc_map = parse_cluster_enum(os.path.join(BASE, "Customer Cluster枚举值.txt"))
tc_map = parse_channel_enum(os.path.join(BASE, "Trade Channel枚举值.txt"))

print(f"  account: {len(acct_map)} mappings")
print(f"  shop_group: {len(sg_map)} mappings")
print(f"  city_level: {len(cl_map)} mappings")
print(f"  customer_cluster: {len(cc_map)} mappings")
print(f"  trade_channel: {len(tc_map)} mappings")

# Create enum tables in DuckDB
con.execute("CREATE TABLE enum_account (code INTEGER PRIMARY KEY, label VARCHAR)")
for code, label in acct_map:
    con.execute("INSERT INTO enum_account VALUES (?, ?)", [code, label])

con.execute("CREATE TABLE enum_shop_group (code INTEGER PRIMARY KEY, label VARCHAR, level INTEGER)")
for code, label, lvl in sg_map:
    con.execute("INSERT INTO enum_shop_group VALUES (?, ?, ?)", [code, label, lvl])

con.execute("CREATE TABLE enum_city_level (code INTEGER PRIMARY KEY, label VARCHAR)")
for code, label in cl_map:
    con.execute("INSERT INTO enum_city_level VALUES (?, ?)", [code, label])

con.execute("CREATE TABLE enum_customer_cluster (code INTEGER PRIMARY KEY, label VARCHAR)")
for code, label in cc_map:
    con.execute("INSERT INTO enum_customer_cluster VALUES (?, ?)", [code, label])

con.execute("CREATE TABLE enum_trade_channel (code INTEGER PRIMARY KEY, label VARCHAR)")
for code, label in tc_map:
    con.execute("INSERT INTO enum_trade_channel VALUES (?, ?)", [code, label])

print("  ✅ 5 enum tables created")

# ============================================================
print("\n" + "=" * 60)
print("STEP 1: Load Excel files into DuckDB staging")
print("=" * 60)

t0 = time.time()

load_excel("project", "项目.xlsx", "bs_project_info")
load_excel("task", "任务.xlsx", "bs_task_exe_record")
load_excel("product", "商品.xlsx", "bs_product_info")
load_excel("arrange", "排班.xlsx", "Sheet1")
load_excel("sales_detail", "销量明细.xlsx")   # <-- NEW: detailed sales
load_excel("shop_plan", "门店计划.xlsx", "bs_shop_plan")
load_excel("shop_plan_brand", "门店计划品牌目标销量.xlsx", "bs_shop_plan_brand")
load_excel("session", "场次表.xlsx", "bs_project_session_table_snapsh")
load_excel("session_expense", "场次费用汇总.xlsx", "bs_project_session_expense_snap")
load_excel("shop_info", "门店库.xlsx", "bs_shop_info")

print(f"\n  Total load time: {time.time()-t0:.1f}s")

# ============================================================
print("\n" + "=" * 60)
print("STEP 2: Build dimension tables (with enum labels)")
print("=" * 60)

# --- Dim_Store: with enum labels ---
con.execute("""
    CREATE TABLE dim_store AS
    SELECT
        si.unicode AS store_unicode,
        si.shop_sn,
        si.shop_name,
        si.mu_unicode, si.mu,
        si.ru_unicode, si.ru,
        si.au_unicode, si.au,
        si.city_unicode, si.city,
        si.province_id, si.province_name,
        si.city_id, si.city_name,
        si.district_id, si.district_name,
        si.address,
        si.account,
        ea.label AS account_label,
        si.shop_group,
        esg.label AS shop_group_label,
        esg.level AS shop_group_level,
        si.trade_channel,
        etc.label AS trade_channel_label,
        si.customer_cluster,
        ecc.label AS customer_cluster_label,
        si.city_level,
        ecl.label AS city_level_label,
        si.top_shop, si.is_top, si.shop_status,
        si.latitude, si.longitude, si.check_range,
        si.store_classification, si.mvs_store_name
    FROM stg_shop_info si
    LEFT JOIN enum_account ea ON si.account = ea.code
    LEFT JOIN enum_shop_group esg ON si.shop_group = esg.code
    LEFT JOIN enum_trade_channel etc ON si.trade_channel = etc.code
    LEFT JOIN enum_customer_cluster ecc ON si.customer_cluster = ecc.code
    LEFT JOIN enum_city_level ecl ON si.city_level = ecl.code
""")
stores = con.execute("SELECT COUNT(*) FROM dim_store").fetchone()[0]
print(f"  ✅ dim_store: {stores:,} stores (with enum labels)")

# --- Dim_Project ---
con.execute("""
    CREATE TABLE dim_project AS
    SELECT
        unicode AS project_unicode,
        project_name, project_sn, status, ascription,
        start_time, end_time,
        schedule_type, activity_type,
        brand_unicode, brand_name,
        brand_sales_enabled, brand_sales_required,
        have_top, top_ratio,
        essentials_enabled, essentials,
        question_enabled,
        sub_brand_sales_enabled, product_sales_enabled,
        gift_enabled, taste_enabled, taste_required,
        project_type,
        have_mvs_store, mvs_store_ratio,
        buy_num_enabled, buy_num_required,
        display_start_date, display_end_date,
        create_time, update_time
    FROM stg_project
""")
projects = con.execute("SELECT COUNT(*) FROM dim_project").fetchone()[0]
print(f"  ✅ dim_project: {projects:,} projects")

# --- Dim_Promoter ---
con.execute("""
    CREATE TABLE dim_promoter AS
    SELECT DISTINCT
        promoter_unicode, promoter_name, promoter_mobile, promoter_sn,
        promoter_avatar_url, promoter_photo_url,
        supervisor_unicode, supervisor_name, supervisor_mobile, supervisor_sn
    FROM (
        SELECT
            promoter_unicode, promoter_name, promoter_mobile, promoter_sn,
            promoter_avatar_url, promoter_photo_url,
            supervisor_unicode, supervisor_name, supervisor_mobile, supervisor_sn
        FROM stg_arrange
        UNION
        SELECT
            NULL AS promoter_unicode, promoter_name, NULL AS promoter_mobile, promoter_sn,
            NULL AS promoter_avatar_url, NULL AS promoter_photo_url,
            NULL AS supervisor_unicode, NULL AS supervisor_name, NULL AS supervisor_mobile, NULL AS supervisor_sn
        FROM stg_session
        WHERE promoter_name IS NOT NULL
    )
""")
promoters = con.execute("SELECT COUNT(*) FROM dim_promoter").fetchone()[0]
print(f"  ✅ dim_promoter: {promoters:,} promoters")

# --- Dim_Product ---
con.execute("""
    CREATE TABLE dim_product AS
    SELECT
        unicode AS product_unicode,
        product_name, product_sn,
        brand_name, brand_unicode,
        sub_brand, sub_brand_unicode,
        single_bag_weight, suggested_retail_price,
        product_photo
    FROM stg_product
""")
products = con.execute("SELECT COUNT(*) FROM dim_product").fetchone()[0]
print(f"  ✅ dim_product: {products:,} products")

# --- Dim_Date ---
con.execute("""
    CREATE TABLE dim_date AS
    WITH date_range AS (
        SELECT MIN(arrange_date) AS min_d, MAX(arrange_date) AS max_d FROM stg_arrange
        UNION ALL
        SELECT MIN(arrange_date), MAX(arrange_date) FROM stg_session
        UNION ALL
        SELECT MIN(start_time), MAX(end_time) FROM stg_shop_plan
    ),
    bounds AS (
        SELECT MIN(min_d) AS start_d, MAX(max_d) AS end_d FROM date_range
    )
    SELECT
        d::DATE AS date_value,
        EXTRACT(YEAR FROM d) AS year,
        EXTRACT(MONTH FROM d) AS month,
        EXTRACT(QUARTER FROM d) AS quarter,
        EXTRACT(YEAR FROM d)::VARCHAR || '-Q' || EXTRACT(QUARTER FROM d)::VARCHAR AS year_quarter,
        EXTRACT(YEAR FROM d)::VARCHAR || '-' || LPAD(EXTRACT(MONTH FROM d)::VARCHAR, 2, '0') AS year_month,
        CASE EXTRACT(DOW FROM d) WHEN 0 THEN 7 ELSE EXTRACT(DOW FROM d) END AS day_of_week
    FROM bounds, generate_series(bounds.start_d, bounds.end_d, INTERVAL 1 DAY) t(d)
""")
dates = con.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
print(f"  ✅ dim_date: {dates:,} dates")

# ============================================================
print("\n" + "=" * 60)
print("STEP 3: Build fact tables")
print("=" * 60)

# --- Fact_StoreExecution ---
con.execute("""
    CREATE TABLE fact_store_execution AS
    SELECT
        a.id, a.unicode AS arrange_unicode,
        a.shop_plan_unicode, a.shop_unicode,
        a.project_unicode,
        a.promoter_unicode, a.promoter_name, a.promoter_mobile,
        a.supervisor_unicode, a.supervisor_name,
        a.arrange_date,
        a.start_time, a.end_time,
        a.start_check_time, a.end_check_time,
        a.start_latitude, a.start_longitude,
        a.end_latitude, a.end_longitude,
        a.start_province, a.start_city, a.start_district, a.start_address,
        a.start_status, a.end_status,
        a.start_position_status, a.end_position_status,
        a.start_position_deviation, a.end_position_deviation,
        a.start_is_late, a.end_is_leave_early,
        a.start_compatibility, a.end_compatibility,
        a.start_check_count, a.end_check_count,
        a.start_check_face_result, a.end_check_face_result,
        a.start_answer_result, a.start_answer_count,
        a.end_pos_audit_status, a.end_pos_audit_reason,
        a.product_sales, a.sub_brand_sales, a.brand_sales,
        a.taste_info, a.gift_quantity, a.buy_num,
        a.fill_buy_num, a.contact_foretaste_num,
        a.work_status,
        a.leave_reason, a.leave_count_num, a.reality_leave_sum_minutes,
        a.start_photo_check_face_result, a.start_photo_compatibility,
        a.end_photo_check_face_result, a.end_photo_compatibility,
        a.on_read_photo, a.on_start_read_photo, a.on_end_read_photo,
        a.project_name, a.shop_name AS arrange_shop_name,
        a.update_time, a.create_time,
        ds.store_unicode AS dim_store_unicode,
        ds.city_name, ds.province_name, ds.city_level_label,
        ds.shop_group_label, ds.trade_channel_label, ds.customer_cluster_label,
        dp.brand_name AS project_brand, dp.activity_type, dp.project_type
    FROM stg_arrange a
    LEFT JOIN dim_store ds ON a.shop_unicode = ds.store_unicode
    LEFT JOIN dim_project dp ON a.project_unicode = dp.project_unicode
""")
exec_rows = con.execute("SELECT COUNT(*) FROM fact_store_execution").fetchone()[0]
print(f"  ✅ fact_store_execution: {exec_rows:,} rows")

# --- Fact_StoreSales: Using NEW 销量明细 ---
con.execute("""
    CREATE TABLE fact_store_sales AS
    SELECT
        sd.project_unicode,
        sd.project_name,
        sd.shop_plan_unicode,
        sd.shop_unicode,
        sd.shop_name,
        sd.promoter_unicode,
        sd.promoter_name,
        sd.product_unicode,
        sd.product_name,
        sd.product_sn,
        sd.total_sales,
        sd.total_volumes,
        sd.totalMoney,
        ds.store_unicode AS dim_store_unicode,
        ds.city_name, ds.province_name, ds.city_level_label,
        ds.shop_group_label, ds.trade_channel_label,
        dp.product_name AS dim_product_name,
        dp.brand_name AS product_brand,
        dp.sub_brand, dp.suggested_retail_price, dp.single_bag_weight
    FROM stg_sales_detail sd
    LEFT JOIN dim_store ds ON sd.shop_unicode = ds.store_unicode
    LEFT JOIN dim_product dp ON sd.product_unicode = dp.product_unicode
""")
sales_rows = con.execute("SELECT COUNT(*) FROM fact_store_sales").fetchone()[0]
print(f"  ✅ fact_store_sales: {sales_rows:,} rows (using 销量明细)")

# --- Fact_StoreCost: with enum labels ---
con.execute("""
    CREATE TABLE fact_store_cost AS
    SELECT
        se.id, se.project_reconciliation_unicode,
        se.project_name, se.mu_name,
        se.ru, se.au,
        se.city_level,
        ecl.label AS city_level_label,
        se.account,
        ea.label AS account_label,
        se.shop_group,
        esg.label AS shop_group_label,
        se.city, se.province_name,
        se.company_name, se.shop_sn, se.shop_name,
        se.promoter_sn, se.promoter_name,
        se.efficacious_session_days,
        se.brand_sales_total, se.single_store_sales,
        se.wages, se.incentive_bonuses, se.three_salary,
        se.total_short_term_expenses,
        se.actual_sales_achieved_amount,
        se.mu_audit_status, se.bu_audit_status,
        se.create_time, se.update_time,
        ds.store_unicode AS dim_store_unicode,
        ds.city_name AS store_city, ds.province_name AS store_province,
        ds.city_level_label AS store_city_level_label,
        ds.shop_group_label AS store_shop_group_label
    FROM stg_session_expense se
    LEFT JOIN dim_store ds ON CAST(se.shop_sn AS VARCHAR) = ds.shop_sn
    LEFT JOIN enum_city_level ecl ON se.city_level = ecl.code
    LEFT JOIN enum_account ea ON se.account = ea.code
    LEFT JOIN enum_shop_group esg ON se.shop_group = esg.code
""")
cost_rows = con.execute("SELECT COUNT(*) FROM fact_store_cost").fetchone()[0]
print(f"  ✅ fact_store_cost: {cost_rows:,} rows (with enum labels)")

# --- Fact_StoreTask ---
con.execute("""
    CREATE TABLE fact_store_task AS
    SELECT
        t.id, t.unicode AS task_unicode,
        t.task_name, t.task_type,
        t.shop_unicode, t.shop_name AS task_shop_name,
        t.promoter_unicode, t.promoter_name,
        t.exe_time, t.exe_count,
        t.status, t.auth_msg,
        t.reward_point,
        t.appeal_status, t.appeal_reason,
        t.execution_personnel_type,
        t.create_time, t.update_time,
        ds.store_unicode AS dim_store_unicode,
        ds.city_name, ds.province_name
    FROM stg_task t
    LEFT JOIN dim_store ds ON t.shop_unicode = ds.store_unicode
""")
task_rows = con.execute("SELECT COUNT(*) FROM fact_store_task").fetchone()[0]
print(f"  ✅ fact_store_task: {task_rows:,} rows")

# --- Fact_StoreSession: with enum labels ---
con.execute("""
    CREATE TABLE fact_store_session AS
    SELECT
        ss.id, ss.project_reconciliation_unicode,
        ss.project_name, ss.mu_name,
        ss.ru, ss.au,
        ss.city_level,
        ecl.label AS city_level_label,
        ss.account,
        ea.label AS account_label,
        ss.shop_group,
        esg.label AS shop_group_label,
        ss.city, ss.province_name,
        ss.company_name, ss.shop_sn, ss.shop_name,
        ss.promoter_sn, ss.promoter_name,
        ss.arrange_date, ss.start_time, ss.end_time,
        ss.brand_sales_total, ss.single_store_sales,
        ss.create_time, ss.update_time,
        ds.store_unicode AS dim_store_unicode,
        ds.city_name AS store_city
    FROM stg_session ss
    LEFT JOIN dim_store ds ON CAST(ss.shop_sn AS VARCHAR) = ds.shop_sn
    LEFT JOIN enum_city_level ecl ON ss.city_level = ecl.code
    LEFT JOIN enum_account ea ON ss.account = ea.code
    LEFT JOIN enum_shop_group esg ON ss.shop_group = esg.code
""")
session_rows = con.execute("SELECT COUNT(*) FROM fact_store_session").fetchone()[0]
print(f"  ✅ fact_store_session: {session_rows:,} rows (with enum labels)")

# --- Fact_ShopPlan ---
con.execute("""
    CREATE TABLE fact_shop_plan AS
    SELECT
        sp.id, sp.unicode AS shop_plan_unicode,
        sp.project_unicode, sp.shop_unicode, sp.shop_name AS plan_shop_name,
        sp.company_name, sp.salesman_name, sp.salesman_phone,
        sp.start_time, sp.end_time,
        sp.total_round, sp.arrange_round, sp.promoter_number,
        sp.auth_status,
        sp.second_display, sp.display_type, sp.display_area,
        sp.have_posm, sp.posm_type,
        sp.have_dm, sp.dm_price,
        sp.brand_sales_total,
        sp.mu_name, sp.mu_code,
        sp.have_pass,
        sp.create_time, sp.update_time,
        ds.store_unicode AS dim_store_unicode,
        ds.city_name, ds.province_name, ds.city_level_label,
        ds.trade_channel_label, ds.shop_group_label,
        dp.brand_name AS project_brand, dp.activity_type
    FROM stg_shop_plan sp
    LEFT JOIN dim_store ds ON sp.shop_unicode = ds.store_unicode
    LEFT JOIN dim_project dp ON sp.project_unicode = dp.project_unicode
""")
sp_rows = con.execute("SELECT COUNT(*) FROM fact_shop_plan").fetchone()[0]
print(f"  ✅ fact_shop_plan: {sp_rows:,} rows")

# --- Bridge_ShopPlanBrand ---
con.execute("""
    CREATE TABLE bridge_shop_plan_brand AS
    SELECT
        spb.id, spb.shop_plan_unicode, spb.brand_unicode,
        spb.brand_name, spb.brand_sales,
        spb.unicode AS unicode,
        fsp.dim_store_unicode, fsp.project_unicode
    FROM stg_shop_plan_brand spb
    LEFT JOIN fact_shop_plan fsp ON spb.shop_plan_unicode = fsp.shop_plan_unicode
""")
spb_rows = con.execute("SELECT COUNT(*) FROM bridge_shop_plan_brand").fetchone()[0]
print(f"  ✅ bridge_shop_plan_brand: {spb_rows:,} rows")

# ============================================================
print("\n" + "=" * 60)
print("STEP 4: Pre-compute store-level metrics")
print("=" * 60)

con.execute("""
    CREATE VIEW v_store_metrics AS
    WITH
    -- Cost metrics
    store_cost AS (
        SELECT
            dim_store_unicode,
            COUNT(DISTINCT project_reconciliation_unicode) AS cost_project_count,
            SUM(efficacious_session_days) AS total_session_days,
            SUM(wages) AS total_wages,
            SUM(incentive_bonuses) AS total_bonuses,
            SUM(three_salary) AS total_three_salary,
            SUM(total_short_term_expenses) AS total_expenses,
            SUM(actual_sales_achieved_amount) AS total_achieved_amount,
            SUM(brand_sales_total) AS cost_brand_sales_total,
            SUM(single_store_sales) AS cost_single_store_sales,
            COUNT(DISTINCT promoter_name) AS cost_promoter_count
        FROM fact_store_cost
        WHERE dim_store_unicode IS NOT NULL
        GROUP BY dim_store_unicode
    ),
    -- Sales from new 销量明细
    store_sales AS (
        SELECT
            dim_store_unicode,
            SUM(totalMoney) AS detail_total_money,
            SUM(total_sales) AS detail_total_qty,
            SUM(total_volumes) AS detail_total_volumes,
            COUNT(DISTINCT product_unicode) AS product_variety,
            COUNT(DISTINCT project_unicode) AS sales_project_count
        FROM fact_store_sales
        WHERE dim_store_unicode IS NOT NULL
        GROUP BY dim_store_unicode
    ),
    -- Execution metrics
    store_exec AS (
        SELECT
            dim_store_unicode,
            COUNT(*) AS total_arranges,
            COUNT(DISTINCT project_unicode) AS arr_project_count,
            COUNT(DISTINCT promoter_name) AS arr_promoter_count,
            COUNT(DISTINCT arrange_date) AS arr_days,
            SUM(CASE WHEN start_status = 1 THEN 1 ELSE 0 END) AS normal_checkins,
            SUM(CASE WHEN start_is_late = 1 THEN 1 ELSE 0 END) AS late_count,
            SUM(CASE WHEN end_is_leave_early = 1 THEN 1 ELSE 0 END) AS early_leave_count,
            SUM(CASE WHEN work_status = 2 THEN 1 ELSE 0 END) AS leave_count,
            AVG(CASE WHEN start_position_deviation > 0 THEN start_position_deviation END) AS avg_start_gps_dev,
            AVG(CASE WHEN end_position_deviation > 0 THEN end_position_deviation END) AS avg_end_gps_dev,
            SUM(COALESCE(brand_sales, 0)) AS exec_brand_sales,
            SUM(COALESCE(product_sales, 0)) AS exec_product_sales,
            SUM(COALESCE(taste_info, 0)) AS total_taste,
            SUM(COALESCE(gift_quantity, 0)) AS total_gift,
            SUM(COALESCE(buy_num, 0)) AS total_buy_num
        FROM fact_store_execution
        WHERE dim_store_unicode IS NOT NULL
        GROUP BY dim_store_unicode
    ),
    -- Task metrics
    store_task AS (
        SELECT
            dim_store_unicode,
            COUNT(*) AS total_tasks,
            SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed_tasks,
            SUM(CASE WHEN status = 'REFUSE' THEN 1 ELSE 0 END) AS refused_tasks,
            COUNT(DISTINCT task_type) AS task_type_count
        FROM fact_store_task
        WHERE dim_store_unicode IS NOT NULL
        GROUP BY dim_store_unicode
    ),
    -- Shop plan metrics
    store_plan AS (
        SELECT
            dim_store_unicode,
            COUNT(*) AS total_shop_plans,
            COUNT(DISTINCT project_unicode) AS plan_project_count,
            AVG(CASE WHEN display_area > 0 THEN display_area END) AS avg_display_area,
            SUM(CASE WHEN have_posm = 1 THEN 1 ELSE 0 END) AS posm_count,
            SUM(CASE WHEN have_dm = 1 THEN 1 ELSE 0 END) AS dm_count
        FROM fact_shop_plan
        WHERE dim_store_unicode IS NOT NULL
        GROUP BY dim_store_unicode
    )
    SELECT
        ds.*,
        COALESCE(sc.cost_project_count, 0) AS cost_project_count,
        COALESCE(sc.total_session_days, 0) AS total_session_days,
        COALESCE(sc.total_wages, 0) AS total_wages,
        COALESCE(sc.total_bonuses, 0) AS total_bonuses,
        COALESCE(sc.total_three_salary, 0) AS total_three_salary,
        COALESCE(sc.total_expenses, 0) AS total_expenses,
        COALESCE(sc.total_achieved_amount, 0) AS total_achieved_amount,
        COALESCE(sc.cost_brand_sales_total, 0) AS cost_brand_sales_total,
        COALESCE(sc.cost_promoter_count, 0) AS cost_promoter_count,
        -- New sales detail metrics
        COALESCE(ss.detail_total_money, 0) AS detail_total_money,
        COALESCE(ss.detail_total_qty, 0) AS detail_total_qty,
        COALESCE(ss.detail_total_volumes, 0) AS detail_total_volumes,
        COALESCE(ss.product_variety, 0) AS product_variety,
        COALESCE(ss.sales_project_count, 0) AS sales_project_count,
        -- Execution
        COALESCE(se.total_arranges, 0) AS total_arranges,
        COALESCE(se.arr_project_count, 0) AS arr_project_count,
        COALESCE(se.arr_promoter_count, 0) AS arr_promoter_count,
        COALESCE(se.arr_days, 0) AS arr_days,
        COALESCE(se.normal_checkins, 0) AS normal_checkins,
        COALESCE(se.late_count, 0) AS late_count,
        COALESCE(se.early_leave_count, 0) AS early_leave_count,
        COALESCE(se.leave_count, 0) AS leave_count,
        se.avg_start_gps_dev, se.avg_end_gps_dev,
        COALESCE(se.exec_brand_sales, 0) AS exec_brand_sales,
        COALESCE(se.exec_product_sales, 0) AS exec_product_sales,
        COALESCE(se.total_taste, 0) AS total_taste,
        COALESCE(se.total_gift, 0) AS total_gift,
        COALESCE(se.total_buy_num, 0) AS total_buy_num,
        -- Tasks
        COALESCE(st.total_tasks, 0) AS total_tasks,
        COALESCE(st.passed_tasks, 0) AS passed_tasks,
        COALESCE(st.refused_tasks, 0) AS refused_tasks,
        COALESCE(st.task_type_count, 0) AS task_type_count,
        -- Shop plans
        COALESCE(sp.total_shop_plans, 0) AS total_shop_plans,
        COALESCE(sp.plan_project_count, 0) AS plan_project_count,
        sp.avg_display_area,
        COALESCE(sp.posm_count, 0) AS posm_count,
        COALESCE(sp.dm_count, 0) AS dm_count,
        -- Computed ratios
        CASE WHEN COALESCE(sc.total_expenses, 0) > 0
             THEN ROUND(COALESCE(sc.total_achieved_amount, 0) / sc.total_expenses, 2)
             ELSE NULL END AS store_roi,
        CASE WHEN COALESCE(sc.total_session_days, 0) > 0
             THEN ROUND(COALESCE(sc.total_achieved_amount, 0) / sc.total_session_days, 0)
             ELSE NULL END AS daily_avg_sales,
        CASE WHEN COALESCE(se.total_arranges, 0) > 0
             THEN ROUND(COALESCE(se.normal_checkins, 0) * 100.0 / se.total_arranges, 1)
             ELSE NULL END AS checkin_rate,
        CASE WHEN COALESCE(st.total_tasks, 0) > 0
             THEN ROUND(COALESCE(st.passed_tasks, 0) * 100.0 / st.total_tasks, 1)
             ELSE NULL END AS task_pass_rate,
        -- Sales-data ROI (from detail)
        CASE WHEN COALESCE(sc.total_expenses, 0) > 0
             THEN ROUND(COALESCE(ss.detail_total_money, 0) / sc.total_expenses, 2)
             ELSE NULL END AS detail_roi
    FROM dim_store ds
    LEFT JOIN store_cost sc ON ds.store_unicode = sc.dim_store_unicode
    LEFT JOIN store_sales ss ON ds.store_unicode = ss.dim_store_unicode
    LEFT JOIN store_exec se ON ds.store_unicode = se.dim_store_unicode
    LEFT JOIN store_task st ON ds.store_unicode = st.dim_store_unicode
    LEFT JOIN store_plan sp ON ds.store_unicode = sp.dim_store_unicode
""")
print("  ✅ v_store_metrics view created (with enum labels + detail sales)")

# --- Store-brand level metrics ---
con.execute("""
    CREATE VIEW v_store_brand_metrics AS
    WITH
    cost_brand AS (
        SELECT
            fsc.dim_store_unicode,
            fsc.project_name,
            SUM(fsc.total_short_term_expenses) AS total_expenses,
            SUM(fsc.actual_sales_achieved_amount) AS total_sales,
            SUM(fsc.efficacious_session_days) AS session_days,
            SUM(fsc.wages + fsc.incentive_bonuses + fsc.three_salary) AS total_labor_cost,
            COUNT(DISTINCT fsc.promoter_name) AS promoter_count
        FROM fact_store_cost fsc
        WHERE fsc.dim_store_unicode IS NOT NULL
        GROUP BY fsc.dim_store_unicode, fsc.project_name
    ),
    -- Brand sales from new detail table
    sales_brand AS (
        SELECT
            fss.dim_store_unicode,
            fss.product_brand AS brand_name,
            fss.project_name,
            SUM(fss.totalMoney) AS detail_sales,
            SUM(fss.total_sales) AS detail_qty,
            COUNT(DISTINCT fss.product_unicode) AS product_count,
            COUNT(DISTINCT fss.promoter_unicode) AS promoter_count
        FROM fact_store_sales fss
        WHERE fss.dim_store_unicode IS NOT NULL AND fss.product_brand IS NOT NULL
        GROUP BY fss.dim_store_unicode, fss.product_brand, fss.project_name
    ),
    plan_brand AS (
        SELECT
            fsp.dim_store_unicode,
            spb.brand_name,
            fsp.activity_type,
            SUM(spb.brand_sales) AS target_sales,
            COUNT(DISTINCT fsp.shop_plan_unicode) AS plan_count,
            AVG(fsp.display_area) AS avg_display_area,
            SUM(CASE WHEN fsp.have_posm = 1 THEN 1 ELSE 0 END) AS posm_count,
            SUM(CASE WHEN fsp.have_dm = 1 THEN 1 ELSE 0 END) AS dm_count
        FROM bridge_shop_plan_brand spb
        JOIN fact_shop_plan fsp ON spb.shop_plan_unicode = fsp.shop_plan_unicode
        WHERE fsp.dim_store_unicode IS NOT NULL AND spb.brand_name IS NOT NULL
        GROUP BY fsp.dim_store_unicode, spb.brand_name, fsp.activity_type
    ),
    exec_brand AS (
        SELECT
            dim_store_unicode,
            project_brand AS brand_name,
            activity_type,
            COUNT(*) AS arrange_count,
            SUM(COALESCE(brand_sales, 0)) AS actual_sales,
            SUM(COALESCE(taste_info, 0)) AS taste_total,
            SUM(COALESCE(buy_num, 0)) AS buy_total,
            AVG(CASE WHEN start_status = 1 AND end_status = 1 THEN 1.0 ELSE 0.0 END) AS attendance_rate
        FROM fact_store_execution
        WHERE dim_store_unicode IS NOT NULL AND project_brand IS NOT NULL
        GROUP BY dim_store_unicode, project_brand, activity_type
    )
    SELECT
        ds.store_unicode, ds.shop_name, ds.city_name, ds.province_name,
        ds.city_level_label AS city_level,
        ds.shop_group_label AS shop_group,
        ds.trade_channel_label AS trade_channel,
        COALESCE(eb.brand_name, pb.brand_name, sb.brand_name) AS brand_name,
        COALESCE(pb.activity_type, eb.activity_type) AS activity_type,
        COALESCE(pb.target_sales, 0) AS target_sales,
        COALESCE(eb.actual_sales, 0) AS actual_sales,
        COALESCE(sb.detail_sales, 0) AS detail_sales,
        COALESCE(sb.detail_qty, 0) AS detail_qty,
        COALESCE(eb.arrange_count, 0) AS arrange_count,
        COALESCE(eb.taste_total, 0) AS taste_total,
        COALESCE(eb.buy_total, 0) AS buy_total,
        cb.total_expenses, cb.total_sales AS cost_sales, cb.session_days,
        cb.total_labor_cost, cb.promoter_count,
        COALESCE(pb.avg_display_area, 0) AS avg_display_area,
        COALESCE(pb.plan_count, 0) AS plan_count,
        -- ROI from cost table
        CASE WHEN COALESCE(cb.total_expenses, 0) > 0
             THEN ROUND(COALESCE(cb.total_sales, 0) / cb.total_expenses, 2)
             ELSE NULL END AS roi,
        -- ROI from detail sales
        CASE WHEN COALESCE(cb.total_expenses, 0) > 0
             THEN ROUND(COALESCE(sb.detail_sales, 0) / cb.total_expenses, 2)
             ELSE NULL END AS detail_roi,
        -- Achievement rate
        CASE WHEN COALESCE(pb.target_sales, 0) > 0
             THEN ROUND(COALESCE(eb.actual_sales, 0) * 100.0 / pb.target_sales, 1)
             ELSE NULL END AS achievement_rate,
        -- Taste conversion
        CASE WHEN COALESCE(eb.taste_total, 0) > 0
             THEN ROUND(COALESCE(eb.buy_total, 0) * 100.0 / eb.taste_total, 1)
             ELSE NULL END AS taste_conversion
    FROM dim_store ds
    LEFT JOIN exec_brand eb ON ds.store_unicode = eb.dim_store_unicode
    LEFT JOIN plan_brand pb ON ds.store_unicode = pb.dim_store_unicode
        AND eb.brand_name = pb.brand_name
        AND eb.activity_type = pb.activity_type
    LEFT JOIN cost_brand cb ON ds.store_unicode = cb.dim_store_unicode
    LEFT JOIN sales_brand sb ON ds.store_unicode = sb.dim_store_unicode
        AND sb.brand_name = COALESCE(eb.brand_name, pb.brand_name)
    WHERE COALESCE(eb.brand_name, pb.brand_name, sb.brand_name) IS NOT NULL
""")
print("  ✅ v_store_brand_metrics view created (with detail sales)")

# ============================================================
print("\n" + "=" * 60)
print("STEP 5: Data quality & stats")
print("=" * 60)

stats = con.execute("""
    SELECT 'Total stores' AS metric, COUNT(*)::VARCHAR AS value FROM dim_store
    UNION ALL
    SELECT 'Stores with cost data', COUNT(DISTINCT dim_store_unicode)::VARCHAR
        FROM fact_store_cost WHERE dim_store_unicode IS NOT NULL
    UNION ALL
    SELECT 'Stores with sales data', COUNT(DISTINCT dim_store_unicode)::VARCHAR
        FROM fact_store_sales WHERE dim_store_unicode IS NOT NULL
    UNION ALL
    SELECT 'Stores with execution data', COUNT(DISTINCT dim_store_unicode)::VARCHAR
        FROM fact_store_execution WHERE dim_store_unicode IS NOT NULL
    UNION ALL
    SELECT 'Total expenses', ROUND(SUM(total_expenses))::VARCHAR
        FROM v_store_metrics WHERE total_expenses > 0
    UNION ALL
    SELECT 'Total achieved sales (cost)', ROUND(SUM(total_achieved_amount))::VARCHAR
        FROM v_store_metrics WHERE total_achieved_amount > 0
    UNION ALL
    SELECT 'Total detail sales (销量明细)', ROUND(SUM(detail_total_money))::VARCHAR
        FROM v_store_metrics WHERE detail_total_money > 0
    UNION ALL
    SELECT 'Overall ROI (cost)', ROUND(SUM(total_achieved_amount) / NULLIF(SUM(total_expenses), 0), 2)::VARCHAR
        FROM v_store_metrics
    UNION ALL
    SELECT 'Overall ROI (detail)', ROUND(SUM(detail_total_money) / NULLIF(SUM(total_expenses), 0), 2)::VARCHAR
        FROM v_store_metrics
""").fetchall()

for metric, value in stats:
    print(f"  {metric}: {value}")

db_size = os.path.getsize(DB_PATH) / (1024*1024)
print(f"\n  💾 Database size: {db_size:.1f} MB")
print(f"  📍 Database path: {DB_PATH}")
print("\n" + "=" * 60)
print("ETL COMPLETE! Database ready for dashboard.")
print("=" * 60)

con.close()
