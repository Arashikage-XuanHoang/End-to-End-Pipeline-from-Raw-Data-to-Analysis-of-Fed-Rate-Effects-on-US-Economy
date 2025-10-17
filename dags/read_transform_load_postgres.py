from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# ============================================================
# 1️⃣ HÀM MERGE DỮ LIỆU
# ============================================================
def merge_data(data, full_df):
    # Chuẩn hóa kiểu dữ liệu
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data = data.rename(columns={'Date': 'date'})

    # Xóa cột không liên quan
    if 'Release Date' in data.columns:
        data = data.drop('Release Date', axis=1)

    # Ép kiểu 'date' cho full_df
    full_df['date'] = pd.to_datetime(full_df['date'], errors='coerce')

    # Merge 2 bảng
    df_merge = pd.merge(full_df, data, on='date', how='outer')

    # Bổ sung loại FOMC meeting
    fomc_dates_with_type_is_statement = [
        "1998-02-03", "1998-02-04", "1998-03-31",
        "1998-10-15", "1998-12-22", "1999-03-30", "1999-11-16"
    ]
    fomc_dates_with_type_is_minutes = [
        '1998-05-19', '1998-06-30', '1998-07-01', '1998-08-18',
        '1998-09-29', '1998-11-17', '1999-02-02', '1999-02-03',
        '1999-05-18', '1999-06-29', '1999-06-30',
        '1999-08-24', '1999-10-05', '1999-12-21'
    ]

    for date in fomc_dates_with_type_is_statement:
        df_merge.loc[df_merge['date'] == pd.to_datetime(date), 'Type'] = 'Statement'
    for date in fomc_dates_with_type_is_minutes:
        df_merge.loc[df_merge['date'] == pd.to_datetime(date), 'Type'] = 'Minute'

    df_merge = df_merge.rename(columns={'Type': 'type', 'Text': 'text'})
    return df_merge

# ============================================================
# 2️⃣ HÀM CLEAN DỮ LIỆU
# ============================================================
def fill_in_null_data(data):
    for col in data.columns:
        if data[col].isna().any() and col not in ['type', 'text']:
            data[col] = data[col].fillna(method='ffill').fillna(method='bfill')
    return data

def transform_data(data):
    if 'Unnamed: 0' in data.columns:
        data = data.drop(columns='Unnamed: 0')
    data = fill_in_null_data(data)
    return data

# ============================================================
# 3️⃣ TẠO BẢNG DIM / FACT & LOAD VÀO GOLD
# ============================================================
def create_dim_fact_table_and_load_to_postgres(df):
    # Dim Date
    dim_date = pd.DataFrame({
        'date': pd.to_datetime(df['date']),
    })
    dim_date['day'] = dim_date['date'].dt.day
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['quarter'] = dim_date['date'].dt.quarter
    dim_date['week'] = dim_date['date'].dt.isocalendar().week
    dim_date['day_of_week'] = dim_date['date'].dt.day_name()
    dim_date['is_holiday'] = False
    dim_date['date_id'] = range(1, len(dim_date) + 1)

    # Dim Monetary Policy
    dim_mp = df[[
        'fed_funds_rate', 'fed_funds_target_rate', 'fed_funds_upper_rate',
        'fed_funds_lower_rate', 'fed_funds_effective_rate', 
        'discount_window_primary_rate', 'treasury_yield_2y',
        'treasury_yield_10y', 'interest_on_reserves'
    ]].copy()
    dim_mp['mp_id'] = range(1, len(dim_mp) + 1)

    # Dim Money Supply
    dim_money = df[['money_supply_m2', 'velocity_m2', 'real_m2_money_stock']].copy()
    dim_money['money_id'] = range(1, len(dim_money) + 1)

    # Dim Inflation
    dim_inflation = df[['cpi', 'core_cpi', 'ppi', 'pce_price_index',
                        'core_pce_inflation', 'breakeven_inflation_10y']].copy()
    dim_inflation['inflation_id'] = range(1, len(dim_inflation) + 1)

    # Dim Consumer Behavior
    dim_cb = df[['consumer_confidence_index', 'retail_sales', 'pce']].copy()
    dim_cb['cb_id'] = range(1, len(dim_cb) + 1)

    # Dim Housing
    dim_housing = df[['housing_starts', 'median_new_home_sales',
                      'average_new_home_sales', 'mortgage_rate_30y']].copy()
    dim_housing['housing_id'] = range(1, len(dim_housing) + 1)

    # Dim International Trade
    dim_intrade = df[['trade_balance', 'current_account_balance',
                      'fdi_asset_current_cost_transactions']].copy()
    dim_intrade['intrade_id'] = range(1, len(dim_intrade) + 1)

    # Dim Economic Output
    dim_eo = df[['gdp', 'real_gdp', 'industrial_production_idx',
                 'capacity_utilization']].copy()
    dim_eo['eo_id'] = range(1, len(dim_eo) + 1)

    # Dim Stock Market
    dim_stock = df[['nasdaq_close', 'sp500_close', 'dowjones_industrial_idx',
                    'vix_index', 'vxn_index', 'rvx_index', 'vxd_index']].copy()
    dim_stock['stock_market_id'] = range(1, len(dim_stock) + 1)

    # Dim Employment
    dim_em = df[['unemployment_rate', 'nonfarm_payrolls',
                 'labor_participation_rate']].copy()
    dim_em['em_id'] = range(1, len(dim_em) + 1)

    # Dim FOMC
    dim_fomc = df[['type', 'text']].copy()
    dim_fomc['fomc_meeting_id'] = range(1, len(dim_fomc) + 1)

    # Fact Economics
    fact = pd.DataFrame({
        'fact_id': range(1, len(df) + 1),
        'date_id': dim_date['date_id'],
        'mp_id': dim_mp['mp_id'],
        'money_id': dim_money['money_id'],
        'inflation_id': dim_inflation['inflation_id'],
        'cb_id': dim_cb['cb_id'],
        'housing_id': dim_housing['housing_id'],
        'intrade_id': dim_intrade['intrade_id'],
        'eo_id': dim_eo['eo_id'],
        'stock_market_id': dim_stock['stock_market_id'],
        'em_id': dim_em['em_id'],
        'fomc_meeting_id': dim_fomc['fomc_meeting_id']
    })

    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/data_warehouse")

    # Save vào schema gold
    dim_date.to_sql("dim_date", con=engine, schema="gold", if_exists="replace", index=False)
    dim_mp.to_sql("dim_monetary_policy", con=engine, schema="gold", if_exists="replace", index=False)
    dim_money.to_sql("dim_money_supply_liquidity", con=engine, schema="gold", if_exists="replace", index=False)
    dim_inflation.to_sql("dim_inflation", con=engine, schema="gold", if_exists="replace", index=False)
    dim_cb.to_sql("dim_consumer_behavior", con=engine, schema="gold", if_exists="replace", index=False)
    dim_housing.to_sql("dim_housing", con=engine, schema="gold", if_exists="replace", index=False)
    dim_intrade.to_sql("dim_international_trading", con=engine, schema="gold", if_exists="replace", index=False)
    dim_eo.to_sql("dim_economic_output", con=engine, schema="gold", if_exists="replace", index=False)
    dim_stock.to_sql("dim_stock_market", con=engine, schema="gold", if_exists="replace", index=False)
    dim_em.to_sql("dim_employment", con=engine, schema="gold", if_exists="replace", index=False)
    dim_fomc.to_sql("dim_fomc_meeting", con=engine, schema="gold", if_exists="replace", index=False)
    fact.to_sql("fact_economics", con=engine, schema="gold", if_exists="replace", index=False)

    print("✅ Đã đẩy toàn bộ bảng Dim và Fact vào schema 'gold' trên PostgreSQL")


# ============================================================
# 4️⃣ HÀM CHÍNH CHẠY TRONG DAG
# ============================================================
def read_csv_from_tmp():
    data1 = pd.read_csv('/tmp/Economics.csv')
    data2 = pd.read_csv('/tmp/fomc_meeting_2000_2025.csv')

    print(data1.head(5))
    print(data2.head(5))

    data = merge_data(data2, data1)
    data_final = transform_data(data)

    # Engine
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/data_warehouse")

    # Lưu vào schema silver
    data_final.to_sql("economics_cleaned", con=engine, schema="silver", if_exists="replace", index=False)
    print("✅ Đã đẩy data_final (Silver Layer) vào PostgreSQL schema 'silver'")

    # Sau đó tạo bảng Gold
    create_dim_fact_table_and_load_to_postgres(data_final)


# ============================================================
# 5️⃣ ĐỊNH NGHĨA DAG
# ============================================================
with DAG(
    dag_id='silver_to_gold_pipeline',
    start_date=datetime(2025, 10, 12),
    schedule=None,
    tags=['silver', 'gold', 'postgres'],
) as dag:

    transform_and_load = PythonOperator(
        task_id='transform_and_load_silver_gold',
        python_callable=read_csv_from_tmp
    )
