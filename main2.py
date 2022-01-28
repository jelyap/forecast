import streamlit as st
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime

st.title("Inventory Forecast")

def conn():
    dbname = st.secrets["DBNAME"]
    host = st.secrets["HOST"]
    port = str(st.secrets["PORT"])
    user = st.secrets["USERDB"]
    password = st.secrets["PASSWORD"]
    engine = create_engine('postgresql://' + user + ':' + password + '@' + host + ':' + str(port) + '/' + dbname)

    return engine


def dev_conn():
    dbname = st.secrets["DBNAME_DEV"]
    host = st.secrets["HOST_DEV"]
    port = str(st.secrets["PORT"])
    user = st.secrets["USERDB_DEV"]
    password = st.secrets["PASSWORD"]
    engine = create_engine('postgresql://' + user + ':' + password + '@' + host + ':' + str(port) + '/' + dbname)

    return engine

def get_data(engine):
    query = 'select "transaction-time" ,"product-sku", "product-name" , "product-brand" ,"product-category" , ' \
            'quantity , "inventory-created-at" , "inventory-level" from public.custom_mview_genv cmg where "product-active" = 1'
    df = pd.read_sql(query, engine)
    # df = pd.read_csv(filename)
    # Ensuring date formats
    df['transaction-time'] = pd.to_datetime(df['transaction-time'], format='%Y%m%d %H:%M:%S')
    df['inventory-created-at'] = pd.to_datetime(df['inventory-created-at'], format='%Y%m%d %H:%M:%S')

    return df


def calculate_monthly_sum(df_initial, num_prev_months):
    month = (datetime.now() - pd.DateOffset(months=num_prev_months)).month
    year = (datetime.now() - pd.DateOffset(months=num_prev_months)).year

    # print(month, year)
    df = df_initial.loc[(df_initial['transaction-month'] == month) &
                        (df_initial['transaction-year'] == year),
                        ['product-sku', 'product-name', 'quantity']].groupby(
        by=['product-sku']).sum().rename(columns={'quantity': '0-month-' + str(num_prev_months)})
    return df


def calculate_weekly_sum(df_initial, num_prev_weeks):
    week_day = (datetime.now() - pd.DateOffset(weeks=num_prev_weeks))
    week_start = (week_day - pd.DateOffset(days=datetime.now().weekday()))
    week_end = (week_start + pd.DateOffset(days=6)).strftime('%Y-%m-%d')
    week_start = week_start.strftime('%Y-%m-%d')

    # print(week_start, week_end)
    df = df_initial.loc[(df_initial['transaction-time'] >= week_start) &
                        (df_initial['transaction-time'] <= week_end),
                        ['product-sku', 'product-name', 'quantity']].groupby(
        by=['product-sku']).sum().rename(columns={'quantity': '0-week-' + str(num_prev_weeks)})
    return df


def do_transform(df):
    # Making sure start_date is valid
    start_date = (pd.Timestamp.now() - pd.DateOffset(months=3)).strftime('%Y-%m-%d')
    # Creating new df with the quantity average
    avg_qty_df = df.loc[df['transaction-time'] >= start_date, ['product-name', 'quantity']]. \
        groupby(by=['product-name']).mean().rename(columns={'quantity': 'avg_qty'})

    # Creating new df with the latest inventory level
    inventory = df[['inventory-created-at', 'product-name', 'inventory-level']]
    latest_inv_df = inventory.sort_values(['product-name', 'inventory-created-at']). \
        drop_duplicates('product-name', keep='last').rename(columns={'inventory-level': 'last-inv-level'})

    # Merging both avg and latest inv dataframes
    final_df = latest_inv_df.merge(avg_qty_df, on='product-name')

    # Adding forecast calculation
    final_df['forecast'] = final_df['avg_qty'] - final_df['last-inv-level']

    return final_df


def calculate_monthly_forecast(df):
    previous_month = (datetime.now() - pd.DateOffset(months=1)).month
    previous_year = (datetime.now() - pd.DateOffset(months=1)).year
    prev_date_mtd = (datetime.now() - pd.DateOffset(months=1)).strftime('%Y-%m-%d')
    # Creating new df with the latest inventory level
    inventory = df[['inventory-created-at', 'product-sku', 'inventory-level']]
    last_inv = inventory.sort_values(['product-sku', 'inventory-created-at']).drop_duplicates(
        'product-sku', keep='last').rename(columns={'inventory-level': 'latest-inv-level'}).drop(
        columns=['inventory-created-at'])

    # Adding months and year to he df for easy data manipulation
    df['transaction-month'] = df['transaction-time'].dt.month
    df['transaction-year'] = df['transaction-time'].dt.year

    # Creating a base dataframe
    df_base = df[['product-name', 'product-sku', 'product-brand', 'product-category']].sort_values(
        by=['product-name']).drop_duplicates().reset_index().drop(columns=['index'])

    # Calculating Previous MTD SUM
    previous_mtd = df.loc[(df['transaction-month'] == previous_month) & (df['transaction-year'] == previous_year) &
                          (df['transaction-time'] <= prev_date_mtd), ['product-sku', 'product-name',
                                                                      'quantity']].groupby(
        by=['product-sku']).sum().rename(columns={'quantity': 'previous_mtd'})

    # Merging and creating previous monthly sums
    df_merged = df_base.merge(last_inv, on='product-sku', how='left').merge(
        calculate_monthly_sum(df, 0), on='product-sku', how='left').merge(
        previous_mtd, on='product-sku', how='left').merge(
        calculate_monthly_sum(df, 1), on='product-sku', how='left').merge(
        calculate_monthly_sum(df, 2), on='product-sku', how='left').merge(
        calculate_monthly_sum(df, 3), on='product-sku', how='left')

    # Adding 3 month svg qty and mtd-performance
    df_merged['3-month-avg-qty'] = df_merged[['0-month-1', '0-month-2', '0-month-3']].sum(axis=1) / 3
    df_merged['mtd-performance'] = df_merged['0-month-0'] / df_merged['previous_mtd'] * 100

    # Adding forecast trend
    conditions_forecast = [
        # Upper
        (df_merged['0-month-0'] > df_merged['previous_mtd']) & (
                df_merged['0-month-1'] > df_merged['3-month-avg-qty']),
        # Lower
        (df_merged['0-month-0'] < df_merged['previous_mtd']) & (
                df_merged['0-month-1'] < df_merged['3-month-avg-qty']),
        # Upper
        (df_merged['0-month-0'] >= df_merged['3-month-avg-qty']) & (df_merged['mtd-performance'] >= 0.65) &
        (df_merged['0-month-1'] > df_merged['3-month-avg-qty']),
        # Upper
        (df_merged['0-month-0'] >= df_merged['previous_mtd']) & (df_merged['mtd-performance'] >= 2) &
        (df_merged['0-month-1'] < df_merged['3-month-avg-qty']),
        # Alert 1
        (df_merged['0-month-0'] == 0) & (df_merged['0-month-1'] == 0) & (df_merged['0-month-2'] != 0) &
        df_merged['3-month-avg-qty'] != 0,
        # Alert 2
        (df_merged['0-month-0'] == 0) & (df_merged['0-month-1'] == 0) & (df_merged['0-month-2'] == 0) &
        (df_merged['3-month-avg-qty'] != 0)
    ]
    choices_forecast = [
        'Upper',
        'Lower',
        'Upper',
        'Upper',
        'Alert 1',
        'Alert 2'
    ]
    df_merged['forecast_trend'] = np.select(conditions_forecast, choices_forecast, default='Standard')

    # Adding projection and reorder
    conditions_projection = [
        df_merged['forecast_trend'] == 'Standard',
        df_merged['forecast_trend'] == 'Upper',
        df_merged['forecast_trend'] == 'Lower',
        df_merged['forecast_trend'] == 'Alert 1',
        # df_merged['forecast_trend'] == 'Alert 2',
    ]
    choices_projection = [
        df_merged['3-month-avg-qty'] * 1.00,
        df_merged['3-month-avg-qty'] * 1.1,
        df_merged['3-month-avg-qty'] * 0.9,
        df_merged['3-month-avg-qty'] * 0.5
    ]
    df_merged['projection'] = np.select(conditions_projection, choices_projection, default=0)
    df_merged['re-order'] = df_merged['projection'] - df_merged['latest-inv-level']

    return df_merged


def calculate_weekly_forecast(df):
    week_day = (datetime.now() - pd.DateOffset(weeks=1))
    week_start = (week_day - pd.DateOffset(days=datetime.now().weekday()))
    week_end = week_day.strftime('%Y-%m-%d')
    week_start = week_start.strftime('%Y-%m-%d')

    # Creating a base dataframe
    df_base = df[['product-name', 'product-sku', 'product-brand', 'product-category']].sort_values(
        by=['product-name']).drop_duplicates().reset_index().drop(columns=['index'])

    # Calculating the previous week WTD
    prev_week_wtd = df.loc[(df['transaction-time'] >= week_start) &
                           (df['transaction-time'] <= week_end),
                           ['product-sku', 'product-name', 'quantity']].groupby(
        by=['product-sku']).sum().rename(columns={'quantity': 'previous-wtd'})

    # Merging and creating previous weekly sums
    df_weekly_mg = df_base.merge(calculate_weekly_sum(df, 0), on='product-sku', how='left').merge(
        prev_week_wtd, on='product-sku', how='left').merge(
        calculate_weekly_sum(df, 1), on='product-sku', how='left').merge(
        calculate_weekly_sum(df, 2), on='product-sku', how='left').merge(
        calculate_weekly_sum(df, 3), on='product-sku', how='left').merge(
        calculate_weekly_sum(df, 4), on='product-sku', how='left')

    # Adding weekly evolution, performance wtd and 4 weekly avg
    df_weekly_mg['week-evolution'] = (df_weekly_mg['0-week-1'] - df_weekly_mg['0-week-2']) / df_weekly_mg['0-week-2']
    df_weekly_mg['performance-wtd'] = df_weekly_mg['0-week-0'] / df_weekly_mg['previous-wtd'] * 100
    df_weekly_mg['4-week-avg-qty'] = df_weekly_mg[['0-week-1', '0-week-2', '0-week-3', '0-week-4']].sum(axis=1) / 4

    return df_weekly_mg


def create_table(df, table_name, engine):
    df.to_sql(name=table_name, con=engine, schema='public', if_exists='replace', index=False, method='multi')


if __name__ == '__main__':

    engine_dev = dev_conn()

    query = """select "product-name" as Product , "product-sku" as SKU , "product-category" as Category , "product-brand" as Brand , "re-order" as Status , 
                projection as Projection, "latest-inv-level" as Inventory , forecast_trend as Trend 
                from public.gen_v_monthly_forecast"""
    df = pd.read_sql(query, engine_dev)
    
    df.set_index('product', inplace=True)
    
    st.dataframe(data=df, width=1200, height=1000)
