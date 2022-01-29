import streamlit as st
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
from st_aggrid import AgGrid

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


def create_table(df, table_name, engine):
    df.to_sql(name=table_name, con=engine, schema='public', if_exists='replace', index=False, method='multi')


if __name__ == '__main__':

    engine_dev = dev_conn()

    query = """
                select "product-name" as "Product", "product-sku" as SKU , "product-category" as Category, "product-brand" as Brand , 
                "re-order" as "ReOrder", projection as Forecast , "latest-inv-level" as Inventory , status as Status
                from public.gen_v_monthly_forecast
                order by status DESC
            """
    
 
    df = pd.read_sql(query, engine_dev)
    
    df['reorder'] =  df['reorder'].astype(int)
    df['forecast'] =  df['forecast'].astype(int)
    df['inventory'] =  df['inventory'].astype(int)
    
    # CSS to inject contained in a string
    hide_table_row_index = """
                <style>
                tbody th {display:none}
                .blank {display:none}
                </style>
                """

    # Inject CSS with Markdown
    st.markdown(hide_table_row_index, unsafe_allow_html=True)
    
    filt = ['Show All','Filter']
    filt_choice = st.sidebar.selectbox('Filter Products?', filt)
    
    status = df['status'].drop_duplicates()
    status_choice = st.sidebar.selectbox('Inventory Status', status)
    
    category = df['category'].loc[df["status"] == status_choice].drop_duplicates()
    category_choice = st.sidebar.selectbox('Category', category)
    
    brand = df["brand"].loc[(df["category"] == category_choice) & (df["status"] == status_choice)].drop_duplicates()
    brand_choice = st.sidebar.selectbox('Brand', brand)
    
    product = df["product"].loc[(df["category"] == category_choice) & (df["brand"] == brand_choice) & (df["status"] == status_choice)].drop_duplicates()
    product_choice = st.sidebar.selectbox('Product', product)
    
    sku = df["sku"].loc[(df["product"] == product_choice) & (df["category"] == category_choice) & (df["brand"] == brand_choice) & (df["status"] == status_choice)].drop_duplicates()
    sku_choice = st.sidebar.selectbox('SKU', sku)
    
    if filt_choice == "Filter":
        df = df.loc[(df["product"] == product_choice) & (df["category"] == category_choice) & (df["brand"] == brand_choice) & (df["status"] == status_choice) & (df["sku"] == sku_choice)]
    
    #st.table(df)
    AgGrid(df)
   
