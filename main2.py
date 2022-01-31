import streamlit as st
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder

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
    
    
    status = df['status'].drop_duplicates().sort_values()
    status = pd.concat([pd.Series(['All']), status])
    status_choice = st.sidebar.selectbox('Inventory Status', status)
    
    if status_choice == "All":
        status_choice_filter = df['status']
    else:
        status_choice_filter = status_choice
    
    category = df['category'].loc[df["status"] == status_choice_filter].drop_duplicates().sort_values()
    category = pd.concat([pd.Series(['All']), category])
    category_choice = st.sidebar.selectbox('Category', category)
    
    if category_choice == "All":
        category_choice_filter = df['category']
    else:
        category_choice_filter = category_choice
    
    brand = df['brand'].loc[(df["status"] == status_choice_filter) & (df["category"] == category_choice_filter)].drop_duplicates().sort_values()
    brand = pd.concat([pd.Series(['All']), brand])
    brand_choice = st.sidebar.selectbox('Brand', brand)
    
    if brand_choice == "All":
        brand_choice_filter = df['brand']
    else:
        brand_choice_filter = brand_choice
    
    product = df['product'].loc[(df["status"] == status_choice_filter) & (df["category"] == category_choice_filter) & (df["brand"] == brand_choice_filter)].drop_duplicates().sort_values()
    product = pd.concat([pd.Series(['All']), product])
    product_choice = st.sidebar.selectbox('Product Name', product)
    
    if product_choice == "All":
        product_choice_filter = df['product']
    else:
        product_choice_filter = product_choice
    
    sku = df['sku'].loc[(df["status"] == status_choice_filter) & (df["category"] == category_choice_filter) & (df["brand"] == brand_choice_filter) & (df["product"] == product_choice_filter)].drop_duplicates().sort_values()
    sku = pd.concat([pd.Series(['All']), sku])
    sku_choice = st.sidebar.selectbox('SKU', sku)
    
    if sku_choice == "All":
        sku_choice_filter = df['sku']
    else:
        sku_choice_filter = sku_choice
        
    df = df.loc[(df["status"] == status_choice_filter) & (df["category"] == category_choice_filter) & (df["brand"] == brand_choice_filter) & (df["product"] == product_choice_filter) & (df["sku"] == sku_choice_filter)]
    
    row_class_rules = {
        "order-red": "data.status == 'Place Order'",
        "check-yellow": "data.status == 'Check Status'",
        "good-green": "data.side == 'Good'",
    }
    
    gb.configure_grid_options(rowClassRules=row_class_rules)
    grid_options = gb.build()
    
    custom_css = {
        ".good-green": {"color": "green !important"},
        ".order-red": {"color": "red !important"},
        ".check-yellow": {"color": "yellow !important"},
    }
    
    AgGrid(df,theme="streamlit", custom_css=custom_css, gridOptions=grid_options)
    
    @st.cache
    
    def convert_df(df):
     # IMPORTANT: Cache the conversion to prevent computation on every rerun
     return df.to_csv().encode('utf-8')

    csv = convert_df(df)

    st.download_button(
         label="Download Forecast As CSV",
         data=csv,
         file_name='Forecast.csv',
         mime='Forecast/csv',
     )

