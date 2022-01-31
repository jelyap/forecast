import streamlit as st
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
from st_aggrid import  GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode, JsCode

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
                "re-order" as "ReOrder", projection as Forecast , "latest-inv-level" as Inventory, forecast_trend as Trend ,status as Status
                from public.gen_v_monthly_forecast
                order by case
                    when status = 'Place Order' then 1
                    when status = 'Check Product' then 2
                    when status = 'Good' then 3
                end asc
            """
    
 
    df = pd.read_sql(query, engine_dev)
    
    df['reorder'] =  df['reorder'].astype(int)
    df['forecast'] =  df['forecast'].astype(int)
    df['inventory'] =  df['inventory'].astype(int)
    
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
    
    gb = GridOptionsBuilder.from_dataframe(df)
    
    #configures last row to use custom styles based on cell's value, injecting JsCode on components front end
    cellsytle_jscode = JsCode("""
    function(params) {
        if (params.value == 'Place Order') {
            return {
                'color': 'white',
                'backgroundColor': 'darkred'
            }
        } else if (params.value == 'Check Product') {
            return {
                'color': 'white',
                'backgroundColor': 'orange'
            }
        } else {
            return {
                'color': 'white',
                'backgroundColor': 'green'
            }
        }
    };
    """)
    gb.configure_column("status", cellStyle=cellsytle_jscode)
    
    cellsytle_jscode_2 = JsCode("""
    function(params) {
        if (params.value == 'Alert 2') {
            return {
                'color': 'black',
                'backgroundColor': 'lightred'
            }
        } else if (params.value == 'Alert 1') {
            return {
                'color': 'black',
                'backgroundColor': 'pink'
            }
        } else if (params.value == 'Lower') {
            return {
                'color': 'black',
                'backgroundColor': 'orange'
            }
        } else if (params.value == 'Upper') {
            return {
                'color': 'black',
                'backgroundColor': 'yellow'
            }
        } else {
            return {
                'color': 'black',
                'backgroundColor': 'lightgreen'
            }
        }
    };
    """)
    gb.configure_column("trend", cellStyle=cellsytle_jscode_2)
    
    gb.configure_grid_options(domLayout='normal')
    gridOptions = gb.build()
    
    AgGrid(df, gridOptions=gridOptions, allow_unsafe_jscode=True)
    
    @st.cache
    
    def convert_df(df):
     # IMPORTANT: Cache the conversion to prevent computation on every rerun
     return df.to_csv().encode('utf-8')

    csv = convert_df(df)

    st.download_button(
         label="Download Forecast As CSV",
         data=csv,
         file_name='InventoryForecast.csv',
         mime='InventoryForecast/csv',
     )
    
    engine = conn()

    query_prod = """
                    select "product-name", "product-sku", "product-category", "product-brand", 
                    to_char("transaction-time", 'YYYY-MM-DD') as "transaction-time", quantity 
                    from public.custom_mview_genv cmg 
                    where "product-active" = 1
                 """
    
    df2 = pd.read_sql(query_prod, engine)
    
    if sku_choice != "All":
        df2.groupby(by=['product-name','product-sku','product-category','product-brand','transaction-time'])["quantity"].sum()
        df2 = df2.loc[df["sku"] == sku_choice]
        df2 = df2['transaction-time','quantity']
        AgGrid(df2)
