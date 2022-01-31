import streamlit as st
import os
import pandas as pd
import numpy as np
import plotly.express as px
from sqlalchemy import create_engine
from datetime import datetime
from st_aggrid import  GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode, JsCode

st.title("Inventory Forecast")
st.subheader("Monthly")

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

if __name__ == '__main__':

    engine_dev = dev_conn()

    query = """
                select 
                "product-name" as "Product" ,
                "product-variant-name" as "Variant" , 
                "product-sku" as SKU , 
                "product-category" as Category, 
                "product-brand" as Brand ,
                "latest-inv-level" as Inventory ,
                projection as Forecast ,  
                "re-order" as "ReOrder" ,
                status as Status ,
                forecast_trend as Trend 
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
        
    variant = df['variant'].loc[(df["status"] == status_choice_filter) & (df["category"] == category_choice_filter) & (df["brand"] == brand_choice_filter) & (df["product"] == product_choice_filter)].drop_duplicates().sort_values()
    variant = pd.concat([pd.Series(['All']), variant])
    variant_choice = st.sidebar.selectbox('Variant', variant)
    
    if variant_choice == "All":
        variant_choice_filter = df['variant']
    else:
        variant_choice_filter = variant_choice
    
    sku = df['sku'].loc[(df["status"] == status_choice_filter) & (df["category"] == category_choice_filter) & (df["brand"] == brand_choice_filter) & (df["product"] == product_choice_filter) & (df["variant"] == variant_choice_filter)].drop_duplicates().sort_values()
    sku = pd.concat([pd.Series(['All']), sku])
    sku_choice = st.sidebar.selectbox('SKU', sku)
    
    if sku_choice == "All":
        sku_choice_filter = df['sku']
    else:
        sku_choice_filter = sku_choice
        
    df = df.loc[(df["status"] == status_choice_filter) & (df["category"] == category_choice_filter) & (df["brand"] == brand_choice_filter) & (df["product"] == product_choice_filter) & (df["sku"] == sku_choice_filter) & (df["variant"] == variant_choice_filter)]
    
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
    
    st.info("Select specific SKU to view historical data.")
    
    if sku_choice != "All":
        
        engine = conn()

        query_prod = """
                        select to_char("transaction-time", 'YYYY-MM') as "transaction-time", sum(quantity) as Historical
                        from public.custom_mview_genv cmg 
                        where "product-active" = 1
                        and "product-sku" = '""" + (str(sku_choice)) + "'" + """ group by 1 order by "transaction-time" """ 
                     

        df2 = pd.read_sql(query_prod, engine)
        
        st.line_chart(df2.rename(columns={'transaction-time':'index'}).set_index('index'),use_container_width=True)
        
