import sqlite3
import pandas as pd
import dask.dataframe as dd

def load_data(db_path):
    con = sqlite3.connect(db_path)

    # Veritabanındaki tablo adlarını almak için sorgu
    tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql_query(tables_query, con)
    
    # Tablo adlarını listeye çevir
    table_names = tables['name'].tolist()
    
    # Tabloları pandas DataFrame olarak yükle
    dataframes = {}
    for table in table_names:
        df = pd.read_sql_query(f"SELECT * FROM {table}", con)
        dataframes[table] = df

    # Her DataFrame'i Dask DataFrame'e dönüştür
    dask_dataframes = {table: dd.from_pandas(df, npartitions=1) for table, df in dataframes.items()}

    return dask_dataframes

def sidebar1(dask_dataframes):
    import streamlit as st

    # Streamlit arayüzü için tablo seçim kutusu
    st.markdown("<hr style='border: 3px solid green;'/>", unsafe_allow_html=True)

    st.title("Data Sample")
    table_name = st.sidebar.selectbox("Tablo Seçin", list(dask_dataframes.keys()))
    st.write(f"Seçilen tablo: {table_name}")

    # Seçilen tablonun ilk birkaç satırını göster
    st.write(dask_dataframes[table_name].head(30))
    st.markdown("<hr style='border: 3px solid green;'/>", unsafe_allow_html=True)

