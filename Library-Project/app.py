from main import render_anasayfa
from daskk import dask1 
from polarss import polars1
import streamlit as st   # type: ignore
from pandass import pandas1 
from Modinn import modin1
from vaexx import vaex1
import sqlite3
import pandas as pd
import dask.dataframe as dd



st.sidebar.markdown(
    """
    <style>
    .center {
        display: block;
        margin-top: -8px;
        margin-right: 10px; 
    }
    </style>
    """,
    unsafe_allow_html=True
)
st.sidebar.image("photo/lotus.png",  width=120, use_column_width=False )


st.sidebar.title("Python Libraries")
page = st.sidebar.radio("", 
                        ('Ana Sayfa','Pandas','Dask', 'Vaex', 'Polars', 'Modin', 'Koalas', 'PySpark'))




if page == 'Ana Sayfa':
    render_anasayfa()
elif page == 'Pandas':
    pandas1()
elif page == 'Dask':
       dask1()

elif page == 'Vaex':
    vaex1()
elif page == 'Polars':
    polars1()
elif page == 'Modin':
            modin1()

 