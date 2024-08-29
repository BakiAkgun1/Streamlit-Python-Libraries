import sqlite3
import pandas as pd # type: ignore
import streamlit as st # type: ignore
from module import load_data, sidebar1
import dask.dataframe as dd

def pandas1():
    
    html_temp = """
    <div style="background-color:darkblue;padding:1.5px">

    <h1 style="color:white;text-align:center;">Pandas</h1>

    </div><br>"""
    st.markdown(html_temp,unsafe_allow_html=True)
    st.markdown('<style>h1{color: darkblue;}</style>', unsafe_allow_html=True)

    db_path = r"C:\Users\Baki Akgun\OneDrive\Masaüstü\Library_project\sqlite_sakila.db"
    con = sqlite3.connect(db_path)

   
    st.image("photo/pandas.png", caption='Pandas', use_column_width=True)



    def load_data(db_path):
        con = sqlite3.connect(db_path)

        tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql_query(tables_query, con)
        
        table_names = tables['name'].tolist()
        
        dataframes = {}
        for table in table_names:
            df = pd.read_sql_query(f"SELECT * FROM {table}", con)
            dataframes[table] = df

        dask_dataframes = {table: dd.from_pandas(df, npartitions=1) for table, df in dataframes.items()}

        return dask_dataframes

    def sidebar1(dask_dataframes):
        import streamlit as st

        st.markdown("<hr style='border: 3px solid green;'/>", unsafe_allow_html=True)

        st.header("Database Sample")
        table_name = st.sidebar.selectbox("Tablo Seçin", list(dask_dataframes.keys()))
        st.write(f"Seçilen tablo: {table_name}")

        st.write(dask_dataframes[table_name].head(10))
        st.markdown("<hr style='border: 3px solid green;'/>", unsafe_allow_html=True)


    dask_dataframes = load_data(db_path)
    sidebar1(dask_dataframes)
#------------------------------
  
    
    st.title("1 - Bazı Tabloların İlk 10 Verisi")

    staff_df = pd.read_sql_query('SELECT * FROM staff;', con)
    rental_df = pd.read_sql_query('SELECT * FROM rental;', con)
    inventory_df = pd.read_sql_query('SELECT * FROM inventory;', con)
    film_df = pd.read_sql_query('SELECT * FROM film;', con)

    staff_ddf = dd.from_pandas(staff_df, npartitions=1)
    rental_ddf = dd.from_pandas(rental_df, npartitions=1)
    inventory_ddf = dd.from_pandas(inventory_df, npartitions=1)
    film_ddf = dd.from_pandas(film_df, npartitions=1)

    staff_head = staff_ddf.head(10)
    rental_head = rental_ddf.head(10)
    inventory_head = inventory_ddf.head(10)
    film_head = film_ddf.head(10)

    st.subheader("Staff Verisi")
    st.write(staff_head)

    st.subheader("İlk 10 Rental Verisi")
    st.write(rental_head)

    st.subheader("İlk 10 Inventory Verisi")
    st.write(inventory_head)

    st.subheader("İlk 10 Film Verisi")
    st.write(film_head)
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)


#---------------------------------------------------------------------------------------------

    st.title("2-Birden fazla DVD teslim etmeyen müşteriler")


    rental_df = pd.read_sql_query('SELECT * FROM rental;', con)
    customer_df = pd.read_sql_query('SELECT * FROM customer;', con)


    not_returned_df = rental_df[rental_df['return_date'].isna()]

    rental_counts = not_returned_df.groupby('customer_id').size()

    multiple_rentals_df = rental_counts[rental_counts > 1]

    merged_df = pd.merge(multiple_rentals_df.reset_index(name='Unreturned DVDs'), customer_df, on='customer_id')

    st.write("Birden fazla DVD'yi iade etmeyen müşteri sayısı:", merged_df['customer_id'].nunique())
    st.dataframe(merged_df[['customer_id', 'first_name', 'last_name', 'Unreturned DVDs']])
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)



#---------------------------------------------------------------------------------------------

    st.title("3.Hangi müşteri en çok DVD kiralamış?")

    rental_counts = rental_df.groupby('customer_id').size()

    max_rentals = rental_counts.max()
    top_customer_id = rental_counts.idxmax()

    top_customer = customer_df[customer_df['customer_id'] == top_customer_id]

    st.write(f"En çok DVD kiralayan müşteri {max_rentals} kiralama yapmıştır.")
    st.write("Müşteri Bilgileri:")
    st.dataframe(top_customer[['customer_id', 'first_name', 'last_name', 'email', 'create_date']])
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)




#---------------------------------------------------------------------------------------------
    st.title("4-En Popüler Film Kategorisi")

        # Verileri yükle
    payment_df = pd.read_sql_query('SELECT * FROM payment;', con)
    rental_df = pd.read_sql_query('SELECT * FROM rental;', con)
    inventory_df = pd.read_sql_query('SELECT * FROM inventory;', con)
    film_df = pd.read_sql_query('SELECT * FROM film;', con)
    film_category_df = pd.read_sql_query('SELECT * FROM film_category;', con)
    category_df = pd.read_sql_query('SELECT * FROM category;', con)

    # Veri çerçevelerini birleştir
    merged_df = pd.merge(payment_df, rental_df, on='rental_id', suffixes=('_payment', '_rental'))
    merged_df = pd.merge(merged_df, inventory_df, on='inventory_id', suffixes=('_payment_rental', '_inventory'))
    merged_df = pd.merge(merged_df, film_df, on='film_id', suffixes=('_inventory', '_film'))
    merged_df = pd.merge(merged_df, film_category_df, on='film_id', suffixes=('_film', '_film_category'))
    merged_df = pd.merge(merged_df, category_df, on='category_id', suffixes=('_film_category', '_category'))

    # Kategori başına toplam geliri hesapla
    category_revenue_df = merged_df.groupby('name')['amount'].sum().reset_index()
    category_revenue_df = category_revenue_df.sort_values(by='amount', ascending=False).head(1)

    # Sonucu göster
    st.write(category_revenue_df)
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)


    
        #---------------------------------------------------------------------------------------------
    st.title("5. En Çok Geliri Hangi Film Getirmiş?")

    payment_df = pd.read_sql_query('SELECT * FROM payment;', con)
    rental_df = pd.read_sql_query('SELECT * FROM rental;', con)
    inventory_df = pd.read_sql_query('SELECT * FROM inventory;', con)
    film_df = pd.read_sql_query('SELECT * FROM film;', con)

    merged_df = pd.merge(payment_df, rental_df, on='rental_id', suffixes=('_payment', '_rental'))
    merged_df = pd.merge(merged_df, inventory_df, on='inventory_id', suffixes=('_payment_rental', '_inventory'))
    merged_df = pd.merge(merged_df, film_df, on='film_id', suffixes=('_inventory', '_film'))

    revenue_df = merged_df.groupby('title')['amount'].sum().reset_index()
    revenue_df = revenue_df.sort_values(by='amount', ascending=False).head(1)

    st.write("En çok geliri getiren film:")
    st.write(revenue_df)
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------

 

    st.title("6. Her Türde En Az Kiralanan Filmi Bulun")

    film_category_df = pd.read_sql_query('SELECT * FROM film_category;', con)
    category_df = pd.read_sql_query('SELECT * FROM category;', con)
    rental_df = pd.read_sql_query('SELECT * FROM rental;', con)
    inventory_df = pd.read_sql_query('SELECT * FROM inventory;', con)
    film_df = pd.read_sql_query('SELECT * FROM film;', con)

    merged_df = pd.merge(rental_df, inventory_df, on='inventory_id', suffixes=('_rental', '_inventory'))
    merged_df = pd.merge(merged_df, film_df, on='film_id', suffixes=('_rental_inventory', '_film'))
    merged_df = pd.merge(merged_df, film_category_df, on='film_id', suffixes=('_film', '_film_category'))
    merged_df = pd.merge(merged_df, category_df, on='category_id', suffixes=('_film_category', '_category'))

    film_rental_counts = merged_df.groupby(['category_id', 'title']).size().reset_index(name='rental_count')

    least_rented_films = film_rental_counts.loc[film_rental_counts.groupby('category_id')['rental_count'].idxmin()]

    least_rented_films = pd.merge(least_rented_films, category_df, on='category_id')

    st.write(least_rented_films[['name', 'title', 'rental_count']])
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)


#---------------------------------------------------------------------------------------------

    st.title("7. En çok kiralanan film hangi mağazada kiralanmıştır?")

    store_df = pd.read_sql_query('SELECT * FROM store;', con)

    merged_df = pd.merge(rental_df, inventory_df, on='inventory_id', suffixes=('_rental', '_inventory'))
    merged_df = pd.merge(merged_df, film_df, on='film_id', suffixes=('_inventory', '_film'))
    merged_df = pd.merge(merged_df, store_df, on='store_id', suffixes=('_film', '_store'))

    film_rental_counts = merged_df.groupby(['film_id', 'title', 'store_id']).size().reset_index(name='rental_count')
    most_rented_film = film_rental_counts.loc[film_rental_counts['rental_count'].idxmax()]

    st.write(f"En çok kiralanan film: {most_rented_film['title']}")
    st.write(f"Bu film en çok {most_rented_film['rental_count']} kez kiralanmış.")
    st.write(f"Bu film, mağaza ID: {most_rented_film['store_id']} olan mağazada kiralanmış.")
    st.table(most_rented_film.to_frame().T)
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------


    st.title("8. En Çok Kiralanan Türdeki Filmler (Top 10)")

    merged_df = pd.merge(rental_df, inventory_df, on='inventory_id', suffixes=('_rental', '_inventory'))
    merged_df = pd.merge(merged_df, film_df, on='film_id', suffixes=('_inventory', '_film'))
    merged_df = pd.merge(merged_df, film_category_df, on='film_id', suffixes=('_film', '_film_category'))
    merged_df = pd.merge(merged_df, category_df, on='category_id', suffixes=('_film_category', '_category'))

    category_rental_counts = merged_df.groupby(['category_id', 'name']).size().reset_index(name='rental_count')
    most_rented_category = category_rental_counts.loc[category_rental_counts['rental_count'].idxmax()]

    most_rented_films_in_category = merged_df[merged_df['name'] == most_rented_category['name']]
    most_rented_films_in_category = most_rented_films_in_category.groupby(['film_id', 'title']).size().reset_index(name='rental_count').sort_values(by='rental_count', ascending=False).head(10)

    st.write(f"En çok kiralanan tür: {most_rented_category['name']}")
    st.write(f"Bu türdeki en çok kiralanan filmler:")
    st.table(most_rented_films_in_category)
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------

    st.title("9. En çok kiralanan filmlerin ortalama kiralama süresi nedir?")

    rental_df = pd.read_sql_query('SELECT * FROM rental;', con)
    inventory_df = pd.read_sql_query('SELECT * FROM inventory;', con)
    film_df = pd.read_sql_query('SELECT * FROM film;', con)

    merged_df = pd.merge(rental_df, inventory_df, on='inventory_id', suffixes=('_rental', '_inventory'))
    merged_df = pd.merge(merged_df, film_df, on='film_id', suffixes=('_inventory', '_film'))

    merged_df['rental_duration'] = pd.to_datetime(merged_df['return_date']) - pd.to_datetime(merged_df['rental_date'])
    merged_df['rental_duration_days'] = merged_df['rental_duration'].dt.days

    rental_counts = merged_df.groupby('title').size().reset_index(name='rental_count')
    top_rented_films = rental_counts.sort_values(by='rental_count', ascending=False).head(10)

    avg_rental_duration = merged_df[merged_df['title'].isin(top_rented_films['title'])] \
        .groupby('title')['rental_duration_days'].mean().reset_index()

    avg_rental_duration = pd.merge(avg_rental_duration, top_rented_films, on='title')

    avg_rental_duration.rename(columns={'rental_count': 'Kiralama Sayısı', 'rental_duration_days': 'Ortalama Kiralama Süresi (Gün)'}, inplace=True)

    st.write("En çok kiralanan 10 filmin ortalama kiralama süresi (gün cinsinden):")
    st.table(avg_rental_duration)
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)


#---------------------------------------------------------------------------------------------

    st.title("10. Hangi aktörler en çok filmde rol almış?")

    actor_df = pd.read_sql_query('SELECT * FROM actor;', con)
    film_actor_df = pd.read_sql_query('SELECT * FROM film_actor;', con)
    film_df = pd.read_sql_query('SELECT * FROM film;', con)

    merged_df = pd.merge(film_actor_df, actor_df, on='actor_id', suffixes=('_film_actor', '_actor'))
    merged_df = pd.merge(merged_df, film_df, on='film_id', suffixes=('_actor', '_film'))

    actor_film_counts = merged_df.groupby(['actor_id', 'first_name', 'last_name']).size().reset_index(name='film_count')
    top_actors = actor_film_counts.sort_values(by='film_count', ascending=False).head(10)

    st.write("En çok filmde rol alan aktörler:")
    st.table(top_actors)
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------

    st.title("11. Hangi kategorilerde en fazla aktör oynamış?")

    merged_df = pd.merge(film_actor_df, film_df, on='film_id', suffixes=('_film_actor', '_film'))
    merged_df = pd.merge(merged_df, film_category_df, on='film_id', suffixes=('_film', '_film_category'))
    merged_df = pd.merge(merged_df, category_df, on='category_id', suffixes=('_film_category', '_category'))
    merged_df = pd.merge(merged_df, actor_df, on='actor_id', suffixes=('_category', '_actor'))

    category_actor_counts = merged_df.groupby(['category_id', 'name']).actor_id.nunique().reset_index(name='actor_count')
    top_categories = category_actor_counts.sort_values(by='actor_count', ascending=False).head(10)

    st.write("En fazla aktörün oynadığı kategoriler:")
    st.table(top_categories)
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------

    st.title("12. En az sayıda film kiralayan mağaza hangisidir ve toplam kiralama sayısı nedir?")

    merged_df = pd.merge(rental_df, inventory_df, on='inventory_id', suffixes=('_rental', '_inventory'))
    rental_counts = merged_df.groupby('store_id').size().reset_index(name='rental_count')
    min_rentals_store = rental_counts[rental_counts['rental_count'] == rental_counts['rental_count'].min()]

    st.write("En az sayıda film kiralayan mağaza:")
    st.table(min_rentals_store)
    st.write("Toplam kiralama sayısı:", min_rentals_store['rental_count'].values[0])
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------

    st.title("13. En fazla sayıda kiralama yapan müşteri hangisidir ve toplam kiralama sayısı nedir?")

    rental_counts = rental_df.groupby('customer_id').size().reset_index(name='rental_count')
    max_rentals_customer = rental_counts[rental_counts['rental_count'] == rental_counts['rental_count'].max()]

    st.write("En fazla sayıda kiralama yapan müşteri:")
    st.table(max_rentals_customer)
    st.write("Toplam kiralama sayısı:", max_rentals_customer['rental_count'].values[0])
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------

    st.title("14. En az sayıda kiralama yapan müşteri hangisidir ve toplam kiralama sayısı nedir?")

    rental_counts = rental_df.groupby('customer_id').size().reset_index(name='rental_count')
    min_rentals_customer = rental_counts[rental_counts['rental_count'] == rental_counts['rental_count'].min()]

    st.write("En az sayıda kiralama yapan müşteri:")
    st.table(min_rentals_customer)
    st.write("Toplam kiralama sayısı:", min_rentals_customer['rental_count'].values[0])
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------

    st.title("15. En fazla sayıda kiralanan film hangisidir ve toplam kiralama sayısı nedir?")

    merged_df = pd.merge(rental_df, inventory_df, on='inventory_id', suffixes=('_rental', '_inventory'))
    merged_df = pd.merge(merged_df, film_df, on='film_id', suffixes=('_inventory', '_film'))

    rental_counts = merged_df.groupby('title').size().reset_index(name='rental_count')
    most_rented_film = rental_counts.loc[rental_counts['rental_count'].idxmax()]

    most_rented_film_df = pd.DataFrame({
        'Film Başlığı': [most_rented_film['title']],
        'Toplam Kiralama Sayısı': [most_rented_film['rental_count']]
    })

    st.table(most_rented_film_df)
    st.markdown("<hr style='border: 1px solid darkblue;'/>", unsafe_allow_html=True)


#---------------------------------------------------------------------------------------------

    con.close()

import streamlit as st
import sqlite3
import pandas as pd
import dask.dataframe as dd

def load_data():
    db_path = r"C:\Users\Baki Akgun\OneDrive\Masaüstü\Library_project\sqlite_sakila.db"
    con = sqlite3.connect(db_path)

    tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql_query(tables_query, con)
    
    table_names = tables['name'].tolist()
    
    dataframes = {}
    for table in table_names:
        df = pd.read_sql_query(f"SELECT * FROM {table}", con)
        dataframes[table] = df

    con.close()

    dask_dataframes = {table: dd.from_pandas(df, npartitions=4) for table, df in dataframes.items()}

    return dask_dataframes

def mainn():
    st.title("Veri Analizi Uygulaması")

    dask_dataframes = load_data()

    table_name = st.sidebar.selectbox("Tablo Seçin", list(dask_dataframes.keys()))
    st.write(f"Seçilen tablo: {table_name}")

    st.write(dask_dataframes[table_name].head())

    st.write("Tablo sütunları:", dask_dataframes[table_name].columns.tolist())

if __name__ == "__main__":
    mainn()
