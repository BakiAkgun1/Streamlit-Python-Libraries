import sqlite3
import pandas as pd # type: ignore
import polars as pl # type: ignore
import streamlit as st  # type: ignore
from module import load_data, sidebar1

def polars1():
     
    html_temp = """
    <div style="background-color:yellow;padding:1.5px">

    <h1 style="color:black;text-align:center;">Polars</h1>

    </div><br>"""
    st.markdown(html_temp,unsafe_allow_html=True)
    st.markdown('<style>h1{color: orange;}</style>', unsafe_allow_html=True)
   
  
    st.image("photo/polars.png", caption='Polars', use_column_width=True)

    db_path = r"C:\Users\Baki Akgun\OneDrive\Masaüstü\Library_project\sqlite_sakila.db"
    con = sqlite3.connect(db_path)

    dask_dataframes = load_data(db_path)
    sidebar1(dask_dataframes)

    
    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)
    film_df = pd.read_sql_query("SELECT * FROM film;", con)
    inventory_df = pd.read_sql_query("SELECT * FROM inventory;", con)
    customer_df = pd.read_sql_query("SELECT * FROM customer;", con)
    store_df = pd.read_sql_query("SELECT * FROM store;", con)
    payment_df = pd.read_sql_query("SELECT * FROM payment;", con)
    film_actor_df = pd.read_sql_query("SELECT * FROM film_actor;", con)
    film_category_df = pd.read_sql_query("SELECT * FROM film_category;", con)
    category_df = pd.read_sql_query("SELECT * FROM category;", con)
    staff_df = pd.read_sql_query("SELECT * FROM staff;", con)

    staff_df = pl.from_pandas(pd.read_sql_query('SELECT * FROM staff  LIMIT 5;', con))
    rental_df = pl.from_pandas(pd.read_sql_query('SELECT * FROM rental  LIMIT 5;', con))
    inventory_df = pl.from_pandas(pd.read_sql_query('SELECT * FROM inventory  LIMIT 5;', con))
    film_df = pl.from_pandas(pd.read_sql_query('SELECT * FROM film  LIMIT 5;', con))

    st.title("Bazı Tabloların İlk 5 Verisi")

    st.subheader("Staff Verisi")
    st.dataframe(staff_df.to_pandas())

    st.subheader("Rental Verisi")
    st.dataframe(rental_df.to_pandas())

    st.subheader("Inventory Verisi")
    st.dataframe(inventory_df.to_pandas())

    st.subheader("Film Verisi")
    st.dataframe(film_df.to_pandas())
    st.markdown("<hr style='border: 2px solid orange;'/>", unsafe_allow_html=True)

#------------------------------------------------------------------------,

    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)

    rental_pl = pl.from_pandas(rental_df)

    unreturned_pl = rental_pl.filter(pl.col('return_date').is_null())

    customer_counts = (
        unreturned_pl
        .groupby('customer_id')
        .agg(pl.col('customer_id').count().alias('unreturned_count'))
    )

    multiple_unreturned_customers = customer_counts.filter(pl.col('unreturned_count') > 1)

    st.title("2-Birden Fazla DVD'yi İade Etmeyen Müşteri Sayısı")
    st.write("Birden Fazla DVD'yi İade Etmeyen Müşteriler:")
    st.dataframe(multiple_unreturned_customers.to_pandas())
    st.markdown("<hr style='border: 2px solid orange;'/>", unsafe_allow_html=True)

#------------------------------------------------------------------------,
    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)
    customer_df = pd.read_sql_query("SELECT * FROM customer;", con)

    rental_pl = pl.from_pandas(rental_df)
    customer_pl = pl.from_pandas(customer_df)

    customer_names = customer_pl.to_pandas().set_index('customer_id')[['first_name', 'last_name']].to_dict(orient='index')

    most_rented_pl = (
        rental_pl
        .select(['customer_id'])
        .groupby(['customer_id'])
        .agg(pl.count().alias('rental_count'))
    )

    most_rented_customer = most_rented_pl.sort('rental_count', descending=True).head(1)
    most_rented_customer_id = most_rented_customer['customer_id'][0]
    max_rentals = most_rented_customer['rental_count'][0]

    customer_info = customer_names.get(most_rented_customer_id, {'first_name': 'Unknown', 'last_name': 'Unknown'})
    most_rented_customer_name = f"{customer_info['first_name']} {customer_info['last_name']}"

    result_df = pl.DataFrame({
        'Customer ID': [most_rented_customer_id],
        'Customer Name': [most_rented_customer_name],
        'Total Rentals': [max_rentals]
    })

    st.title("3-En Fazla Kiralama Yapan Müşteri")
    st.dataframe(result_df.to_pandas())

    st.markdown("<hr style='border: 2px solid orange;'/>", unsafe_allow_html=True)


#------------------------------------------------------------------------,
  

        
    st.title("4- En az sayıda film kiralayan mağaza")

    inventory_df = pd.read_sql_query('SELECT * FROM inventory;', con)
    rental_df = pd.read_sql_query('SELECT * FROM rental;', con)
    store_df = pd.read_sql_query('SELECT * FROM store;', con)

   
    inventory_polars_df = pl.DataFrame(inventory_df)
    rental_polars_df = pl.DataFrame(rental_df)
    store_polars_df = pl.DataFrame(store_df)

    merged_df = rental_polars_df.join(inventory_polars_df, on='inventory_id')

    rental_counts = merged_df.groupby('store_id').agg(pl.count().alias('rental_count'))

    least_rented_store = rental_counts.sort('rental_count').head(1)

    final_df = least_rented_store.join(store_polars_df, on='store_id')

    st.write("En az sayıda film kiralayan mağaza:")
    st.write(final_df.to_pandas())
    st.markdown("<hr style='border: 2px solid orange;'/>", unsafe_allow_html=True)

#------------------------------------------------------------------------,


    st.title("5-En Çok Geliri Hangi Film Getirmiş")


    film_df = pd.read_sql_query('SELECT film_id, rental_rate, title FROM film;', con)
    inventory_df = pd.read_sql_query('SELECT inventory_id, film_id FROM inventory;', con)
    rental_df = pd.read_sql_query('SELECT inventory_id FROM rental;', con)

    rental_with_film_df = pd.merge(rental_df, inventory_df, on='inventory_id')

    rental_with_film_df = pd.merge(rental_with_film_df, film_df, on='film_id')

    rental_count_per_film = rental_with_film_df.groupby('film_id').size().reset_index(name='rental_count')
    film_revenue_df = pd.merge(rental_count_per_film, film_df, on='film_id')
    film_revenue_df['total_revenue'] = film_revenue_df['rental_count'] * film_revenue_df['rental_rate']

    highest_revenue_film = film_revenue_df.sort_values(by='total_revenue', ascending=False).head(1)

    st.write("En çok geliri hangi film getirmiş:")
    st.write(highest_revenue_film[['title', 'total_revenue']])
    st.markdown("<hr style='border: 2px solid orange;'/>", unsafe_allow_html=True)

#------------------------------------------------------------------------,
    
    
    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)

    rental_pl = pl.from_pandas(rental_df)

    unreturned_pl = rental_pl.filter(pl.col('return_date').is_null())

    customer_counts = (
        unreturned_pl
        .select(['customer_id'])
        .groupby(['customer_id'])
        .agg(pl.count())
        .rename({'count': 'unreturned_count'})
    )

    multiple_unreturned_customers = customer_counts.filter(pl.col('unreturned_count') > 1)

    st.title("6-Birden Fazla DVD'yi İade Etmeyen Müşteri Sayısı")
    st.write("Birden Fazla DVD'yi İade Etmeyen Müşteriler:")
    st.dataframe(multiple_unreturned_customers.to_pandas())
    st.markdown("<hr style='border: 2px solid orange;'/>", unsafe_allow_html=True)
 
#------------------------------------------------------------------------,
   
        
   # Pandas DataFrame'lerini Polars DataFrame'lerine dönüştürün
    # rental_pl = pl.DataFrame(rental_df)
    # inventory_pl = pl.DataFrame(inventory_df)
    # film_pl = pl.DataFrame(film_df)

    # # Tabloları birleştirin
    # # Sütun çakışmalarını önlemek için 'suffix' kullanın
    # merged_df = rental_pl.join(inventory_pl, on='inventory_id', how='left', suffix='_inventory')
    # merged_df = merged_df.join(film_pl, on='film_id', how='left', suffix='_film')

    # # Gereksiz 'last_update' sütunlarını kaldırın
    # merged_df = merged_df.drop(["last_update_inventory", "last_update_film"])

    # # En çok kiralanan filmi bulun
    # film_rental_counts = merged_df.groupby('film_id').agg(pl.count().alias('rental_count'))
    # most_rented_film_id = film_rental_counts.sort('rental_count', descending=True).head(1)['film_id'].item()

    # # En çok kiralanan filmin mağaza bilgilerini alın
    # most_rented_film_inventory = merged_df.filter(pl.col('film_id') == most_rented_film_id)
    # most_rented_film_store = most_rented_film_inventory.groupby('store_id').agg(pl.count().alias('rental_count'))

    # # Sonuçları Streamlit ile gösterin
    # st.title("7-En Çok Kiralanan Film Hangi Mağazada Kiralanmış?")
    # st.write("En çok kiralanan film ve mağaza bilgisi:")
    # st.write(most_rented_film_store.to_pandas())
#------------------------------------------------------------------------,

    # # Pandas DataFrame'lerini Polars DataFrame'lerine dönüştürün
    # film_category_pl = pl.DataFrame(film_category_df)
    # category_pl = pl.DataFrame(category_df)
    # rental_pl = pl.DataFrame(rental_df)
    # inventory_pl = pl.DataFrame(inventory_df)

    # # Tabloları birleştirin
    # merged_df = film_category_pl.join(category_pl, on='category_id', how='left')
    # merged_df = merged_df.join(inventory_pl, on='film_id', how='left')
    # merged_df = merged_df.join(rental_pl, on='inventory_id', how='left')

    # # Kiralanma sayısını hesaplayın
    # most_rented_categories = (
    #     merged_df
    #     .groupby('name')
    #     .agg(pl.count().alias('rental_count'))
    #     .sort('rental_count', descending=True)
    #     .head(10)  # İlk 10 türü göster
    # )

    # # Sonuçları göster
    # st.title("8-En Çok Kiralanan Türler")
    # st.write(most_rented_categories.to_pandas())


#------------------------------------------------------------------------,


    # rental_pl = pl.DataFrame(rental_df)
    # inventory_pl = pl.DataFrame(inventory_df)
    # film_pl = pl.DataFrame(film_df)

    # # Tabloları birleştirin
    # merged_df = rental_pl.join(inventory_pl, on='inventory_id', how='left')
    # merged_df = merged_df.join(film_pl, on='film_id', how='left')

    # # Kiralama süresini hesapla
    # merged_df = merged_df.with_columns(
    #     (pl.col('return_date') - pl.col('rental_date')).alias('rental_duration')
    # )

    # # Ortalama kiralama süresini hesapla
    # avg_rental_duration = merged_df.groupby('film_id').agg(pl.mean('rental_duration').alias('avg_duration'))

    # # En çok kiralanan filmler için ortalama süreyi bulun
    # most_rented_film_duration = (
    #     merged_df
    #     .groupby('film_id')
    #     .agg(pl.count().alias('rental_count'))
    #     .sort('rental_count', descending=True)
    #     .join(avg_rental_duration, on='film_id')
    # )

    # # Sonuçları göster
    # st.title("9-En Çok Kiralanan Filmlerin Ortalama Kiralama Süresi")
    # st.write(most_rented_film_duration.head(10).to_pandas())  # İlk 10 filmi göster
#------------------------------------------------------------------------,


    actor_df = pd.read_sql_query('SELECT * FROM actor', con)
    film_actor_df = pd.read_sql_query('SELECT * FROM film_actor', con)

    actor_pl = pl.DataFrame(actor_df)
    film_actor_pl = pl.DataFrame(film_actor_df)

    merged_df = film_actor_pl.join(actor_pl, on='actor_id', how='left')

    most_films_by_actor = (
        merged_df
        .groupby('actor_id')
        .agg(pl.count().alias('film_count'))
        .sort('film_count', descending=True)
        .join(actor_pl, on='actor_id')
    )

    st.title("10-En Çok Filmde Rol Alan Aktörler")
    st.write(most_films_by_actor.select(['first_name', 'last_name', 'film_count']).head(10).to_pandas())  # İlk 10 aktörü göster
    st.markdown("<hr style='border: 2px solid orange;'/>", unsafe_allow_html=True)

#------------------------------------------------------------------------,
   # # Pandas DataFrame'lerini Polars DataFrame'lerine dönüştürün
    # film_category_pl = pl.DataFrame(film_category_df)
    # category_pl = pl.DataFrame(category_df)
    # film_actor_pl = pl.DataFrame(film_actor_df)
    # actor_pl = pl.DataFrame(actor_df)

    # # Tabloları birleştirin
    # merged_df = film_category_pl.join(film_actor_pl, on='film_id', how='left')
    # merged_df = merged_df.join(category_pl, on='category_id', how='left')

    # # Kategorilerde oynayan benzersiz aktör sayısını bulun
    # actors_per_category = (
    #     merged_df
    #     .groupby('name')
    #     .agg(pl.col('actor_id').n_unique().alias('unique_actors_count'))
    #     .sort('unique_actors_count', descending=True)
    # )

    # # Sonuçları göster
    # st.title("11-Hangi Kategorilerde En Fazla Aktör Oynamış?")
    # st.write(actors_per_category.to_pandas().head(10))  # İlk 10 kategoriyi göster

#------------------------------------------------------------------------,

    # # Pandas DataFrame'lerini Polars DataFrame'lerine dönüştürün
    # rental_pl = pl.DataFrame(rental_df)
    # inventory_pl = pl.DataFrame(inventory_df)
    # store_pl = pl.DataFrame(store_df)

    # # Tabloları birleştirin
    # merged_df = rental_pl.join(inventory_pl, on='inventory_id', how='left')
    # merged_df = merged_df.join(store_pl, on='store_id', how='left')

    # # Her mağazanın toplam kiralama sayısını bulun
    # rentals_per_store = (
    #     merged_df
    #     .groupby('store_id')
    #     .agg(pl.count().alias('total_rentals'))
    #     .sort('total_rentals', descending=True)
    # )

    # # En az kiralama yapan mağazayı bulun
    # least_rentals_store = rentals_per_store.tail(1)

    # # Sonuçları göster
    # st.title("12-En Az Sayıda Film Kiralayan Mağaza ve Toplam Kiralama Sayısı")
    # st.write(least_rentals_store.to_pandas())
#------------------------------------------------------------------------,
    rental_df = pd.read_sql_query('SELECT * FROM rental', con)
    customer_df = pd.read_sql_query('SELECT * FROM customer', con)

    rental_pl = pl.DataFrame(rental_df)
    customer_pl = pl.DataFrame(customer_df)

    merged_df = rental_pl.join(customer_pl, on='customer_id', how='left')

    rentals_per_customer = (
        merged_df
        .groupby(['customer_id', 'first_name', 'last_name'])
        .agg(pl.count().alias('total_rentals'))
        .sort('total_rentals', descending=True)
    )

    most_rentals_customer = rentals_per_customer.head(1)

    st.title("13-En Fazla Sayıda Kiralama Yapan Müşteri ve Toplam Kiralama Sayısı")
    st.write(most_rentals_customer.to_pandas())
    st.markdown("<hr style='border: 2px solid orange;'/>", unsafe_allow_html=True)

#------------------------------------------------------------------------,

    least_rentals_customer = rentals_per_customer.tail(1)

    st.title("14-En Az Sayıda Kiralama Yapan Müşteri ve Toplam Kiralama Sayısı")
    st.write(least_rentals_customer.to_pandas())
    st.markdown("<hr style='border: 2px solid orange;'/>", unsafe_allow_html=True)

#------------------------------------------------------------------------,


    # # Verileri Pandas DataFrame'lerine yükleyin
    # rental_df = pd.read_sql_query('SELECT * FROM rental', con)
    # inventory_df = pd.read_sql_query('SELECT * FROM inventory', con)
    # film_df = pd.read_sql_query('SELECT * FROM film', con)

    # # Pandas DataFrame'lerini Polars DataFrame'lerine dönüştürün
    # rental_pl = pl.DataFrame(rental_df)
    # inventory_pl = pl.DataFrame(inventory_df)
    # film_pl = pl.DataFrame(film_df)

    # # Tabloları birleştirin
    # merged_df = rental_pl.join(inventory_pl, on='inventory_id', how='left')
    # merged_df = merged_df.join(film_pl, on='film_id', how='left')

    # # Her filmin toplam kiralama sayısını bulun
    # rentals_per_film = (
    #     merged_df
    #     .groupby('film_id')
    #     .agg(pl.count().alias('total_rentals'))
    #     .sort('total_rentals', descending=True)
    # )

    # # En fazla kiralanan filmi bulun
    # most_rented_film = rentals_per_film.head(1)

    # # Sonuçları göster
    # st.title("15-En Fazla Sayıda Kiralanan Film ve Toplam Kiralama Sayısı")
    # st.write(most_rented_film.to_pandas())

# Veritabanı bağlantısını kapatın
    con.close()







