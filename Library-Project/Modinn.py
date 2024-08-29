import streamlit as st  # type: ignore
import sqlite3
import modin.pandas as mpd  # type: ignore
import pandas as pd  # type: ignore
from module import load_data, sidebar1

def modin1():

    html_temp = """
        <div style="background-color:Blue;padding:1.5px">
        <h1 style="color:white;text-align:center;">Modin</h1>
        </div><br>"""
    st.markdown(html_temp, unsafe_allow_html=True)
    st.markdown('<style>h1{color: blue;}</style>', unsafe_allow_html=True)


    db_path = r"C:\Users\Baki Akgun\OneDrive\Masaüstü\Library_project\sqlite_sakila.db"
    con = sqlite3.connect(db_path)


    dask_dataframes = load_data(db_path)
    sidebar1(dask_dataframes)

# #---------------------------------------------------------------------------------------------
    staff_df = mpd.read_sql('SELECT * FROM staff;', con)
    store_df = mpd.read_sql('SELECT * FROM store;', con)
    film_actor_df = mpd.read_sql('SELECT * FROM film_actor;', con)

    rental_df = mpd.read_sql('SELECT * FROM rental;', con)
    inventory_df = mpd.read_sql('SELECT * FROM inventory;', con)
    film_df = mpd.read_sql('SELECT * FROM film;', con)
    customer_df = mpd.read_sql('SELECT * FROM customer;', con)
    payment_df = mpd.read_sql('SELECT * FROM payment;', con)
    category_df = mpd.read_sql('SELECT * FROM category;', con)
    film_category_df = mpd.read_sql('SELECT * FROM film_category;', con)

    st.title("1.Bazı Tabloların ilk 5 Verisi")
    st.subheader("Staff Verisi")
    st.write(staff_df.head())

    st.subheader("Rental Verisi")
    st.write(rental_df.head())

    st.subheader("Inventory Verisi")
    st.write(inventory_df.head())

    st.subheader("Film Verisi")
    st.write(film_df.head())

    st.subheader("Customer Verisi")
    st.write(customer_df.head())

    st.subheader("Payment Verisi")
    st.write(payment_df.head())

    st.subheader("Category Verisi")
    st.write(category_df.head())

    st.subheader("Film Category Verisi")
    st.write(film_category_df.head())

    unreturned_rentals = rental_df[rental_df['return_date'].isna()]
    unreturned_customers = unreturned_rentals.groupby('customer_id').size()
    multiple_unreturned_customers = unreturned_customers[unreturned_customers > 1].reset_index().rename(columns={0: 'Unreturned DVDs Count'})

    multiple_unreturned_customers = multiple_unreturned_customers.merge(customer_df[['customer_id', 'first_name', 'last_name']], on='customer_id', how='left')

    st.title("2. Birden fazla DVD'yi iade etmeyen müşteriler")
    st.table(multiple_unreturned_customers[['customer_id', 'first_name', 'last_name', 'Unreturned DVDs Count']])

    rental_counts = rental_df.groupby('customer_id').size().reset_index(name='Rental Count')
    most_rentals_customer = rental_counts.loc[rental_counts['Rental Count'].idxmax()]

    most_rentals_customer_df = most_rentals_customer.to_frame().T  # Convert Series to DataFrame
    most_rentals_customer_df = most_rentals_customer_df.merge(customer_df[['customer_id', 'first_name', 'last_name']], on='customer_id', how='left')

    st.title("3. En çok DVD kiralayan müşteri")
    st.table(most_rentals_customer_df[['customer_id', 'first_name', 'last_name', 'Rental Count']])
    st.markdown("<hr style='border: 2px solid blue;'/>", unsafe_allow_html=True)


# #---------------------------------------------------------------------------------------------

    st.title("4. En Popüler Film Kategorisi")

    film_df = mpd.DataFrame(film_df)
    category_df = mpd.DataFrame(category_df)
    film_category_df = mpd.DataFrame(film_category_df)

    merged_df = film_category_df.merge(category_df, on='category_id', suffixes=('', '_cat'))
    final_df = merged_df.merge(film_df, on='film_id', suffixes=('', '_film'))

    final_df['rental_count'] = final_df.groupby('name')['film_id'].transform('count')
    most_popular_category = final_df.groupby('name')['rental_count'].sum().sort_values(ascending=False).head(1)

    st.write("En Popüler Kategori:")
    st.write(most_popular_category)
    st.markdown("<hr style='border: 2px solid blue;'/>", unsafe_allow_html=True)


# #---------------------------------------------------------------------------------------------

    st.title("5-En Çok Geliri Getiren Film")

    rental_df = mpd.DataFrame(rental_df)
    payment_df = mpd.DataFrame(payment_df)
    film_df = mpd.DataFrame(film_df)
    inventory_df = mpd.DataFrame(inventory_df)

    rental_payment_df = rental_df.merge(inventory_df, on='inventory_id', suffixes=('', '_inventory'))
    rental_payment_df = rental_payment_df.merge(payment_df, on='rental_id', suffixes=('', '_rental'))
    rental_payment_film_df = rental_payment_df.merge(film_df, on='film_id', suffixes=('', '_film'))

    total_revenue_df = rental_payment_film_df.groupby('title')['amount'].sum()
    most_revenue_film = total_revenue_df.sort_values(ascending=False).head(1)

    st.write("En Çok Geliri Getiren Film:")
    st.dataframe(most_revenue_film.reset_index())
    st.markdown("<hr style='border: 2px solid blue;'/>", unsafe_allow_html=True)

# #---------------------------------------------------------------------------------------------
    st.title("6-Her Türde En Az Kiralanan Film")

    inventory_df = mpd.DataFrame(inventory_df)
    film_df = mpd.DataFrame(film_df).reset_index()
    category_df = mpd.DataFrame(category_df)
    film_category_df = mpd.DataFrame(film_category_df)
    rental_df = mpd.DataFrame(rental_df)

    rental_film_category_df = rental_df.merge(inventory_df, on='inventory_id', suffixes=('', '_inventory'))
    rental_film_category_df = rental_film_category_df.merge(film_df, on='film_id', suffixes=('', '_ilm'))
    rental_film_category_df = rental_film_category_df.merge(film_category_df, on='film_id', suffixes=('', '_filmid'))
    rental_film_category_df = rental_film_category_df.merge(category_df, on='category_id', suffixes=('', '_category'))

    film_rentals_df = rental_film_category_df.groupby(['name', 'title']).size()
    least_rented_film = film_rentals_df.groupby('name').idxmin()

    st.write("Her Türde En Az Kiralanan Film:")
    st.dataframe(least_rented_film.reset_index())
    st.markdown("<hr style='border: 2px solid blue;'/>", unsafe_allow_html=True)


# #---------------------------------------------------------------------------------------------
    st.title("7-En Çok Kiralanan Film Hangi Mağazada Kiralanmış?(Hatalı)")

    rental_df = mpd.DataFrame(rental_df)
    inventory_df = mpd.DataFrame(inventory_df)
    store_df = mpd.DataFrame(store_df)
    film_df = mpd.DataFrame(film_df)

    rental_inventory_df = rental_df.merge(inventory_df, on='inventory_id', suffixes=('', '_inventory'))
    rental_inventory_film_df = rental_inventory_df.merge(film_df, on='film_id', suffixes=('', '_film'))
    rental_inventory_film_store_df = rental_inventory_film_df.merge(store_df, on='store_id', suffixes=('', '_store'))

    film_rentals_count_df = rental_inventory_film_df['film_id'].value_counts()
    most_rented_film_id = film_rentals_count_df.idxmax()

    most_rented_film_stores_df = rental_inventory_film_store_df[rental_inventory_film_store_df['film_id'] == most_rented_film_id]
    result_df = most_rented_film_stores_df[['film_id', 'store_id']].drop_duplicates()
    result_df = result_df.merge(film_df[['film_id', 'title']], on='film_id', how='left')

    st.write("En Çok Kiralanan Film Hangi Mağazada Kiralanmış?")
    st.dataframe(result_df[['title', 'store_id']].head(1))

    st.markdown("<hr style='border: 2px solid blue;'/>", unsafe_allow_html=True)

# #---------------------------------------------------------------------------------------------
 
    st.title("8 - En Çok Kiralanan Türler Ve Filmleri")

    rental_df = mpd.DataFrame(rental_df)
    film_category_df = mpd.DataFrame(film_category_df)
    category_df = mpd.DataFrame(category_df)
    film_df = mpd.DataFrame(film_df)
    inventory_df = mpd.DataFrame(inventory_df)

    film_category_merged_df = film_category_df.merge(category_df, on='category_id', suffixes=('_film', '_category'))
    rental_film_category_df = rental_df.merge(inventory_df, on='inventory_id', suffixes=('_rental', '_inventory'))
    rental_film_df = rental_film_category_df.merge(film_category_merged_df, on='film_id', suffixes=('_inventory', '_category'))
    rental_film_df = rental_film_df.merge(film_df, on='film_id', suffixes=('_category', '_film'))

    category_rentals_count_df = rental_film_df['name'].value_counts().reset_index()
    category_rentals_count_df.columns = ['Category', 'Rental Count']
    top_10_categories_df = category_rentals_count_df.head(10)

    st.subheader("Top 10 En Çok Kiralanan Türler")
    st.dataframe(top_10_categories_df)

    film_rentals_count_df = rental_film_df.groupby('film_id')['inventory_id'].count().reset_index()
    film_rentals_count_df.columns = ['film_id', 'Rental Count']

    film_details_df = film_df[['film_id', 'title']].merge(film_category_df[['film_id', 'category_id']], on='film_id')
    film_details_df = film_details_df.merge(category_df[['category_id', 'name']], on='category_id')
    film_details_df = film_details_df.merge(film_rentals_count_df, on='film_id')

    top_10_film_details = []
    for category in top_10_categories_df['Category']:
        category_films_df = film_details_df[film_details_df['name'] == category]
        top_films = category_films_df.sort_values(by='Rental Count', ascending=False).head(10)
        top_10_film_details.append(top_films)

    top_10_film_details_df = mpd.concat(top_10_film_details)
    st.subheader("Top 10 En Çok Kiralanan Türlerin Filmleri")
    st.dataframe(top_10_film_details_df)
    st.markdown("<hr style='border: 2px solid blue;'/>", unsafe_allow_html=True)

# #---------------------------------------------------------------------------------------------
    st.title("9-En Çok Kiralanan Filmlerin Ortalama Kiralama Süresi")

    rental_df = mpd.DataFrame(rental_df)
    inventory_df = mpd.DataFrame(inventory_df)
    film_df = mpd.DataFrame(film_df)

    film_rentals_df = rental_df.merge(inventory_df, on='inventory_id')
    film_rentals_df = film_rentals_df.merge(film_df, on='film_id')
    film_rentals_df['rental_duration'] = pd.to_datetime(film_rentals_df['return_date']) - pd.to_datetime(film_rentals_df['rental_date'])
    film_rentals_df['rental_duration_days'] = film_rentals_df['rental_duration'].dt.days

    film_rental_duration_avg_df = film_rentals_df.groupby('title')['rental_duration_days'].mean()
    top_10_films_avg_duration = film_rental_duration_avg_df.sort_values(ascending=False).head(10)

    st.write("En Çok Kiralanan Filmlerin Ortalama Kiralama Süresi:")
    st.dataframe(top_10_films_avg_duration.reset_index())
    st.markdown("<hr style='border: 2px solid blue;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------
    st.title("10 - En Çok Kiralanan Filmler")

    rental_df = mpd.DataFrame(rental_df)
    film_df = mpd.DataFrame(film_df)
    inventory_df = mpd.DataFrame(inventory_df)

    rental_inventory_film_df = rental_df.merge(inventory_df, on='inventory_id', suffixes=('', '_inventory'))
    rental_inventory_film_df = rental_inventory_film_df.merge(film_df, on='film_id', suffixes=('', '_film'))

    film_rental_counts_df = rental_inventory_film_df['film_id'].value_counts().reset_index()
    film_rental_counts_df.columns = ['film_id', 'rental_count']

    most_rented_films_df = film_rental_counts_df.merge(film_df[['film_id', 'title']], on='film_id')
    most_rented_films_df = most_rented_films_df.sort_values(by='rental_count', ascending=False)

    st.write("En Çok Kiralanan Filmler:")
    st.dataframe(most_rented_films_df.head(10))
    st.markdown("<hr style='border: 2px solid blue;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------

    st.title("11-En Fazla Aktör Oynayan Kategoriler")

    film_actor_df = mpd.DataFrame(film_actor_df)
    film_category_df = mpd.DataFrame(film_category_df)
    category_df = mpd.DataFrame(category_df)

    film_actor_category_df = film_actor_df.merge(film_category_df, on='film_id')
    film_actor_category_df = film_actor_category_df.merge(category_df, on='category_id')

    category_actor_count_df = film_actor_category_df.groupby('name')['actor_id'].nunique().reset_index()
    category_actor_count_df.columns = ['category_name', 'actor_count']

    top_categories_df = category_actor_count_df.nlargest(10, 'actor_count')

    st.dataframe(top_categories_df[['category_name', 'actor_count']])
    st.markdown("<hr style='border: 2px solid blue;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------


    st.title("12-En Az Sayıda Film Kiralayan Mağaza ve Toplam Kiralama Sayısı")

    rental_df = mpd.DataFrame(rental_df)
    inventory_df = mpd.DataFrame(inventory_df)
    store_df = mpd.DataFrame(store_df)

    rental_inventory_df = rental_df.merge(inventory_df, on='inventory_id')
    rental_store_df = rental_inventory_df.merge(store_df, on='store_id')

    store_rental_count_df = rental_store_df.groupby('store_id').size().reset_index()
    store_rental_count_df.columns = ['store_id', 'rental_count']

    least_rented_store_df = store_rental_count_df.nsmallest(1, 'rental_count')

    st.subheader("En Az Kiralayan Mağaza")
    st.dataframe(least_rented_store_df)

#---------------------------------------------------------------------------------------------
 
    # st.title("13 - En Fazla Sayıda Kiralama Yapan Müşteri ve Toplam Kiralama Sayısı")

    # # Modin DataFrame'lerini oluşturun
    # rental_df = mpd.DataFrame(rental_df)
    # customer_df = mpd.DataFrame(customer_df)

    # # Tabloları birleştirin
    # rental_customer_df = rental_df.merge(customer_df, on='customer_id')

    # # Müşterilerin toplam kiralama sayılarını hesaplayın
    # customer_rental_count_df = rental_customer_df.groupby('customer_id').size().reset_index()
    # customer_rental_count_df.columns = ['customer_id', 'rental_count']

    # # Müşteri isimlerini ekleyin
    # customer_rental_info_df = customer_rental_count_df.merge(customer_df[['customer_id', 'first_name', 'last_name']], on='customer_id')

    # # En fazla sayıda kiralama yapan müşteriyi bulun
    # most_rented_customer_df = customer_rental_info_df.nlargest(1, 'rental_count')

    # # Streamlit ile sonucu gösterin
    # st.subheader("En Fazla Kiralayan Müşteri")
    # st.dataframe(most_rented_customer_df)
    # st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)

#---------------------------------------------------------------------------------------------
    # st.title("14-En Az Sayıda Kiralama Yapan Müşteri ve Toplam Kiralama Sayısı")

    # # Modin DataFrame'lerini oluşturun
    # rental_df = mpd.DataFrame(rental_df)
    # customer_df = mpd.DataFrame(customer_df)

    # # Tabloları birleştirin
    # rental_customer_df = rental_df.merge(customer_df, on='customer_id')

    # # Müşterilerin toplam kiralama sayılarını hesaplayın
    # customer_rental_count_df = rental_customer_df.groupby('customer_id').size().reset_index()
    # customer_rental_count_df.columns = ['customer_id', 'rental_count']

    # # Müşteri isimlerini ekleyin
    # customer_rental_info_df = customer_rental_count_df.merge(customer_df[['customer_id', 'first_name', 'last_name']], on='customer_id')

    # # En az sayıda kiralama yapan müşteriyi bulun
    # least_rented_customer_df = customer_rental_info_df.nsmallest(1, 'rental_count')

    # # Streamlit ile sonucu gösterin
    # st.subheader("En Az Kiralayan Müşteri")
    # st.dataframe(least_rented_customer_df)
    # st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)


#---------------------------------------------------------------------------------------------

    st.title("15-En Fazla Sayıda Kiralanan Film ve Toplam Kiralama Sayısı")
    rental_df = mpd.DataFrame(rental_df)
    inventory_df = mpd.DataFrame(inventory_df)
    film_df = mpd.DataFrame(film_df)

    rental_inventory_df = rental_df.merge(inventory_df, on='inventory_id')
    rental_film_df = rental_inventory_df.merge(film_df, on='film_id')

    film_rental_count_df = rental_film_df.groupby('film_id').size().reset_index()
    film_rental_count_df.columns = ['film_id', 'rental_count']

    most_rented_film_df = film_rental_count_df.nlargest(1, 'rental_count')

    film_titles_df = film_df[['film_id', 'title']].set_index('film_id')
    most_rented_film_with_title_df = most_rented_film_df.merge(film_titles_df, on='film_id')

    st.subheader("En Fazla Kiralanan Film")
    st.dataframe(most_rented_film_with_title_df)
    st.markdown("<hr style='border: 2px solid blue;'/>", unsafe_allow_html=True)


#---------------------------------------------------------------------------------------------


#     con.close()