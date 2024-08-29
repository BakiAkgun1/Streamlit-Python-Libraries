import sqlite3
import pandas as pd #type: ignore
import streamlit as st #type: ignore
import dask.dataframe as dd
from module import load_data, sidebar1


def dask1():  
    html_temp = """
        <div style="background-color:gold;padding:1.5px">
    
        <h1 style="color:white;text-align:center;">Dask</h1>

        </div><br>"""
    st.markdown(html_temp,unsafe_allow_html=True)
    st.markdown('<style>h1{color: gold;}</style>', unsafe_allow_html=True)
        
    st.image("photo/Dask_Logo.svg.png", caption='Pandas',  use_column_width=True)

    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)

  

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

 #-----------------------------------------------------------------------------------
    st.title("1 - Bazı Tabloların İlk 10 Verisi")

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

    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)

#-----------------------------------------------------------------------------------


    st.title("2-Birden Fazla DVD'yi İade Etmeyen Müşteri Sayısı")


    rental_ddf = dd.from_pandas(rental_df, npartitions=1)

    def get_unreturned_customers():
        unreturned_ddf = rental_ddf[rental_ddf['return_date'].isnull()]
        unreturned_count = unreturned_ddf.groupby('customer_id').size().compute()
        multiple_unreturned_customers = unreturned_count[unreturned_count > 1]
        return multiple_unreturned_customers

    unreturned_customers = get_unreturned_customers()
    st.write("Birden Fazla DVD'yi İade Etmeyen Müşteriler:")
    st.dataframe(unreturned_customers.reset_index())

    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)

#-----------------------------------------------------------------------------------



    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)
    customer_df = pd.read_sql_query("SELECT * FROM customer;", con)

    rental_ddf = dd.from_pandas(rental_df, npartitions=1)
    customer_ddf = dd.from_pandas(customer_df, npartitions=1)

    customer_names = customer_df.set_index('customer_id')[['first_name', 'last_name']].to_dict(orient='index')

    def get_most_rented_customer():
        most_rented_ddf = rental_ddf.groupby('customer_id')['rental_id'].count().compute()
        most_rented_customer_id = most_rented_ddf.idxmax()
        max_rentals = most_rented_ddf.max()
        
        customer_info = customer_names.get(most_rented_customer_id, {'first_name': 'Unknown', 'last_name': 'Unknown'})
        most_rented_customer_name = f"{customer_info['first_name']} {customer_info['last_name']}"
        
        return pd.DataFrame({
            'Customer ID': [most_rented_customer_id],
            'Customer Name': [most_rented_customer_name],
            'Total Rentals': [max_rentals]
        })

    most_rented_customer_df = get_most_rented_customer()

    # Streamlit ile sonucu gösterin
    st.title("3-En Fazla Kiralama Yapan Müşteri")
    st.dataframe(most_rented_customer_df)
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)


#-----------------------------------------------------------------------------------


    st.title("4. En Popüler Film Kategorisi")

 

    film_ddf = dd.from_pandas(film_df, npartitions=1)
    category_ddf = dd.from_pandas(category_df, npartitions=1)
    film_category_ddf = dd.from_pandas(film_category_df, npartitions=1)

    merged_ddf = film_category_ddf.merge(category_ddf, on='category_id', suffixes=('', '_cat'))

    final_ddf = merged_ddf.merge(film_ddf, on='film_id', suffixes=('', '_film'))

    final_ddf['rental_count'] = final_ddf.groupby('name')['film_id'].transform('count')

    most_popular_category = final_ddf.groupby('name')['rental_count'].sum().compute().sort_values(ascending=False).head(1)

    st.write("En Popüler Kategori:")
    st.write(most_popular_category)
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)

#-----------------------------------------------------------------------------------


    st.title("5-En Çok Geliri Getiren Film")

    rental_ddf = dd.from_pandas(rental_df, npartitions=1)
    payment_ddf = dd.from_pandas(payment_df, npartitions=1)
    film_ddf = dd.from_pandas(film_df, npartitions=1)
    inventory_ddf = dd.from_pandas(inventory_df, npartitions=1)


    rental_payment_ddf = rental_ddf.merge(inventory_ddf, on='inventory_id', suffixes=('', '_inventory'))
    rental_payment_ddf = rental_payment_ddf.merge(payment_ddf, on='rental_id', suffixes=('', '_rental'))
    rental_payment_film_ddf = rental_payment_ddf.merge(film_ddf, on='film_id', suffixes=('', '_film'))

    total_revenue_ddf = rental_payment_film_ddf.groupby('title')['amount'].sum().compute()

    most_revenue_film = total_revenue_ddf.sort_values(ascending=False).head(1)

    st.write("En Çok Geliri Getiren Film:")
    st.dataframe(most_revenue_film.reset_index())
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)

#-----------------------------------------------------------------------------------


    st.title("6-Her Türde En Az Kiralanan Film")


    inventory_ddf = dd.from_pandas(inventory_df, npartitions=1)
    film_ddf = dd.from_pandas(film_df, npartitions=1).reset_index()
    category_ddf = dd.from_pandas(category_df, npartitions=1)
    film_category_ddf = dd.from_pandas(film_category_df, npartitions=1)
    rental_ddf = dd.from_pandas(rental_df, npartitions=1)

    rental_film_category_ddf = rental_ddf.merge(inventory_ddf, on='inventory_id', suffixes=('', '_inventory'))
    rental_film_category_ddf = rental_film_category_ddf.merge(film_ddf, on='film_id', suffixes=('', '_ilm'))
    rental_film_category_ddf = rental_film_category_ddf.merge(film_category_ddf, on='film_id', suffixes=('', '_filmid'))
    rental_film_category_ddf = rental_film_category_ddf.merge(category_ddf, on='category_id', suffixes=('', '_category'))

    film_rentals_ddf = rental_film_category_ddf.groupby(['name', 'title']).size().compute()

    least_rented_film = film_rentals_ddf.groupby('name').idxmin()

    st.write("Her Türde En Az Kiralanan Film:")
    st.dataframe(least_rented_film.reset_index())
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)

 



#-----------------------------------------------------------------------------------

    rental_ddf = dd.from_pandas(rental_df, npartitions=1)
    inventory_ddf = dd.from_pandas(inventory_df, npartitions=1)
    store_ddf = dd.from_pandas(store_df, npartitions=1)
    film_ddf = dd.from_pandas(film_df, npartitions=1)

    rental_inventory_ddf = rental_ddf.merge(inventory_ddf, on='inventory_id', suffixes=('', '_inventory'))
    rental_inventory_film_ddf = rental_inventory_ddf.merge(film_ddf, on='film_id', suffixes=('', '_film'))
    rental_inventory_film_store_ddf = rental_inventory_film_ddf.merge(store_ddf, on='store_id', suffixes=('', '_store'))

    film_rentals_count = rental_inventory_film_ddf['film_id'].value_counts().compute()
    most_rented_film_id = film_rentals_count.idxmax()

    most_rented_film_stores_ddf = rental_inventory_film_store_ddf[rental_inventory_film_store_ddf['film_id'] == most_rented_film_id]

    result_ddf = most_rented_film_stores_ddf[['film_id', 'store_id']].drop_duplicates()

    result_ddf = result_ddf.merge(film_ddf[['film_id', 'title']], on='film_id', how='left')

    st.title("7-En Çok Kiralanan Film Hangi Mağazada Kiralanmış?")
    st.dataframe(result_ddf[['title', 'store_id']].compute().head(1))
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)


#--------------------------

    st.title("8 - En Çok Kiralanan Türler Ve Filmleri")
    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)
    film_category_df = pd.read_sql_query("SELECT * FROM film_category;", con)
    category_df = pd.read_sql_query("SELECT * FROM category;", con)
    film_df = pd.read_sql_query("SELECT * FROM film;", con)
    inventory_df = pd.read_sql_query("SELECT * FROM inventory;", con)

    rental_ddf = dd.from_pandas(rental_df, npartitions=1)
    film_category_ddf = dd.from_pandas(film_category_df, npartitions=1)
    category_ddf = dd.from_pandas(category_df, npartitions=1)
    film_ddf = dd.from_pandas(film_df, npartitions=1)
    inventory_ddf = dd.from_pandas(inventory_df, npartitions=1)

    film_category_merged_ddf = film_category_ddf.merge(category_ddf, on='category_id', suffixes=('_film', '_category'))
    rental_film_category_ddf = rental_ddf.merge(inventory_ddf, on='inventory_id', suffixes=('_rental', '_inventory'))
    rental_film_ddf = rental_film_category_ddf.merge(film_category_merged_ddf, on='film_id', suffixes=('_inventory', '_category'))
    rental_film_ddf = rental_film_ddf.merge(film_ddf, on='film_id', suffixes=('_category', '_film'))

    category_rentals_count_ddf = rental_film_ddf['name'].value_counts().reset_index()
    category_rentals_count_ddf.columns = ['Category', 'Rental Count']

    top_10_categories_ddf = category_rentals_count_ddf.head(10)

    st.subheader("Top 10 En Çok Kiralanan Türler")
    st.dataframe(top_10_categories_ddf)

    film_rentals_count_ddf = rental_film_ddf.groupby('film_id')['inventory_id'].count().reset_index()
    film_rentals_count_ddf.columns = ['film_id', 'Rental Count']

    film_details_ddf = film_ddf[['film_id', 'title']].merge(film_category_ddf[['film_id', 'category_id']], on='film_id')
    film_details_ddf = film_details_ddf.merge(category_ddf[['category_id', 'name']], on='category_id')
    film_details_ddf = film_details_ddf.merge(film_rentals_count_ddf, on='film_id')

    top_10_film_details = []
    for category in top_10_categories_ddf['Category']:
        category_films_ddf = film_details_ddf[film_details_ddf['name'] == category]
        top_films = category_films_ddf.sort_values(by='Rental Count', ascending=False).head(10)
        top_10_film_details.append(top_films)

    top_10_film_details_ddf = pd.concat(top_10_film_details)
    st.subheader("Top 10 En Çok Kiralanan Türlerin Filmleri")
    st.dataframe(top_10_film_details_ddf)
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)



#-----------------------------------------------------------------------------------

    rental_ddf = dd.from_pandas(rental_df, npartitions=1)
    film_ddf = dd.from_pandas(film_df, npartitions=1)
    inventory_ddf = dd.from_pandas(inventory_df, npartitions=1)

    rental_inventory_ddf = rental_ddf.merge(inventory_ddf, on='inventory_id', suffixes=('_rental', '_inventory'))
    rental_film_ddf = rental_inventory_ddf.merge(film_ddf, on='film_id', suffixes=('_inventory', '_film'))

    film_rentals_count_ddf = rental_film_ddf['film_id'].value_counts().reset_index()
    film_rentals_count_ddf.columns = ['film_id', 'rental_count']

    top_rented_films_ddf = film_rentals_count_ddf.head(10)['film_id']

    top_rented_films_ddf = rental_film_ddf[rental_film_ddf['film_id'].isin(top_rented_films_ddf)]

    average_rental_duration_ddf = top_rented_films_ddf.groupby('film_id')['rental_duration'].mean().reset_index()
    average_rental_duration_ddf.columns = ['film_id', 'average_rental_duration (days)']

    film_titles_ddf = rental_film_ddf[['film_id', 'title']].drop_duplicates().set_index('film_id')
    average_rental_duration_with_titles_ddf = average_rental_duration_ddf.merge(film_titles_ddf, on='film_id')

    st.title("9-En Çok Kiralanan Filmlerin Ortalama Kiralama Süresi")

    st.subheader("Ortalama Kiralama Süresi (Gün)")
    st.dataframe(average_rental_duration_with_titles_ddf[['title', 'average_rental_duration (days)']].compute())
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)


#-----------------------------------------------------------------------------------

    st.title("10-En Çok Filmde Rol Alan Aktörler")     
    film_actor_df = pd.read_sql_query("SELECT * FROM film_actor;", con)
    actor_df = pd.read_sql_query("SELECT * FROM actor;", con)

    film_actor_ddf = dd.from_pandas(film_actor_df, npartitions=1)
    actor_ddf = dd.from_pandas(actor_df, npartitions=1)

    actor_film_count_ddf = film_actor_ddf['actor_id'].value_counts().reset_index()
    actor_film_count_ddf.columns = ['actor_id', 'film_count']

    actor_film_count_ddf = actor_film_count_ddf.merge(actor_ddf, on='actor_id', suffixes=('_film', '_actor'))

    top_actors_ddf = actor_film_count_ddf.nlargest(10, 'film_count')

   

    st.subheader("Top 10 Aktör ve Rol Aldıkları Film Sayısı")
    st.dataframe(top_actors_ddf[['first_name', 'last_name', 'film_count']].compute())
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)




#-----------------------------------------------------------------------------------
    st.title("11-En Fazla Aktör Oynayan Kategoriler")     

    film_actor_ddf = dd.from_pandas(film_actor_df, npartitions=1)
    film_category_ddf = dd.from_pandas(film_category_df, npartitions=1)
    category_ddf = dd.from_pandas(category_df, npartitions=1)

    film_actor_category_ddf = film_actor_ddf.merge(film_category_ddf, on='film_id')
    film_actor_category_ddf = film_actor_category_ddf.merge(category_ddf, on='category_id')

    category_actor_count_ddf = film_actor_category_ddf.groupby('name')['actor_id'].nunique().reset_index()
    category_actor_count_ddf.columns = ['category_name', 'actor_count']

    top_categories_ddf = category_actor_count_ddf.nlargest(10, 'actor_count')


    st.dataframe(top_categories_ddf[['category_name', 'actor_count']].compute())
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)



    #-----------------------------------------------------------------------------------

    rental_ddf = dd.from_pandas(rental_df, npartitions=1)
    inventory_ddf = dd.from_pandas(inventory_df, npartitions=1)
    store_ddf = dd.from_pandas(store_df, npartitions=1)

    rental_inventory_ddf = rental_ddf.merge(inventory_ddf, on='inventory_id')
    rental_store_ddf = rental_inventory_ddf.merge(store_ddf, on='store_id')

    store_rental_count_ddf = rental_store_ddf.groupby('store_id').size().reset_index()
    store_rental_count_ddf.columns = ['store_id', 'rental_count']  

    least_rented_store_ddf = store_rental_count_ddf.nsmallest(1, 'rental_count')

    st.title("12-En Az Sayıda Film Kiralayan Mağaza ve Toplam Kiralama Sayısı")
    st.subheader("En Az Kiralayan Mağaza")
    st.dataframe(least_rented_store_ddf.compute())
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)


#-----------------------------------------------------------------------------------


    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)
    customer_df = pd.read_sql_query("SELECT * FROM customer;", con)

    rental_ddf = dd.from_pandas(rental_df, npartitions=1)
    customer_ddf = dd.from_pandas(customer_df, npartitions=1)

    rental_customer_ddf = rental_ddf.merge(customer_ddf, on='customer_id')

    customer_rental_count_ddf = rental_customer_ddf.groupby('customer_id').size().reset_index()
    customer_rental_count_ddf.columns = ['customer_id', 'rental_count']  # Sütun adlarını manuel olarak ayarlayın

    customer_rental_info_ddf = customer_rental_count_ddf.merge(customer_ddf[['customer_id', 'first_name', 'last_name']], on='customer_id')

    most_rented_customer_ddf = customer_rental_info_ddf.nlargest(1, 'rental_count')

    st.title("13-En Fazla Sayıda Kiralama Yapan Müşteri ve Toplam Kiralama Sayısı")
    st.subheader("En Fazla Kiralayan Müşteri")
    st.dataframe(most_rented_customer_ddf.compute())
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)


#-----------------------------------------------------------------------------------

    rental_ddf = dd.from_pandas(rental_df, npartitions=1)
    customer_ddf = dd.from_pandas(customer_df, npartitions=1)

    rental_customer_ddf = rental_ddf.merge(customer_ddf, on='customer_id')

    customer_rental_count_ddf = rental_customer_ddf.groupby('customer_id').size().reset_index()
    customer_rental_count_ddf.columns = ['customer_id', 'rental_count']

    customer_rental_info_ddf = customer_rental_count_ddf.merge(customer_ddf[['customer_id', 'first_name', 'last_name']], on='customer_id')

    least_rented_customer_ddf = customer_rental_info_ddf.nsmallest(1, 'rental_count')

    st.title("14-En Az Sayıda Kiralama Yapan Müşteri ve Toplam Kiralama Sayısı")
    st.subheader("En Az Kiralayan Müşteri")
    st.dataframe(least_rented_customer_ddf.compute())
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)


#-----------------------------------------------------------------------------------

  
    rental_ddf = dd.from_pandas(rental_df, npartitions=1)
    film_ddf = dd.from_pandas(film_df, npartitions=1)

    rental_inventory_ddf = rental_ddf.merge(inventory_ddf, on='inventory_id')
    rental_film_ddf = rental_inventory_ddf.merge(film_ddf, on='film_id')

    film_rental_count_ddf = rental_film_ddf.groupby('film_id').size().reset_index()
    film_rental_count_ddf.columns = ['film_id', 'rental_count']

    most_rented_film_ddf = film_rental_count_ddf.nlargest(1, 'rental_count')

    film_titles_ddf = film_df[['film_id', 'title']].set_index('film_id')
    most_rented_film_with_title_ddf = most_rented_film_ddf.merge(film_titles_ddf, on='film_id')

    st.title("15-En Fazla Sayıda Kiralanan Film ve Toplam Kiralama Sayısı")
    st.subheader("En Fazla Kiralanan Film")
    st.dataframe(most_rented_film_with_title_ddf.compute())
    st.markdown("<hr style='border: 1px solid yellow;'/>", unsafe_allow_html=True)


    con.close()

