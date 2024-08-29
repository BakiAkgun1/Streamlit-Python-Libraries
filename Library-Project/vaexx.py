import streamlit as st  # type: ignore
import sqlite3
import vaex as vx # type: ignore
import pandas as pd # type: ignore
from module import load_data, sidebar1

def vaex1():
    html_temp = """
    <div style="background-color:black;padding:1.5px">
    <h1 style="color:white;text-align:center;">VAEX</h1>
    </div><br>"""
    st.markdown(html_temp, unsafe_allow_html=True)
    st.markdown('<style>h1{color: black;}</style>', unsafe_allow_html=True)
    st.image("photo/vaex1.png", use_column_width=True)

    db_path = r"C:\Users\Baki Akgun\OneDrive\Masaüstü\Library_project\sqlite_sakila.db"
    con = sqlite3.connect(db_path)

    
    dask_dataframes = load_data(db_path)
    sidebar1(dask_dataframes)



#-----------------------------------------------------------------------------------
    st.title("1-Bazı tabloların ilk 10 Verisi")

    staff_df = pd.read_sql_query('SELECT * FROM staff;', con)
    rental_df = pd.read_sql_query('SELECT * FROM rental;', con)
    inventory_df = pd.read_sql_query('SELECT * FROM inventory;', con)
    film_df = pd.read_sql_query('SELECT * FROM film;', con)


    staff_vx = vx.from_pandas(staff_df)
    rental_vx = vx.from_pandas(rental_df)
    inventory_vx = vx.from_pandas(inventory_df)
    film_vx = vx.from_pandas(film_df)

    staff_head = staff_vx.head(10)
    rental_head = rental_vx.head(10)
    inventory_head = inventory_vx.head(10)
    film_head = film_vx.head(10)

    st.subheader("Staff Verisi")
    st.write(staff_head.to_pandas_df())

    st.subheader("İlk 10 Rental Verisi")
    st.write(rental_head.to_pandas_df())

    st.subheader("İlk 10 Inventory Verisi")
    st.write(inventory_head.to_pandas_df())

    st.subheader("İlk 10 Film Verisi")
    st.write(film_head.to_pandas_df())
    st.markdown("<hr style='border: 2px solid green;'/>", unsafe_allow_html=True)


#-----------------------------------------------------------------------------------
    st.title("2-Birden Fazla DVD Teslim Etmeyen Müşteriler")

    rental_df = pd.read_sql_query('SELECT * FROM rental;', con)
    customer_df = pd.read_sql_query('SELECT * FROM customer;', con)

    rental_vx = vx.from_pandas(rental_df)
    customer_vx = vx.from_pandas(customer_df)
    not_returned = rental_vx[rental_vx['return_date'].isna()]

    customer_not_returned = not_returned.groupby('customer_id', agg={'count': vx.agg.count()})

    multiple_not_returned_customers = customer_not_returned[customer_not_returned['count'] > 1]['customer_id'].tolist()

    customers_info = customer_vx[customer_vx['customer_id'].isin(multiple_not_returned_customers)]

    st.write("Birden Fazla DVD'yi Teslim Etmeyen Müşteriler:")
    st.write(customers_info.to_pandas_df())
    st.markdown("<hr style='border: 2px solid green;'/>", unsafe_allow_html=True)

#-----------------------------------------------------------------------------------
    st.title("3. En Çok DVD Kiralayan Müşteri")

    rental_df = pd.read_sql_query('SELECT * FROM rental;', con)
    customer_df = pd.read_sql_query('SELECT * FROM customer;', con)

    rental_vx = vx.from_pandas(rental_df)
    customer_vx = vx.from_pandas(customer_df)

    customer_rentals = rental_vx.groupby(by='customer_id', agg={'rental_count': vx.agg.count()})

    most_rentals_customer = customer_rentals.sort(by='rental_count', ascending=False).head(1)
    most_rentals_customer_id = most_rentals_customer['customer_id'].values[0]

    customer_info = customer_vx[customer_vx['customer_id'] == most_rentals_customer_id].to_pandas_df()

    st.subheader("En çok DVD kiralayan müşteri:")
    st.write(customer_info)
    st.markdown("<hr style='border: 2px solid green;'/>", unsafe_allow_html=True)

#-----------------------------------------------------------------------------------
    st.title("4. En Popüler Film Kategorisi")
    category_df = pd.read_sql_query("SELECT * FROM category;", con)
    film_category_df = pd.read_sql_query("SELECT * FROM film_category;", con)

    film_vdf = vx.from_pandas(film_df)
    category_vdf = vx.from_pandas(category_df)
    film_category_vdf = vx.from_pandas(film_category_df)

    merged_vdf = film_category_vdf.join(category_vdf, on='category_id', rsuffix='_cat')

    final_vdf = merged_vdf.join(film_vdf, on='film_id', rsuffix='_film')

    final_vdf['rental_count'] = final_vdf.groupby(by='name', agg={'film_id': 'count'})['film_id']

    most_popular_category = final_vdf.groupby('name', agg={'rental_count': 'sum'}).sort('rental_count', ascending=False).head(1)

    st.write("En Popüler Kategori:")
    st.write(most_popular_category[['name', 'rental_count']].to_pandas_df())
    st.markdown("<hr style='border: 2px solid green;'/>", unsafe_allow_html=True)
    #----------------------------------------------------------


    st.title("5. En çok geliri hangi film getirmiş?")

    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)
    payment_df = pd.read_sql_query("SELECT * FROM payment;", con)
    inventory_df = pd.read_sql_query("SELECT * FROM inventory;", con)

    rental_vaex_df = vx.from_pandas(rental_df)
    payment_vaex_df = vx.from_pandas(payment_df)
    inventory_vaex_df = vx.from_pandas(inventory_df)
    film_vaex_df = vx.from_pandas(film_df)

    final_df = rental_vaex_df.join(inventory_vaex_df, on='inventory_id', rsuffix='_inv')
    final_df = final_df.join(film_vaex_df, on='film_id', rsuffix='_film')
    final_df = final_df.join(payment_vaex_df, on='rental_id', rsuffix='_pay')

    total_revenue_df = final_df.groupby('title', agg={'total_revenue': vx.agg.sum('amount')})

    most_revenue_film = total_revenue_df.sort('total_revenue', ascending=False).head(1)

    st.write("En Çok Geliri Getiren Film:")
    st.write(most_revenue_film.to_pandas_df())

    st.markdown("<hr style='border: 2px solid green;'/>", unsafe_allow_html=True)

   #--------------------------------------------------------------------
    st.title("6-Her Türde En Az Kiralanan Film")

    inventory_vdf = vx.from_pandas(inventory_df)
    film_vdf = vx.from_pandas(film_df)
    category_vdf = vx.from_pandas(category_df)
    film_category_vdf = vx.from_pandas(film_category_df)
    rental_vdf = vx.from_pandas(rental_df)

    rental_film_category_vdf = rental_vdf.join(inventory_vdf, on='inventory_id', rsuffix='_inventory')
    rental_film_category_vdf = rental_film_category_vdf.join(film_vdf, on='film_id', rsuffix='_film')
    rental_film_category_vdf = rental_film_category_vdf.join(film_category_vdf, on='film_id', rsuffix='_filmid')
    rental_film_category_vdf = rental_film_category_vdf.join(category_vdf, on='category_id', rsuffix='_category')

    film_rentals_vdf = rental_film_category_vdf.groupby(by=['name', 'title'], agg={'rental_count': vx.agg.count()})

    min_rentals_vdf = film_rentals_vdf.groupby(by='name', agg={'min_rentals': vx.agg.min('rental_count')})

    least_rented_film_vdf = film_rentals_vdf.join(min_rentals_vdf, on='name')
    least_rented_film_vdf = least_rented_film_vdf[least_rented_film_vdf['rental_count'] == least_rented_film_vdf['min_rentals']]

    st.write("Her Türde En Az Kiralanan Film:")
    st.dataframe(least_rented_film_vdf.to_pandas_df())
    st.markdown("<hr style='border: 2px solid green;'/>", unsafe_allow_html=True)
   #--------------------------------------------------------------------
    st.title("7-En Çok Kiralanan Film Hangi Mağazada Kiralanmış?")


    store_df = pd.read_sql_query("SELECT * FROM store;", con)

    rental_vdf = vx.from_pandas(rental_df)
    inventory_vdf = vx.from_pandas(inventory_df)
    store_vdf = vx.from_pandas(store_df)
    film_vdf = vx.from_pandas(film_df)

    rental_inventory_vdf = rental_vdf.join(inventory_vdf, on='inventory_id', rsuffix='_inventory')
    rental_inventory_film_vdf = rental_inventory_vdf.join(film_vdf, on='film_id', rsuffix='_film')
    rental_inventory_film_store_vdf = rental_inventory_film_vdf.join(store_vdf, on='store_id', rsuffix='_store')

    film_rentals_vdf = rental_inventory_film_vdf.groupby(by='film_id', agg={'rental_count': vx.agg.count()})
    most_rented_film_id = film_rentals_vdf.sort(by='rental_count', ascending=False)['film_id'].values[0]

    most_rented_film_stores_vdf = rental_inventory_film_store_vdf[rental_inventory_film_store_vdf['film_id'] == most_rented_film_id]

    result_vdf = most_rented_film_stores_vdf[['film_id', 'store_id']].head(1)
    result_vdf = result_vdf.join(film_vdf[['film_id', 'title']], on='film_id', rsuffix='_film')

    st.dataframe(result_vdf[['title', 'store_id']].to_pandas_df())
    st.markdown("<hr style='border: 1px solid green;'/>", unsafe_allow_html=True)
#--------------------------------------------------------------------

    st.title("8 - En Çok Kiralanan Türler ve Filmleri")


    rental_inventory_vdf = rental_vdf.join(inventory_vdf, on='inventory_id', how='left', rsuffix='_inventory')
    rental_film_vdf = rental_inventory_vdf.join(film_vdf, on='film_id', how='left', rsuffix='_film')
    film_category_vdf = film_category_vdf.join(category_vdf, on='category_id', how='left', rsuffix='_category')
    rental_film_category_vdf = rental_film_vdf.join(film_category_vdf, on='film_id', how='left', rsuffix='_category')

    category_rentals_vdf = rental_film_category_vdf.groupby(by='name', agg={'rental_count': vx.agg.count()})

    top_10_categories_vdf = category_rentals_vdf.sort(by='rental_count', ascending=False).head(10)

    st.subheader("Top 10 En Çok Kiralanan Türler")
    st.dataframe(top_10_categories_vdf.to_pandas_df())

    film_rentals_vdf = rental_film_category_vdf.groupby(by='film_id', agg={'rental_count': vx.agg.count()})

    film_details_vdf = film_vdf[['film_id', 'title']].join(film_category_vdf[['film_id', 'category_id']], on='film_id', how='left')
    film_details_vdf = film_details_vdf.join(category_vdf[['category_id', 'name']], on='category_id', how='left')
    film_details_vdf = film_details_vdf.join(film_rentals_vdf, on='film_id', how='left')

    top_10_film_details_vdf = []
    for category in top_10_categories_vdf['name'].values:
        category_films_vdf = film_details_vdf[film_details_vdf['name'] == category]
        top_films_vdf = category_films_vdf.sort(by='rental_count', ascending=False).head(10)
        top_10_film_details_vdf.append(top_films_vdf)

    top_10_film_details_vdf = vx.concat(top_10_film_details_vdf)

    st.subheader("Top 10 En Çok Kiralanan Türlerin Filmleri")
    st.dataframe(top_10_film_details_vdf.to_pandas_df())
    st.markdown("<hr style='border: 1px solid green;'/>", unsafe_allow_html=True)


#--------------------------------------------------------------------

    film_df = pd.read_sql_query("SELECT * FROM film;", con)
    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)

    film_vaex_df = vx.from_pandas(film_df)
    rental_vaex_df = vx.from_pandas(rental_df)

    rental_inventory_vdf = rental_vdf.join(inventory_vdf, on='inventory_id', rsuffix='_inventory')
    rental_inventory_film_vdf = rental_inventory_vdf.join(film_vdf, on='film_id', rsuffix='_film')

    film_rentals_vdf = rental_inventory_film_vdf.groupby(by='film_id', agg={
        'rental_count': vx.agg.count(),
        'average_rental_duration': vx.agg.mean('rental_duration')
    })

    sorted_film_rentals_vdf = film_rentals_vdf.sort(by='rental_count', ascending=False)

    top_10_films_vdf = sorted_film_rentals_vdf.head(10)

    result_vdf = top_10_films_vdf.join(film_vaex_df[['film_id', 'title']], on='film_id', how='left')

    st.title("9-En Çok Kiralanan Filmlerin Ortalama Kiralama Süresi")   
    st.dataframe(result_vdf[['title', 'rental_count', 'average_rental_duration']].to_pandas_df())
    st.markdown("<hr style='border: 1px solid green;'/>", unsafe_allow_html=True)

#    #--------------------------------------------------------------------
#    
    st.title("10 - En Çok Filmde Rol Alan Aktörler")

    film_actor_df = pd.read_sql_query("SELECT * FROM film_actor;", con)
    actor_df = pd.read_sql_query("SELECT * FROM actor;", con)

    film_actor_vdf = vx.from_pandas(film_actor_df)
    actor_vdf = vx.from_pandas(actor_df)

    actor_film_count_vdf = film_actor_vdf.groupby(by='actor_id', agg={'film_count': vx.agg.count()})

    actor_film_count_vdf = actor_film_count_vdf.join(actor_vdf, on='actor_id', how='left')

    top_actors_vdf = actor_film_count_vdf.sort(by='film_count', ascending=False).head(10)

    st.subheader("Top 10 Aktör ve Rol Aldıkları Film Sayısı")
    st.dataframe(top_actors_vdf[['first_name', 'last_name', 'film_count']].to_pandas_df())
    st.markdown("<hr style='border: 1px solid green;'/>", unsafe_allow_html=True)
        
#    #--------------------------------------------------------------------

    st.title("11 - En Fazla Aktör Oynayan Kategoriler")

    film_actor_df = pd.read_sql_query("SELECT * FROM film_actor;", con)
    film_category_df = pd.read_sql_query("SELECT * FROM film_category;", con)
    category_df = pd.read_sql_query("SELECT * FROM category;", con)

    film_actor_vdf = vx.from_pandas(film_actor_df)
    film_category_vdf = vx.from_pandas(film_category_df)
    category_vdf = vx.from_pandas(category_df)

    film_actor_category_vdf = film_actor_vdf.join(film_category_vdf, on='film_id', rsuffix='_film_category')
    film_actor_category_vdf = film_actor_category_vdf.join(category_vdf, on='category_id', rsuffix='_category')

    category_actor_count_vdf = film_actor_category_vdf.groupby(by='name', agg={'actor_count': vx.agg.nunique('actor_id')})

    top_categories_vdf = category_actor_count_vdf.sort(by='actor_count', ascending=False).head(10)

    st.subheader("Top 10 En Fazla Aktör Oynayan Kategori")
    st.dataframe(top_categories_vdf[['name', 'actor_count']].to_pandas_df())
    

 #--------------------------------------------------------------------
    st.title("12 - En Az Sayıda Film Kiralayan Mağaza ve Toplam Kiralama Sayısı")

    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)
    inventory_df = pd.read_sql_query("SELECT * FROM inventory;", con)
    store_df = pd.read_sql_query("SELECT * FROM store;", con)

    rental_vdf = vx.from_pandas(rental_df)
    inventory_vdf = vx.from_pandas(inventory_df)
    store_vdf = vx.from_pandas(store_df)

    rental_inventory_vdf = rental_vdf.join(inventory_vdf, on='inventory_id', rsuffix='_inventory')
    rental_store_vdf = rental_inventory_vdf.join(store_vdf, on='store_id', rsuffix='_store')

    store_rental_count_vdf = rental_store_vdf.groupby(by='store_id', agg={'rental_count': vx.agg.count()})

    least_rented_store_vdf = store_rental_count_vdf.sort(by='rental_count', ascending=True).head(1)

    st.subheader("En Az Kiralayan Mağaza")
    st.dataframe(least_rented_store_vdf[['store_id', 'rental_count']].to_pandas_df())
    st.markdown("<hr style='border: 1px solid green;'/>", unsafe_allow_html=True)
#--------------------------------------------------------------------

    st.title("13 - En Fazla Sayıda Kiralama Yapan Müşteri ve Toplam Kiralama Sayısı")

    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)
    customer_df = pd.read_sql_query("SELECT * FROM customer;", con)

    rental_vdf = vx.from_pandas(rental_df)
    customer_vdf = vx.from_pandas(customer_df)

    rental_customer_vdf = rental_vdf.join(customer_vdf, on='customer_id', rsuffix='_customer')

    customer_rental_count_vdf = rental_customer_vdf.groupby(by='customer_id', agg={'rental_count': vx.agg.count()})

    customer_rental_info_vdf = customer_rental_count_vdf.join(customer_vdf, on='customer_id', rsuffix='_info')

    most_rented_customer_vdf = customer_rental_info_vdf.sort(by='rental_count', ascending=False).head(1)

    st.subheader("En Fazla Kiralayan Müşteri")
    st.dataframe(most_rented_customer_vdf[['first_name', 'last_name', 'rental_count']].to_pandas_df())
    st.markdown("<hr style='border: 1px solid green;'/>", unsafe_allow_html=True)

#--------------------------------------------------------------------

    st.title("14 - En Az Sayıda Kiralama Yapan Müşteri ve Toplam Kiralama Sayısı")

    least_rented_customer_vdf = customer_rental_info_vdf.sort(by='rental_count', ascending=True).head(1)

    st.subheader("En Az Kiralayan Müşteri")
    st.dataframe(least_rented_customer_vdf[['first_name', 'last_name', 'rental_count']].to_pandas_df())
    st.markdown("<hr style='border: 1px solid green;'/>", unsafe_allow_html=True)
#--------------------------------------------------------------------

    st.title("15 - En Fazla Sayıda Kiralanan Film ve Toplam Kiralama Sayısı")

    film_df = pd.read_sql_query("SELECT * FROM film;", con)
    rental_df = pd.read_sql_query("SELECT * FROM rental;", con)

    film_vdf = vx.from_pandas(film_df)
    rental_vdf = vx.from_pandas(rental_df)

    rental_inventory_vdf = rental_vdf.join(inventory_vdf, on='inventory_id', rsuffix='_inventory')
    rental_film_vdf = rental_inventory_vdf.join(film_vdf, on='film_id', rsuffix='_film')

    film_rental_count_vdf = rental_film_vdf.groupby(by='film_id', agg={'rental_count': vx.agg.count()})

    most_rented_film_vdf = film_rental_count_vdf.sort(by='rental_count', ascending=False).head(1)

    film_titles_vdf = film_vdf[['film_id', 'title']]
    most_rented_film_with_title_vdf = most_rented_film_vdf.join(film_titles_vdf, on='film_id', rsuffix='_title')

    st.subheader("En Fazla Kiralanan Film")
    st.dataframe(most_rented_film_with_title_vdf[['title', 'rental_count']].to_pandas_df())
    st.markdown("<hr style='border: 1px solid green;'/>", unsafe_allow_html=True)


    con.close()











    con.close()
