import streamlit as st # type: ignore
import base64


def render_anasayfa():   
    html_temp = """



    <div style="background-color:cyan;padding:1.5px">

    <h1 style="color:black;text-align:center;">PYTHON BIG DATA LIBRARIES</h1>

    </div><br>"""

    st.markdown(html_temp,unsafe_allow_html=True)
    st.markdown('<style>h1{color: purple;}</style>', unsafe_allow_html=True)

    st.image("photo/python.jpeg", caption='Python', use_column_width=True)



    
    st.title("1.Pandas")


    text_data = "Pandas, Python programlama dilinde kullanılan, veri manipülasyonu ve analizi için güçlü ve esnek bir kütüphanedir. Özellikle tablo şeklindeki verilerle (yani satırlar ve sütunlar halinde organize edilmiş veriler) çalışmak için geliştirilmiştir. Pandas, veri okuma, temizleme, işleme ve analiz etme gibi işlemleri kolaylaştırır."

    st.text_area("", value=text_data, height=150)

    st.markdown("""
        <h3 style="font-family: 'Times New Roman', serif; color: darkblue;">Özellikler</h3> 
        <ul style="font-family: 'Times New Roman', serif; font-size: 16px;">
            <li>Kolay veri işleme ve analiz.</li>
            <li>Veri gruplama ve istatistiksel analiz imkanı.</li>
            <li>Büyük veri setleriyle çalışabilir.</li>
        </ul>
                    <hr style="border: 1px solid black;"/> <!-- Çizgi ekleme -->

    """, unsafe_allow_html=True)

    st.image("photo/pandas.png", caption='Pandas', use_column_width=True)



    #----------------------------------------------------------------------------


    st.title("2.Dask")


    text_data = "Dask, Python'da büyük veri kümeleri üzerinde çalışmanıza olanak tanıyan bir paralel hesaplama kütüphanesidir. Dask, NumPy, Pandas ve scikit-learn gibi popüler Python kütüphaneleriyle uyumlu olup, bunların veri çerçeveleri ve dizileri üzerinde paralel işlemler yapabilmenizi sağlar."

    st.text_area("", value=text_data, height=150)

    st.markdown("""
        <h3 style="font-family: 'Times New Roman', serif; color: darkblue;">Özellikler</h3> 
        <ul style="font-family: 'Times New Roman', serif; font-size: 16px;">
            <li>Verileri bölümlere ayırarak paralel işlem yapar.</li>
            <li>Büyük veri kümeleri üzerinde çalışırken bellek kullanımını optimize eder.</li>
            <li>Dağıtık sistemlerde çalışabilir.</li>
        </ul>
                    <hr style="border: 1px solid black;"/> <!-- Çizgi ekleme -->

    """, unsafe_allow_html=True)

    st.image("photo/Dask_Logo.svg.png", caption='Dask', use_column_width=True)



    #----------------------------------------------------------------------------

    st.title("3.Vaex")


    text_data = "Vaex, özellikle büyük veri setleriyle hızlı ve verimli bir şekilde çalışmak için tasarlanmış hafif bir kütüphanedir. Verileri belleğe yüklemeden işleyebilir ve milyarlarca satırı saniyeler içinde analiz edebilir."

    st.text_area("", value=text_data, height=150)

    st.markdown("""
        <h3 style="font-family: 'Times New Roman', serif; color: darkblue;">Özellikler</h3>
        <ul style="font-family: 'Times New Roman', serif; font-size: 16px;">
            <li>    Bellek dışı veri işleme yeteneği.</li>
            <li>	Hızlı veri kesitleri, filtreleme ve özetleme.</li>
            <li>	Geniş veri kümeleri üzerinde işlem yaparken düşük bellek tüketimi.</li>
        </ul>
                    <hr style="border: 1px solid black;"/> <!-- Çizgi ekleme -->

    """, unsafe_allow_html=True)

    st.image("photo/vaex1.png", caption='Vaex', use_column_width=True)


    #-----------------------------------


    st.title("4.Polars")
    text_data = "Polars, çok hızlı ve verimli bir DataFrame kütüphanesidir. Rust programlama dilinde yazılmış olup, özellikle performans açısından Pandas'a bir alternatif olarak tasarlanmıştır."

    st.text_area("", value=text_data, height=150)

    st.markdown("""
        <h3 style="font-family: 'Times New Roman', serif; color: darkblue;">Özellikler</h3>
        <ul style="font-family: 'Times New Roman', serif; font-size: 16px;">
            <li>	Kolon bazlı veri işleme.</li>
            <li>	Paralel işlemlerle yüksek performans.</li>
            <li>	Düşük bellek kullanımı.</li>
        </ul>
                    <hr style="border: 1px solid black;"/> <!-- Çizgi ekleme -->

    """, unsafe_allow_html=True) 

    st.image("photo/polars.png", caption='Polars', use_column_width=True)

    #-----------------------------------
    print("/n")


    st.title("5.Modin")
    text_data = "Modin, Pandas kullanıcıları için aynı API'yi kullanarak büyük veri kümeleri üzerinde daha hızlı işlem yapma olanağı sunan bir kütüphanedir. Arka planda Dask veya Ray gibi motorlar kullanarak paralel işlem yapar."

    st.text_area("", value=text_data, height=150)

    st.markdown("""
        <h3 style="font-family: 'Times New Roman', serif; color: darkblue;">Özellikler</h3>
        <ul style="font-family: 'Times New Roman', serif; font-size: 16px;">
            <li>	Pandas ile uyumlu API.</li>
            <li>	Paralel ve dağıtık veri işleme.</li>
            <li>	Performans optimizasyonu.</li>
        </ul>
        <hr style="border: 1px solid black;"/> <!-- Çizgi ekleme -->
            
        
    """, unsafe_allow_html=True) 

    st.image("photo/modin.png", caption='Modin', use_column_width=True)

    #-----------------------------------
    print("/n")


    st.title("6.Koalas")
    text_data = "Koalas, Pandas ve Apache Spark'ın birlikte kullanılmasına olanak tanıyan bir kütüphanedir. Pandas API'sini Spark üzerine inşa ederek büyük veri setleri üzerinde veri analizi yapmanızı sağlar."

    st.text_area("", value=text_data, height=150)

    st.markdown("""
        <h3 style="font-family: 'Times New Roman', serif; color: darkblue;">Özellikler</h3>
        <ul style="font-family: 'Times New Roman', serif; font-size: 16px;">
            <li>	Pandas API'si ile büyük veri setlerinde işlem yapma.</li>
            <li>	    Spark'ın paralel ve dağıtık işlem yeteneklerinden yararlanma.</li>
            <li>	Pandas'tan Spark'a geçişi kolaylaştırır.</li>
        </ul>
                    <hr style="border: 1px solid black;"/> <!-- Çizgi ekleme -->
    """, unsafe_allow_html=True) 


    st.image("photo/koalas.png", caption='Koalas', use_column_width=True)
    st.markdown("<hr style='border: 1px solid black;'/>", unsafe_allow_html=True)

    #-----------------------------------
    print("/n")


    st.title("7.PySpark")
    text_data = "PySpark, Apache Spark'ın Python API'sidir. Büyük veri kümeleri üzerinde paralel ve dağıtık işlem yapabilmenizi sağlar. PySpark, veri işleme, makine öğrenimi ve veri analizi gibi işlemler için güçlü bir araçtır."

    st.text_area("", value=text_data, height=150)

    st.markdown("""
        <h3 style="font-family: 'Times New Roman', serif; color: darkblue;">Özellikler</h3>
        <ul style="font-family: 'Times New Roman', serif; font-size: 16px;">
            <li>	Dağıtık veri işleme.</li>
            <li>	Spark'ın güçlü veri işleme motoru.</li>
            <li>	Veri analizi ve makine öğrenimi için geniş bir araç seti.</li>
        </ul>
    """, unsafe_allow_html=True) 
    st.image("photo/pyspark.png", caption='PySpark', use_column_width=True)

    st.markdown("<hr style='border: 3px solid darkblue;'/>", unsafe_allow_html=True)

    print("/n")
    st.markdown("""
        <h1 style="font-family: 'Times New Roman', serif; color: red;">Karşılaştırma Tablosu</h1>
    
        </ul>
    """, unsafe_allow_html=True) 

    pdf_path = 'photo/Özellik.pdf'
    with open(pdf_path, "rb") as f:
        pdf_data = f.read()
        pdf_base64 = base64.b64encode(pdf_data).decode()

    # PDF'yi iframe içinde görüntüleme
    st.markdown(f'<iframe src="data:application/pdf;base64,{pdf_base64}" width="700" height="1000" type="application/pdf"></iframe>', unsafe_allow_html=True)

