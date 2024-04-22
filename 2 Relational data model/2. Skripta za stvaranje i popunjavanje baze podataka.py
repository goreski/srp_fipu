# Imports
import pandas as pd
import numpy as np
import json
import requests
import random
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

"""
2. Skripta za stvaranje sheme baze podataka i popunjavanje tablica

U ovom koraku potrebno je stvoriti shemu baze podataka i popuniti tablice podacima iz predprocesiranog skupa podataka.
Skripta koristi SQLAlchemy ORM za stvaranje sheme i popunjavanje tablica.
U našem primjeru izvodi se ~2 min.
"""

CSV_FILE_PATH = "2 Relational data model/processed/WA_Sales_Products_2012-14_PROCESSED.csv" # Putanja do predprocesiranog skupa podataka
df = pd.read_csv(CSV_FILE_PATH, delimiter=',') # Učitavanje predprocesiranog skupa podataka
print("CSV size: ", df.shape) # Ispis broja redaka i stupaca
print(df.head()) # Ispis prvih redaka dataframe-a


Base = declarative_base() # Stvaranje baze

# Definiranje sheme baze
#-----------------------------------------------------------------------------------------------------
# Tablica Country
class Country(Base):
    __tablename__ = 'country' # Naziv tablice
    id = Column(Integer, primary_key=True) # Primarni ključ
    name = Column(String(45), nullable=False, unique=True) # Stupac s nazivom države
    population = Column(Integer, nullable=False) # Stupac s populacijom države
    region = Column(String(45), nullable=False) # Stupac s regijom države

# Tablica OrderMethod
class OrderMethod(Base):
    __tablename__ = 'order_method'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)

# Tablica RetailerType
class RetailerType(Base):
    __tablename__ = 'retailer_type'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)
    speciality_store = Column(Integer, nullable=False)

# Tablica ProductType
class ProductType(Base):
    __tablename__ = 'product_type'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(75), nullable=False, unique=True)

# Tablica Product
class Product(Base):
    __tablename__ = 'product'
    id = Column(Integer, primary_key=True)
    name = Column(String(45))
    product_type_fk = Column(Integer, ForeignKey('product_type.id')) # Strani ključ za ProductType

# Tablica Sales
class Sales(Base):
    __tablename__ = 'sales'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    quarter = Column(String(45), nullable=False)
    revenue = Column(Float, nullable=False)
    quantity = Column(Integer, nullable=False)
    gross_margin = Column(Float, nullable=False)
    country_fk = Column(Integer, ForeignKey('country.id')) # Strani ključ za Country
    order_method_fk = Column(Integer, ForeignKey('order_method.id')) # Strani ključ za OrderMethod
    product_fk = Column(Integer, ForeignKey('product.id')) # Strani ključ za Product
    retailer_type_fk = Column(Integer, ForeignKey('retailer_type.id')) # Strani ključ za RetailerType

engine = create_engine('mysql+pymysql://root:root@localhost:3306/pentaho' , echo=False) # Stvaranje konekcije na bazu
Base.metadata.drop_all(engine) # Brisanje tablica ako već postoje, kako bi se izbjegli problemi s dupliciranjem podataka
Base.metadata.create_all(engine) # Stvaranje tablica

Session = sessionmaker(bind=engine) # Stvaranje sesije
session = Session() # Stvaranje instance sesije


# Popunjavanje tablica
# popunjavanje je specifično za svaki skup podataka, u pravilu nećete morati generirati svoje podatke već ćete koristiti podatke iz skupa
# -------------------------------------------------------------------------------------------------------
country_names = df['Retailer country'].unique().tolist() # Popis država iz skupa podataka (bez duplikata)   
for i, name in enumerate(country_names): # Iteracija kroz države
    response = requests.get("https://restcountries.com/v3.1/name/" + name + "?fullText=true") # Poziv REST API-a za svaku državu, ovo smo morali napraviti jer nam nedostaju podaci u našem skupu
    data = json.loads(response.content) # Parsiranje JSON odgovora
    country = Country(id=i+1, name=name, population=data[0]['population'], region=data[0]['region']) # Stvaranje instance Country
    session.add(country) # Dodavanje instance u sesiju
session.commit() # Commit nakon svih dodavanja

# Popunjavanje OrderMethod tablice
order_method_names = df['Order method type'].unique().tolist()
for i, name in enumerate(order_method_names):
    order_method = OrderMethod(id=i+1, name=name)
    session.add(order_method)
session.commit()

# Popunjavanje RetailerType tablice
retailer_type_names = df['Retailer type'].unique().tolist()
my_list = [0]*(len(retailer_type_names)-3) + [1]*3 
random.shuffle(my_list)
for i, name in enumerate(retailer_type_names):
    retailer_type = RetailerType(id=i+1, name=name, speciality_store=my_list[i])
    session.add(retailer_type)
session.commit()

# Popunjavanje ProductType i Product tablica
product_names = df['Product'].unique().tolist()
product_type_names = df['Product type'].unique().tolist()
for product_type in product_type_names:
    product_type_instance = ProductType(name=product_type)
    session.add(product_type_instance)
    session.commit()  # Commit za svaki ProductType

    for product in product_names:
        product_type_df = df['Product type'].loc[df['Product'] == product].unique()
        if product_type_df[0] == product_type:
            product_instance = Product(name=product, product_type_fk=product_type_instance.id)
            session.add(product_instance)
session.commit()  # commit za sve proizvode

# Popunjavanje Sales tablice
for i, row in df.iterrows():
    country = session.query(Country).filter_by(name=row['Retailer country']).first() # Dohvaćanje instance države
    product = session.query(Product).filter_by(name=row['Product']).first() # Dohvaćanje instance proizvoda
    order_method = session.query(OrderMethod).filter_by(name=row['Order method type']).first() # Dohvaćanje instance order metode
    retailer_type = session.query(RetailerType).filter_by(name=row['Retailer type']).first() # Dohvaćanje instance tipa trgovca
    sale = Sales(id=i+1, year=row['Year'], quarter=row['Quarter'], revenue=row['Revenue'], quantity=row['Quantity'], gross_margin=row['Gross margin'], country_fk=country.id, order_method_fk=order_method.id, product_fk=product.id, retailer_type_fk=retailer_type.id)
    session.add(sale)
session.commit()

"""
OUTPUT:
CSV size:  (62345, 10)
  Retailer country Order method type Retailer type Product type         Product  Year  Quarter   Revenue  Quantity  Gross margin
0           Canada               Web  Sports Store   Binoculars   Ranger Vision  2012  Q2 2012  11520.00        72      0.537500
1           Canada               Web  Sports Store   Navigation  Glacier Deluxe  2012  Q2 2012   8249.15        91      0.379702
2           Canada               Web  Sports Store   Navigation     Glacier GPS  2012  Q2 2012  20080.59       183      0.284152
3           Canada               Web  Sports Store   Navigation    Trail Master  2012  Q2 2012   1460.00         4      0.350822
4           Canada               Web  Sports Store   Navigation     Trail Scout  2012  Q2 2012  11424.00        48      0.347059
"""
