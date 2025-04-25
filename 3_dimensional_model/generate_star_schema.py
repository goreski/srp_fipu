'''
Skripta za generiranje dimenzijskog modela podataka -> star schema

U ovom koraku generiramo dimenzijski model podataka.
Dimenzijski model podataka je zvjezdasti model koji se sastoji od jedne tablice činjenica i više tablica dimenzija (data mart).
Ovom skriptom samo stvaramo shemu, popunjavanje ostavljamo za ETL proces.

U nastavku je prikazan primjer generiranja dimenzijskog modela podataka našeg case Oprema d.d.
'''

from sqlalchemy import create_engine, Column, Integer, BigInteger, String, DateTime, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the database connection
DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/dw"
engine = create_engine(DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Define Dimensional Model Tables
class DimCountry(Base):
    __tablename__ = 'dim_country'
    __table_args__ = {'schema': 'dw'}

    country_tk = Column(BigInteger, primary_key=True)
    version = Column(Integer)
    date_from = Column(DateTime)
    date_to = Column(DateTime)
    country_id = Column(Integer, index=True)
    name = Column(String(45))
    population = Column(Integer)
    region = Column(String(45))


class DimProduct(Base):
    __tablename__ = 'dim_product'
    __table_args__ = {'schema': 'dw'}

    product_tk = Column(BigInteger, primary_key=True)
    version = Column(Integer)
    date_from = Column(DateTime)
    date_to = Column(DateTime)
    product_id = Column(Integer, index=True)
    product_name = Column(String(256))
    product_type_name = Column(String(256))
    product_line_name = Column(String(256))


class DimRetailer(Base):
    __tablename__ = 'dim_retailer'
    __table_args__ = {'schema': 'dw'}

    retailer_tk = Column(BigInteger, primary_key=True)
    version = Column(Integer)
    date_from = Column(DateTime)
    date_to = Column(DateTime)
    retailer_id = Column(Integer, index=True)
    name = Column(String(256))
    speciality_store = Column(Integer)

class DimDate(Base):
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'dw'}

    date_tk = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)


class FactSales(Base):
    __tablename__ = 'fact_sales'
    __table_args__ = {'schema': 'dw'}

    fact_sales_tk = Column(BigInteger, primary_key=True)
    country_tk = Column(BigInteger, ForeignKey('dw.dim_country.country_tk'))
    order_method = Column(String(255))  # TINYTEXT converted to VARCHAR(255)
    retailer_tk = Column(BigInteger, ForeignKey('dw.dim_retailer.retailer_tk'))
    product_tk = Column(BigInteger, ForeignKey('dw.dim_product.product_tk'))
    date_tk = Column(Integer, ForeignKey('dw.dim_date.date_tk'))
    revenue = Column(Float)
    quantity = Column(BigInteger)

# Create Tables in the Database
Base.metadata.create_all(engine)

print("Dimensional model tables created successfully!")

