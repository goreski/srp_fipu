/* Kreiranje tablica za dimenzijski model podataka
SQL naredbe su preuzimaju iz Dimension Lookup/Update i Table output (za tablicu činjenica) elementa u Pentaho Data Integration alatu
u detaljima elementa se nalazi SQL kod koji se koristi za kreiranje tablica
*/

-- Dimenzija za države
CREATE TABLE pentaho.dim_country
(
  country_tk BIGINT NOT NULL PRIMARY KEY
, version INT
, date_from DATETIME
, date_to DATETIME
, country_id INT
, country VARCHAR(45)
, region VARCHAR(45)
);
CREATE INDEX idx_dim_country_lookup ON pentaho.dim_country(country_id);
CREATE INDEX idx_dim_country_tk ON pentaho.dim_country(country_tk);


-- Dimenzija za proizvode
CREATE TABLE pentaho.dim_product
(
  product_tk BIGINT NOT NULL PRIMARY KEY
, version INT
, date_from DATETIME
, date_to DATETIME
, product_id INT
, product_name VARCHAR(33)
, product_type_name VARCHAR(20)
);
CREATE INDEX idx_dim_product_lookup ON pentaho.dim_product(product_id);
CREATE INDEX idx_dim_product_tk ON pentaho.dim_product(product_tk);


-- Dimenzija za tipove trgovaca
CREATE TABLE pentaho.dim_retailer
(
  retailer_tk BIGINT NOT NULL PRIMARY KEY
, version INT
, date_from DATETIME
, date_to DATETIME
, retailer_id INT
, name VARCHAR(45)
);
CREATE INDEX idx_dim_retailer_lookup ON pentaho.dim_retailer(retailer_id);
CREATE INDEX idx_dim_retailer_tk ON pentaho.dim_retailer(retailer_tk);


-- Tablica činjenica
CREATE TABLE pentaho.fact_sales
(
  fact_sales_tk BIGINT NOT NULL PRIMARY KEY
,  country_id BIGINT
, order_method TINYTEXT
, retailer_id BIGINT
, product_id BIGINT
, date_id INT
, revenue DOUBLE
, quantity BIGINT
, sales_tk INT
, FOREIGN KEY (country_id) REFERENCES pentaho.dim_country(country_tk)
, FOREIGN KEY (retailer_id) REFERENCES pentaho.dim_retailer(retailer_tk)
, FOREIGN KEY (product_id) REFERENCES pentaho.dim_product(product_tk)
, FOREIGN KEY (date_id) REFERENCES pentaho.dim_date(date_tk)
);

