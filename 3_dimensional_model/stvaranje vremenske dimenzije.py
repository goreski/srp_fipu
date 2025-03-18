
import pandas as pd
from sqlalchemy import create_engine

# Database connection
user = 'root'
passw = 'root'
host =  'localhost' 
port = 3306 
database = 'pentaho'

mydb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database , echo=False)
print(mydb)
connection = mydb.connect()

dim_date_ddl = "CREATE TABLE pentaho.dim_date (date_tk INT NOT NULL AUTO_INCREMENT, year INT NOT NULL, quarter INT NOT NULL, PRIMARY KEY (date_tk), UNIQUE INDEX date_tk_UNIQUE (date_tk ASC));"
connection.execute(dim_date_ddl)

start_year = 2012
end_year = 2015
years, quarters = [], []
year_range = range(start_year, end_year)
for year in year_range:
  for i in range(1,5):
    years.append(year)
    quarters.append(i)
dim_date_data = pd.DataFrame({'date_tk':list(range(1,len(years)+1)), 'year':years, 'quarter':quarters})
dim_date_data.to_sql(con=mydb, name='dim_date', if_exists='append', index=False)