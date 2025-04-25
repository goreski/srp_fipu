import json
import requests
import pandas as pd
import numpy as np

df = pd.read_csv(r"G:\\My Drive\\UniPu\\~ Skladišta i rudarenje podataka\\Projekt\dw_case\\2 Relational data model\\processed\WA_Sales_Products_2012-14_PROCESSED_20.csv", delimiter=',')
unique_rows = df['Retailer country'].drop_duplicates()

country_names = unique_rows.values
region = list()
for name in country_names:
	print(name)
	response = requests.get("https://restcountries.com/v3.1/name/" + name + "?fullText=true")
	data = json.loads(response.content)
	region.append(data[0]['region'])

unique_rows = pd.DataFrame(unique_rows)
unique_rows['country'] = country_names
unique_rows['region'] = region

print(unique_rows)