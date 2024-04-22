import pandas as pd

"""
1. Skripta za predprocesiranje skupa podataka

Predprocesiranje ovisi o skupu podataka. Potrebno je prilagoditi skriptu za vlastiti skup.
Checkpoint 1 je obuhvaćao pronalazak skupa podataka i analizu. Ovdje je potrebno napraviti predprocesiranje na temelju analize.
U nastavku je prikazan primjer predprocesiranja skupa podataka naš case Oprema d.d.
"""

# Određivanje putanje do CSV datoteke
CSV_FILE_PATH = "data/WA_Sales_Products_2012-14.csv"

# Učitavanje CSV datoteke (provjerite svoje delimiter u csv datoteci), ispis broja redaka i stupaca
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print("CSV size before: ", df.shape)

df = df.drop(columns=['Product line']) # Brisanje nepotrebnih stupaca  (TODO: dodati Product line u kao još jednu hierarhiju u dimenziji)
df['Retailer country'] = df['Retailer country'].replace('USA', 'United States') # Zamjena vrijednosti zbog API-a koji pozivamo kasnije u drugom koraku
df['Retailer country'] = df['Retailer country'].replace('UK', 'United Kingdom') # Zamjena vrijednosti zbog API-a koji pozivamo kasnije u drugom koraku
df = df.dropna() # Brisanje redaka s nedostajućim vrijednostima
print("CSV size after: ", df.shape) # Ispis broja redaka i stupaca nakon predprocesiranja
print(df.head()) # Ispis prvih redaka dataframe-a

# Random dijeljenje skupa podataka na dva dijela 80:20 (trebati će nam kasnije)
df20 = df.sample(frac=0.2, random_state=1)
df = df.drop(df20.index)
print("CSV size 80: ", df.shape)
print("CSV size 20: ", df20.shape)

# Spremanje predprocesiranog skupa podataka u novu CSV datoteku
df.to_csv("2 Relational data model/processed/WA_Sales_Products_2012-14_PROCESSED.csv", index=False) # Spremanje predprocesiranog skupa podataka u novu CSV datoteku
df20.to_csv("2 Relational data model/processed/WA_Sales_Products_2012-14_PROCESSED_20.csv", index=False) # Spremanje 20% skupa podataka u novu CSV datoteku


'''
OUTPUT:
CSV size before:  (78475, 11)
CSV size after:  (77931, 10)
  Retailer country Order method type Retailer type Product type         Product  Year  Quarter   Revenue  Quantity  Gross margin
0           Canada               Web  Sports Store   Binoculars   Ranger Vision  2012  Q2 2012  11520.00        72      0.537500
1           Canada               Web  Sports Store   Navigation   Glacier Basic  2012  Q2 2012  13918.38       434      0.376364
2           Canada               Web  Sports Store   Navigation  Glacier Deluxe  2012  Q2 2012   8249.15        91      0.379702
3           Canada               Web  Sports Store   Navigation     Glacier GPS  2012  Q2 2012  20080.59       183      0.284152
4           Canada               Web  Sports Store   Navigation    Trail Master  2012  Q2 2012   1460.00         4      0.350822
CSV size 80:  (62345, 10)
CSV size 20:  (15586, 10)
'''