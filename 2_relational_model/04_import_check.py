import unittest
import pandas as pd
import sqlalchemy
from pandas.testing import assert_frame_equal

"""
4. Skripta za testiranje importa u bazu podataka (opcionalno, možete ručno provjeriti import u bazu podataka)

U ovom koraku testirmo import u bazu podataka. 
Skripta uspoređuje CSV datoteku s tablicama u bazi podataka.
Rade se dva testa:
1. Testiranje stupaca
2. Testiranje podataka
"""

class TestDatabase(unittest.TestCase):
    def setUp(self):
        # Spajanje na bazu podataka
        self.engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost:3306/dw')
        self.connection = self.engine.connect()

        # Učitavanje CSV datoteke
        self.df = pd.read_csv("2_relational_model/processed/WA_Sales_Products_2012-14_PROCESSED.csv")

        # Upit na bazu koji dohvaća sve podatke u tablicama u obliku dataframe-a
        query = """
        SELECT cy.name 'retailer_country'
        , od.name 'order_method_type'
        , re.name 'retailer_type'
        , pl.name 'product_line'
        , pe.name 'product_type'
        , pt.name 'product'
        , ss.year 'year'
        , ss.quarter 'quarter'
        , ss.revenue 'revenue'
        , ss.quantity 'quantity'
        , ss.gross_margin 'gross_margin'
        FROM product pt
        , product_type pe
        , product_line pl
        , sales ss
        , order_method od
        , retailer_type re
        , country cy
        WHERE pt.product_type_fk = pe.id
        AND pe.product_line_fk = pl.id
        AND ss.product_fk = pt.id
        AND ss.order_method_fk = od.id
        AND ss.retailer_type_fk = re.id
        AND ss.country_fk = cy.id
        ORDER BY ss.id ASC
        """
        result = self.connection.execute(query) # Izvršavanje upita
        self.db_df = pd.DataFrame(result.fetchall()) # Dohvaćanje rezultata upita
        self.db_df.columns = result.keys() # Dohvaćanje naziva stupaca

    # Testiranje stupaca
    def test_columns(self):
        self.assertListEqual(list(self.df.columns), list(self.db_df.columns))

    # Testiranje podataka
    def test_dataframes(self):
        self.df = self.df.reset_index(drop=True)
        self.db_df = self.db_df.reset_index(drop=True)
        assert_frame_equal(self.df, self.db_df)

    # Zatvaranje konekcije
    def tearDown(self):
        self.connection.close()

if __name__ == '__main__':
    unittest.main()

"""
OUTPUT:
----------------------------------------------------------------------
Ran 2 tests in 5.476s

OK
"""