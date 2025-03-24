from pyspark.sql import SparkSession

# 🔌 Pokretanje SparkSession s JDBC konekcijom
spark = SparkSession.builder \
    .appName("ETL Extract - MySQL") \
    .config("spark.jars", "Connectors/mysql-connector-j-9.2.0.jar") \
    .getOrCreate()

# 📋 Parametri konekcije
jdbc_url = "jdbc:mysql://127.0.0.1:3306/dw?useSSL=false"
connection_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# 🛠️ SQL upit
query = """
(
SELECT cy.name AS retailer_country,
       od.name AS order_method_type,
       re.name AS retailer_type,
       pl.name AS product_line,
       pe.name AS product_type,
       pt.name AS product,
       ss.year AS year,
       ss.quarter AS quarter,
       ss.revenue AS revenue,
       ss.quantity AS quantity,
       ss.gross_margin AS gross_margin
  FROM product pt
  JOIN product_type pe ON pt.product_type_fk = pe.id
  JOIN product_line pl ON pe.product_line_fk = pl.id
  JOIN sales ss ON ss.product_fk = pt.id
  JOIN order_method od ON ss.order_method_fk = od.id
  JOIN retailer_type re ON ss.retailer_type_fk = re.id
  JOIN country cy ON ss.country_fk = cy.id
) AS subquery
"""

# 🔄 Čitanje podataka u Spark DataFrame
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

# ✅ Provjera u terminalu
df.show()
df.printSchema()

# --------------------------------------------
# Extract podataka iz CSV datoteke
csv_df = spark.read.option("header", True).option("inferSchema", True).csv("2_relational_model/processed/WA_Sales_Products_2012-14_PROCESSED_20.csv")

# Show sample rows
csv_df.show()
csv_df.printSchema()

'''
OUTPUT:
+----------------+-----------------+----------------+--------------------+------------+-------------+----+-------+-------+--------+------------+
|retailer_country|order_method_type|   retailer_type|        product_line|product_type|      product|year|quarter|revenue|quantity|gross_margin|
+----------------+-----------------+----------------+--------------------+------------+-------------+----+-------+-------+--------+------------+
|          Canada|              Web|    Sports Store|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|11520.0|      72|      0.5375|
|          Mexico|              Web|       Golf Shop|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012| 4160.0|      26|    0.534875|
|          Mexico|              Web|Department Store|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|14720.0|      92|      0.5375|
|          Mexico|              Web|   Outdoors Shop|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|10400.0|      65|        0.36|
|          Mexico|              Web|   Eyewear Store|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|23040.0|     144|    0.518089|
|          Brazil|              Web|       Golf Shop|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012| 8000.0|      50|    0.533087|
|          Brazil|              Web|Department Store|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|42880.0|     268|    0.512036|
|          Brazil|              Web|   Outdoors Shop|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|40960.0|     256|    0.443896|
|          Brazil|              Web|    Sports Store|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|43680.0|     273|    0.455641|
|           Japan|              Web|Department Store|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|24480.0|     153|    0.470944|
|           Japan|              Web|   Outdoors Shop|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|86400.0|     540|    0.438767|
|           Japan|              Web|    Sports Store|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|89440.0|     559|    0.474421|
|       Singapore|              Web|       Golf Shop|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|15360.0|      96|    0.389583|
|       Singapore|              Web|Department Store|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|23680.0|     148|    0.433057|
|       Singapore|              Web|   Outdoors Shop|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|45600.0|     285|    0.421282|
|          Poland|              Web|Department Store|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|27200.0|     170|    0.409115|
|          Poland|              Web|    Sports Store|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012| 3200.0|      20|      0.5375|
|           China|              Web|       Golf Shop|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|26400.0|     165|    0.443909|
|           China|              Web|Department Store|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|22720.0|     142|        0.51|
|           China|              Web|   Outdoors Shop|Personal Accessories|  Binoculars|Ranger Vision|2012|Q2 2012|84800.0|     530|    0.446696|
+----------------+-----------------+----------------+--------------------+------------+-------------+----+-------+-------+--------+------------+
only showing top 20 rows

root
 |-- retailer_country: string (nullable = true)
 |-- order_method_type: string (nullable = true)
 |-- retailer_type: string (nullable = true)
 |-- product_line: string (nullable = true)
 |-- product_type: string (nullable = true)
 |-- product: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- quarter: string (nullable = true)
 |-- revenue: double (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- gross_margin: double (nullable = true)

+----------------+-----------------+--------------------+--------------------+-----------------+--------------------+----+-------+---------+--------+------------+
|retailer_country|order_method_type|       retailer_type|        product_line|     product_type|             product|year|quarter|  revenue|quantity|gross_margin|
+----------------+-----------------+--------------------+--------------------+-----------------+--------------------+----+-------+---------+--------+------------+
|     Netherlands|              Web|       Eyewear Store|Personal Accessories|          Watches|                  TX|2012|Q3 2012|  39784.0|     209|  0.45365976|
|           Spain|              Web|        Sports Store|Personal Accessories|          Eyewear|             Inferno|2014|Q1 2014|122453.75|    1799|  0.45001864|
|           Italy|              Web|       Eyewear Store|Personal Accessories|          Eyewear|           Polar Sun|2013|Q1 2013| 31635.38|     520|   0.5701648|
|          Poland|              Web|    Department Store|Personal Accessories|          Eyewear|           Polar Ice|2012|Q4 2012|  22253.0|     214|  0.52214713|
|          Brazil|              Web|        Sports Store|Personal Accessories|           Knives|           Max Gizmo|2013|Q3 2013|  32644.7|     807|  0.54797471|
|          Sweden|      Sales visit|        Sports Store|Personal Accessories|          Eyewear|           Polar Sun|2013|Q3 2013|   4019.6|      65|  0.57713454|
|          Brazil|              Web|       Outdoors Shop|Personal Accessories|       Binoculars|      Seeker Extreme|2012|Q3 2012|  7726.95|      45|  0.45186652|
|          France|              Web|    Department Store|Personal Accessories|          Eyewear|               Bella|2012|Q4 2012|  10575.0|     141|  0.41746667|
|           China|              Web|    Department Store|Personal Accessories|           Knives|        Pocket Gizmo|2012|Q4 2012|   5697.4|     467|  0.59344262|
|           Japan|              Web|        Sports Store|Personal Accessories|          Eyewear|             Cat Eye|2014|Q2 2014| 133732.9|    4542|   0.3390432|
|          Sweden|              Web|       Outdoors Shop|Mountaineering Eq...|             Rope|       Husky Rope 50|2014|Q2 2014|  29032.0|     191|  0.33611842|
|          Poland|        Telephone|        Sports Store|   Camping Equipment|     Cooking Gear|       TrailChef Cup|2012|Q2 2012|  5200.75|    1465|  0.76056338|
|       Australia|              Web|       Eyewear Store|Personal Accessories|          Eyewear|          Polar Wave|2014|Q3 2014|  3155.46|      33|  0.56996444|
|          Mexico|      Sales visit|     Warehouse Store|  Outdoor Protection|Insect Repellents|   BugShield Natural|2013|Q1 2013|  12888.0|    2148|        0.69|
|   United States|              Web|Equipment Rental ...|Mountaineering Eq...|           Safety|Granite Signal Mi...|2014|Q2 2014|  21549.0|     653|  0.52393939|
|         Finland|              Web|    Department Store|   Camping Equipment|    Sleeping Bags|      Hibernator Pad|2014|Q1 2014| 31933.68|    1192|  0.38596491|
|       Singapore|              Web|    Department Store|Personal Accessories|       Binoculars|           Seeker 35|2013|Q2 2013|110233.06|    1102|  0.28831351|
|       Singapore|              Web|    Department Store|      Golf Equipment| Golf Accessories|   Course Pro Gloves|2014|Q1 2014|   7969.5|     759|  0.75714286|
|          France|      Sales visit|       Outdoors Shop|Mountaineering Eq...|           Safety|       Husky Harness|2013|Q4 2013| 11053.25|     179|  0.29117409|
|           Japan|              Web|    Department Store|Personal Accessories|          Eyewear|             Cat Eye|2012|Q4 2012|  56675.5|    1546|  0.33195208|
+----------------+-----------------+--------------------+--------------------+-----------------+--------------------+----+-------+---------+--------+------------+
only showing top 20 rows

root
 |-- retailer_country: string (nullable = true)
 |-- order_method_type: string (nullable = true)
 |-- retailer_type: string (nullable = true)
 |-- product_line: string (nullable = true)
 |-- product_type: string (nullable = true)
 |-- product: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- quarter: string (nullable = true)
 |-- revenue: double (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- gross_margin: double (nullable = true)

'''