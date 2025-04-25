# extract/extract_mysql.py
from spark_session import get_spark_session

def extract_table(table_name):
    spark = get_spark_session("ETL_App")

    jdbc_url = "jdbc:mysql://127.0.0.1:3306/dw?useSSL=false"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    return df

def extract_all_tables():
    return {
        "product": extract_table("product"),
        "product_type": extract_table("product_type"),
        "product_line": extract_table("product_line"),
        "sales": extract_table("sales"),
        "order_method": extract_table("order_method"),
        "retailer_type": extract_table("retailer_type"),
        "country": extract_table("country"),
    }