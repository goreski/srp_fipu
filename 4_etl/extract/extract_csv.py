# extract/extract_csv.py
from spark_session import get_spark_session

def extract_from_csv(file_path):
    spark = get_spark_session("ETL Extract - CSV")
    df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
    return df