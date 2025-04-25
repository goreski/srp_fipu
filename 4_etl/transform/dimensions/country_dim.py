import requests
import json
from pyspark.sql import Row
from pyspark.sql.functions import col, trim, initcap, lit, when, isnull, row_number, coalesce, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType
from spark_session import get_spark_session

def transform_country_dim(mysql_country_df, csv_country_df=None):
    spark = get_spark_session()

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_country_df
        .select(
            col("id").cast("long").alias("country_id"),
            initcap(trim(col("name"))).alias("name"),
            col("population").cast("long"),
            initcap(trim(col("region"))).alias("region")
        )
        .dropDuplicates(["name"])
    )

    # --- Step 2: Normalize CSV data ---
    if csv_country_df:
        csv_df = (
            csv_country_df
            .selectExpr("retailer_country as name")
            .withColumn("name", initcap(trim(col("name"))))
            .withColumn("country_id", lit(None).cast("long"))
            .withColumn("population", lit(None).cast("long"))
            .withColumn("region", lit(None).cast("string"))
            .dropDuplicates(["name"])
        )
        combined_df = mysql_df.unionByName(csv_df)
    else:
        combined_df = mysql_df

    combined_df = combined_df.dropDuplicates(["name"])

    # --- Step 3: Add missing population/region from API with cache ---
    missing = combined_df.filter(isnull("population") | isnull("region")) \
                         .select("name") \
                         .distinct() \
                         .collect()

    enriched_rows = []
    cache = {}

    for row in missing:
        name = row["name"]

        if name in cache:
            # Use cached value
            population, region = cache[name]
        else:
            try:
                response = requests.get(f"https://restcountries.com/v3.1/name/{name}?fullText=true", timeout=5)
                data = json.loads(response.content)
                if data and isinstance(data, list):
                    population = data[0].get("population")
                    region = data[0].get("region", "Unknown")
                else:
                    population, region = None, "Unknown"
            except:
                population, region = None, "Unknown"

            # Cache the result
            cache[name] = (population, region)

        enriched_rows.append(Row(name=name, population=population, region=region))

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("population", LongType(), True),
        StructField("region", StringType(), True)
    ])

    if enriched_rows:
        enriched_df = spark.createDataFrame(enriched_rows, schema=schema)
    else:
        # Force empty RDD explicitly (safe)
        empty_rdd = spark.sparkContext.emptyRDD()
        enriched_df = spark.createDataFrame(empty_rdd, schema=schema)

    # Left join and fill missing values using coalesce (cleaner than when/otherwise)
    combined_df = (
        combined_df.alias("base")
        .join(enriched_df.alias("api"), on="name", how="left")
        .select(
            col("base.country_id"),
            col("base.name").alias("name"),
            coalesce(col("base.population"), col("api.population")).alias("population"),
            coalesce(col("base.region"), col("api.region")).alias("region")
        )
    )

    # --- Step 4: Add surrogate key ---
    window = Window.orderBy("region", "name")

    final_df = (
        combined_df
        .withColumn("country_tk", row_number().over(window))
        .withColumn("version", lit(1))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit(None).cast("timestamp"))
        .select("country_tk", "version", "date_from", "date_to", "country_id", "name", "population", "region")
    )

    assert final_df.count() == 21, "Number of countries from step one of the project."
    return final_df
