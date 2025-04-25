from pyspark.sql.functions import col, trim, initcap, lit, rand, when, row_number, current_timestamp
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_retailer_dim(mysql_retailer_df, csv_retailer_df=None):
    spark = get_spark_session()

    # --- Step 1: Prepare MySQL lookup (retailer types) ---
    retailer_type_lookup = (
        mysql_retailer_df
        .select(
            col("id").cast("long").alias("retailer_id"),
            initcap(trim(col("name"))).alias("retailer_type"),
            col("speciality_store").cast("int")
        )
    )

    # --- Step 2: Prepare CSV with actual retailer names ---
    if csv_retailer_df:
        csv_df = (
            csv_retailer_df
            .selectExpr("retailer_type as name")  # 'retailer_type' in CSV is actual retailer name
            .withColumn("name", initcap(trim(col("name"))))
            .dropDuplicates(["name"])
        )

        # --- Step 3: Join with retailer type info from MySQL (optional match) ---
        retailer_df = (
            csv_df.alias("csv")
            .join(
                retailer_type_lookup.alias("db"),
                col("csv.name") == col("db.retailer_type"),  # match by type name
                how="left"
            )
            .select(
                col("db.retailer_id"),     # no ID for CSV rows
                col("csv.name").alias("name"),
                col("db.speciality_store")
            )
            .withColumn(
                "speciality_store",
                when(col("speciality_store").isNull(), when(rand() < 0.4, lit(1)).otherwise(lit(0)))
                .otherwise(col("speciality_store"))
            )
        )
    else:
        # Empty schema with correct structure if no CSV input
        retailer_df = spark.createDataFrame([], "retailer_id long, name string, speciality_store int")

    # --- Step 4: Add surrogate key ---
    window = Window.orderBy("name")
    final_df = (
        retailer_df
        .dropDuplicates(["name"])
        .withColumn("retailer_tk", row_number().over(window))
        .withColumn("version", lit(1))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit(None).cast("timestamp"))
        .select("retailer_tk", "version", "date_from", "date_to", "retailer_id", "name", "speciality_store")
    )

    assert final_df.count() == 8, "Number of retailers from step one of the project."
    return final_df
