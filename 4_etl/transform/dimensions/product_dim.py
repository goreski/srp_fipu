from pyspark.sql.functions import col, lit, trim, initcap, monotonically_increasing_id, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def transform_product_dim(product_df, product_type_df, product_line_df, csv_sales_df=None):
    # Aliases
    p = product_df.alias("p")
    pt = product_type_df.alias("pt")
    pl = product_line_df.alias("pl")

    # Join DB tables
    merged_df = (
        p.join(pt, col("p.product_type_fk") == col("pt.id"), "left")
         .join(pl, col("pt.product_line_fk") == col("pl.id"), "left")
         .select(
             col("p.id").alias("product_id"),
             trim(col("p.name")).alias("product_name"),
             trim(col("pt.name")).alias("product_type_name"),
             trim(col("pl.name")).alias("product_line_name")
         )
          .withColumn("product_name", initcap(trim(col("product_name"))))
          .withColumn("product_type_name", initcap(trim(col("product_type_name"))))
          .withColumn("product_line_name", initcap(trim(col("product_line_name"))))
          .fillna({"product_type_name": "Unknown Type", "product_line_name": "Misc Line"})
    )

    # If CSV data exists
    if csv_sales_df:
        # Normalize CSV records (fill missing, clean up names)
        csv_product_df = (
            csv_sales_df
            .selectExpr("product as product_name", "product_type as product_type_name", "product_line as product_line_name")
            .withColumn("product_name", initcap(trim(col("product_name"))))
            .withColumn("product_type_name", initcap(trim(col("product_type_name"))))
            .withColumn("product_line_name", initcap(trim(col("product_line_name"))))
            .fillna({"product_type_name": "Unknown Type", "product_line_name": "Misc Line"})
            .dropDuplicates()
        )

        # Add a null `product_id` column to match schema
        csv_product_df = csv_product_df.withColumn("product_id", lit(None).cast("long"))

        # Align column order and union
        merged_df = merged_df.select("product_id", "product_name", "product_type_name", "product_line_name") \
                             .unionByName(csv_product_df) \
                             .dropDuplicates(["product_name", "product_type_name", "product_line_name"])

    # Add surrogate key using row_number
    window = Window.orderBy("product_line_name", "product_type_name", "product_name")
    merged_df = merged_df.withColumn("product_tk", row_number().over(window))

    # ✅ Add SCD Type 2 metadata
    merged_df = (
        merged_df
        .withColumn("version", lit(1))
        .withColumn("date_from", current_timestamp())
        .withColumn("date_to", lit(None).cast("timestamp"))
    )

    final_df = merged_df.select(
        "product_tk",
        "version",
        "date_from",
        "date_to",
        "product_id",
        "product_name",
        "product_type_name",
        "product_line_name"
    )

    assert final_df.count() == 143, "Number of products from step one of the project."
    
    return final_df
