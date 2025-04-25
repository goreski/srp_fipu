from pyspark.sql.functions import col, trim, initcap, regexp_extract, row_number
from pyspark.sql.window import Window

def transform_sales_fact(
    raw_data,
    dim_product_df,
    dim_country_df,
    dim_retailer_df,
    dim_date_df  
):
    # Extract all raw tables
    sales_df = raw_data["sales"]
    product_df = raw_data["product"]
    product_type_df = raw_data["product_type"]
    product_line_df = raw_data["product_line"]
    order_method_df = raw_data["order_method"]
    retailer_type_df = raw_data["retailer_type"]
    country_df = raw_data["country"]
    csv_sales_df = raw_data.get("csv_sales")

    # Normalize MySQL sales
    enriched_mysql_sales = (
        sales_df.alias("s")
        .join(product_df.alias("pt"), col("s.product_fk") == col("pt.id"), "left")
        .join(product_type_df.alias("pe"), col("pt.product_type_fk") == col("pe.id"), "left")
        .join(product_line_df.alias("pl"), col("pe.product_line_fk") == col("pl.id"), "left")
        .join(order_method_df.alias("om"), col("s.order_method_fk") == col("om.id"), "left")
        .join(retailer_type_df.alias("re"), col("s.retailer_type_fk") == col("re.id"), "left")
        .join(country_df.alias("cy"), col("s.country_fk") == col("cy.id"), "left")
        .select(
            col("pt.name").alias("product_name"),
            col("cy.name").alias("country_name"),
            col("re.name").alias("retailer_name"),
            col("om.name").alias("order_method"),
            col("s.year").cast("int"),
            regexp_extract(trim(col("s.quarter")), r"Q([1-4])", 1).cast("int").alias("quarter"),
            col("s.quantity").cast("int"),
            col("s.revenue").cast("double")
        )
    ).withColumn("product_name", initcap(trim(col("product_name"))))

    # print("MySQL sales data count:", enriched_mysql_sales.count())

    # Normalize CSV sales (if any)
    if csv_sales_df:
        cleaned_csv_sales = (
            csv_sales_df
            .withColumn("product_name", initcap(trim(col("product"))))
            .withColumn("country_name", initcap(trim(col("retailer_country"))))
            .withColumn("retailer_name", initcap(trim(col("retailer_type"))))
            .withColumn("order_method", initcap(trim(col("order_method_type"))))
            .withColumn("year", col("year").cast("int"))
            .withColumn("quarter", regexp_extract(trim(col("quarter")), r"Q([1-4])", 1).cast("int"))
            .withColumn("quantity", col("quantity").cast("int"))
            .withColumn("revenue", col("revenue").cast("double"))
            .select("product_name", "country_name", "retailer_name", "order_method", "year", "quarter", "quantity", "revenue")  # <== drop quarter_str
        )
    else:
        cleaned_csv_sales = None

    # print("CSV sales data count:", cleaned_csv_sales.count() if cleaned_csv_sales else 0)

    # Merge MySQL and CSV sales
    combined_sales = enriched_mysql_sales
    if cleaned_csv_sales:
        combined_sales = combined_sales.unionByName(cleaned_csv_sales)

    # Join with dimensions
    fact_df = (
        combined_sales.alias("s")
        .join(dim_product_df.alias("p"), col("s.product_name") == col("p.product_name"), "left")
        .join(dim_country_df.alias("c"), col("s.country_name") == col("c.name"), "left")
        .join(dim_retailer_df.alias("r"), col("s.retailer_name") == col("r.name"), "left")
        .join(dim_date_df.alias("t"),
              (col("s.year") == col("t.year")) & (col("s.quarter") == col("t.quarter")),
              "left")
        .select(
            col("p.product_tk"),
            col("c.country_tk"),
            col("r.retailer_tk"),
            col("t.date_tk"),
            col("s.quantity"),
            col("s.revenue"),
            col("s.order_method")
        )
    )


    fact_df = fact_df.withColumn(
        "fact_sales_tk",
        row_number().over(Window.orderBy("p.product_tk", "c.country_tk", "r.retailer_tk", "date_tk"))
    )

    # print("Final fact sales row count:", fact_df.count())
    assert fact_df.count() == 77931, "Number of sales records from step one of the project."
    return fact_df
