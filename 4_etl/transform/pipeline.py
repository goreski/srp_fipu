from transform.dimensions.product_dim import transform_product_dim
from transform.dimensions.country_dim import transform_country_dim
from transform.facts.sales_fact import transform_sales_fact
from transform.dimensions.retailer_dim import transform_retailer_dim
from transform.dimensions.date_dim import transform_date_dim


def run_transformations(raw_data):
    # Transform dimensions
    product_dim = transform_product_dim(
    raw_data["product"],
    raw_data["product_type"],
    raw_data["product_line"],
    csv_sales_df=raw_data.get("csv_sales")
    )
    print("1️⃣ Product dimension complete")
    
    country_dim = transform_country_dim(
        raw_data["country"],
        csv_country_df=raw_data.get("csv_sales")
    )
    print("2️⃣ Country dimension complete")

    retailer_dim = transform_retailer_dim(
        raw_data["retailer_type"],
        csv_retailer_df=raw_data.get("csv_sales")
    )
    print("3️⃣ Retailer dimension complete")

    date_dim = transform_date_dim(
        raw_data["sales"],
        csv_date_df=raw_data.get("csv_sales")
    )
    print("4️⃣ Time dimension complete")

    fact_sales = transform_sales_fact(
        raw_data,
        product_dim,
        country_dim,
        retailer_dim,
        date_dim
    )
    print("5️⃣ Sales fact table complete")

    return {
        "dim_product": product_dim,
        "dim_country": country_dim,
        "dim_retailer": retailer_dim,
        "dim_date": date_dim,
        "fact_sales": fact_sales
    }