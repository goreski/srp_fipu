from pyspark.sql.functions import col, trim, regexp_extract
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from spark_session import get_spark_session

def transform_date_dim(mysql_date_df, csv_date_df=None):
    spark = get_spark_session()

    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_date_df
        .select(
            col("year").cast("int"),
            trim(col("quarter")).alias("quarter_str")
        )
        .withColumn("quarter", regexp_extract("quarter_str", r"Q([1-4])", 1).cast(IntegerType()))
        .select("year", "quarter")
        .dropna()
        .dropDuplicates()
    )

    # --- Step 2: Normalize CSV data ---
    if csv_date_df:
        csv_df = (
            csv_date_df
            .select(
                col("year").cast("int"),
                trim(col("quarter")).alias("quarter_str")
            )
            .withColumn("quarter", regexp_extract("quarter_str", r"Q([1-4])", 1).cast(IntegerType()))
            .select("year", "quarter")
            .dropna()
            .dropDuplicates()
        )

        combined_df = mysql_df.unionByName(csv_df).dropDuplicates(["year", "quarter"])
    else:
        combined_df = mysql_df

    window = Window.orderBy("year", "quarter")
    combined_df = combined_df.withColumn("date_tk", row_number().over(window))

    return combined_df.orderBy("year", "quarter")
