import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re
from pyspark.sql.functions import col, regexp_replace, year, month, dayofmonth, to_timestamp, trim, upper, lit, initcap, when
import pyspark.sql.functions as F

# Initialising the context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job.init(args['JOB_NAME'], args)

BASE_PROCESSED_BUCKET = "s3://jenit-processed-bucket/"
DATABASE = "jenit_raw_db"
null_list = ["NA", "N/A", "N.A.", "n.a", "null", "Null", "NULL", "none", "None", "NONE", ""] 

def read_dyf_from_raw_catalog(tableName, tableBookmark):
    return glueContext.create_dynamic_frame.from_catalog(
        database=DATABASE, 
        table_name=tableName,
        transformation_ctx=tableBookmark)

# --- THE FIX: Dynamic Column Renamer ---
# This loops through whatever columns exist and renames them safely. No more IndexErrors!
def apply_dynamic_snake_case(df):
    new_column_names = []
    for old_col in df.columns:
        new_name = re.sub(r"[^\w]+", "_", old_col)
        new_name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", new_name)
        new_name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", new_name)
        new_name = new_name.lower().strip("_")
        new_column_names.append(col(old_col).alias(new_name))
    return df.select(*new_column_names)

def drop_None_critical_columns(df, critical_cols):
    valid_cols = [c for c in critical_cols if c in df.columns]
    return df.na.drop(how="any", subset=valid_cols)

def drop_duplicates(df, critical_cols):
    valid_cols = [c for c in critical_cols if c in df.columns]
    return df.dropDuplicates(valid_cols)

# ==========================================
# 1. PROCESS CUSTOMERS
# ==========================================
def process_customers():
    print("Processing Customers...")
    dyf = read_dyf_from_raw_catalog("raw_customers", "customers_bookmark")
    
    if dyf.count() == 0:
        return
    
    df = dyf.toDF()

    # DYNAMIC RENAMING 
    df_raw = apply_dynamic_snake_case(df)
    
    df_raw = df_raw.replace(null_list, None)

    # only id
    critical_columns = ["id", "email", "registration_date"]
    df_raw = drop_None_critical_columns(df_raw, critical_columns)
    # drop exact duplicates here
    df_raw = drop_duplicates(df_raw, critical_columns)

    # THE FIX: Changed timestamp format to "yyyy-MM-dd" so dates stop turning into NULL
    # df_processed = df_raw.withColumn("registration_date", to_timestamp(col("registration_date"), "yyyy-MM-dd")) \
    #     .withColumn("full_name", trim(initcap(col("full_name")))) \
    #     .withColumn("address_city", trim(upper(col("address_city"))))
    
    # THE FIX: Try full timestamp first, fallback to just date if time is missing
    df_processed = df_raw.withColumn(
        "registration_date", 
        F.coalesce(
            to_timestamp(col("registration_date"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("registration_date"), "yyyy-MM-dd")
        )
    ) \
    .withColumn("full_name", trim(initcap(col("full_name")))) \
    .withColumn("address_city", trim(upper(col("address_city"))))
    
    target = f"{BASE_PROCESSED_BUCKET}processed_customers/"
    df_processed.write.mode("append").parquet(target)

# ==========================================
# 2. PROCESS PRODUCTS
# ==========================================
def process_products():
    print("Processing Products...")
    dyf = read_dyf_from_raw_catalog("raw_products", "products_bookmark")
    
    if dyf.count() == 0:
        return
    
    df = dyf.toDF()

    # DYNAMIC RENAMING
    df_raw = apply_dynamic_snake_case(df)

    df_raw = df_raw.replace(null_list, None)

    critical_columns = ["product_id", "product_name", "category", "list_price"] 
    df_raw = drop_None_critical_columns(df_raw, critical_columns)
    df_raw = drop_duplicates(df_raw, critical_columns)
    
    # do typecasting only in gold job
    df_processed = df_raw.withColumn("list_price", col("list_price").cast("double")) \
                     .withColumn("product_name", trim(col("product_name"))) \
                     .withColumn("category", upper(trim(col("category"))))
    
    target = f"{BASE_PROCESSED_BUCKET}processed_products/"
    df_processed.write.mode("append").parquet(target)

# ==========================================
# 3. PROCESS ORDERS
# ==========================================
def process_orders():
    print("Processing Orders...")
    dyf = read_dyf_from_raw_catalog("raw_orders", "orders_bookmark")
    
    if dyf.count() == 0:
        return
    
    df = dyf.toDF()

    # DYNAMIC RENAMING
    df_raw = apply_dynamic_snake_case(df)

    df_raw = df_raw.replace(null_list, None)

    critical_columns = ["order_id", "customer_id", "product_id", "order_amount_str", "order_date"]
    df_raw = drop_None_critical_columns(df_raw, critical_columns)
    df_raw = drop_duplicates(df_raw, critical_columns)

    df_processed = df_raw.withColumn("final_amount", regexp_replace(col("order_amount_str"), "[^0-9.]", "").cast("decimal(10,2)")) \
        .withColumn("order_date_ts", to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("year", year(col("order_date_ts"))) \
        .withColumn("month", month(col("order_date_ts"))) \
        .withColumn("day", dayofmonth(col("order_date_ts"))) \
        .withColumn("price_range", 
                    when(col("final_amount") <= 500, "BUDGET")
                   .when((col("final_amount") > 500) & (col("final_amount") <= 2000), "MID RANGE")
                   .otherwise("PREMIUM"))
    
    target = f"{BASE_PROCESSED_BUCKET}processed_orders/"
    df_processed.write.mode("append").partitionBy("year", "month", "day").parquet(target)

# Main Execution
try:
    process_customers()
    process_products()
    process_orders()
    job.commit()
except Exception as e:
    print(f"CRITICAL ERROR: {str(e)}")
    raise e