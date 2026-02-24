import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# --- CONFIGURATION ---
# 1. UPDATE THESE VALUES EXOCTLY
# Go to your Glue Connection -> Edit -> Copy the "JDBC URL" string
# It looks like: jdbc:redshift://workgroup-name...:5439/dev
JDBC_URL = "jdbc:redshift://project-workgroup.655707937094.eu-north-1.redshift-serverless.amazonaws.com:5439/dev" 

# 2. Database Credentials (Retrieving from Glue Connection is cleaner, but let's be direct)
# Ensure your Glue Connection name is correct
CONNECTION_NAME = "RedshiftConnection" 
PROCESSED_DB_NAME = "jenit_processed_db"
REDSHIFT_USER = "admin"  # Or your DB user
REDSHIFT_PASSWORD = "Admin1234!" # ⚠️ REPLACE THIS (Or use Secrets Manager if you know how)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ==========================================
# HELPER: DIRECT JDBC WRITE
# ==========================================
def write_direct_jdbc(catalog_table, redshift_table, bookmark_key):
    print(f"--- Processing {catalog_table} ---")
    
    # 1. Read from Catalog (Incremental)
    dyf = glueContext.create_dynamic_frame.from_catalog(
        database = PROCESSED_DB_NAME,
        table_name = catalog_table,
        transformation_ctx = bookmark_key
    )
    
    if dyf.count() == 0:
        print(f"Skipping {redshift_table}: No new data.")
        return

    print(f"✅ Found rows for {redshift_table}. Writing Direct JDBC...")
    
    # 2. Convert to Spark DataFrame
    df = dyf.toDF()

    # 3. Truncate Table First (Manual SQL via Spark Driver)
    # We use the driver manager to run the DELETE/TRUNCATE command
    try:
        # Note: Using 'dbtable' option with 'truncate' mode in Spark JDBC is easier
        # This writes the data directly. 
        # Mode 'overwrite' in Redshift JDBC usually drops/recreates or truncates.
        df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", redshift_table) \
            .option("user", REDSHIFT_USER) \
            .option("password", REDSHIFT_PASSWORD) \
            .option("driver", "com.amazon.redshift.jdbc42.Driver") \
            .mode("overwrite") \
            .save()
            
        print(f"Successfully wrote {redshift_table}")
        
    except Exception as e:
        print(f"❌ Error writing {redshift_table}: {str(e)}")
        raise e

# ==========================================
# MAIN EXECUTION
# ==========================================
try:
    # 1. Customers
    write_direct_jdbc("processed_customers", "staging_customers", "bookmark_staging_cust")

    # 2. Products
    write_direct_jdbc("processed_products", "staging_products", "bookmark_staging_prod")

    # 3. Orders
    write_direct_jdbc("processed_orders", "staging_orders", "bookmark_staging_ord")

    job.commit()
    print("--- Job Completed ---")

except Exception as e:
    print(f"CRITICAL FAILURE: {str(e)}")
    raise e