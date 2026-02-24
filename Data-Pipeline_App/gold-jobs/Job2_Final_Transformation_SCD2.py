import time
import boto3
import sys
from awsglue.utils import getResolvedOptions

required_parameters = ['WORKGROUP_NAME','DATABASE','REGION']

args = getResolvedOptions(sys.argv, required_parameters)

WORKGROUP_NAME = args['WORKGROUP_NAME']
DATABASE = args['DATABASE']
REGION = args['REGION']

client = boto3.client("redshift-data",region_name=REGION)

dim_cust_ddl =  """CREATE TABLE IF NOT EXISTS dim_customer (
        customer_sk INT IDENTITY(1,1) PRIMARY KEY, 
        customer_id INT, 
        full_name VARCHAR(255),
        email VARCHAR(255), 
        phone VARCHAR(50), 
        city VARCHAR(100),
        gender INT, 
        registration_date TIMESTAMP, 
        start_date TIMESTAMP DEFAULT SYSDATE, 
        end_date TIMESTAMP DEFAULT '9999-12-31', 
        is_active BOOLEAN DEFAULT TRUE
    );"""

dim_prod_ddl = """CREATE TABLE IF NOT EXISTS dim_product (
        product_sk INT IDENTITY(1,1) PRIMARY KEY, 
        product_id INT, 
        product_name VARCHAR(100),
        category VARCHAR(50), 
        list_price DECIMAL(10,2),
        start_date TIMESTAMP DEFAULT SYSDATE, 
        end_date TIMESTAMP DEFAULT '9999-12-31', 
        is_active BOOLEAN DEFAULT TRUE
    );"""

fact_ord_ddl = """CREATE TABLE IF NOT EXISTS fact_orders (
        order_sk INT IDENTITY(1,1) PRIMARY KEY, 
        order_id INT, 
        customer_sk INT, 
        product_sk INT,
        amount DECIMAL(10,2), 
        order_date TIMESTAMP, 
        order_status INT, 
        payment_mode INT, 
        price_range VARCHAR(20),
        FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
        FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk)
    );"""

dim_cust_scd = """START TRANSACTION;

	UPDATE dim_customer
	SET end_date = SYSDATE, is_active = FALSE
	FROM staging_customers s
	WHERE dim_customer.customer_id = CAST(s.id AS INTEGER)
	AND dim_customer.is_active = TRUE
	AND ((s.op = 'U' AND (
			s.full_name!= dim_customer.full_name OR
			s.email!= dim_customer.email OR
			s.phone!= dim_customer.phone OR
			s.address_city!= dim_customer.city OR
			CAST(s.gender AS INTEGER) != dim_customer.gender
		)) OR (s.op = 'D'));


	INSERT INTO dim_customer (customer_id, full_name, email, phone, city, gender, registration_date, start_date, is_active)
	SELECT 
	CAST(s.id AS INTEGER), s.full_name, s.email, s.phone, s.address_city, CAST(s.gender AS INTEGER), s.registration_date, SYSDATE, TRUE
	FROM staging_customers s
	WHERE s.op IN ('U', 'I')
	AND NOT EXISTS(
		SELECT 1
		FROM dim_customer dc
		WHERE dc.customer_id = CAST(s.id AS INTEGER)
		AND dc.is_active = TRUE
	);

COMMIT;
"""

dim_prod_scd = """
START TRANSACTION;

	UPDATE dim_product
	SET end_date = SYSDATE, is_active = FALSE
	FROM staging_products p
	WHERE dim_product.product_id = CAST(p.product_id AS INTEGER)
	AND dim_product.is_active = TRUE
	AND ((p.op = 'U' AND (
			p.product_name!= dim_product.product_name OR
			p.category!= dim_product.category OR
			p.list_price!= dim_product.list_price
		)) OR (p.op = 'D'));


	INSERT INTO dim_product (product_id, product_name, category, list_price, start_date, is_active)
	SELECT 
	CAST(p.product_id AS INTEGER), p.product_name, p.category, p.list_price, SYSDATE, TRUE
	FROM staging_products p
	WHERE p.op IN ('U', 'I')
	AND NOT EXISTS(
		SELECT 1
		FROM dim_product dp
		WHERE dp.product_id = CAST(p.product_id AS INTEGER)
		AND dp.is_active = TRUE
	);

COMMIT;
"""

fact_sql = """
INSERT INTO fact_orders (order_id, customer_sk, product_sk, amount, order_date, order_status, payment_mode, price_range)
SELECT CAST(s.order_id AS INTEGER),
COALESCE(dc.customer_sk,-1),
COALESCE(dp.product_sk,-1),
s.final_amount,
s.order_date_ts,
CAST(s.order_status AS INTEGER),
CAST(s.payment_mode AS INTEGER),
s.price_range
FROM staging_orders as s
LEFT JOIN dim_customer dc ON dc.customer_id = CAST(s.customer_id AS INTEGER) AND s.order_date_ts BETWEEN dc.start_date AND dc.end_date
LEFT JOIN dim_product dp ON dp.product_id = CAST(s.product_id AS INTEGER) AND s.order_date_ts BETWEEN dp.start_date AND dp.end_date
WHERE NOT EXISTS(
	SELECT 1
	FROM fact_orders f
	WHERE f.order_id = CAST(s.order_id AS INTEGER)
);
"""

def run_sql(sql_query):
    try:
        response = client.execute_statement(
            WorkgroupName = WORKGROUP_NAME, Database = DATABASE, Sql = sql_query
        )
        query_id = response['Id']

        while True:
            status = client.describe_statement(Id=query_id)
            state = status['Status']
            time.sleep(1)
            if state == "FAILED":
                raise Exception(f"SQL Error : {status['Error']}")
            elif state == "FINISHED":
                break
    except Exception as e:
        raise e

try:
    run_sql(dim_cust_ddl)
    run_sql(dim_prod_ddl)
    run_sql(fact_ord_ddl)
    run_sql(dim_cust_scd)
    run_sql(dim_prod_scd)
    run_sql(fact_sql)
except Exception as e:
    raise e