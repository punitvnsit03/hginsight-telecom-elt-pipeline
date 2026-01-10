import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sha2, concat_ws, regexp_replace, lit, substring
from faker import Faker
import duckdb

def transform_and_load_to_warehouse():
    """
    Transform data using PySpark: handle missing values, anonymize PII, load to DuckDB
    """
    print("Starting transformation with PySpark")

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TelecomETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .getOrCreate()

    # Read from PostgreSQL staging
    jdbc_url = f"jdbc:postgresql://{os.getenv('POSTGRES_STAGING_HOST')}:5432/{os.getenv('POSTGRES_STAGING_DB')}"
    jdbc_properties = {
        "user": os.getenv('POSTGRES_STAGING_USER'),
        "password": os.getenv('POSTGRES_STAGING_PASSWORD'),
        "driver": "org.postgresql.Driver"
    }

    df = spark.read.jdbc(url=jdbc_url, table="staging_telecom_churn", properties=jdbc_properties)
    print(f"Read {df.count()} records from staging")

    # Handle missing values with defaults
    for column in df.columns:
        if column == 'load_timestamp':
            continue
        df = df.withColumn(column, when(col(column).isNull(), lit("UNKNOWN")).otherwise(col(column)))

    # PII Anonymization
    fake = Faker()

    # Hash customer IDs (deterministic)
    if 'CustomerID' in df.columns:
        df = df.withColumn('CustomerID', sha2(col('CustomerID').cast('string'), 256))

    # Mask phone numbers (keep last 4 digits)
    if 'PhoneNumber' in df.columns:
        df = df.withColumn('PhoneNumber',
                           concat_ws('', lit('XXX-XXX-'), substring(col('PhoneNumber'), -4, 4)))

    # Mask email addresses (keep domain)
    if 'Email' in df.columns:
        df = df.withColumn('Email',
                           regexp_replace(col('Email'), '^[^@]+', 'user_xxxxx'))

    # Remove or mask names
    if 'Name' in df.columns:
        df = df.withColumn('Name', lit('ANONYMIZED'))

    # Convert to Pandas for DuckDB loading
    pandas_df = df.toPandas()
    spark.stop()

    print(f"Transformation complete. Loading {len(pandas_df)} records to DuckDB")

    # Load to DuckDB warehouse
    duckdb_path = os.getenv('DUCKDB_PATH')
    conn = duckdb.connect(duckdb_path)

    # Create analytics schema and table
    conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    conn.execute("DROP TABLE IF EXISTS analytics.telecom_churn")
    conn.execute("CREATE TABLE analytics.telecom_churn AS SELECT * FROM pandas_df")

    # Create aggregated views for reporting
    conn.execute("""
        CREATE OR REPLACE VIEW analytics.churn_summary AS
        SELECT 
            "Churn" as churn_status,
            COUNT(*) as customer_count,
            ROUND(AVG(CAST("MonthlyCharges" AS DOUBLE)), 2) as avg_monthly_charges,
            ROUND(AVG(CAST("TotalCharges" AS DOUBLE)), 2) as avg_total_charges
        FROM analytics.telecom_churn
        WHERE "Churn" IS NOT NULL
        GROUP BY "Churn"
    """)

    conn.close()
    print(f"Data warehouse updated successfully")
    return True
