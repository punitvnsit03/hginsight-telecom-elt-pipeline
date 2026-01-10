import os
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

def extract_and_load_to_staging(source_url, local_path):
    """
    Extract CSV data from URL or local file and load to PostgreSQL staging
    """
    print(f"Starting extraction at {datetime.now()}")

    # Extract: Download or read CSV
    if source_url.startswith('http'):
        print(f"Downloading data from {source_url}")
        response = requests.get(source_url, timeout=120)
        with open(local_path, 'wb') as f:
            f.write(response.content)
    else:
        print(f"Using local file: {source_url}")
        local_path = source_url

    # Read CSV
    df = pd.read_csv(
        local_path,
        on_bad_lines='skip',           # Skip bad lines
        quotechar='"',                 # Standard quote character
        escapechar='\\',               # Escape backslashes
        skipinitialspace=True,         # Ignore spaces after delimiters
        engine='python'                # More flexible parser (slower but handles edge cases)
    )
    print(f"Extracted {len(df)} records with {len(df.columns)} columns")

    # Load to PostgreSQL staging
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_STAGING_HOST'),
        database=os.getenv('POSTGRES_STAGING_DB'),
        user=os.getenv('POSTGRES_STAGING_USER'),
        password=os.getenv('POSTGRES_STAGING_PASSWORD')
    )

    cursor = conn.cursor()

    # Create staging table (drop if exists)
    columns = ', '.join([f'"{col}" TEXT' for col in df.columns])
    cursor.execute(f"""
        DROP TABLE IF EXISTS staging_telecom_churn;
        CREATE TABLE staging_telecom_churn (
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            {columns}
        );
    """)

    # Insert data
    columns_list = list(df.columns)
    insert_query = f"""
        INSERT INTO staging_telecom_churn ({', '.join([f'"{col}"' for col in columns_list])})
        VALUES %s
    """

    data_tuples = [tuple(row) for row in df.values]
    execute_values(cursor, insert_query, data_tuples)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Loaded {len(df)} records to staging_telecom_churn at {datetime.now()}")
    return True
