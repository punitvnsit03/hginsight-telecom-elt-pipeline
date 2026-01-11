# Telecom Customer Churn ELT Pipeline

Production-ready ELT pipeline with PII anonymization, containerized deployment, and BI reporting.

## Features
- **Hourly orchestration** via Apache Airflow (configurable schedule)
- **Staging layer** in PostgreSQL for raw data ingestion
- **PySpark transformation** with PII anonymization (hashing, masking)
- **DuckDB warehouse** optimized for analytical queries
- **Metabase** for interactive dashboards
- **Docker Compose** for one-command deployment

## Quick Start

### Prerequisites
- Docker & Docker Compose installed
- 8GB RAM minimum
- Ports 8080, 8088,8090, 5433, 5432 must be available

### Setup

1. **Clone and prepare data**
git clone https://github.com/punitvnsit03/hginsight-telecom-elt-pipeline.git
cd hginsight-telecom-elt-pipeline
mkdir -p data/raw duckdb_data
push csv file donloaded from https://www.kaggle.com/datasets/abdullah0a/telecom-customer-churn-insights-for-analysis?resource=download into  data/raw.


2.**Install Docker for Desktop**
   Install docker Desktop based on your OS from "https://docs.docker.com/?_gl=1*94vg5z*_gcl_au*MTc4NDcwMjM0NC4xNzY4MDI3NDM2*_ga*MTI2NDExNTEzNS4xNzY4MDI3NDM4*_ga_XJWPQMJYHQ*czE3NjgxMjU5NzgkbzIkZzEkdDE3NjgxMjU5OTgkajQwJGwwJGgw"
   Enable the docker on your desktop.
   Pull and build all Docker images (first time only - takes 5-10 minutes)
   docker-compose build

    # Start all services in detached mode
    docker-compose up -d

    # Wait for services to be healthy (check status)
    docker-compose ps


    # Check container logs for any errors
    docker-compose logs airflow-webserver
    docker-compose logs airflow-scheduler
    docker-compose logs superset

    # Ensure all containers are healthy
    docker-compose ps | grep -i "healthy"


2.**Execute Pipeline from Airflow UI**
    Go to localhost:8080 from your browser.
    Execute the Dag telcom_elt_pipeline.

3. **to check data in staging**
   Execute below from the project root directory: <change the sql as per your wish>
    
   docker exec -it postgres-staging psql -U staging_user -d staging_db -c "SELECT COUNT(*) FROM staging_telecom_churn;"
   docker exec -it postgres-staging psql -U staging_user -d staging_db -c "\d staging_telecom_churn"
   docker exec -it postgres-staging psql -U staging_user -d staging_db -c "SELECT * FROM staging_telecom_churn LIMIT 3;"

4. **Check data in analytical satge (Transformed Data)**
   Execute below from the project root directory: <change the sql as per your wish>

   docker exec -it airflow-scheduler python -c "
   import duckdb
   con = duckdb.connect('/opt/airflow/duckdb_data/warehouse.duckdb')
   print('Row count:', con.execute('SELECT COUNT(*) FROM analytics.telecom_churn;').fetchone()[0])
   print(con.execute('SELECT * FROM analytics.telecom_churn LIMIT 3;').fetchdf())"

5. **To Access Report**
   Visit http://localhost:8090/collection/4-punit-pathak-s-personal-collection 
   
