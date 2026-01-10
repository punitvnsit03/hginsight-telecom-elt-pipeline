# Telecom Customer Churn ELT Pipeline

Production-ready ELT pipeline with PII anonymization, containerized deployment, and BI reporting.

## Features
- **Hourly orchestration** via Apache Airflow (configurable schedule)
- **Staging layer** in PostgreSQL for raw data ingestion
- **PySpark transformation** with PII anonymization (hashing, masking)
- **DuckDB warehouse** optimized for analytical queries
- **Apache Superset** for interactive dashboards
- **Docker Compose** for one-command deployment

## Quick Start

### Prerequisites
- Docker & Docker Compose installed
- 8GB RAM minimum
- Ports 8080, 8088, 5433 available

### Setup

1. **Clone and prepare data**
```bash
git clone <your-repo>
cd telecom-elt-pipeline
mkdir -p data/raw duckdb_data
