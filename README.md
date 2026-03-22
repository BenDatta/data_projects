# 🚀 Data Engineering & Analytics Portfolio

> *From raw data to actionable insights — building production-ready data pipelines and analytics solutions*

A comprehensive showcase of data engineering projects featuring Apache Airflow orchestration, medallion architecture ETL pipelines, Docker containerization, S3 bucket storage, and scalable data processing workflows.

---


## 🛠️ Tech Stack

### **Orchestration & Workflow**
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

### **Data Processing**
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![PySpark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)

### **Data Sources & APIs**
![Alpha Vantage](https://img.shields.io/badge/Alpha%20Vantage-00C853?style=for-the-badge&logo=chart-line&logoColor=white)
![REST API](https://img.shields.io/badge/REST%20APIs-009688?style=for-the-badge&logo=fastapi&logoColor=white)

### **Data Storage**
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-50ABF1?style=for-the-badge&logo=apache&logoColor=white)

### **Analytics & Visualization**
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-11557c?style=for-the-badge&logo=python&logoColor=white)
![Seaborn](https://img.shields.io/badge/Seaborn-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 🏗️ Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Bronze    │─────▶│    Silver    │─────▶│    Gold     │
│ (Raw Data)  │      │ (Cleaned)    │      │ (Analytics) │
└─────────────┘      └──────────────┘      └─────────────┘
       ▲                    ▲                      ▲
       └────────────────────┴──────────────────────┘
                    Apache Airflow DAGs
```

---
## ✨ Highlights

🎯 **Production-Ready Pipelines** — Automated ETL workflows orchestrated with Apache Airflow  
⚙️ **Medallion Architecture** — Bronze → Silver → Gold data transformation layers  
🐳 **Containerized Deployment** — Dockerized infrastructure for seamless deployment  
📡 **API Integration** — Real-time data ingestion from external APIs (Alpha Vantage, REST)  
📊 **End-to-End Solutions** — From data ingestion to analytics-ready datasets  

---
## 📁 Project Portfolio

### � **Market Data API Pipeline**
**Tech Stack:** Apache Airflow · Alpha Vantage API · Pandas · XCom · Python 

Real-time financial market data ingestion pipeline with automated weekly stock data collection:
- **API Integration**: Alpha Vantage stock market data retrieval
- **Data Flattening**: JSON to structured CSV transformation  
- **XCom Integration**: Airflow task communication for downstream processing
- **Time-Series Ready**: Clean date-indexed market analytics data

📂 [`data/market_api_etl/bronze_data.py`](data/market_api_etl/bronze_data.py)

---

### �🛫 **Flight Data ETL Pipeline**
**Tech Stack:** Apache Airflow · Python · Pandas · Medallion Architecture

A robust ETL pipeline processing flight data through three transformation stages:
- **Bronze Layer**: Raw data ingestion from external APIs
- **Silver Layer**: Data cleaning, validation, and standardization
- **Gold Layer**: Aggregated analytics and business metrics

📂 [`dags/flight_dag.py`](dags/flight_dag.py) | [`data/flight_etl/`](data/flight_etl/)

---

### 🏦 **Bank Customer Churn Analytics**
**Tech Stack:** Apache Airflow · Pandas · Excel/CSV Processing · SQL · Python

Automated daily processing of bank customer data to identify churn patterns:
- Multi-sheet Excel data extraction
- Customer and account information merging
- Churn risk scoring and segmentation
- Analytics-ready dataset generation

📂 [`dags/bank_churn_dag.py`](dags/bank_churn_dag.py)

---

### 🚆 **Transport Data Pipeline**
**Tech Stack:** Apache Airflow · Pandas · Parquet · Hourly Scheduling · Python

High-frequency ETL pipeline processing transport and city data:
- Hourly data ingestion and processing
- Parquet format optimization for analytics
- Medallion architecture implementation
- Scalable data transformation workflows

📂 [`dags/transport_etl.py`](dags/transport_etl.py)

---

### 🛒 **E-commerce Data Pipeline**
**Tech Stack:** Apache Airflow · PostgreSQL · Pandas · SQL · Python

Comprehensive e-commerce data ingestion pipeline for analytics:
- **6 Data Sources**: Orders, order items, refunds, products, page views, and sessions
- **Dynamic Schema Detection**: Automatic column discovery from PostgreSQL
- **Bulk Data Loading**: Efficient CSV to PostgreSQL data transfer
- **Upsert Support**: Replace-on-conflict for idempotent data loads

📂 [`dags/ecommerce_dag.py`](dags/ecommerce_dag.py) | [`data/ecommerce_pipeline/`](data/ecommerce_pipeline/)

---

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/BenDatta/data_projects.git
cd data_projects

# Install Poetry and dependencies
pip install poetry
poetry install

# Activate the environment
poetry shell

# Start Airflow with Docker Compose
docker compose up -d
```

---

## 📊 Key Features

### ⚡ **Automated Scheduling**
- Daily, hourly, and custom schedules via Airflow
- Automatic retry mechanisms with exponential backoff
- Comprehensive logging and monitoring

### 🎯 **Data Quality**
- Validation at each transformation layer
- Type checking and schema enforcement
- Error handling and data reconciliation

### 🔄 **Scalable Architecture**
- Modular, reusable ETL components
- Medallion architecture for data maturity
- Containerized for cloud deployment

### 📈 **Production Best Practices**
- Version control with Git
- Code quality enforcement (Ruff linting)
- Comprehensive error handling
- Retry logic and failure recovery

---

## 🗂️ Repository Structure

```
data_projects/
├── dags/                      # Airflow DAG definitions
│   ├── flight_dag.py         # Flight data ETL pipeline
│   ├── bank_churn_dag.py     # Bank customer churn analysis
│   ├── transport_etl.py      # Transport data processing
│   ├── ecommerce_dag.py      # E-commerce data pipeline
│   └── sql/                   # SQL schemas
│       └── create_tables.sql # E-commerce table definitions
├── data/                      # Data modules and storage
│   ├── flight_etl/           # Flight pipeline implementation
│   │   ├── bronze_ingest.py  # Raw data ingestion
│   │   ├── silver_transform.py # Data cleaning & validation
│   │   └── gold.py           # Analytics aggregations
│   ├── market_api_etl/       # Market data API pipeline
│   │   └── bronze_data.py    # Stock market data ingestion
│   └── ecommerce_pipeline/   # E-commerce pipeline
│       ├── extract.py        # CSV data extraction
│       └── data/             # Source CSV files
├── config/                    # Configuration files
│   └── airflow.cfg           # Airflow settings
├── projects/                  # Additional projects
│   ├── data analysis/        # Jupyter notebooks & visualizations
│   ├── databricks/           # Databricks notebooks
│   └── PySpark/              # PySpark analytics
├── docker-compose.yaml       # Container orchestration
├── Dockerfile                # Airflow container definition
├── pyproject.toml            # Poetry project configuration
├── poetry.lock               # Locked dependencies
└── requirements.txt          # pip dependencies (exported)
```

---

## 📚 Learning & Exploration

This repository demonstrates:

✅ Building production-grade data pipelines  
✅ Implementing medallion (Bronze-Silver-Gold) architecture  
✅ Orchestrating complex workflows with Apache Airflow  
✅ Integrating external APIs for real-time data ingestion  
✅ Using XCom for inter-task communication in Airflow  
✅ Containerizing data infrastructure with Docker  
✅ Processing data with Pandas and PySpark  
✅ Applying data engineering best practices  
