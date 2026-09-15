# ExchangeRate-PipeLine-with-Airflow-in-MySQL-Python-Project
## 📌 Project Overview

Exchange Pipeline with Airflow in MySQL Python Project is an automated data engineering pipeline that retrieves the latest foreign exchange rates from the ExchangeRate API, processes and cleans the data using Python and Pandas, and loads the exchange-rate data into a MySQL database.The pipeline is orchestrated using Apache Airflow, which allows the process to run automatically on a scheduled basis and provides monitoring of pipeline execution.The project uses Docker to run Airflow and its supporting services, while MySQL 8.0 is used as the destination database and can be managed and analyzed through MySQL Workbench.

## 🎯 Project Objectives

The main objectives of this project are:
- Extract the latest foreign exchange rates from an API.
- Process and clean the API response using Python.
- Convert the API response into a structured Pandas DataFrame.
- Validate and remove invalid exchange-rate records.
- Detect whether new exchange-rate data is available.
- Load new data incrementally into MySQL.
- Prevent duplicate exchange-rate snapshots.
- Maintain a pipeline watermark to track the last successfully loaded API timestamp.
- Automate the pipeline using Apache Airflow.
- Run Airflow using Docker.
- Monitor pipeline execution through the Airflow Web UI.

## 🏗️ Project Architecture
                    ExchangeRate API
                           │
                           │
                           ▼
                 Python API Client
              requests + Pandas
                           │
                           ▼
                  Data Cleaning
                           │
                           ▼
                    Apache Airflow
                    Docker Container
                           │
                 ┌─────────┴─────────┐
                 │                   │
                 ▼                   ▼
          Check Watermark       Load New Rates
                 │                   │
                 │                   ▼
                 │             MySQL 8.0
                 │                   │
                 │                   ▼
                 │             FOREIGN_RATES
                 │              ┌───────────────┐
                 │              │ EXCHANGE_RATES│
                 │              │ PIPELINE_      │
                 │              │ WATERMARK      │
                 │              └───────────────┘
                 │                   │
                 │                   ▼
                 │            MySQL Workbench
                 │
                 ▼
          Skip if no new data
          
## Airflow Metadata
Airflow uses a separate PostgreSQL container for its internal metadata only:


              Airflow
                 │
                 ▼
      PostgreSQL Docker Container
                 │
                 └── Airflow metadata
                     - DAG runs
                     - Task states
                     - Users
                     - Scheduling information

## PostgreSQL is not used for the exchange-rate data.
The actual exchange-rate data is stored in MySQL.

## 🛠️ Technologies Used
Technology	                     Purpose
Python	                         Data extraction and processing
Pandas	                         Data transformation and cleaning
Requests	                       API communication
ExchangeRate API	               Source of exchange-rate data
Apache Airflow	                 Pipeline orchestration and scheduling
Docker Desktop	                 Containerized Airflow environment
MySQL 8.0	                       Destination database
MySQL Workbench	                 Database management and analysis
PostgreSQL	                     Airflow metadata database
VS Code	                         Development environment

## 📂 Project Structure
                Exchange Rate Pipeline with Airflow - Python Project/
                │
                ├── dags/
                │   └── exchange_rate_incremental_dag.py
                │
                ├── scripts/
                │   └── exchange_rate_client.py
                │
                ├── sql/
                │   └── Exchange_Rates.sql
                │
                ├── logs/
                │
                ├── .env
                │    
                ├── docker-compose.yml
                │
                └── README.md

## 💡 Key Learning Outcomes
This project demonstrates practical knowledge of:

# Python
- API requests
- Object-oriented programming
- Pandas
- Data cleaning
- Exception handling
- Data transformation
# SQL / MySQL
- Database creation
- Table creation
- Primary keys
- Unique constraints
- Indexes
- INSERT
- ON DUPLICATE KEY UPDATE
- Incremental loading
- Watermark implementation
# Apache Airflow
- DAG creation
- Tasks
- Task dependencies
- Scheduling
- Variables
- Connections
- MySqlHook
- Retries
- Short-circuiting downstream tasks
- Airflow metadata management
# Docker
- Docker Compose
- Containers
- Volumes
- Networks
- Environment variables
- Container-to-host communication
- Running Airflow services
 
 ## 🎯 Future Improvements
Possible future enhancements include:
- Add historical exchange-rate extraction.
- Add more currencies/base currencies.
- Add data-quality checks.
- Add logging and monitoring.
- Add email/Slack notifications for failed DAGs.
- Create a Power BI dashboard using the MySQL data.
- Containerize the MySQL database if required.
- Use a custom Airflow Docker image instead of _PIP_ADDITIONAL_REQUIREMENTS.
- Add unit tests for the Python API client.
- Add CI/CD using GitHub Actions.

## 👨‍💻 Author
Nilanshu Vishwakarma

Skills demonstrated:
Python · Pandas · SQL · MySQL · Apache Airflow · Docker · ETL · API Integration · Data Engineering

## ⭐ Project Summary

Exchange Pipeline with Airflow in MySQL Python Project is an automated ETL pipeline that extracts foreign exchange rates from an API, transforms and validates the data using Python/Pandas, checks for new data using an incremental watermark strategy, and loads the results into MySQL. Apache Airflow manages the workflow and scheduling, while Docker provides the execution environment. MySQL Workbench is used to manage and verify the resulting exchange-rate data.
