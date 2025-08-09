# 📊 Stock Data Extraction and Storage Pipeline

## 📌 Overview
This project automates the process of fetching historical stock data, storing both raw and parsed versions in a **PostgreSQL** database, and running the process on a schedule using **Jenkins** inside a **Docker** container.

It uses the **yfinance** Python library to pull stock price data for a set of predefined tickers over a given date range.

## 🚀 Process Flow
- Fetch stock data from Yahoo Finance.
- Store raw JSON in PostgreSQL table (`raw_stock_data`).
- Parse and clean the data into a structured format (`stock_data`).
- Automate execution via Jenkins (running in Docker).

## 🛠 Tech Stack
- **Python 3.x** – Core language for data extraction and processing  
- **yfinance** – Fetches historical market data from Yahoo Finance API  
- **pandas** – Data cleaning and transformation  
- **psycopg2** – PostgreSQL database interaction  
- **PostgreSQL** – Storage for raw and parsed stock data  
- **Jenkins (Dockerized)** – CI/CD automation for scheduled execution  
- **Docker** – Containerized Jenkins environment  

## 🔄 Data Flow

### Step 1 – Data Extraction
- `yfinance` downloads stock data for multiple tickers within a given date range.  
- Data is retrieved as a **Pandas DataFrame**.

### Step 2 – Raw Data Storage
- DataFrame is converted to JSON (`orient="index"`).  
- Stored in PostgreSQL table `raw_stock_data` with:  
  - `symbol` (stock ticker)  
  - `raw` (JSONB data)  

### Step 3 – Parsed Data Storage
- DataFrame is flattened (removing multi-level columns).  
- Column names are converted to lowercase.  
- Each row is inserted into `stock_data` table with fields:  
  - `symbol`  
  - `date`  
  - `open`, `high`, `low`, `close` (NUMERIC)  
  - `volume` (BIGINT)  

### Step 4 – Automation with Jenkins
- Jenkins job runs Python script on a schedule.  
- Jenkins is deployed inside Docker for isolated, reproducible builds.  
- Jenkinsfile or freestyle job triggers the script inside the container.
