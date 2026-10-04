# NASA Web Server Log Analysis

A Big Data log-analysis project using **PySpark** to process and analyze the NASA HTTP Web Server Logs dataset containing approximately **1.57 million web server requests**.

The project parses raw NASA web server logs into structured data, performs large-scale analytics using Apache Spark, exports the complete processed dataset to Parquet, and prepares the data for visualization in **Microsoft Power BI**.

---

## Project Overview

Web server logs contain valuable information about user requests, requested resources, HTTP status codes, client hosts, and data transferred.

However, analyzing millions of raw log entries using traditional Python file processing can become inefficient.

This project uses **PySpark** to:

- Read a large NASA access log
- Parse unstructured log lines
- Convert them into structured columns
- Analyze HTTP requests
- Identify frequently accessed URLs
- Identify frequent client hosts
- Analyze HTTP methods
- Analyze HTTP status codes
- Calculate total response traffic
- Export the complete processed dataset
- Prepare the data for Power BI visualization

---

## Dataset

### NASA HTTP Web Server Logs

The project uses the NASA HTTP access log dataset.

Raw log example:

```text
in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839
```

Each log entry contains information such as:

- Client host
- Timestamp
- HTTP method
- Requested URL
- HTTP protocol
- HTTP status code
- Response size

### Dataset Size

```text
Total log entries: 1,569,898
```

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Application development |
| PySpark | Distributed / large-scale data processing |
| Apache Spark | Data processing engine |
| Java 21 | Spark runtime |
| Hadoop | Spark filesystem dependency |
| Parquet | Processed data storage |
| Power BI | Data visualization |
| Git | Version control |
| GitHub | Project repository |
| uv | Python environment and dependency management |

---

## Project Architecture

```text
                    NASA Access Log
                          |
                          v
                +-------------------+
                |    PySpark        |
                |   SparkSession    |
                +-------------------+
                          |
                          v
                +-------------------+
                |    Raw Log Data   |
                +-------------------+
                          |
                          v
                +-------------------+
                |   Log Parser      |
                +-------------------+
                          |
                          v
                +-------------------+
                | Structured Data   |
                +-------------------+
                          |
             +------------+------------+
             |            |            |
             v            v            v
          Status        URLs         Hosts
          Analysis      Analysis     Analysis
             |            |            |
             +------------+------------+
                          |
                          v
                +-------------------+
                |  Full Parquet     |
                |     Dataset       |
                +-------------------+
                          |
                          v
                +-------------------+
                |     Power BI      |
                |   Visualization   |
                +-------------------+
```

---

## Project Structure

```text
Log-Analysis/
│
├── data/
│   └── access.log
│
├── output/
│   └── nasa_logs.parquet
│
├── src/
│   ├── analytics.py
│   ├── parser.py
│   └── spark_session.py
│
├── .gitignore
├── main.py
├── pyproject.toml
├── requirements.txt
├── README.md
└── uv.lock
```

---

# Implementation

## 1. Creating the PySpark Session

The project starts by creating a Spark session.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Learning Pyspark")
    .getOrCreate()
)
```

The `SparkSession` is the main entry point for working with DataFrames and Spark SQL.

---

## 2. Reading the NASA Log

The raw `access.log` file is loaded using Spark.

```python
log_df = spark.read.text("data/access.log")
```

Initially, Spark treats each log line as a single column called:

```text
value
```

Example:

```text
in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/... HTTP/1.0" 200 1839
```

---

## 3. Inspecting the Raw Data

The first few records were inspected using:

```python
log_df.show(5, truncate=False)
```

The total number of records was verified using:

```python
log_df.count()
```

Result:

```text
1,569,898
```

---

# Log Parsing

The raw log entries were transformed into structured columns.

The resulting schema is:

```text
root
 |-- host: string
 |-- timestamp: string
 |-- method: string
 |-- url: string
 |-- protocol: string
 |-- status: integer
 |-- response_size: long
```

The raw log:

```text
in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839
```

becomes:

| Column | Value |
|---|---|
| host | in24.inetnebr.com |
| timestamp | 01/Aug/1995:00:00:01 -0400 |
| method | GET |
| url | /shuttle/missions/sts-68/news/sts-68-mcc-05.txt |
| protocol | HTTP/1.0 |
| status | 200 |
| response_size | 1839 |

---

# Analytics Completed

The project currently performs the following analysis.

## HTTP Status Analysis

The HTTP status distribution was calculated.

Current results include:

```text
200     1,397,050
304       134,146
302        22,654
500             3
NULL         16,045
```

This helps identify successful responses, redirects, cached responses, and server errors.

---

## Top URLs

The most frequently requested URLs were identified.

Examples include:

```text
/images/NASA-logosmall.gif
/images/KSC-logosmall.gif
/images/MOSAIC-logosmall.gif
/images/USA-logosmall.gif
/images/WORLD-logosmall.gif
/images/ksclogo-medium.gif
/ksc.html
```

---

## Top Hosts

The most active client hosts were identified.

Examples include:

```text
edams.ksc.nasa.gov
piweba4y.prodigy.com
163.206.89.4
piweba5y.prodigy.com
www-d1.proxy.aol.com
```

---

## HTTP Methods

The HTTP request methods were analyzed.

Current results include:

```text
GET      1,549,814
HEAD         3,962
POST            77
NULL        16,045
```

---

## Total Response Traffic

The total response size was calculated:

```text
26,794,218,044 bytes
```

This represents the total amount of response data recorded in the dataset.

---

# Full Dataset Export

After processing the complete dataset, the structured DataFrame was exported to Parquet.

Output:

```text
output/nasa_logs.parquet
```

The Parquet file contains the complete processed dataset rather than only the summarized analytics.

Dataset size:

```text
1,569,898 records
```

Parquet was selected because it is a columnar storage format and is well suited for analytical workloads.

---

# Power BI

The processed Parquet dataset has been successfully imported into **Microsoft Power BI**.

The following fields are available:

```text
host
timestamp
method
url
protocol
status
response_size
```

The Power BI dashboard is currently being developed.

Planned visualizations include:

- Total Requests
- HTTP Status Distribution
- HTTP Method Distribution
- Top 10 URLs
- Top 10 Hosts
- Total Response Traffic
- Request trends over time
- Interactive filters

---

# Challenges Solved

## Java and PySpark Compatibility

The project initially encountered compatibility problems when running PySpark with Java 26.

The environment was switched to:

```text
Java 21 LTS
```

which allowed Spark to run correctly.

---

## Windows Hadoop / winutils Issue

Running Spark on Windows produced Hadoop warnings related to:

```text
winutils.exe
```

The Spark processing and analysis stages were successfully completed despite these warnings.

The Parquet export was ultimately handled successfully using a Python/Parquet-based export approach.

---

# Current Status

```text
[✓] Python environment created
[✓] PySpark installed
[✓] SparkSession configured
[✓] NASA access.log loaded
[✓] 1,569,898 log records processed
[✓] Raw log inspected
[✓] Log parser implemented
[✓] Structured schema created
[✓] HTTP status analysis
[✓] URL analysis
[✓] Host analysis
[✓] HTTP method analysis
[✓] Response size analysis
[✓] Full dataset exported to Parquet
[✓] Parquet dataset imported into Power BI
[ ] Power BI dashboard design
[ ] Advanced analytics
[ ] Final documentation
```

---

# Future Enhancements

The project can be extended with:

- Request traffic trends by hour
- Daily request analysis
- Error-rate analysis
- Geographic analysis of client hosts
- Peak traffic detection
- Anomaly detection
- URL category analysis
- Interactive Power BI dashboard
- Automated data pipeline
- Dockerized PySpark environment
- Streaming log analysis

---

# How to Run

## 1. Clone the repository

```bash
git clone <your-github-repository-url>
cd Log-Analysis
```

## 2. Create the environment

Using `uv`:

```bash
uv venv
```

Activate it on Windows:

```powershell
.venv\Scripts\activate
```

## 3. Install dependencies

```bash
uv sync
```

or:

```bash
pip install -r requirements.txt
```

## 4. Run the project

```powershell
py main.py
```

---

# Output

The project produces:

```text
output/
└── nasa_logs.parquet
```

The dataset can then be imported into Power BI for visualization and dashboard development.

---

# Learning Outcomes

Through this project, the following technologies and concepts were practiced:

- PySpark
- SparkSession
- Spark DataFrames
- Spark SQL
- Large-scale data processing
- Log parsing
- Data cleaning
- Data aggregation
- Data analysis
- Parquet storage
- Power BI
- Git and GitHub
- Python virtual environments
- Java/Spark environment configuration

---

## Project Status

**In Progress — Power BI Dashboard Development**

The complete NASA web server log has been successfully processed with PySpark and exported as a structured Parquet dataset. The next stage is to build the interactive Power BI dashboard.