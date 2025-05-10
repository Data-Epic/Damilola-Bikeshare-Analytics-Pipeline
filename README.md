# 🚲 Damilola Bikeshare Analytics Pipeline — Batch + Real-Time

# Overview
This project builds a data pipeline for Capital Bikeshare's December 2022 trip data.

It supports:

- Weekly batch reporting
- Real-time alerts for key events

You'll use containerized components for cleaning, storing, streaming, and orchestrating data processes.

# 🚧 Problem Statement
i am working with a micromobility company to improve operations and monitor rider behavior.

Data: [data-source](dags/data/202212-capitalbikeshare-tripdata.csv)

Real-Time Alerts Required:
- Casual rider starts a trip at midnight
- Any ride that lasts over 45 minutes

# 🧰 Tech Stack
| Purpose                | Tool Used                    |
| ---------------------- | ---------------------------- |
| Containerization       | Docker                       |
| Workflow Orchestration | Apache Airflow               |
| Batch Processing       | Pandas                       |
| Storage Format         | Parquet (partitioned)        |
| Real-Time Streaming    | Python generator (simulated) |
| Deployment Management  | Docker Compose               |


# Deliverables
- Cleaned, partitioned Parquet dataset (by user type and week)
- Airflow DAG for weekly batch ETL (runs every Monday, 10 AM UTC)
- Dockerfile + docker-compose for full pipeline
- Real-time stream processor:
    - Logs trips > 45 minutes
    - Logs midnight casual rider starts

# 🔁 Pipeline Architecture

[DAG](screenshots/bikeshare_dag_v0-graph.png)

# 🧪 Setup & Run

```
git clone https://github.com/Data-Epic/your-repo-name.git
cd your-repo-name

// Start pipeline
docker-compose up --build
```
Airflow UI: http://localhost:8080
Logs: Stream output are written to [file](logs/dag_id=bikeshare_dag_v0)
