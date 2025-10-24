# 🌱 Sustainability DataOps Platform

A fully open-source, end-to-end **DataOps Platform** for sustainability and environmental data — built using **Apache Airflow**, **Apache Iceberg**, **Project Nessie**, **Trino**, **MinIO**, and **Great Expectations** — with no reliance on paid cloud services.

This project demonstrates how to orchestrate **multi-source ingestion**, **data lakehouse management**, and **data quality validation** entirely in a **local, containerized environment**.

---

## 🚀 Architecture Overview

### Core Components

| Layer | Technology | Purpose |
|-------|-------------|----------|
| **Orchestration** | Apache Airflow | Schedule and manage multi-source ingestion & ETL DAGs |
| **Storage** | MinIO (S3-compatible) | Object storage for raw, bronze, and curated data |
| **Catalog & Versioning** | Project Nessie | Git-like version control for Iceberg tables |
| **Query Engine** | Trino (Iceberg Connector) | Interactive SQL querying, analytics, and metadata exploration |
| **Data Validation** | Great Expectations | Automatic schema & data quality validation |
| **Processing (Optional)** | Apache Spark | Batch or streaming data transformations |
| **Monitoring** | Prometheus + Grafana | Metrics collection and dashboard visualization |

---

## 🧱 Architecture Diagram

```
          ┌─────────────────────────────┐
          │         Airflow DAGs        │
          │ (Ingest → Transform → Load) │
          └──────────────┬──────────────┘
                         │
                         ▼
              ┌───────────────────────┐
              │       MinIO (S3)      │
              │ raw / bronze / silver │
              └────────────┬──────────┘
                           │
                           ▼
                ┌──────────────────┐
                │ Iceberg Tables   │
                │ (via Nessie)     │
                └───────┬──────────┘
                        │
                        ▼
             ┌────────────────────────┐
             │ Trino Query Engine     │
             │  (SQL + Lakehouse)     │
             └────────────────────────┘
                        │
                        ▼
           ┌────────────────────────────┐
           │ Grafana + Prometheus       │
           │ Observability + Monitoring │
           └────────────────────────────┘
```

---

## ⚙️ Setup Instructions

### 1️⃣ Clone Repository
```bash
git clone https://github.com/harishg4/sustainability-dataops-platform.git
cd sustainability-dataops-platform
```

### 2️⃣ Start Docker Stack
```bash
docker compose -f docker-compose-advanced.yml up -d
```

This spins up:
- Airflow (scheduler + webserver)
- MinIO
- Trino
- Nessie
- Postgres
- Spark (optional)
- Grafana + Prometheus

---

## 🗺️ Airflow DAGs

### `multi_source_sustainability_pipeline`

ETL pipeline that:
1. **Ingests** data from multiple free APIs:
   - UK Carbon Intensity (`api.carbonintensity.org.uk`)
   - Open Meteo (Weather)
   - RandomUser (Sample user data)
   - Official Joke API
2. **Transforms & merges** data into Bronze JSON
3. **Validates** data quality via Great Expectations
4. **Loads** cleaned Bronze data into Iceberg (Trino)
5. Optionally promotes to **Silver** and **Gold** tables

---

## 📊 Data Lake Layers

| Layer | Description | Location |
|--------|--------------|-----------|
| **Raw** | API responses stored as JSON | `/data/raw/` |
| **Bronze** | Flattened, merged datasets | `/data/bronze/` |
| **Silver** | Typed, validated tables | `iceberg.silver.merged_silver` |
| **Gold** | Aggregated, business-ready data | `iceberg.gold.daily_summary` |

All Iceberg tables are version-controlled in **Nessie** and stored as Parquet files in **MinIO** (`s3://lakehouse/warehouse/...`).

---

## 🧪 Data Validation

- Validation handled via **Great Expectations** integrated into Airflow.
- Simple rule-based checks (e.g., non-null, valid ranges).
- Future-ready for HTML validation docs stored in MinIO (`s3://lakehouse/gx_docs/`).

---

## 🔍 Querying with Trino

### Access Trino UI
👉 [http://localhost:8082/ui](http://localhost:8082/ui)

### Access via CLI
```bash
docker compose -f docker-compose-advanced.yml exec trino-coordinator trino   --server http://localhost:8080 --catalog iceberg --schema bronze
```

Sample queries:
```sql
SHOW TABLES FROM iceberg.bronze;
SELECT * FROM merged_dataset LIMIT 10;
SELECT avg(carbon_avg), avg(temperature) FROM iceberg.gold.daily_summary;
```

---

## 📈 Observability & Monitoring

- **Prometheus** collects metrics from Trino, Airflow, and MinIO.
- **Grafana** dashboards available at → [http://localhost:3000](http://localhost:3000)
  - Username: `admin`
  - Password: `admin`

---

## 🧠 Future Enhancements

| Category | Description |
|-----------|--------------|
| **Streaming Ingestion** | Connect Kafka → Spark → Iceberg (real-time sustainability metrics) |
| **ML Feature Store** | Build a feature table (temperature vs carbon intensity trends) |
| **Data Lineage** | Integrate Apache Atlas or OpenLineage with Airflow |
| **Access Control** | Add Apache Ranger for policy-based governance |
| **Branching & Version Control** | Use Nessie branches for dev/prod sync and safe schema evolution |
| **CI/CD** | Automate pipeline testing using `pytest` + GitHub Actions |

---

## 🧰 Tech Stack

| Category | Tools |
|-----------|-------|
| **Data Orchestration** | Apache Airflow |
| **Storage** | MinIO (S3) |
| **Catalog & Versioning** | Apache Iceberg + Project Nessie |
| **Query Engine** | Trino |
| **Data Quality** | Great Expectations |
| **Processing** | Apache Spark |
| **Monitoring** | Prometheus + Grafana |
| **Containerization** | Docker Compose |

---

## 🧾 Useful Commands

### Airflow
```bash
docker compose -f docker-compose-advanced.yml exec airflow-webserver bash
airflow dags list
airflow dags trigger multi_source_sustainability_pipeline
```

### Trino
```bash
docker compose -f docker-compose-advanced.yml exec trino-coordinator trino   --server http://localhost:8080 --catalog iceberg --schema bronze
```

### MinIO
Login: [http://localhost:9001](http://localhost:9001)
```
Username: minioadmin
Password: minioadmin
```

---

## 🏁 Author

**Harish Naidu Gaddam**  
*Data Engineer | Cloud & Lakehouse Enthusiast*  
📧 hnaidugaddam@gmail.com  
🔗 [GitHub](https://github.com/harishg4) • [LinkedIn](https://linkedin.com/in/harish-gaddam)

---

## 🪪 License

This project is licensed under the [MIT License](LICENSE).

---

## 🌟 Summary

This project serves as a **blueprint for modern DataOps and Lakehouse architecture**, demonstrating how organizations can:
- Build **governed, versioned data pipelines**
- Orchestrate **multi-source ingestion**
- Manage **data quality and lineage**
- Enable **querying, monitoring, and automation** — all locally.
