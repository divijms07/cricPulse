# 🏏 CricPulse — Real-Time Cricket Data Pipeline with Airflow, Kafka & PySpark

CricPulse is an end-to-end **real-time cricket analytics pipeline** that simulates live match streams, processes ball-by-ball data using **Kafka + PySpark**, and orchestrates workflows using **Apache Airflow**.  
The system computes key cricket metrics like **strike rate, economy, team run rate**, and more — enabling contextual anomaly detection and insights.

---

## 🚀 Tech Stack
- **Apache Kafka** → Real-time message streaming
- **Apache PySpark** → Stream processing & feature computation
- **Apache Airflow** → Workflow orchestration & scheduling
- **PostgreSQL** → Airflow metadata database
- **Docker Compose** → Containerized environment
- **Python 3.11+**

---

## 🧩 Project Architecture
```
CricPulse/
│
├── airflow/
│   ├── dags/                → Airflow DAGs (workflow definitions)
│   ├── logs/                → Airflow runtime logs
│   ├── plugins/             → Custom Airflow plugins
│   ├── docker-compose.yml   → Airflow + PostgreSQL + Scheduler setup
│
├── kafka/
│   ├── docker-compose-kafka.yml → Kafka + Zookeeper setup
│
├── scripts/
│   ├── kafka_producer.py    → Sends cricket event JSONs to Kafka topic
│   ├── pyspark_consumer.py  → Consumes Kafka stream & processes with PySpark
│
├── data/
│   ├── bronze/              → Raw streamed data written by Spark
│   ├── ipl25/               → Sample IPL JSON files (input for producer)
│
├── checkpoints/             → Spark streaming checkpoints
│
├── requirements.txt         → Python dependencies
├── .gitignore               → Ignored files & folders
└── README.md                → Project documentation
```

---

## 🧠 Core Features
✅ Real-time data ingestion from simulated cricket JSON feeds  
✅ Kafka-based streaming between producer and PySpark consumer  
✅ Airflow DAGs to orchestrate ingestion, processing, and anomaly detection  
✅ Spark computation of:
- Batsman Strike Rate  
- Bowler Economy  
- Team Run Rate 
✅ PostgreSQL backend for Airflow (via Docker Compose)

---

## 🧪 Setup Guide — How to Run CricPulse

### 1️⃣ Clone the Repository
```bash
git clone https://github.com/<your-username>/CricPulse.git
cd CricPulse
```

### 2️⃣ Start Airflow (Webserver + Scheduler + PostgreSQL)
```bash
cd airflow
docker compose up -d
```
Visit **http://localhost:8080**  
Default login (after creating a user):
```
Username: admin
Password: admin
```

### 3️⃣ Start Kafka + Zookeeper
```bash
cd ../kafka
docker compose -f docker-compose-kafka.yml up -d
```

### 4️⃣ Produce Match Data
```bash
cd ../scripts
python kafka_producer.py
```

### 5️⃣ Consume Stream with PySpark
```bash
python pyspark_consumer.py
```

You’ll see computed metrics written to `/data/bronze/raw_events/` and streaming checkpoints saved under `/checkpoints/`.

---

## 🌟 Why This Project Stands Out
- **End-to-End Streaming System**: Integrates ingestion → processing → orchestration seamlessly.  
- **Portfolio-Ready**: Demonstrates strong Big Data, Cloud, and Realtime Engineering skills.  
- **Modular Design**: Each component (Airflow, Kafka, PySpark) is isolated for clarity.  

---

## ⚙️ Future Enhancements
- Add **Delta Lake** for bronze → silver → gold transformation  
- Deploy **Airflow on EC2 / GCP Composer**  
- Integrate **ML anomaly detection** using PySpark MLlib  
- Build a **Streamlit dashboard** for live match insights  

---
