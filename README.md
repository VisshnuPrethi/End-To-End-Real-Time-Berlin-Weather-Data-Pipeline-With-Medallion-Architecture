**🌦️ Real-Time Weather Analytics Pipeline (Berlin)**
An end-to-end Big Data solution utilizing a distributed Hadoop/Spark cluster to process and visualize Berlin's weather data through a Medallion Architecture. This project converts raw environmental telemetry into actionable business KPIs regarding energy costs and logistics risks.

**Overview**
This project implements an end‑to‑end big data pipeline that ingests real‑time weather data for berlin from the open-meteo “current weather data” api and processes it using a medallion (raw(bronze)/silver/gold) architecture on a distributed hadoop/spark cluster. The pipeline lands raw data in hdfs, cleans and aggregates it with spark, stores time‑series metrics in influxdb, and visualizes them on a grafana dashboard for near real‑time monitoring.

**The main objectives are:**
Ingest live api data into kafka and persist it reliably in a data lake (hdfs).
Build raw(bronze)/silver/gold layers for reproducible analytics and daily weather summaries.
Expose berlin weather kpis through influxdb and grafana for time‑series analysis.

**Data Source**
The pipeline uses the open-meteo current weather data api to fetch live measurements for berlin. The api is called via https with parameters specifying location and units
**https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m**

The json payload contains:
**Location:** coord.lat, coord.lon, name (city name).
Core weather metrics: main.temp, main.humidity, main.pressure, wind.speed, clouds.all, weather[0].description, and timestamp dt in unix utc.

**🏗️ Cluster Setup & Roles**
The pipeline runs on a distributed multi-node environment. Ensure all nodes are SSH-accessible.
Node,Role,Services
**Master:**"NameNode, ResourceManager","HDFS Master, YARN, Spark Master,
**Worker 1:**"DataNode, NodeManager","HDFS Storage, Spark Executor"
**Worker 2:**"DataNode, NodeManager","HDFS Storage, Spark Executor"

**Starting Services**
Execute these commands on the Master Node in order:

**Hadoop/HDFS:**
start-dfs.sh && start-yarn.sh

**Kafka (Zookeeper first):**
zookeeper-server-start.sh config/zookeeper.properties &
kafka-server-start.sh config/server.properties &

**InfluxDB:**
sudo systemctl start influxdb
systemctl is-active influxdb

**Grafana:**
**Reload systemd to recognize the new file**
sudo systemctl daemon-reload

**Enable it to start on boot**
sudo systemctl enable grafana

**Start the service**
sudo systemctl start grafana

**Check the status again**
sudo systemctl status grafana

🚀 **How to Run the Pipeline**
**1. Initialize Kafka Topic**
 kafka-topics.sh --create --topic weather_berlin --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2

**2. Start the Data Producer (Python)**
Fetches data from Open-Meteo and pushes to Kafka.
python3 weather_producer.py

**3. Submit the Spark Streaming Job**
Processes data and writes to InfluxDB and HDFS.
spark-submit --master yarn \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.influxdb:influxdb-client-java:6.7.0 \
  spark_processor.py

Software,Version
Java,OpenJDK 11
Hadoop,3.3.x
Spark,3.3.x
Kafka,3.2.x
Python,"3.9+ (Libraries: kafka-python, requests)"
InfluxDB,2.x

**📊 Monitoring & Visualization**
**Grafana Dashboard**

**Access:** Navigate to http://localhost:3000/

**Data Source:** Connect to InfluxDB using Flux (Port 8086).

**Operational View:** Live temperature, humidity, and wind trends for Berlin.

**Business View:**  Energy Surcharge ($): Hourly cost calculation for cooling.

**Productivity Loss:** Real-time man-hour risk audit.

**System Health:** Kafka consumer lag and Spark processing latency.

**Resource Management**

HDFS: http://localhost:8088/
InfluxDB: http://localhost:8086/
Grafana: http://localhost:3000/

**📈 Business Logic (Gold Layer)**
The pipeline doesn't just store data; it calculates:

**Energy Loss:** (Temp - 22°C) * $0.50 per hour.

**Logistics Risk:** High wind speeds (>50km/h) trigger "Critical" status in the Vulnerability Table.
**Conclusion:**
This project successfully delivers an end-to-end, production-style data pipeline for real-time berlin weather analytics, demonstrating mastery of ingestion, storage, processing, and visualization components in a distributed environment. The system reliably captures live data from a public weather api, streams it through kafka, and persists it in hdfs using a medallion (bronze/silver/gold) architecture that separates raw, cleaned, and business-ready layers for robust analytics and future backfills.

By integrating spark structured streaming and batch jobs, the pipeline performs schema enforcement, deduplication, and daily aggregations, turning noisy api events into high-quality features and kpis suitable for decision-making. The use of influxdb as a time-series database, combined with grafana dashboards, enables intuitive real-time and historical monitoring of key metrics such as temperature, humidity, pressure, and wind speed, highlighting the practical value of the architecture for operational observability.

Finally, deploying the solution on a multi-node cluster with clearly defined master and worker roles (for hdfs, yarn, spark, kafka, influxdb, and grafana) shows that the design is not only conceptually sound but also mindful of distributed systems concerns like scalability, fault tolerance, and service separation. Overall, the project meets the stated academic requirements while providing a realistic blueprint for real-world streaming analytics on weather data.
