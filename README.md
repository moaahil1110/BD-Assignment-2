```
Real-Time Server Monitoring with Apache Kafka and Spark
📋 Overview
This project demonstrates a real-time distributed server monitoring pipeline using Apache Kafka and Apache Spark. It monitors CPU, memory, network, and disk utilization across multiple distributed machines connected through ZeroTier VPN. The system performs real-time streaming, aggregation, and anomaly detection using Spark analytics.

🏗️ Architecture
The pipeline includes these components:

Producer: Streams metrics from dataset to Kafka topics.

Kafka Broker: Manages topics and message routing.

Consumers: Collect categorized metrics for analysis.

Spark Jobs: Perform windowed aggregation and alert computation.


┌─────────────┐         ┌──────────────┐
│  Producer   │────────>│ Kafka Broker │
│             │         │              │
│ Streams:    │         │  Topics:     │
│ - CPU       │         │  - topic-cpu │
│ - Memory    │         │  - topic-mem │
│ - Network   │         │  - topic-net │
│ - Disk      │         │  - topic-disk│
└─────────────┘         └──────────────┘
                               │
                               │
                               v
                        ┌─────────────┐
                        │  ZeroTier   │
                        │     VPN     │
                        └─────────────┘
                           │       │
             (via ZeroTier)│       │(via ZeroTier)
                           │       │
                           v       v
                    ┌─────────────┐ ┌─────────────┐
                    │ Consumer 1  │ │ Consumer 2  │
                    │ (CPU/MEM)   │ │ (NET/DISK)  │
                    └─────────────┘ └─────────────┘
                           │               │
                           v               v
                    ┌─────────────┐ ┌─────────────┐
                    │ Spark Job 1 │ │ Spark Job 2 │
                    │ (CPU/MEM)   │ │ (NET/DISK)  │
                    └─────────────┘ └─────────────┘
Both Consumer 1 and Consumer 2 connect to the broker only through ZeroTier, ensuring secure networking between distributed nodes.

🎯 Features
Real-time Kafka-based metric ingestion

Multi-machine distributed processing

30 s windowed Spark analytics (10 s slide)

Automated alert detection with configurable thresholds

Structured data persistence in CSV format

⚠️ Alert Criteria
CPU & Memory Alerts

CPU ≥ 80%: Performance spike suspected

Memory ≥ 89%: Memory saturation suspected

Both thresholds = Intense resource spike

Network & Disk Alerts

Network ≥ 6100 KB/s: Potential DDoS detection

Disk I/O ≥ 1800 ops/s: Disk thrash warning

Combined event = Sustained load alert

📦 Prerequisites
Software:

Python 3.7 or later

Apache Kafka (with Zookeeper)

Apache Spark 3.x

ZeroTier (for VPN networking)

Python Dependencies:

text
pip install kafka-python pandas pyspark
Dataset (Required):

text
dataset.csv
Sample fields:

text
server_id,timestamp,cpu_pct,mem_pct,net_in,net_out,disk_io
server_1,20:52:00,87.78,93.74,1234,5678,910
🚀 Setup Instructions
1. ZeroTier Network Setup
Install ZeroTier on all machines and join the same network:

text
curl -s https://install.zerotier.com | sudo bash
sudo zerotier-cli join <network-id>
sudo zerotier-cli listnetworks
Verify connectivity:

text
ping <broker-zerotier-ip>
2. Kafka Broker Setup
Start Zookeeper and Kafka:

text
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
Create topics:

text
for t in topic-cpu topic-mem topic-net topic-disk; do
  bin/kafka-topics.sh --bootstrap-server <broker-zerotier-ip>:9092 \
  --create --topic "$t" --partitions 1 --replication-factor 1 --if-not-exists
done
3. Producer Execution
Run on producer machine:

text
python3 producer.py
Prompts for Kafka broker’s ZeroTier IP and streams metrics in real-time.

4. Consumers Execution
Run on Consumer machines:

text
python3 consumer_cpu_mem.py     # handles CPU & Memory
python3 consumer_net_disk.py    # handles Network & Disk
Generated outputs:

text
cpu_data.csv
mem_data.csv
net_data.csv
disk_data.csv
5. Spark Jobs
Aggregate metrics after consumption:

text
python3 spark_cpu_mem.py --cpu cpu_data.csv --mem mem_data.csv --out CPU_MEM.csv
python3 spark_net_disk.py --net net_data.csv --disk disk_data.csv --out NET_DISK.csv
Outputs time-windowed data and alert status.

📊 Pipeline Flow
Producer publishes metrics → Kafka topics.

Broker distributes messages to topic subscribers.

Consumers persist metrics into CSV storage.

Spark processes windowed aggregations.

Alerts trigger when thresholds are exceeded.

🧩 File Structure
File	Description
 producer.py 	Kafka producer that streams metrics.
 consumer_cpu_mem.py 	CPU & memory consumer module.
 consumer_net_disk.py 	Network & disk consumer module.
 spark_cpu_mem.py 	Spark analytics for CPU & Memory.
 spark_net_disk.py 	Spark analytics for Network & Disk.
 dataset.csv 	Input metrics dataset file.
 kafka-broker-commands.txt 	Reference Kafka shell commands.
🛠️ Troubleshooting
Kafka Broker Unreachable:
Check if Kafka is listening on port 9092 and ZeroTier IP reachable.

Topic Errors:
Ensure topics exist using:

text
bin/kafka-topics.sh --list --bootstrap-server <broker-zerotier-ip>:9092
Consumer Timeout:
Expected after all messages processed (success indicator).

Spark File Not Found:
Ensure consumers generated CSV files before running Spark jobs.

📄 License
This repository demonstrates a real-time streaming data pipeline for distributed system monitoring using Apache Kafka and Apache Spark.
Intended for educational, research, and portfolio use.
```
