# Real-time Server Monitoring with Apache Kafka and Spark

## 📋 Project Overview

This project implements a **distributed real-time server monitoring pipeline** using Apache Kafka and Apache Spark for Big Data Assignment 2 (Team 4). The system collects, streams, processes, and analyzes server metrics (CPU, Memory, Network, and Disk) across multiple machines connected via ZeroTier VPN.

### Architecture

The pipeline consists of:
- **1 Producer Machine**: Streams server metrics to Kafka topics
- **1 Kafka Broker Machine**: Manages Kafka topics and message routing
- **2 Consumer Machines**: Consume and store metrics data
  - Consumer 1: Handles CPU and Memory metrics
  - Consumer 2: Handles Network and Disk metrics
- **2 Spark Jobs**: Process windowed analytics and generate alerts

```
┌─────────────┐         ┌──────────────┐         ┌─────────────┐
│  Producer   │────────>│ Kafka Broker │────────>│ Consumer 1  │
│             │         │              │         │ (CPU/MEM)   │
│ Streams:    │         │  Topics:     │         └─────────────┘
│ - CPU       │         │  - topic-cpu │                │
│ - Memory    │         │  - topic-mem │                │
│ - Network   │         │  - topic-net │                v
│ - Disk      │         │  - topic-disk│         ┌─────────────┐
└─────────────┘         └──────────────┘         │  Spark Job 1│
                               │                 │ (CPU/MEM)   │
                               │                 └─────────────┘
                               │
                               v                 ┌─────────────┐
                        ┌─────────────┐         │ Consumer 2  │
                        │             │<────────│ (NET/DISK)  │
                        │  ZeroTier   │         └─────────────┘
                        │     VPN     │                │
                        │             │                │
                        └─────────────┘                v
                                                ┌─────────────┐
                                                │ Spark Job 2 │
                                                │ (NET/DISK)  │
                                                └─────────────┘
```

## 🎯 Features

- **Real-time Data Streaming**: Streams server metrics via Kafka in real-time
- **Distributed Processing**: Multi-machine setup with ZeroTier VPN for network connectivity
- **Windowed Analytics**: 30-second sliding windows with 10-second intervals
- **Smart Alerting**: Automatic anomaly detection based on configurable thresholds
- **Data Persistence**: Stores raw metrics and processed analytics in CSV format

### Alert Types

**CPU & Memory Alerts:**
- `CPU spike suspected`: CPU usage ≥ 80.07%
- `Memory saturation suspected`: Memory usage ≥ 89.36%
- `CPU and Memory spike`: Both thresholds exceeded

**Network & Disk Alerts:**
- `Possible DDoS`: Network traffic ≥ 6104.83 KB/s
- `Disk thrash suspected`: Disk I/O ≥ 1839.94 ops/s
- `Network flood + Disk thrash suspected`: Both thresholds exceeded

## 📦 Prerequisites

### Software Requirements

- **Python 3.7+**
- **Apache Kafka** (installed on broker machine)
- **Apache Spark 3.x** (for Spark jobs)
- **ZeroTier** (for VPN connectivity across machines)

### Python Dependencies

```bash
pip install kafka-python pandas pyspark
```

### Required Files

- `dataset.csv`: Server metrics dataset (required for producer)

## 🚀 Setup Instructions

### 1. ZeroTier Network Setup

Install ZeroTier on all machines:

```bash
# Install ZeroTier
curl -s https://install.zerotier.com | sudo bash

# Join the ZeroTier network
sudo zerotier-cli join EBE7FBD4452A43CE

# Verify connection
sudo zerotier-cli listnetworks
```

**IP Assignments (Example):**
- Broker Machine: `192.168.191.50`
- Producer Machine: `192.168.191.233`
- Consumer 1 Machine: `192.168.191.42`
- Consumer 2 Machine: `192.168.191.83`

Test connectivity between machines:
```bash
ping 192.168.191.50   # Ping broker from any machine
```

### 2. Kafka Broker Setup

On the **Broker Machine**, configure and start Kafka:

#### Configure Kafka Server
```bash
# Edit server.properties to allow external connections
sudo sed -i 's|#\?listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092|' /opt/kafka/config/server.properties

# Verify configuration
grep -E "^(listeners|advertised.listeners|retention.ms|delete.topic.enable)=" /opt/kafka/config/server.properties
```

#### Start Zookeeper (Terminal 1)
```bash
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

#### Start Kafka Server (Terminal 2)
```bash
cd /opt/kafka
bin/kafka-server-start.sh config/server.properties
```

#### Create Kafka Topics (Terminal 3)
```bash
cd /opt/kafka

# Create all required topics
for t in topic-cpu topic-mem topic-net topic-disk; do
  bin/kafka-topics.sh --bootstrap-server 192.168.191.50:9092 \
    --create --topic "$t" --partitions 1 --replication-factor 1 --if-not-exists
done

# Verify topics
bin/kafka-topics.sh --bootstrap-server 192.168.191.50:9092 --list

# Describe a topic (optional)
bin/kafka-topics.sh --bootstrap-server 192.168.191.50:9092 --describe --topic topic-cpu
```

### 3. Producer Setup

On the **Producer Machine**:

1. Ensure `dataset.csv` is in the same directory as the producer script
2. Run the producer:

```bash
python3 team_04_producer.py
```

3. When prompted, enter the **Kafka Broker ZeroTier IP** (e.g., `192.168.191.50`)

The producer will:
- Test connectivity to the Kafka broker
- Create topics if they don't exist
- Stream metrics from `dataset.csv` to Kafka topics
- Send one batch per second (grouped by timestamp)

### 4. Consumer Setup

#### Consumer 1 (CPU & Memory)

On **Consumer 1 Machine**:

```bash
python3 team_04_consumer1.py
```

When prompted, enter the **Kafka Broker ZeroTier IP**.

**Output Files:**
- `cpu_data.csv`: Raw CPU metrics
- `mem_data.csv`: Raw memory metrics

#### Consumer 2 (Network & Disk)

On **Consumer 2 Machine**:

```bash
python3 team_04_consumer2.py
```

When prompted, enter the **Kafka Broker ZeroTier IP**.

**Output Files:**
- `net_data.csv`: Raw network metrics
- `disk_data.csv`: Raw disk metrics

### 5. Spark Jobs Execution

After consumers have collected data, run the Spark jobs to perform windowed analytics:

#### Spark Job 1 (CPU & Memory Analytics)

```bash
python3 team_04_sparkjob1.py \
  --cpu cpu_data.csv \
  --mem mem_data.csv \
  --out team_04_CPU_MEM.csv \
  --cpu-threshold 80.07 \
  --mem-threshold 89.36
```

**Output:** `team_04_CPU_MEM.csv` with columns:
- `server_id`: Server identifier
- `window_start`: Window start time (HH:MM:SS)
- `window_end`: Window end time (HH:MM:SS)
- `avg_cpu`: Average CPU usage in the window
- `avg_mem`: Average memory usage in the window
- `alert`: Alert message (if any threshold is exceeded)

#### Spark Job 2 (Network & Disk Analytics)

```bash
python3 team_04_sparkjob2.py \
  --net net_data.csv \
  --disk disk_data.csv \
  --out team_04_NET_DISK.csv \
  --network-threshold 6104.83 \
  --disk-threshold 1839.94
```

**Output:** `team_04_NET_DISK.csv` with columns:
- `server_id`: Server identifier
- `window_start`: Window start time (HH:MM:SS)
- `window_end`: Window end time (HH:MM:SS)
- `max_net_in`: Maximum network inbound traffic in the window (KB/s)
- `max_disk_io`: Maximum disk I/O in the window (ops/s)
- `alert`: Alert message (if any threshold is exceeded)

## 📊 Data Flow

1. **Producer** reads `dataset.csv` and streams metrics to Kafka topics
2. **Kafka Broker** manages and distributes messages across topics
3. **Consumers** subscribe to their assigned topics and save raw data to CSV
4. **Spark Jobs** process raw CSV data:
   - Apply 30-second sliding windows (10-second slide)
   - Calculate aggregations (avg for CPU/MEM, max for NET/DISK)
   - Drop first 2 windows per server (warm-up period)
   - Generate alerts based on thresholds
   - Output processed analytics to final CSV files

## 🔧 Configuration

### Team 4 Thresholds

The following thresholds are configured in the code:

| Metric | Threshold | Consumer | Alert Type |
|--------|-----------|----------|------------|
| CPU | 80.07% | Consumer 1 | CPU spike suspected |
| Memory | 89.36% | Consumer 1 | Memory saturation suspected |
| Network | 6104.83 KB/s | Consumer 2 | Possible DDoS |
| Disk I/O | 1839.94 ops/s | Consumer 2 | Disk thrash suspected |

### Windowing Configuration

- **Window Size**: 30 seconds
- **Slide Interval**: 10 seconds
- **Warm-up Windows**: First 2 windows per server are dropped
- **Start Offset**: Aligned with earliest event timestamp (modulo 10s)

## 🛠️ Troubleshooting

### Common Issues

#### 1. Cannot Connect to Kafka Broker

**Symptoms:**
```
❌ Unable to connect to Kafka broker at <IP>:9092
```

**Solutions:**
- Verify ZeroTier is running: `sudo zerotier-cli listnetworks`
- Check broker IP is correct
- Ensure Kafka is running on broker machine
- Test connectivity: `ping <broker-ip>`
- Check firewall rules allow port 9092

#### 2. Topics Not Found

**Symptoms:**
```
❌ Topic does not exist
```

**Solutions:**
- Create topics manually (see Kafka Broker Setup)
- Ensure producer has created topics before starting consumers
- Verify topics exist: `bin/kafka-topics.sh --list --bootstrap-server <broker-ip>:9092`

#### 3. Consumer Timeout

**Symptoms:**
```
✅ Consumer timeout reached - All messages consumed!
```

**Note:** This is **normal behavior**. The consumer stops after 30 seconds of inactivity, indicating all messages have been consumed.

#### 4. Spark Job Errors

**Symptoms:**
```
❌ Input CSV files not found
```

**Solutions:**
- Ensure consumers have completed and generated CSV files
- Check CSV files exist: `ls -la *.csv`
- Verify file paths in Spark job arguments

#### 5. Empty/Null Timestamps

**Symptoms:**
```
❌ All timestamps are null/empty; cannot window
```

**Solutions:**
- Check producer is sending timestamps correctly
- Verify CSV files contain valid `ts` column with HH:MM:SS format
- Ensure data was collected properly by consumers

## 📝 File Descriptions

### Python Scripts

| File | Description |
|------|-------------|
| `team_04_producer.py` | Streams server metrics from dataset.csv to Kafka topics |
| `team_04_consumer1.py` | Consumes CPU and Memory metrics, saves to CSV |
| `team_04_consumer2.py` | Consumes Network and Disk metrics, saves to CSV |
| `team_04_sparkjob1.py` | Spark analytics for CPU/Memory with windowing |
| `team_04_sparkjob2.py` | Spark analytics for Network/Disk with windowing |

### Configuration Files

| File | Description |
|------|-------------|
| `kafka-broker-commands.txt` | Reference commands for Kafka broker setup |
| `dataset.csv` | Input dataset with server metrics (user-provided) |

### Generated Files

| File | Description |
|------|-------------|
| `cpu_data.csv` | Raw CPU metrics from Consumer 1 |
| `mem_data.csv` | Raw memory metrics from Consumer 1 |
| `net_data.csv` | Raw network metrics from Consumer 2 |
| `disk_data.csv` | Raw disk metrics from Consumer 2 |
| `team_04_CPU_MEM.csv` | Final analytics for CPU/Memory with alerts |
| `team_04_NET_DISK.csv` | Final analytics for Network/Disk with alerts |

## 🎓 Assignment Information

- **Course**: Big Data Analytics
- **Assignment**: Assignment 2 - Apache Kafka + Spark
- **Team**: Team 4
- **Topic**: Real-time Server Monitoring Distributed Pipeline

## 👥 Team Members

Team 4

## 📄 License

This project is created for educational purposes as part of a Big Data course assignment.

## 🔗 Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [ZeroTier Documentation](https://docs.zerotier.com/)
- [Kafka Python Client](https://kafka-python.readthedocs.io/)

## 🚦 Quick Start Guide

For a quick deployment, follow these steps in order:

1. **Setup ZeroTier** on all machines and verify connectivity
2. **Start Kafka** (Zookeeper → Kafka Server → Create Topics)
3. **Start Consumers** (Consumer 1 and Consumer 2 in parallel)
4. **Start Producer** (begins streaming data)
5. **Wait for completion** (consumers will timeout after processing all data)
6. **Run Spark Jobs** (Job 1 and Job 2 can run in parallel)
7. **Review Results** (check generated CSV files for analytics and alerts)

---

**Note**: Ensure all machines are on the same ZeroTier network before starting the pipeline. Monitor console output for real-time progress and statistics.
