# Big Data Assignment 2: Real-Time Server Metrics Streaming with Apache Kafka

## 📋 Overview

This project implements a real-time server metrics streaming system using **Apache Kafka** and **Apache Spark**. It demonstrates distributed computing concepts by streaming server performance metrics (CPU, Memory, Network, and Disk I/O) across multiple machines connected via ZeroTier VPN.

The system consists of:
- **Kafka Producer**: Streams server metrics from a CSV dataset to Kafka topics
- **Kafka Broker**: Manages message queuing and distribution
- **Kafka Consumers**: Process and aggregate metrics data (implementation varies)

## 🏗️ Architecture

```
[CSV Dataset] → [Python Producer] → [Kafka Broker] → [Multiple Topics]
                                          ↓
                                    topic-cpu
                                    topic-mem
                                    topic-net
                                    topic-disk
                                          ↓
                                   [Consumers/Spark]
```

### Data Flow
1. Producer reads server metrics from `team_04_CPU_MEM_RAW.csv`
2. Metrics are sent to separate Kafka topics based on metric type
3. Each message is keyed by `server_id` for proper partitioning
4. Consumers process streaming data in real-time
5. Results show windowed aggregations (30-second sliding windows)

## 🔧 Prerequisites

### Software Requirements
- **Apache Kafka** (with Zookeeper)
- **Python 3.x**
- **ZeroTier** (for multi-machine networking)

### Python Dependencies
```bash
pip install kafka-python pandas
```

## 🚀 Setup Instructions

### 1. ZeroTier Network Setup

Join the ZeroTier network on all machines (broker, producer, consumers):

```bash
sudo zerotier-cli join EBE7FBD4452A43CE
sudo zerotier-cli listnetworks
```

Verify connectivity between machines:
```bash
ping <broker-zerotier-ip>
ping <producer-zerotier-ip>
ping <consumer-zerotier-ip>
```

### 2. Kafka Broker Configuration

On the **Kafka Broker** machine:

#### Terminal 1: Start Zookeeper
```bash
cd /opt/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

#### Terminal 2: Configure and Start Kafka
```bash
# Configure Kafka to listen on all interfaces
sudo sed -i 's|#\?listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092|' /opt/kafka/config/server.properties

# Remove retention settings (if needed)
sudo sed -i '/retention.ms=/d' /opt/kafka/config/server.properties

# Verify configuration
grep -E "^(listeners|advertised.listeners|retention.ms|delete.topic.enable)=" /opt/kafka/config/server.properties

# Start Kafka
cd /opt/kafka
bin/kafka-server-start.sh config/server.properties
```

#### Terminal 3: Create Kafka Topics
```bash
cd /opt/kafka

# Create all required topics
for t in topic-cpu topic-mem topic-net topic-disk; do
  bin/kafka-topics.sh --bootstrap-server <broker-zerotier-ip>:9092 \
    --create --topic "$t" --partitions 1 --replication-factor 1 --if-not-exists
done

# List all topics
bin/kafka-topics.sh --bootstrap-server <broker-zerotier-ip>:9092 --list

# Describe a topic
bin/kafka-topics.sh --bootstrap-server <broker-zerotier-ip>:9092 --describe --topic topic-cpu
```

### 3. Producer Setup

On the **Producer** machine:

1. Ensure you have the dataset file: `team_04_CPU_MEM_RAW.csv` or `dataset.csv`
2. Run the producer script:

```bash
python3 team_04_producer.py
```

The producer will:
- Prompt for the Kafka Broker's ZeroTier IP address
- Test connectivity to the broker
- Create topics automatically if they don't exist
- Stream metrics to Kafka topics every 1 second per timestamp group

## 📊 Dataset Format

### Input Dataset (`team_04_CPU_MEM_RAW.csv`)
```csv
server_id,timestamp,cpu_pct,mem_pct
server_1,20:52:00,87.78,93.74
server_1,20:52:05,24.23,45.92
...
```

### Expected Output (`expected mem.csv`)
Windowed aggregations (30-second sliding windows with 10-second slide):
```csv
server_id,window_start,window_end,avg_cpu,avg_mem,alert
server_1,20:52:00,20:52:30,56.47,42.78,
server_1,20:52:10,20:52:40,57.9,24.99,
...
```

## 📝 File Descriptions

| File | Description |
|------|-------------|
| `team_04_producer.py` | Main producer script that streams metrics to Kafka |
| `team_04_CPU_MEM_RAW.csv` | Raw server metrics dataset |
| `expected mem.csv` | Expected output showing windowed aggregations |
| `kafka-broker-commands.txt` | Reference commands for Kafka broker setup |

## 🔍 Producer Features

The producer script (`team_04_producer.py`) includes:

- ✅ **Connectivity Testing**: Validates connection to Kafka broker before streaming
- ✅ **Automatic Topic Creation**: Creates required topics if they don't exist
- ✅ **Multi-Topic Streaming**: Sends different metrics to separate topics
- ✅ **Key-Based Partitioning**: Uses `server_id` as message key for proper distribution
- ✅ **Batch Processing**: Groups data by timestamp for realistic streaming
- ✅ **Error Handling**: Graceful error handling and informative messages

### Message Format

Each message sent to Kafka includes:
```json
{
  "timestamp": "05:47:30",
  "original_ts": "20:52:00",
  "server_id": "server_1",
  "cpu_pct": 87.78,
  "mem_pct": 93.74,
  "net_in": 1234,
  "net_out": 5678,
  "disk_io": 910
}
```

## 🎯 Usage Example

1. **Start Kafka Infrastructure** (on broker machine):
   ```bash
   # Terminal 1: Zookeeper
   cd /opt/kafka && bin/zookeeper-server-start.sh config/zookeeper.properties
   
   # Terminal 2: Kafka
   cd /opt/kafka && bin/kafka-server-start.sh config/server.properties
   ```

2. **Run Producer** (on producer machine):
   ```bash
   python3 team_04_producer.py
   # Enter broker IP when prompted: 192.168.191.50
   ```

3. **Expected Output**:
   ```
   =================================================================
   SERVER METRICS PRODUCER (ZeroTier Multi-Machine)
   Assignment 2: Apache Kafka + Spark
   =================================================================
   Enter Kafka Broker ZeroTier IP: 192.168.191.50
   ✅ Successfully connected to Kafka broker at 192.168.191.50:9092
   ✅ Topics created: ['topic-cpu', 'topic-mem', 'topic-net', 'topic-disk']
   ✅ Loaded dataset with 1000 records.
   🚀 Streaming metrics to Kafka every 1 second (per timestamp group)...
   📦 Sent metrics for timestamp: 20:52:00, batch size: 4
   📦 Sent metrics for timestamp: 20:52:05, batch size: 4
   ...
   🎉 Metrics streaming complete. All data sent.
   ```

## 🐛 Troubleshooting

### Connection Issues
```bash
# Check if Kafka is listening
netstat -tulpn | grep 9092

# Test connectivity from producer machine
telnet <broker-zerotier-ip> 9092

# Verify ZeroTier network
sudo zerotier-cli listnetworks
```

### Topic Issues
```bash
# List all topics
bin/kafka-topics.sh --bootstrap-server <broker-ip>:9092 --list

# Delete a topic (if needed)
bin/kafka-topics.sh --bootstrap-server <broker-ip>:9092 --delete --topic topic-cpu
```

### Consumer Testing
```bash
# Test consuming messages from a topic
bin/kafka-console-consumer.sh --bootstrap-server <broker-ip>:9092 \
  --topic topic-cpu --from-beginning
```

## 📚 Key Concepts Demonstrated

- **Distributed Messaging**: Using Kafka for reliable message delivery
- **Topic-Based Architecture**: Separating metrics by type into different topics
- **Partitioning**: Using message keys for proper data distribution
- **Real-Time Streaming**: Simulating live server metrics streaming
- **Multi-Machine Setup**: Using ZeroTier for distributed system networking
- **Data Serialization**: JSON serialization for message payloads

## 👥 Team Information

- **Team**: Team 04
- **Assignment**: Big Data Assignment 2
- **Topic**: Apache Kafka + Apache Spark for Real-Time Data Processing

## 📄 License

This is an educational project for Big Data coursework.

---

**Note**: Ensure all machines are properly connected via ZeroTier before starting the producer and consumers. The broker must be running and accessible from the producer machine.
