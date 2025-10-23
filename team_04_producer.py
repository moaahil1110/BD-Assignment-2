#!/usr/bin/env python3
import pandas as pd
import json
import time
import socket
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

def test_kafka_connectivity(host, port, timeout=5):
    try:
        sock = socket.create_connection((host, port), timeout)
        sock.close()
        print(f"✅ Successfully connected to Kafka broker at {host}:{port}")
    except Exception as e:
        print(f"❌ Unable to connect to Kafka broker at {host}:{port}. Error: {e}")
        exit(1)

def main():
    print("="*65)
    print("SERVER METRICS PRODUCER (ZeroTier Multi-Machine)")
    print("Assignment 2: Apache Kafka + Spark")
    print("="*65)
    
    # Get broker ZeroTier IP from user
    broker_ip = input("Enter Kafka Broker ZeroTier IP: ").strip()
    if not broker_ip:
        print("❌ Broker IP required!")
        return

    kafka_bootstrap = f"{broker_ip}:9092"
    test_kafka_connectivity(broker_ip, 9092)

    # Initialize Kafka Admin Client for topic creation
    admin = KafkaAdminClient(bootstrap_servers=[kafka_bootstrap], client_id='producer_admin')
    topics = ["topic-cpu", "topic-mem", "topic-net", "topic-disk"]
    existing = admin.list_topics()
    topic_objs = []
    for t in topics:
        if t not in existing:
            topic_objs.append(NewTopic(name=t, num_partitions=2, replication_factor=1))
    if topic_objs:
        admin.create_topics(topic_objs, validate_only=False)
        print("✅ Topics created:", [t.name for t in topic_objs])
    else:
        print("ℹ Required topics already exist.")

    # Initialize producer
    producer = KafkaProducer(
        bootstrap_servers=[kafka_bootstrap],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None,
        acks='all'
    )

    # Load dataset
    try:
        df = pd.read_csv("dataset.csv")
        print(f"✅ Loaded dataset with {len(df)} records.")
    except Exception as e:
        print(f"❌ Error loading dataset.csv. Error: {e}")
        return

    print("🚀 Streaming metrics to Kafka every 1 second (per timestamp group)...")

    for ts, batch in df.groupby("ts"):
        now = datetime.now().strftime('%H:%M:%S')
        for _, row in batch.iterrows():
            server_id = row["server_id"]
            # Send each metric to its topic
            producer.send("topic-cpu", key=str(server_id), value={
                "timestamp": now, "original_ts": ts, "server_id": server_id, "cpu_pct": row["cpu_pct"]
            })
            producer.send("topic-mem", key=str(server_id), value={
                "timestamp": now, "original_ts": ts, "server_id": server_id, "mem_pct": row["mem_pct"]
            })
            producer.send("topic-net", key=str(server_id), value={
                "timestamp": now, "original_ts": ts, "server_id": server_id,
                "net_in": row["net_in"], "net_out": row["net_out"]
            })
            producer.send("topic-disk", key=str(server_id), value={
                "timestamp": now, "original_ts": ts, "server_id": server_id, "disk_io": row["disk_io"]
            })
        producer.flush()
        print(f"📦 Sent metrics for timestamp: {ts}, batch size: {len(batch)}")
        time.sleep(1)

    producer.close()
    print("🎉 Metrics streaming complete. All data sent.")

if _name_ == "_main_":
    main()
