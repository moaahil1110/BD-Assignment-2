#!/usr/bin/env python3
"""
Consumer 1: CPU and Memory Metrics Processing (TEAM 4 - KAFKA ONLY VERSION)
Assignment 2: Real-time Server Monitoring Distributed Pipeline
To be deployed on Consumer 1 Machine - TEAM 4
Handles topic-cpu and topic-mem - Collects data and saves to CSV
"""

from kafka import KafkaConsumer
import json
import pandas as pd
import sys
import socket
import threading
import logging

class CPUMemoryConsumer:
    def __init__(self, broker_zerotier_ip):
        """Initialize Consumer 1 for CPU and Memory metrics"""
        self.broker_servers = [f'{broker_zerotier_ip}:9092']
        self.topics = ['topic-cpu', 'topic-mem']
        
        # Assignment thresholds (REPLACE WITH YOUR TEAM 4's VALUES)
        self.cpu_threshold = 80.07
        self.memory_threshold = 89.36
        
        # Data storage
        self.cpu_data = []
        self.memory_data = []
        self.data_lock = threading.Lock()
        
        # Message tracking for console display
        self.cpu_message_count = 0
        self.mem_message_count = 0
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Test connectivity
        self.test_broker_connectivity(broker_zerotier_ip, 9092)
        
        print("✅ Consumer 1 (CPU/Memory) initialized successfully")
        print(f"📡 Connected to broker: {self.broker_servers[0]}")
        print(f"🔥 CPU threshold: {self.cpu_threshold}%")
        print(f"💾 Memory threshold: {self.memory_threshold}%")

    def test_broker_connectivity(self, host, port, timeout=10):
        """Test if broker is reachable via ZeroTier"""
        try:
            sock = socket.create_connection((host, port), timeout)
            sock.close()
            print(f"✅ ZeroTier connection to broker {host}:{port} successful")
        except socket.error:
            print(f"❌ Cannot connect to broker {host}:{port}")
            sys.exit(1)

    def create_kafka_consumer(self):
        """Create Kafka consumer for CPU and Memory topics"""
        try:
            consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.broker_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='cpu-memory-consumer-group',
                enable_auto_commit=True,
                consumer_timeout_ms=30000  # 30 second timeout for auto-stop
            )
            print(f"✅ Kafka Consumer created for topics: {self.topics}")
            return consumer
        except Exception as e:
            print(f"❌ Failed to create Kafka consumer: {e}")
            sys.exit(1)

    def display_message_details(self, topic, value, msg_num):
        """Display received message details on console"""
        print(f"\n{'='*70}")
        print(f"📨 MESSAGE #{msg_num} | Topic: {topic}")
        print(f"{'='*70}")
        
        if isinstance(value, dict):
            print(f"  🕐 Timestamp    : {value.get('original_ts', 'N/A')}")
            print(f"  🖥️  Server ID    : {value.get('server_id', 'N/A')}")
            
            if topic == 'topic-cpu':
                print(f"  💻 CPU Usage    : {value.get('cpu_pct', 'N/A')}%")
            elif topic == 'topic-mem':
                print(f"  💾 Memory Usage : {value.get('mem_pct', 'N/A')}%")
        else:
            print(f"  ⚠️  Raw data: {value}")
        
        print(f"{'='*70}\n")

    def save_raw_data_to_csv(self):
        """Save raw data to CSV files - HANDLES MISSING DATA"""
        with self.data_lock:
            # Always save CPU data if it exists
            if self.cpu_data:
                cpu_df = pd.DataFrame(self.cpu_data)
                cpu_df.to_csv('cpu_data.csv', index=False)
                print(f"💾 Saved {len(self.cpu_data)} CPU records to cpu_data.csv")
            else:
                # Create empty CPU file with proper structure
                empty_cpu_df = pd.DataFrame(columns=['ts', 'server_id', 'cpu_pct'])
                empty_cpu_df.to_csv('cpu_data.csv', index=False)
                print(f"⚠️ Created empty cpu_data.csv (no CPU messages received)")
            
            # Always save Memory data if it exists
            if self.memory_data:
                mem_df = pd.DataFrame(self.memory_data)
                mem_df.to_csv('mem_data.csv', index=False)
                print(f"💾 Saved {len(self.memory_data)} memory records to mem_data.csv")
            else:
                # Create empty Memory file with proper structure
                empty_mem_df = pd.DataFrame(columns=['ts', 'server_id', 'mem_pct'])
                empty_mem_df.to_csv('mem_data.csv', index=False)
                print(f"⚠️ Created empty mem_data.csv (no memory messages received)")

    def start_consuming(self):
        """Start consuming messages from Kafka topics with enhanced display"""
        print("\n" + "="*70)
        print("🚀 STARTING KAFKA CONSUMPTION")
        print("="*70)
        print(f"📡 Topics: {self.topics}")
        print(f"📍 Broker: {self.broker_servers[0]}")
        print("="*70 + "\n")
        
        consumer = self.create_kafka_consumer()
        total_message_count = 0
        
        try:
            print("👂 Listening for messages...\n")
            
            for message in consumer:
                topic = message.topic
                value = message.value
                total_message_count += 1
                
                # Track per-topic counts
                if topic == 'topic-cpu':
                    self.cpu_message_count += 1
                elif topic == 'topic-mem':
                    self.mem_message_count += 1
                
                # Display message details on console
                self.display_message_details(topic, value, total_message_count)
                
                # Use only the producer's original HH:MM:SS timestamp field
                ts_value = value.get('original_ts') if isinstance(value, dict) else None
                
                with self.data_lock:
                    if topic == 'topic-cpu':
                        self.cpu_data.append({
                            'ts': ts_value,
                            'server_id': value.get('server_id'),
                            'cpu_pct': value.get('cpu_pct')
                        })
                    elif topic == 'topic-mem':
                        # Use only the 'mem_pct' field provided by the producer
                        mem_val = value.get('mem_pct') if isinstance(value, dict) else None
                        self.memory_data.append({   
                            'ts': ts_value,
                            'server_id': value.get('server_id') if isinstance(value, dict) else None,
                            'mem_pct': mem_val
                        })
                
                # Progress summary every 50 messages
                if total_message_count % 50 == 0:
                    print(f"\n{'🎯 PROGRESS SUMMARY':^70}")
                    print(f"{'='*70}")
                    print(f"  Total Messages  : {total_message_count}")
                    print(f"  CPU Messages    : {self.cpu_message_count}")
                    print(f"  Memory Messages : {self.mem_message_count}")
                    print(f"  CPU Records     : {len(self.cpu_data)}")
                    print(f"  Memory Records  : {len(self.memory_data)}")
                    print(f"{'='*70}\n")
                
                # Save data periodically
                if total_message_count % 100 == 0:
                    print("💾 Performing periodic save...")
                    self.save_raw_data_to_csv()
                    
        except KeyboardInterrupt:
            print("\n⏸️ Consumption interrupted by user")
        except Exception as e:
            if "timeout" in str(e).lower():
                print("\n✅ Consumer timeout reached - All messages consumed!")
            else:
                self.logger.error(f"Error during consumption: {e}")
        finally:
            # Display final statistics
            print("\n" + "="*70)
            print("📊 FINAL CONSUMPTION STATISTICS")
            print("="*70)
            print(f"  Total Messages Received : {total_message_count}")
            print(f"  CPU Topic Messages      : {self.cpu_message_count}")
            print(f"  Memory Topic Messages   : {self.mem_message_count}")
            print(f"  CPU Records Stored      : {len(self.cpu_data)}")
            print(f"  Memory Records Stored   : {len(self.memory_data)}")
            print("="*70 + "\n")
            
            print("💾 Saving final data to CSV files...")
            self.save_raw_data_to_csv()
            
            consumer.close()
            print("\n✅ Kafka 1 closed successfully")

def main():
    """Main function to run Consumer 1"""
    print("=" * 70)
    print("🔥 CONSUMER 1: CPU & MEMORY PROCESSOR (TEAM 4)")
    print("📋 Assignment 2: Real-time Monitoring Pipeline")  
    print("="* 70)
    
    # Get broker ZeroTier IP from user
    broker_ip = input("\n🎯 Enter Broker ZeroTier IP: ").strip()
    
    if not broker_ip:
        print("❌ Broker IP is required!")
        sys.exit(1)
    
    print(f"\n{'⚙️  CONFIGURATION':^70}")
    print("=" * 70)
    print(f"  📡 Kafka Broker      : {broker_ip}:9092")
    print(f"  📊 Processing Metrics: CPU + Memory")
    print(f"  🎯 Kafka Topics      : topic-cpu, topic-mem")
    print(f"  📁 Output File       : team_04_CPU_MEM.csv")
    print(f"  🔥 CPU Threshold     : 80.07%")
    print(f"  💾 Memory Threshold  : 89.36%")
    print(f"  👥 Team ID           : 4")
    print("=" * 70)
    
    consumer = CPUMemoryConsumer(broker_ip)
    
    try:
        consumer.start_consuming()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n" + "=" * 70)
        print("👋 CONSUMER 1 FINISHED")
        print("=" * 70)

if __name__ == "__main__":
    main()