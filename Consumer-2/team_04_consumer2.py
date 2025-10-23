#!/usr/bin/env python3
"""
Consumer 2: Network and Disk Metrics Processing (TEAM 4 - KAFKA ONLY VERSION)
Assignment 2: Real-time Server Monitoring Distributed Pipeline
To be deployed on Consumer 2 Machine  
Handles topic-net and topic-disk - Collects data and saves to CSV
"""

from kafka import KafkaConsumer
import json
import pandas as pd
import sys
import socket
import threading
import logging

class NetworkDiskConsumer:
    def __init__(self, broker_zerotier_ip):
        """Initialize Consumer 2 for Network and Disk metrics"""
        self.broker_servers = [f'{broker_zerotier_ip}:9092']
        self.topics = ['topic-net', 'topic-disk']
        
        # Assignment thresholds (REPLACE WITH YOUR TEAM 4's VALUES)
        self.network_threshold = 6104.83
        self.disk_threshold = 1839.94
        
        # Data storage
        self.network_data = []
        self.disk_data = []
        self.data_lock = threading.Lock()
        
        # Message tracking for console display
        self.net_message_count = 0
        self.disk_message_count = 0
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Test connectivity
        self.test_broker_connectivity(broker_zerotier_ip, 9092)
        
        print("✅ Consumer 2 (Network/Disk) initialized successfully")
        print(f"📡 Connected to broker: {self.broker_servers[0]}")
        print(f"🌐 Network threshold: {self.network_threshold}")
        print(f"💽 Disk threshold: {self.disk_threshold}")

    def test_broker_connectivity(self, host, port, timeout=10):
        """Test if broker is reachable via ZeroTier"""
        try:
            sock = socket.create_connection((host, port), timeout)
            sock.close()
            print(f"✅ ZeroTier connection to broker {host}:{port} successful")
        except socket.error:
            print(f"❌ Cannot connect to broker {host}:{port}")
            print("💡 Check: ZeroTier connection, Kafka running, firewall")
            sys.exit(1)

    def create_kafka_consumer(self):
        """Create Kafka consumer for Network and Disk topics"""
        try:
            consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.broker_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='network-disk-consumer-group',
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
            
            if topic == 'topic-net':
                print(f"  📥 Network In   : {value.get('net_in', 'N/A')} KB/s")
                print(f"  📤 Network Out  : {value.get('net_out', 'N/A')} KB/s")
            elif topic == 'topic-disk':
                print(f"  💽 Disk I/O     : {value.get('disk_io', 'N/A')} ops/s")
        else:
            print(f"  ⚠️  Raw data: {value}")
        
        print(f"{'='*70}\n")

    def save_raw_data_to_csv(self):
        """Save raw data to CSV files - HANDLES MISSING DATA"""
        with self.data_lock:
            # Always save network data if it exists
            if self.network_data:
                net_df = pd.DataFrame(self.network_data)
                net_df.to_csv('net_data.csv', index=False)
                print(f"💾 Saved {len(self.network_data)} network records to net_data.csv")
            else:
                # Create empty network file with proper structure
                empty_net_df = pd.DataFrame(columns=['ts', 'server_id', 'net_in', 'net_out'])
                empty_net_df.to_csv('net_data.csv', index=False)
                print(f"⚠️ Created empty net_data.csv (no network messages received)")
            
            # Always save disk data if it exists
            if self.disk_data:
                disk_df = pd.DataFrame(self.disk_data)
                disk_df.to_csv('disk_data.csv', index=False)
                print(f"💾 Saved {len(self.disk_data)} disk records to disk_data.csv")
            else:
                # Create empty disk file with proper structure
                empty_disk_df = pd.DataFrame(columns=['ts', 'server_id', 'disk_io'])
                empty_disk_df.to_csv('disk_data.csv', index=False)
                print(f"⚠️ Created empty disk_data.csv (no disk messages received)")

    def start_consuming(self):
        """Start consuming messages from Kafka topics with enhanced display"""
        print("\n" + "="*70)
        print("🚀 STARTING KAFKA CONSUMPTION")
        print("="*70)
        print(f"📡 Topics: {self.topics}")
        print(f"📍 Broker: {self.broker_servers[0]}")
        print(f"🌐 ZeroTier Network Connection Active")
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
                if topic == 'topic-net':
                    self.net_message_count += 1
                elif topic == 'topic-disk':
                    self.disk_message_count += 1

                # Display message details on console
                self.display_message_details(topic, value, total_message_count)

                # Use 'original_ts' if present, otherwise None
                ts_value = value.get('original_ts') if isinstance(value, dict) else None

                # Thread-safe data appending using only expected fields
                with self.data_lock:
                    if topic == 'topic-net':
                        self.network_data.append({
                            'ts': ts_value,
                            'server_id': value.get('server_id') if isinstance(value, dict) else None,
                            'net_in': value.get('net_in') if isinstance(value, dict) else None,
                            'net_out': value.get('net_out') if isinstance(value, dict) else None
                        })
                    elif topic == 'topic-disk':
                        self.disk_data.append({
                            'ts': ts_value,
                            'server_id': value.get('server_id') if isinstance(value, dict) else None,
                            'disk_io': value.get('disk_io') if isinstance(value, dict) else None
                        })

                # Progress summary every 50 messages
                if total_message_count % 50 == 0:
                    print(f"\n{'🎯 PROGRESS SUMMARY':^70}")
                    print(f"{'='*70}")
                    print(f"  Total Messages   : {total_message_count}")
                    print(f"  Network Messages : {self.net_message_count}")
                    print(f"  Disk Messages    : {self.disk_message_count}")
                    print(f"  Network Records  : {len(self.network_data)}")
                    print(f"  Disk Records     : {len(self.disk_data)}")
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
            print(f"  Total Messages Received  : {total_message_count}")
            print(f"  Network Topic Messages   : {self.net_message_count}")
            print(f"  Disk Topic Messages      : {self.disk_message_count}")
            print(f"  Network Records Stored   : {len(self.network_data)}")
            print(f"  Disk Records Stored      : {len(self.disk_data)}")
            print("="*70 + "\n")

            print("💾 Saving final data to CSV files...")
            self.save_raw_data_to_csv()

            consumer.close()
            print("\n✅ Kafka 2 closed successfully")

def main():
    """Main function to run Consumer 2"""
    print("=" * 70)
    print("🌐 CONSUMER 2: NETWORK & DISK PROCESSOR (TEAM 4)")
    print("📋 Assignment 2: Real-time Monitoring Pipeline")
    print("=" * 70)

    # Get broker ZeroTier IP from user
    print("\n❓ Enter the ZeroTier IP address of your Kafka Broker machine:")
    broker_ip = input("🎯 Broker ZeroTier IP: ").strip()
    
    if not broker_ip:
        print("❌ Broker IP is required!")
        sys.exit(1)

    print(f"\n{'⚙️  CONFIGURATION':^70}")
    print("=" * 70)
    print(f"  📡 Kafka Broker        : {broker_ip}:9092")
    print(f"  📊 Processing Metrics  : Network + Disk")
    print(f"  🎯 Kafka Topics        : topic-net, topic-disk")
    print(f"  📁 Output File         : team_04_NET_DISK.csv")
    print(f"  🌐 Network Threshold   : 6104.83 KB/s")
    print(f"  💽 Disk Threshold      : 1839.94 ops/s")
    print(f"  👥 Team ID             : 4")
    print(f"  🌐 Connection          : ZeroTier VPN")
    print("=" * 70)

    consumer = NetworkDiskConsumer(broker_ip)
    
    try:
        consumer.start_consuming()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n" + "=" * 70)
        print("👋 CONSUMER 2 FINISHED")
        print("=" * 70)

if __name__ == "__main__":
    main()