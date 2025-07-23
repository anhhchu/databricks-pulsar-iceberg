#!/usr/bin/env python3
"""
Simple Pulsar Consumer to view financial messages
This script reads messages from the financial-messages topic and displays them
"""

import sys
import pulsar
import json
import logging

# Add parent directory to path to import shared config
sys.path.append('..')
from config import PROD_CONFIG

# Set up logging using config
logging.basicConfig(level=getattr(logging, PROD_CONFIG.get('log_level', 'INFO')))
logger = logging.getLogger(__name__)

def consume_messages():
    """Consume and display messages from the financial-messages topic"""
    
    print("🔍 Pulsar Financial Message Consumer")
    print("=" * 45)
    
    # Use configuration from config.py
    service_url = PROD_CONFIG['service_url']
    topic = PROD_CONFIG['topic']
    auth_config = PROD_CONFIG['auth']
    subscription = "message-viewer"
    
    print(f"📋 Configuration loaded:")
    print(f"   Service URL: {service_url}")
    print(f"   Topic: {topic}")
    print(f"   Auth: {auth_config}")
    
    client = None
    consumer = None
    
    try:
        print(f"🔗 Connecting to Pulsar at {service_url}")
        print(f"📥 Reading messages from topic: {topic}")
        print("-" * 45)
        
        # Create Pulsar client with authentication if configured
        auth = None
        if auth_config:
            if 'oauth2' in auth_config:
                auth = pulsar.AuthenticationOauth2(**auth_config['oauth2'])
            elif 'token' in auth_config:
                auth = pulsar.AuthenticationToken(auth_config['token'])
        
        client = pulsar.Client(
            service_url=service_url,
            authentication=auth
        )
        
        # Create consumer
        consumer = client.subscribe(
            topic=topic,
            subscription_name=subscription,
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        print("✅ Connected! Waiting for messages...")
        print("📋 Press Ctrl+C to stop consuming")
        print("-" * 45)
        
        message_count = 0
        
        while True:
            try:
                # Receive message (with timeout)
                msg = consumer.receive(timeout_millis=5000)  # 5 second timeout
                
                message_count += 1
                print(f"\n📨 Message #{message_count}")
                print(f"🆔 Message ID: {msg.message_id()}")
                print(f"🔑 Partition Key: {msg.partition_key()}")
                print(f"📅 Publish Time: {msg.publish_timestamp()}")
                
                # Show properties
                if msg.properties():
                    print("🏷️  Properties:")
                    for key, value in msg.properties().items():
                        print(f"    {key}: {value}")
                
                # Decode and pretty-print the message content
                try:
                    message_data = msg.data().decode('utf-8')
                    json_data = json.loads(message_data)
                    print("💼 Message Content:")
                    print(json.dumps(json_data, indent=2))
                except json.JSONDecodeError:
                    print("📄 Raw Message Content:")
                    print(message_data)
                except Exception as e:
                    print(f"⚠️  Could not decode message: {e}")
                    print("📄 Raw bytes:")
                    print(msg.data())
                
                # Acknowledge the message
                consumer.acknowledge(msg)
                print("✅ Message acknowledged")
                print("-" * 45)
                
            except Exception as e:
                if "Timeout" in str(e):
                    print("⏱️  No new messages (5 second timeout)")
                    print("📊 Use Ctrl+C to exit, or wait for new messages...")
                    continue
                else:
                    print(f"❌ Error receiving message: {e}")
                    break
                    
    except KeyboardInterrupt:
        print(f"\n\n👋 Stopping consumer. Total messages consumed: {message_count}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Make sure Pulsar is running: pulsar standalone")
        print("2. Make sure you've sent messages: python ../producer/pulsar_producer.py")
        print("3. Check if the topic exists with: curl http://localhost:8080/admin/v2/persistent/public/default")
        print("4. Verify configuration in ../config.py")
        
    finally:
        if consumer:
            consumer.close()
        if client:
            client.close()
        print("🔌 Disconnected from Pulsar")

if __name__ == "__main__":
    consume_messages() 