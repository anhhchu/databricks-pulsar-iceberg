#!/usr/bin/env python3
"""
Simple example of using the Pulsar Financial Message Producer with Local Pulsar
This script demonstrates basic usage with local macOS Pulsar setup

Prerequisites:
1. Install Pulsar: brew install apache-pulsar
2. Start Pulsar: pulsar standalone (in another terminal)
3. Run this script: python pulsar_producer.py
"""

import sys
import logging
from pathlib import Path

# Add parent directory to path to import shared config
sys.path.append('..')
from config import PROD_CONFIG, PRODUCER_CONFIG

from pulsar_financial_message_producer import PulsarFinancialMessageProducer

# Set up basic logging
logging.basicConfig(
    level=getattr(logging, PROD_CONFIG.get('log_level', 'INFO')),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    """Simple example of sending financial messages to local Pulsar"""
    
    print("🚀 Pulsar Financial Message Producer - Local Setup")
    print("=" * 55)
    
    # Use configuration from config.py
    service_url = PROD_CONFIG['service_url']
    topic = PROD_CONFIG['topic']
    auth_config = PROD_CONFIG['auth']
    
    print(f"📋 Configuration loaded:")
    print(f"   Service URL: {service_url}")
    print(f"   Topic: {topic}")
    print(f"   Auth: {auth_config}")
    
    # Create producer with configuration
    producer = PulsarFinancialMessageProducer(service_url, topic, auth_config)
    
    try:
        print(f"🔗 Connecting to Pulsar at {service_url}")
        print("📋 Make sure Pulsar is running in another terminal:")
        print("   pulsar standalone")
        print("-" * 50)
        
        # Connect to Pulsar
        producer.connect()
        print("✅ Connected successfully!")
        
        # Send a sample message
        print("\n📤 Generating and sending financial message...")
        message_id = producer.send_sample_message()
        
        print(f"✅ Message sent successfully!")
        print(f"📝 Message ID: {message_id}")
        print(f"📊 Topic: {topic}")
        
        print("\n🎉 Success! Your financial message has been sent to Pulsar")
        print("📊 View Pulsar admin UI: http://localhost:8080")
        print("📋 Check topics and messages in the web interface")
        
        print("\n🔍 Verify your topic and messages with these commands:")
        print("# List all topics:")
        print("curl http://localhost:8080/admin/v2/persistent/public/default")
        print("\n# Get topic statistics (message count, etc.):")
        print(f"curl http://localhost:8080/admin/v2/persistent/public/default/{topic}/stats")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Start Pulsar: pulsar standalone")
        print("2. Wait 10-15 seconds for Pulsar to fully start")
        print("3. Check if port 6650 is available: lsof -i :6650")
        print("4. Visit http://localhost:8080 to verify Pulsar is running")
        print("5. Check terminal running 'pulsar standalone' for errors")
        print("6. Verify configuration in ../config.py")
        
    finally:
        producer.disconnect()
        print("\n👋 Disconnected from Pulsar")

if __name__ == "__main__":
    main() 