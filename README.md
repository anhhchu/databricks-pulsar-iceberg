# Databricks Pulsar Iceberg Integration

This project provides Python scripts for generating and consuming financial instrument analysis messages through Apache Pulsar, designed for integration with Databricks and Iceberg data lakehouse architectures.

## Features

- 🏦 **Financial Instrument Modeling**: Complete data structures for bonds, risk metrics, and cash flows
- 🔐 **Authentication Support**: JWT tokens, OAuth2, and AWS Cognito  
- 📊 **Risk Analytics**: Comprehensive risk metrics including PD, LGD, EAD, and CECL calculations
- 🚀 **High Performance**: Batching, compression, and efficient message delivery
- 📝 **Comprehensive Logging**: Detailed logging for monitoring and debugging
- 🔧 **Configurable**: Simplified configuration for local development
- 🗂️ **Organized Structure**: Separate producer and consumer components

## File Structure

```
databricks-pulsar-iceberg/
├── config.py                          # Simplified configuration (shared)
├── requirements.txt                    # Python dependencies
├── README.md                          # This file
├── producer/
│   ├── pulsar_producer.py             # Producer usage example
│   ├── pulsar_financial_message_producer.py  # Main producer class
│   └── persistMessageSchema.json      # Sample financial message schema
└── consumer/
    └── pulsar_consumer.py             # Message consumer script
```

## Working with the Project Structure

The project is organized into separate producer and consumer components:

- **`producer/`**: Contains all message production logic and financial data generation
- **`consumer/`**: Contains message consumption and display logic  
- **`config.py`**: Shared configuration file in the base directory with simplified `DEV_CONFIG`

To run scripts, navigate to their respective directories:
```bash
# For producer operations
cd producer
python pulsar_producer.py

# For consumer operations (in another terminal)
cd consumer
python pulsar_consumer.py
```

Both scripts can import the shared configuration:
```python
import sys
sys.path.append('..')
from config import DEV_CONFIG
```

## Prerequisites

Python 3.8 or higher

### Step 1: Create Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
# On macOS/Linux:
source .venv/bin/activate

# On Windows:
.venv\Scripts\activate

# Verify activation (you should see the environment name in your prompt)
which python  # Should point to your virtual environment
```

### Step 2: Install Dependencies

With your virtual environment activated:

```bash
# Install required packages
pip install -r requirements.txt

# Verify installation
pip list
```
---

# 🏠 Section 1: Standalone Pulsar Setup

This section covers setting up and using Apache Pulsar locally on your machine. This is the **recommended approach for development** as it's free, fast, and doesn't require any cloud resources.


## Step 1: Install Apache Pulsar Locally

### On macOS:
```bash
# Method 1: Using Homebrew (easiest)
brew install apache-pulsar

# Method 2: Direct download
wget https://archive.apache.org/dist/pulsar/pulsar-3.1.0/apache-pulsar-3.1.0-bin.tar.gz
tar xvfz apache-pulsar-3.1.0-bin.tar.gz
cd apache-pulsar-3.1.0
```

### On Linux:
```bash
# Direct download
wget https://archive.apache.org/dist/pulsar/pulsar-3.1.0/apache-pulsar-3.1.0-bin.tar.gz
tar xvfz apache-pulsar-3.1.0-bin.tar.gz
cd apache-pulsar-3.1.0
```

## Step 2: Configure Your Application

The project includes a simplified configuration file in the base directory. You can use the `DEV_CONFIG` for local development:

```python
# From producer/ or consumer/ directories, import the shared config
import sys
sys.path.append('..')
from config import DEV_CONFIG

# Use the DEV_CONFIG for local Pulsar
producer = PulsarFinancialMessageProducer(
    service_url=DEV_CONFIG['service_url'],   # "pulsar://localhost:6650"
    topic=DEV_CONFIG['topic'],               # "financial-messages"
    auth_params=DEV_CONFIG['auth']           # No authentication needed
)
```

## Step 3: Test Your Setup

```bash
# In terminal 1: Start Pulsar (if not already running)
pulsar standalone
# OR if installed via Homebrew:
/opt/homebrew/opt/apache-pulsar/bin/pulsar standalone

# In terminal 2: Run the producer
source .venv/bin/activate
cd producer
python pulsar_producer.py

# In terminal 3: Run the consumer (optional)
source .venv/bin/activate  
cd consumer
python pulsar_consumer.py
```

### Expected Output:

**Producer Output:**
```
🚀 Pulsar Financial Message Producer - Local Setup
======================================================
🔗 Connecting to local Pulsar at pulsar://localhost:6650
✅ Connected successfully!
📤 Generating and sending financial message...
✅ Message sent successfully!
📝 Message ID: (1,0,-1,0)
📊 Topic: financial-messages
```

**Consumer Output:**
```
🔍 Pulsar Financial Message Consumer
============================================
🔗 Connecting to Pulsar at pulsar://localhost:6650
📥 Reading messages from topic: financial-messages
✅ Connected! Waiting for messages...

📨 Message #1
🆔 Message ID: (1,0,-1,0)
💼 Message Content:
{
  "jobidentifier": "uuid-string",
  "analysisidentifier": "uuid-string", 
  "data": [...]
}
✅ Message acknowledged
```

## Standalone Pulsar Administration

```bash
# Create a topic
bin/pulsar-admin topics create persistent://public/default/financial-messages

# List topics
bin/pulsar-admin topics list public/default

# Check topic stats
bin/pulsar-admin topics stats persistent://public/default/financial-messages

# Monitor messages
bin/pulsar-client consume persistent://public/default/financial-messages -s "test-subscription"
```

---

# ☁️ Section 2: AWS and Pulsar on EC2 Setup

This section covers setting up Apache Pulsar on AWS EC2 instances. This approach provides a **production-like environment** and is suitable for testing cloud deployments.

## Step 1: Install and Configure AWS CLI

### Install AWS CLI

#### On macOS:
```bash
# Using Homebrew (recommended)
brew install awscli

# OR using pip
pip install awscli

# OR download installer
curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
sudo installer -pkg AWSCLIV2.pkg -target /
```

### Configure AWS CLI

1. **Get your AWS credentials:**
   - Go to [AWS IAM Console](https://console.aws.amazon.com/iam/)
   - Create a new user or use existing user
   - Generate Access Key ID and Secret Access Key
   - Note down the keys (you won't be able to see the secret again)

2. **Configure AWS CLI:**
   ```bash
   aws configure
   ```
   
   You'll be prompted to enter:
   ```
   AWS Access Key ID [None]: YOUR_ACCESS_KEY_ID
   AWS Secret Access Key [None]: YOUR_SECRET_ACCESS_KEY
   Default region name [None]: us-east-1
   Default output format [None]: json
   ```

3. **Test AWS CLI configuration:**
   ```bash
   # Test connectivity
   aws sts get-caller-identity
   
   # Should return your account information
   ```

### Alternative: Using Environment Variables

Instead of `aws configure`, you can set environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_access_key
export AWS_DEFAULT_REGION=us-east-1
```

## Step 2: Set Up IAM Permissions

Before proceeding, ensure your AWS user has the necessary IAM permissions. Your AWS user needs these **minimal permissions** to create EC2 resources for Pulsar:

### Minimal IAM Policy

Create and attach this custom IAM policy to your AWS user:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateSecurityGroup",
                "ec2:DeleteSecurityGroup",
                "ec2:DescribeSecurityGroups",
                "ec2:AuthorizeSecurityGroupIngress",
                "ec2:RunInstances",
                "ec2:TerminateInstances",
                "ec2:StopInstances",
                "ec2:StartInstances",
                "ec2:DescribeInstances",
                "ec2:DescribeImages",
                "ec2:DescribeKeyPairs",
                "ec2:CreateKeyPair"
            ],
            "Resource": "*"
        }
    ]
}
```

### How to Add These Permissions

**Option 1: AWS Console (Recommended)**
1. Go to [AWS IAM Console](https://console.aws.amazon.com/iam/)
2. Click "Users" → Find your user → "Permissions" tab
3. Click "Add permissions" → "Attach policies directly"
4. Search for and attach **`AmazonEC2FullAccess`** (easiest), OR create a custom policy with the JSON above

**Option 2: AWS CLI**
```bash
# Attach the AWS managed EC2 full access policy (easiest)
aws iam attach-user-policy \
    --user-name YOUR_USERNAME \
    --policy-arn arn:aws:iam::aws:policy/AmazonEC2FullAccess
```

### Verify Permissions
Test your permissions before proceeding:
```bash
# This should work without errors
aws ec2 describe-security-groups
```

If you get an "UnauthorizedOperation" error, you need to add the permissions above.

## Step 3: Set Up EC2 Instance for Pulsar

### Create SSH Key Pair

```bash
# Create a new key pair for SSH access (replace "ahc-pulsar-key" with your preferred name)
aws ec2 create-key-pair \
  --key-name ahc-pulsar-key \
  --query 'KeyMaterial' \
  --output text > ahc-pulsar-key.pem

# Set proper permissions (required for SSH)
chmod 400 ahc-pulsar-key.pem

# Verify the key was created
aws ec2 describe-key-pairs --key-names ahc-pulsar-key
```

**Security Note:** Keep your `.pem` file secure - anyone with this file can access your EC2 instance. Never share or commit it to version control.

### Create Security Group and Launch Instance

```bash
# Create security group
aws ec2 create-security-group \
  --group-name ahc-pulsar-sg \
  --description "Security group for Pulsar"

# Get your current public IP address for security
MY_IP=$(curl -s checkip.amazonaws.com)
echo "Your IP: $MY_IP"

# Add multiple IPs for SSH access (port 22)
aws ec2 authorize-security-group-ingress \
  --group-name ahc-pulsar-sg \
  --ip-permissions '[
    {
      "IpProtocol": "tcp",
      "FromPort": 22,
      "ToPort": 22,
      "IpRanges": [
                {"CidrIp": "'${MY_IP}'/32", "Description": "Current IP"}
      ]
    }
  ]'

# Add multiple IPs for Pulsar client connections (port 6650)
aws ec2 authorize-security-group-ingress \
  --group-name ahc-pulsar-sg \
  --ip-permissions '[
    {
      "IpProtocol": "tcp",
      "FromPort": 6650,
      "ToPort": 6650,
      "IpRanges": [
        {"CidrIp": "'${MY_IP}'/32", "Description": "Current IP"}
      ]
    }
  ]'

# Add multiple IPs for Pulsar admin web interface (port 8080)
aws ec2 authorize-security-group-ingress \
  --group-name ahc-pulsar-sg \
  --ip-permissions '[
    {
      "IpProtocol": "tcp",
      "FromPort": 8080,
      "ToPort": 8080,
      "IpRanges": [
        {"CidrIp": "'${MY_IP}'/32", "Description": "Current IP"}
      ]
    }
  ]'

# Get the latest Amazon Linux 2 AMI ID
AMI_ID=$(aws ec2 describe-images \
  --owners amazon \
  --filters "Name=name,Values=amzn2-ami-hvm-*-x86_64-gp2" \
  --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' \
  --output text)

# Launch instance with a name
aws ec2 run-instances \
  --image-id $AMI_ID \
  --instance-type t3.small \
  --security-groups ahc-pulsar-sg \
  --key-name ahc-pulsar-key \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=ahc-pulsar-server}]'
```

### Find Your EC2 Instance Information

```bash

# List instances by name
aws ec2 describe-instances \
  --query 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value|[0],InstanceId,PublicIpAddress,State.Name, InstanceType]' \
  --output table

# Find your specific instance
aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=ahc-pulsar-server" \
  --query 'Reservations[*].Instances[*].[InstanceId,PublicIpAddress,State.Name, InstanceType]' \
  --output table
```

## Step 6: Install Pulsar on EC2

```bash
# SSH into your instance (replace YOUR_EC2_PUBLIC_IP with your actual IP)
ssh -i ahc-pulsar-key.pem ec2-user@YOUR_EC2_PUBLIC_IP

# Install Java 17 (required for Pulsar 3.x)
sudo yum update -y
sudo yum install -y java-17-amazon-corretto-headless

# Verify Java installation
java -version

# Note: Pulsar 3.x requires Java 17+
# If you need Java 11, use Pulsar 2.x instead

# Download and install Pulsar
wget https://archive.apache.org/dist/pulsar/pulsar-3.1.0/apache-pulsar-3.1.0-bin.tar.gz
tar xvfz apache-pulsar-3.1.0-bin.tar.gz
cd apache-pulsar-3.1.0

# start Pulsar
bin/pulsar standalone
```

## Step 7: Configure Authentication (Optional)

### Option A: No Authentication (Default for Standalone)

For standalone Pulsar installations, authentication is typically disabled by default:

```python
# No authentication needed for standalone Pulsar
auth_config = {}

producer = PulsarFinancialMessageProducer(
    service_url="pulsar://YOUR_EC2_PUBLIC_IP:6650",
    topic="financial-messages"
    # No auth_params needed
)
```

### Option B: JWT Token Authentication

```bash
# On your Pulsar server (if you want to enable authentication)
bin/pulsar tokens create \
  --subject test-user \
  --secret-key file:///path/to/secret.key
```

Update your Python configuration:
```python
auth_config = {
    'token': 'your-jwt-token-here'
}
```

## Step 8: Update Configuration for EC2

1. **The configuration is already simplified in the base directory** (`config.py`)
   
2. **For EC2, you can modify the DEV_CONFIG or create a new config:**
   ```python
   # Update these values in config.py
   
   # Your EC2 Pulsar service URL (replace with your EC2 public IP)
   PULSAR_SERVICE_URL = "pulsar://YOUR_EC2_PUBLIC_IP:6650"
   
   # Your topic name
   TOPIC_NAME = "persistent://public/default/financial-analysis"
   
   # Authentication configuration (for standalone Pulsar, often no auth needed)
   AUTH_CONFIG = {}
   ```

3. **Update pulsar_producer.py:**
   ```python
   # Replace YOUR_EC2_PUBLIC_IP with your actual EC2 public IP
   PULSAR_URL = "pulsar://YOUR_EC2_PUBLIC_IP:6650"
   TOPIC = "financial-messages"
   
   producer = PulsarFinancialMessageProducer(PULSAR_URL, TOPIC)
   ```

## Step 9: Test Your EC2 Setup

```bash
# Run the producer
cd producer
python pulsar_producer.py
```

### Expected Output:
```
2024-01-15 10:30:45,123 - INFO - Connecting to Pulsar at pulsar://YOUR_EC2_IP:6650...
2024-01-15 10:30:45,456 - INFO - Connected to Pulsar at pulsar://YOUR_EC2_IP:6650, topic: financial-messages
Connected successfully!
Generating and sending financial message...
2024-01-15 10:30:46,789 - INFO - Message sent successfully. Message ID: (1,0,-1,0)
Message sent with ID: (1,0,-1,0)
```

## AWS Cost Considerations

### Estimated Monthly Costs

**EC2 Standalone Pulsar:**
- 1 x t3.small instance: ~$15/month (development)
- 1 x t3.large instance: ~$60/month (production)
- EBS storage (20GB): ~$2/month
- Data transfer: ~$1-5/month
- **Total: ~$18-67/month**

### Cost Optimization Tips

1. **Use Spot Instances for development (up to 90% savings):**
   ```bash
   aws ec2 run-instances \
     --image-id $AMI_ID \
     --instance-type t3.small \
     --security-groups ahc-pulsar-sg \
     --key-name ahc-pulsar-key \
     --instance-market-options MarketType=spot \
     --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=ahc-pulsar-spot-server}]'
   ```

2. **Schedule instance shutdown for development:**
   ```bash
   # Stop EC2 instance when not in use
   aws ec2 stop-instances --instance-ids i-1234567890abcdef0
   
   # Start when needed
   aws ec2 start-instances --instance-ids i-1234567890abcdef0
   ```

## Cleanup AWS Resources

```bash
# Terminate instance
aws ec2 terminate-instances --instance-ids i-1234567890abcdef0

# Delete security group
aws ec2 delete-security-group --group-name ahc-pulsar-sg

# Delete key pair and local file
aws ec2 delete-key-pair --key-name ahc-pulsar-key
rm ahc-pulsar-key.pem
```

## Troubleshooting AWS Setup

### Connection Issues

1. **Check EC2 instance is running:**
   ```bash
   aws ec2 describe-instances --instance-ids i-1234567890abcdef0
   ```

2. **Verify Pulsar is running on EC2:**
   ```bash
   # SSH into your EC2 instance
   ssh -i your-key.pem ec2-user@YOUR_EC2_PUBLIC_IP
   
   # Check if Pulsar is running
   ps aux | grep pulsar
   ```

3. **Test port connectivity:**
   ```bash
   # From your local machine
   telnet YOUR_EC2_PUBLIC_IP 6650
   ```

---

# 🔧 Common Configuration and Usage

## Message Schema

The producer generates messages compatible with the provided financial schema:

```json
{
    "jobidentifier": "uuid",
    "analysisidentifier": "uuid", 
    "data": [
        {
            "type": "instrument",
            "instrumentreference": { /* instrument details */ },
            "instrumentriskmetric": [ /* risk calculations */ ],
            "instrumentcashflow": null,
            "instrumenttimebucketmeasures": null,
            "instrumenterror": [ /* validation errors */ ],
            "accounttimebucketmeasures": null,
            "accountcashflow": null
        }
    ]
}
```


### Message Optimization

- Use batching for high-throughput scenarios
- Enable compression for large messages
- Use partition keys for ordered delivery
- Monitor message size and adjust accordingly

## License

This project is provided as-is for educational and development purposes.

## Additional Resources

- [Apache Pulsar Documentation](https://pulsar.apache.org/docs/)
- [AWS EC2 User Guide](https://docs.aws.amazon.com/ec2/latest/userguide/)
- [Pulsar Python Client](https://pulsar.apache.org/docs/client-libraries-python/)
- [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)

## Contributing

To contribute to this project:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with both local and AWS Pulsar
5. Submit a pull request 