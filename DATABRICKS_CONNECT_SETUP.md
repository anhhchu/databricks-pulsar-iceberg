# Databricks Connect Setup Guide

This guide will help you set up Databricks Connect to run your Pulsar-Iceberg notebook locally while executing on a Databricks cluster using the Databricks CLI and configuration profiles.

## Prerequisites

- **Python 3.10, 3.11, or 3.12** (depending on your Databricks Runtime version)
- Access to a Databricks workspace
- A running Databricks cluster **OR** serverless compute with appropriate libraries installed  
- **Databricks Runtime 13.3 LTS or higher** (for Databricks Connect compatibility)
- **No PySpark installed locally** (conflicts with Databricks Connect)

```bash
# Create virtual environment (strongly recommended by Databricks)
python -m venv databricks-env
source databricks-env/bin/activate  # On Windows: databricks-env\Scripts\activate

# Install requirements
pip install -r requirements.txt

```

## Version Compatibility

### Official Version Support Matrix

Based on the [official Databricks documentation](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/install), here are the exact version requirements:

| Compute Type | Databricks Connect Version | Compatible Python Version |
|-------------|----------------------------|---------------------------|
| Serverless  | 16.1 to 16.4              | **3.12**                  |
| Serverless  | 15.4.10 to below 16.0     | **3.11**                  |
| Cluster     | 16.1 and above            | **3.12**                  |
| Cluster     | 15.4 LTS                  | **3.11**                  |
| Cluster     | 13.3 LTS to 14.3 LTS      | **3.10**                  |

**Important Notes:**
- **Exact Python version matching required**: The minor Python version on your local machine must match exactly
- **Python 3.12 now supported** in Databricks Connect 16.1+
- **For UDFs**: Local Python version must exactly match the cluster's Python version
- **Version correspondence**: Databricks Connect version numbers correspond to Databricks Runtime version numbers

### Databricks Runtime Compatibility

Your Databricks Connect version must match your cluster's Databricks Runtime version:

- **Databricks Connect 16.4** ↔ **Databricks Runtime 16.4**
- **Databricks Connect 15.4 LTS** ↔ **Databricks Runtime 15.4 LTS**  
- **Databricks Connect 13.3 LTS** ↔ **Databricks Runtime 13.3 LTS**

**Recommended Setup for Different Scenarios:**
- **Latest features**: Python 3.12 + Databricks Connect 16.4 + DBR 16.4
- **LTS stability**: Python 3.11 + Databricks Connect 15.4 LTS + DBR 15.4 LTS
- **Older clusters**: Python 3.10 + Databricks Connect 13.3 LTS + DBR 13.3 LTS

### Check Your Versions

```bash
# Check your Python version
python --version

# Check your Databricks Connect version (after installation)
pip show databricks-connect

# Check your cluster's Databricks Runtime in the workspace
# Go to Compute → Your Cluster → Configuration → Runtime Version
```

## 1. Installation

**Important**: Remove PySpark first if installed (it conflicts with Databricks Connect):

```bash
# Check if PySpark is installed
pip show pyspark

# Remove PySpark if found (required step)
pip uninstall pyspark
```

Install databricks-connect version that matches your Databricks Runtime

```bash
# For DBR 16.4 (Latest) - Python 3.12 required
pip install "databricks-connect==16.4.*"

# For DBR 15.4 LTS - Python 3.11 required  
pip install "databricks-connect==15.4.*"

# For DBR 13.3 LTS - Python 3.10 required
pip install "databricks-connect==13.3.*"
```

**Note**: The `.*` notation ensures you get the latest patch version (e.g., `16.4.3` instead of exactly `16.4.0`).


### Installation Troubleshooting

**Common Issues:**

- **PySpark conflicts**: Databricks Connect conflicts with PySpark. Always uninstall PySpark first: `pip uninstall pyspark`
- **Version mismatch errors**: Ensure your local Databricks Connect version exactly matches your cluster's DBR version
- **Python version errors**: Use the exact Python version required (3.10 for DBR 13.3-14.3, 3.11 for DBR 15.4, 3.12 for DBR 16.1+)
- **Dependency conflicts**: Use a virtual environment (strongly recommended by Databricks)


**Reference**: [Official Databricks Connect Installation Guide](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/install)

## 2. Configure Databricks CLI

The Databricks CLI manages authentication through configuration profiles stored in `~/.databrickscfg`.

### Setup Authentication Profile

Choose one of the following authentication methods:

#### Option A: Personal Access Token (PAT)

1. **Generate Personal Access Token:**
   - Log into your Databricks workspace
   - Click on your username in the top-right corner
   - Select **User Settings** → **Developer** → **Access tokens**
   - Click **Generate new token**
   - Give it a name (e.g., "Local Development") and set expiration
   - Copy the generated token

2. **Configure CLI with PAT:**
   ```bash
   databricks configure --token --profile dev
   ```
   
   You'll be prompted for:
   - **Databricks Host**: `https://your-workspace.cloud.databricks.com`
   - **Token**: Paste your generated token

    This will update DEFAULT profile in `~/.databrickscfg` 

#### Option B: OAuth Authentication

Configure OAuth (recommended for production):

```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
```

This will:
- Open your browser for authentication
- Create a profile in `~/.databrickscfg`
- Store OAuth credentials securely

### Multiple Profiles (Optional)

You can create multiple profiles for different environments:

```bash
# Development profile
databricks configure --token --profile dev

# Production profile  
databricks auth login --host https://your-prod-workspace.com --profile prod
```

## 3. Verify Configuration

Check your configuration:

```bash
# List all profiles
cat ~/.databrickscfg
```

Your `~/.databrickscfg` should look like:

```ini
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = dapi123abc...

[dev]
host = https://dev-workspace.cloud.databricks.com
token = dapi456def...

[prod]
host = https://prod-workspace.cloud.databricks.com
token = databricks-cli
```

## 4. Find Your Cluster ID

You'll need your cluster ID for Databricks Connect:

1. Go to **Compute** in your Databricks workspace
2. Click on your cluster name
3. Copy the cluster ID from the URL: `/compute/clusters/CLUSTER_ID/configuration`
4. Or go to **Configuration** → **Advanced options** → **Tags** and look for `ClusterId`

Example cluster ID: `1234-567890-abc123`

## 5. Using Jupyter Notebook with Databricks Connect

### Start Jupyter Notebook

```bash
jupyter notebook
```

### Connect to Databricks in Your Notebook

Add this code to the first cell of your notebook:

```python
from databricks.connect import DatabricksSession

# Create Databricks Connect session using default profile
spark = DatabricksSession.builder \
    .remote(cluster_id="YOUR_CLUSTER_ID") \
    .getOrCreate()

print(f"✅ Connected to Databricks! Spark version: {spark.version}")

# Test the connection
df = spark.sql("SELECT current_timestamp() as current_time")
df.show()
```

### Using Specific Profiles

If you have multiple profiles, specify which one to use:

```python
from databricks.connect import DatabricksSession

# Use a specific profile (e.g., 'dev')
spark = DatabricksSession.builder \
    .remote(
        cluster_id="YOUR_CLUSTER_ID",
        profile="dev"  # Use 'dev' profile from ~/.databrickscfg
    ) \
    .getOrCreate()

print(f"✅ Connected using 'dev' profile! Spark version: {spark.version}")
```

### Example: Adapting Your Pulsar-Iceberg Notebook

Here's how to adapt your existing notebook to use Databricks Connect:

```python
# Cell 1: Setup Databricks Connect
from databricks.connect import DatabricksSession
from pyspark.sql.functions import col, from_json, explode
from consumer.schemas import schema, instrument_ref_schema, instrument_error_schema, instrument_risk_schema

# Connect to your cluster
spark = DatabricksSession.builder \
    .remote(cluster_id="1234-567890-abc123") \
    .getOrCreate()

print(f"✅ Connected to Databricks! Spark version: {spark.version}")
```

```python
# Cell 2: Your existing streaming code
fin_df = (
    spark.readStream
    .format("pulsar")
    .option("service.url", "pulsar://6.tcp.us-cal-1.ngrok.io:13185")
    .option("topics", "financial-messages")
    .option("startingOffsets", "latest")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("value"))
    .select("value.*")
)

# Create bronze table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS users.anhhoang_chu.bronze_fin_instrument ( {schema} )
USING ICEBERG
""")

# Write to bronze table
(
    fin_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("schemaLocation", "/Volumes/users/anhhoang_chu/iceberg/bronze1/_schema")
    .option("checkpointLocation", "/Volumes/users/anhhoang_chu/iceberg/bronze1/_checkpoint")
    .trigger(availableNow=True)
    .toTable("users.anhhoang_chu.bronze_fin_instrument")
)
```

## 6. Troubleshooting

### Common Issues:

**Connection timeout:**
- Check if your cluster is running in the Databricks workspace
- Verify your cluster ID is correct
- Ensure your IP is not blocked by workspace network policies

**Authentication errors:**
- Check your `~/.databrickscfg` file exists and has correct credentials
- For PAT: Verify token hasn't expired and has proper permissions
- For OAuth: Re-authenticate with `databricks auth login`

**Profile errors:**
- List profiles: `cat ~/.databrickscfg`
- Test CLI connection: `databricks clusters list`
- Recreate profile if needed: `databricks configure --token`

**Library import errors:**
- Ensure required libraries are installed on your Databricks cluster
- Install missing packages on cluster: `%pip install pulsar-client boto3`

**Schema/table not found:**
- Verify you have access to the required catalogs and schemas
- Check workspace permissions and table ownership

**Version compatibility errors:**
- Check version compatibility: `python -c "import databricks.connect; print(databricks.connect.__version__)"`
- Ensure Databricks Connect version exactly matches your cluster's DBR version
- Verify Python version matches requirements: 3.10 (DBR 13.3-14.3), 3.11 (DBR 15.4), 3.12 (DBR 16.1+)
- Common error: `"Connect client does not support..."` indicates version mismatch
- **PySpark conflicts**: Error messages about Spark may indicate PySpark is installed alongside Databricks Connect

### Quick Diagnostics:

```bash
# Check CLI configuration
databricks configure --token

# List available clusters
databricks clusters list

# Test cluster connectivity
databricks clusters get --cluster-id YOUR_CLUSTER_ID

# Check version compatibility
python --version
python -c "import databricks.connect; print(f'Databricks Connect: {databricks.connect.__version__}')"
pip show databricks-connect
```

### Version Debugging:

```python
# Add this to check version compatibility in a notebook
import sys
import databricks.connect

print(f"Python version: {sys.version}")
print(f"Databricks Connect version: {databricks.connect.__version__}")

# Check if cluster runtime matches (run this after connecting)
try:
    spark = DatabricksSession.builder.remote(cluster_id="YOUR_CLUSTER_ID").getOrCreate()
    print(f"Spark version: {spark.version}")
    print("✅ Connection successful - versions are compatible")
except Exception as e:
    print(f"❌ Version mismatch or connection issue: {e}")
```

### Test Connection in Notebook:

```python
# Add this to a notebook cell to test your setup
import sys
import databricks.connect
from databricks.connect import DatabricksSession

# Check versions first
print(f"Python version: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
print(f"Databricks Connect version: {databricks.connect.__version__}")

try:
    spark = DatabricksSession.builder.remote(cluster_id="YOUR_CLUSTER_ID").getOrCreate()
    print(f"✅ Connected successfully!")
    print(f"Spark version: {spark.version}")
    print(f"Databricks Runtime: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion', 'Unknown')}")
    
    # Test basic functionality
    spark.sql("SELECT current_timestamp() as now, current_user() as user").show()
    
except Exception as e:
    print(f"❌ Connection failed: {e}")
    print("Troubleshooting tips:")
    print("1. Check your cluster ID and ensure cluster is running")
    print("2. Verify Databricks Connect version exactly matches cluster DBR version")
    print("3. Ensure Python version matches: 3.10 (DBR 13.3-14.3), 3.11 (DBR 15.4), 3.12 (DBR 16.1+)")
    print("4. Check if PySpark is installed (conflicts with Databricks Connect): pip show pyspark")
```

## 7. Best Practices

1. **Security**: 
   - Store credentials in `~/.databrickscfg`, never in code
   - Use OAuth for production environments
   - Set appropriate token expiration times

2. **Profile Management**: 
   - Use separate profiles for dev/staging/prod environments
   - Name profiles descriptively (e.g., `dev`, `staging`, `prod`)

3. **Cluster Management**:
   - Use dedicated clusters for development to avoid conflicts
   - Consider SQL warehouses for better cost efficiency
   - Install required libraries on cluster beforehand

4. **Development Workflow**:
   - Keep notebooks in version control (convert .ipynb to .py when needed)
   - Use checkpoint locations for streaming jobs
   - Monitor cluster usage to control costs

## 8. Next Steps

Once connected, you can:
- ✅ Run your Pulsar-Iceberg streaming pipeline locally in Jupyter
- ✅ Debug interactively with full Python debugging capabilities
- ✅ Leverage Databricks compute while using your local development environment
- ✅ Version control your notebooks and streaming jobs
- ✅ Use your favorite IDE extensions and tools

### Converting Your Existing Notebook

Your original `iceberg-streaming.ipynb` can now be run locally! Just:

1. Add the Databricks Connect setup cell at the beginning
2. Replace your cluster ID in the connection code
3. Run cells interactively while leveraging Databricks compute

For more information, see the [official Databricks Connect documentation](https://docs.databricks.com/dev-tools/databricks-connect.html). 