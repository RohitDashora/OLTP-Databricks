# Databricks Lakebase Setup Guide

This guide walks you through setting up a Databricks Lakebase instance (managed PostgreSQL) and obtaining the credentials needed for the OLTP-Databricks project.

> **Note**: This guide assumes you already have a Databricks workspace set up. If you need to create a Databricks workspace first, please refer to the [Databricks documentation](https://docs.databricks.com/getting-started/index.html).

## Prerequisites

- An existing Databricks workspace
- Workspace or account admin permissions (for enabling Lakebase preview)
- Basic familiarity with SQL and database concepts

## Step 1: Enable Lakebase Preview

Lakebase (managed PostgreSQL) is currently in Public Preview. You need to enable it first:

1. **Log into your Databricks workspace**
2. **Click your username** in the top bar
3. **Select "Previews"** from the menu
4. **Turn on "Lakebase: Managed Postgres OLTP Database"**
5. **Click "Save"**

> **Note**: Lakebase is available in the following regions: `us-east-1`, `us-west-2`, `eu-west-1`, `ap-southeast-1`, `ap-southeast-2`, `eu-central-1`, `us-east-2`, `ap-south-1`.

## Step 2: Create a Database Instance

### 2.1 Using the Databricks UI

1. **Click "Compute"** in the workspace sidebar
2. **Click the "Database instances" tab**
3. **Click "Create database instance"**
4. **Enter a database instance name** (1-63 characters, letters and hyphens only)
5. **Choose an Instance Size**:
   - For learning: Start with `CU_1` (smallest)
   - For production: Choose based on your workload needs
6. **Optional: Click "Advanced Settings"** for additional configurations:
   - **Restore window**: Set retention period (2-35 days, default 7)
   - **Create from parent**: For copy-on-write clones
   - **Allow readable secondaries**: For high availability
7. **Click "Create"**

### 2.2 Using Python SDK

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance

# Initialize the Workspace client
w = WorkspaceClient()

# Create a database instance
instance = w.database.create_database_instance(
    DatabaseInstance(
        name="my-database-instance",
        capacity="CU_1"
    )
)

print(f"Created database instance: {instance.name}")
print(f"Connection endpoint: {instance.read_write_dns}")
```

### 2.3 Using CLI

```bash
# Create a database instance
databricks database create-database-instance my-database-instance \
  --capacity CU_1

# Create with advanced options
databricks database create-database-instance \
  --json '{
    "name": "my-database-instance",
    "capacity": "CU_2",
    "retention_window_in_days": 14
  }'
```

## Step 3: Get Connection Information

### 3.1 Find Your Connection Details

Once your database instance is created and running:

1. **Go to Compute → Database instances**
2. **Click on your database instance**
3. **Note the connection details**:
   - **Host**: The `read_write_dns` endpoint
   - **Port**: `5432` (standard PostgreSQL port)
   - **Database name**: Usually `postgres` (default database)
   - **Username**: Your Databricks username (email address)

### 3.2 Get Your Username

1. **Click on your profile** in the top-right corner
2. **Note your email address** - this is your username
3. **Example**: `john.doe@company.com`

## Step 4: Generate Access Token (Password)

### 4.1 Create Access Token

1. **Click on Generate Access Token** in the OLTP instance details

## Step 5: Create a Database (Optional)

### 5.1 Connect Using SQL Editor

1. **Go to SQL** in the left sidebar
2. **Click "Create" → "Query"**
3. **Select your database instance** from the dropdown
4. **Run the following SQL**:

```sql
-- Create a new database for your project
CREATE DATABASE IF NOT EXISTS oltp_demo;

-- Use the database
USE oltp_demo;

-- Create a sample table to test
CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data
INSERT INTO test_table (name) VALUES ('Test Entry');

-- Verify it works
SELECT * FROM test_table;
```

## Step 6: Configure Your Environment

### 6.1 Update export_my_env.sh

1. **Open your `scripts/export_my_env.sh` file**
2. **Replace the placeholder values** with your actual Lakebase credentials:

```bash
#!/bin/bash

# Export Databricks Lakebase PostgreSQL configuration as environment variables
echo "�� Exporting database configuration as environment variables..."

export DATABRICKS_DB_HOST="your-instance-name.database.cloud.databricks.com"
export DATABRICKS_DB_USER="your-email@company.com"
export DATABRICKS_DB_NAME="postgres"  # or "oltp_demo" if you created a custom database
export DATABRICKS_DB_PORT="5432"
export DATABRICKS_DB_SSLMODE="require"
export DATABRICKS_DB_PASSWORD="dapi1234567890abcdef..."  # Your access token

echo "✅ Environment variables exported:"
echo "  DATABRICKS_DB_HOST: $DATABRICKS_DB_HOST"
echo "  DATABRICKS_DB_USER: $DATABRICKS_DB_USER"
echo "  DATABRICKS_DB_NAME: $DATABRICKS_DB_NAME"
echo "  DATABRICKS_DB_PORT: $DATABRICKS_DB_PORT"
echo "  DATABRICKS_DB_SSLMODE: $DATABRICKS_DB_SSLMODE"
echo "  DATABRICKS_DB_PASSWORD: [HIDDEN]"
```

### 6.2 Test Your Configuration

1. **Activate your environment**:
   ```bash
   source scripts/activate_env.sh
   ```

2. **Test the configuration**:
   ```bash
   python -m oltp_databricks.test_env_config
   ```

3. **Test the connection**:
   ```bash
   python examples/test_connection.py
   ```

## Step 7: Manage Your Database Instance

### 7.1 Start/Stop Instance

**Using UI:**
1. Go to **Compute → Database instances**
2. Click on your instance
3. Click **"Start"** or **"Stop"** in the upper-right corner

**Using Python SDK:**
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance

w = WorkspaceClient()

# Stop instance
w.database.update_database_instance(
    name="my-database-instance",
    database_instance=DatabaseInstance(name="my-database-instance", stopped=True),
    update_mask="*"
)

# Start instance
w.database.update_database_instance(
    name="my-database-instance",
    database_instance=DatabaseInstance(name="my-database-instance", stopped=False),
    update_mask="*"
)
```

**Using CLI:**
```bash
# Stop instance
databricks database update-database-instance my-database-instance \
  --json '{"stopped": true}'

# Start instance
databricks database update-database-instance my-database-instance \
  --json '{"stopped": false}'
```

### 7.2 Instance Behavior

**When Stopped:**
- Data is preserved
- No read/write operations possible
- Synced tables don't serve reads
- Lakeflow Declarative Pipelines may return errors

**When Started:**
- Instance enters `STARTING` state
- Becomes `AVAILABLE` when ready

