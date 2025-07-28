#!/bin/bash

# =============================================================
# INSTRUCTIONS:
# 1. Copy this file to 'export_my_env.sh':
#      cp export_env.sh export_my_env.sh
# 2. Edit 'export_my_env.sh' and fill in your own credentials.
# 3. Source 'export_my_env.sh' to set your environment variables:
#      source export_my_env.sh
# 4. DO NOT check 'export_my_env.sh' into git (add to .gitignore).
# =============================================================

# Export Databricks PostgreSQL configuration as environment variables
# (Replace these placeholder values with your own in export_my_env.sh)
echo "🔧 Exporting database configuration as environment variables..."

export DATABRICKS_DB_HOST="your-instance.database.cloud.databricks.com"
export DATABRICKS_DB_USER="your-email@databricks.com"
export DATABRICKS_DB_NAME="your-database-name"
export DATABRICKS_DB_PORT="5432"
export DATABRICKS_DB_SSLMODE="require"
export DATABRICKS_DB_PASSWORD="your-jwt-token-or-password"

echo "✅ Environment variables exported:"
echo "  DATABRICKS_DB_HOST: $DATABRICKS_DB_HOST"
echo "  DATABRICKS_DB_USER: $DATABRICKS_DB_USER"
echo "  DATABRICKS_DB_NAME: $DATABRICKS_DB_NAME"
echo "  DATABRICKS_DB_PORT: $DATABRICKS_DB_PORT"
echo "  DATABRICKS_DB_SSLMODE: $DATABRICKS_DB_SSLMODE"
echo "  DATABRICKS_DB_PASSWORD: [HIDDEN]"

echo ""
echo "💡 To use these variables in your current shell session, run:"
echo "  source export_my_env.sh"
echo ""
echo "💡 To make them permanent, add them to your ~/.zshrc or ~/.bashrc file" 