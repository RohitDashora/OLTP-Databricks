#!/bin/bash

# Activate PostgreSQL virtual environment
echo "🐍 Activating pgconnect virtual environment..."
source pgconnect/bin/activate

# Install/upgrade packages from requirements.txt
echo "📦 Installing/upgrading packages from requirements.txt..."
pip install -r requirements.txt --upgrade

# Export database environment variables
if [ -f scripts/export_my_env.sh ]; then
  echo "🔧 Exporting database configuration from export_my_env.sh..."
  source scripts/export_my_env.sh
else
  echo "🔧 Exporting database configuration from export_env.sh..."
  source scripts/export_env.sh
fi

echo "✅ Virtual environment activated!"
echo "📦 Installed package versions from requirements.txt:"
while IFS= read -r line; do
  # Ignore comments and blank lines
  pkg=$(echo "$line" | sed 's/[>=<].*//' | xargs)
  if [[ -n "$pkg" && ! "$pkg" =~ ^# ]]; then
    pip show "$pkg" 2>/dev/null | grep -E '^(Name|Version):'
  fi
done < requirements.txt

echo ""
echo "🚀 Available scripts:"
echo "  python -m oltp_databricks.test_env_config  # Test environment configuration"
echo "  python examples/test_connection.py         # Test connection"
echo "  python examples/connect_with_password.py   # Test connection with password"
echo "  python examples/database_manager.py        # Run basic example"
echo "  python examples/example_queries.py         # Run advanced queries"
echo "  python examples/insert_data.py             # Interactive data insertion"
echo "  python examples/quick_insert.py            # Quick data insertion"
echo ""
echo "💡 To deactivate the environment, run: deactivate" 