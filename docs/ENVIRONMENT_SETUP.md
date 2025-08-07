# Environment Variable Setup Guide

This guide explains how to use environment variables for database configuration in the OLTP-Databricks project.

## Quick Start

1. **Copy the template file:**
   ```bash
   cp scripts/export_env.sh scripts/export_my_env.sh
   # Edit scripts/export_my_env.sh with your credentials
   ```
2. **Activate your environment:**
   ```bash
   source scripts/activate_env.sh
   ```
3. **Test your configuration:**
   ```bash
   python -m oltp_databricks.test_env_config
   ```
4. **🚀 Set up database tables (REQUIRED):**
   ```bash
   python examples/quick_insert.py
   # Choose option 1 to create all tables and insert sample data
   ```

## Cleaning Up Demo Tables

If you run examples multiple times, you may need to clean up demo tables to avoid duplicate key or table exists errors. Connect to your database and run:
```sql
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS employees CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TABLE IF EXISTS projects CASCADE;
```
Or use a SQL client/GUI to drop tables.

**Note:** After cleaning up, you'll need to run `python examples/quick_insert.py` again to recreate the tables before running other examples.

## Resetting Your Environment

If you want to start fresh:
```bash
rm -rf pgconnect/
python -m venv pgconnect
source scripts/activate_env.sh
```

## Tips
- Always use `scripts/export_my_env.sh` for your real credentials (never commit this file).
- Use unique table names or data for repeated runs, or clean up demo tables between runs.
- For more, see the FAQ in the main README. 