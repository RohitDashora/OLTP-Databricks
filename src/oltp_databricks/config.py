"""
config.py

Environment-based configuration for OLTP-Databricks PostgreSQL connection.
"""

import os
from dotenv import load_dotenv
from typing import Optional, Dict

# Load environment variables from .env file if it exists
load_dotenv()

# Database connection parameters - ONLY from environment variables
DB_CONFIG: Dict[str, Optional[str]] = {
    'host': os.getenv('DATABRICKS_DB_HOST'),
    'user': os.getenv('DATABRICKS_DB_USER'),
    'dbname': os.getenv('DATABRICKS_DB_NAME'),
    'port': os.getenv('DATABRICKS_DB_PORT'),
    'sslmode': os.getenv('DATABRICKS_DB_SSLMODE'),
    'password': os.getenv('DATABRICKS_DB_PASSWORD')
}

# Connection string for SQLAlchemy
DATABASE_URL: Optional[str] = (
    f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}?sslmode={DB_CONFIG['sslmode']}"
    if all(DB_CONFIG.values()) else None
) 