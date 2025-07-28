"""
OLTP-Databricks: PostgreSQL Database Manager for Databricks

A comprehensive Python solution for connecting to and managing PostgreSQL databases 
hosted on Databricks. This project provides a robust database manager class with 
table creation, data insertion, querying, and advanced SQL operations.

Features:
- Secure SSL-enabled PostgreSQL connection to Databricks
- Table management (create, drop, manage database tables)
- Data operations (insert, update, delete, query data)
- Pandas integration (query results returned as pandas DataFrames)
- Advanced queries (complex SQL operations including JOINs, window functions)
- Error handling and logging
- Connection pooling
"""

__version__ = "1.0.0"
__author__ = "Rohit Dashora"
__email__ = "rohit.dashora@databricks.com"

from .database_manager import DatabaseManager
from .config import DB_CONFIG, DATABASE_URL

__all__ = [
    "DatabaseManager",
    "DB_CONFIG", 
    "DATABASE_URL"
] 