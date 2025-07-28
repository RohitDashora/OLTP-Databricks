"""
database_manager.py

Core database manager for OLTP-Databricks.
Provides connection management, table operations, data manipulation, and advanced queries for PostgreSQL on Databricks.
"""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import List, Dict, Any, Optional, Tuple
from .config import DB_CONFIG, DATABASE_URL

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Manages PostgreSQL database connections and operations for Databricks.
    """
    def __init__(self) -> None:
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.engine = None

    def connect(self) -> bool:
        """
        Establish a connection to the PostgreSQL database.
        Returns True if successful, False otherwise.
        """
        try:
            self.conn = psycopg2.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                dbname=DB_CONFIG['dbname'],
                port=DB_CONFIG['port'],
                sslmode=DB_CONFIG['sslmode'],
                password=DB_CONFIG['password']
            )
            # Create SQLAlchemy engine with password
            if DB_CONFIG['password']:
                engine_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}?sslmode={DB_CONFIG['sslmode']}"
            else:
                engine_url = DATABASE_URL
            self.engine = create_engine(engine_url)
            logger.info("Connected to database successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False

    def disconnect(self) -> None:
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.")
        self.conn = None
        self.engine = None

    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> Optional[List[Tuple[Any, ...]]]:
        """
        Execute a SQL query with optional parameters.
        Returns the result as a list of tuples, or None on error.
        """
        if not self.conn:
            logger.error("No active database connection.")
            return None
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                if cur.description:
                    result = cur.fetchall()
                    logger.debug(f"Query result: {result}")
                    return result
                self.conn.commit()
                return None
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            self.conn.rollback()
            return None

    def create_table(self, table_name: str, columns: List[Dict[str, str]]) -> bool:
        """
        Create a new table with the given name and columns.
        Returns True if successful, False otherwise.
        """
        col_defs = ", ".join([f"{col['name']} {col['type']}" for col in columns])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs});"
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
            self.conn.commit()
            logger.info(f"Table '{table_name}' created successfully.")
            return True
        except Exception as e:
            logger.error(f"Error creating table '{table_name}': {e}")
            self.conn.rollback()
            return False

    def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> bool:
        """
        Insert data into a table.
        Returns True if successful, False otherwise.
        """
        if not data:
            logger.warning("No data provided for insertion.")
            return False
        columns = data[0].keys()
        values = [[row[col] for col in columns] for row in data]
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        try:
            with self.conn.cursor() as cur:
                cur.executemany(query, values)
            self.conn.commit()
            logger.info(f"Inserted {len(data)} rows into {table_name}.")
            return True
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            self.conn.rollback()
            return False

    def insert_data_ignore_duplicates(self, table_name: str, data: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Insert data into a table, ignoring duplicates.
        Returns a tuple of (inserted_count, skipped_count).
        """
        if not data:
            logger.warning("No data provided for insertion.")
            return 0, 0
        
        inserted_count = 0
        skipped_count = 0
        
        for row in data:
            try:
                columns = row.keys()
                values = [row[col] for col in columns]
                placeholders = ", ".join(["%s"] * len(columns))
                query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                
                with self.conn.cursor() as cur:
                    cur.execute(query, values)
                    if cur.rowcount > 0:
                        inserted_count += 1
                    else:
                        skipped_count += 1
                
            except Exception as e:
                logger.error(f"Error inserting row {row}: {e}")
                skipped_count += 1
        
        self.conn.commit()
        logger.info(f"Inserted {inserted_count} rows, skipped {skipped_count} duplicates in {table_name}.")
        return inserted_count, skipped_count

    def query_to_dataframe(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> Optional[pd.DataFrame]:
        """
        Execute a query and return the result as a pandas DataFrame.
        Returns None on error.
        """
        if not self.engine:
            logger.error("No SQLAlchemy engine available.")
            return None
        try:
            df = pd.read_sql_query(text(query), self.engine, params=params)
            logger.debug(f"Query returned DataFrame with {len(df)} rows.")
            return df
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error: {e}")
            return None
        except Exception as e:
            logger.error(f"Error querying to DataFrame: {e}")
            return None

    def get_table_info(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get information about the columns of a table.
        Returns a list of column info dicts, or None on error.
        """
        query = f"SELECT column_name AS name, data_type AS type FROM information_schema.columns WHERE table_name = %s;"
        result = self.execute_query(query, (table_name,))
        if result is None:
            return None
        return [{"name": row[0], "type": row[1]} for row in result]

    def list_tables(self) -> Optional[List[str]]:
        """
        List all tables in the current database.
        Returns a list of table names, or None on error.
        """
        query = """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
        """
        result = self.execute_query(query)
        if result is None:
            return None
        return [row[0] for row in result]

    def drop_table(self, table_name: str) -> bool:
        """
        Drop a table by name.
        Returns True if successful, False otherwise.
        """
        query = f"DROP TABLE IF EXISTS {table_name};"
        return self.execute_query(query) is not None


# Example usage and table creation functions
def create_sample_tables(db_manager: DatabaseManager):
    """Create sample tables for demonstration"""
    
    # Create users table
    users_columns = [
        {'name': 'user_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'username', 'type': 'VARCHAR(50) UNIQUE NOT NULL'},
        {'name': 'email', 'type': 'VARCHAR(100) UNIQUE NOT NULL'},
        {'name': 'created_at', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'},
        {'name': 'is_active', 'type': 'BOOLEAN DEFAULT TRUE'}
    ]
    
    # Create orders table
    orders_columns = [
        {'name': 'order_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'user_id', 'type': 'INTEGER REFERENCES users(user_id)'},
        {'name': 'order_date', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'},
        {'name': 'total_amount', 'type': 'DECIMAL(10,2) NOT NULL'},
        {'name': 'status', 'type': 'VARCHAR(20) DEFAULT \'pending\''}
    ]
    
    # Create products table
    products_columns = [
        {'name': 'product_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'name', 'type': 'VARCHAR(100) NOT NULL'},
        {'name': 'description', 'type': 'TEXT'},
        {'name': 'price', 'type': 'DECIMAL(10,2) NOT NULL'},
        {'name': 'category', 'type': 'VARCHAR(50)'},
        {'name': 'stock_quantity', 'type': 'INTEGER DEFAULT 0'}
    ]
    
    # Create order_items table
    order_items_columns = [
        {'name': 'order_item_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'order_id', 'type': 'INTEGER REFERENCES orders(order_id)'},
        {'name': 'product_id', 'type': 'INTEGER REFERENCES products(product_id)'},
        {'name': 'quantity', 'type': 'INTEGER NOT NULL'},
        {'name': 'unit_price', 'type': 'DECIMAL(10,2) NOT NULL'}
    ]
    
    tables = [
        ('users', users_columns),
        ('orders', orders_columns),
        ('products', products_columns),
        ('order_items', order_items_columns)
    ]
    
    for table_name, columns in tables:
        if db_manager.create_table(table_name, columns):
            logger.info(f"Table '{table_name}' created successfully")
        else:
            logger.error(f"Failed to create table '{table_name}'")


def insert_sample_data(db_manager: DatabaseManager):
    """Insert sample data into tables"""
    
    # Sample users data
    users_data = [
        {'username': 'john_doe', 'email': 'john@example.com'},
        {'username': 'jane_smith', 'email': 'jane@example.com'},
        {'username': 'bob_wilson', 'email': 'bob@example.com'}
    ]
    
    # Sample products data
    products_data = [
        {'name': 'Laptop', 'description': 'High-performance laptop', 'price': 999.99, 'category': 'Electronics', 'stock_quantity': 50},
        {'name': 'Mouse', 'description': 'Wireless mouse', 'price': 29.99, 'category': 'Electronics', 'stock_quantity': 100},
        {'name': 'Keyboard', 'description': 'Mechanical keyboard', 'price': 89.99, 'category': 'Electronics', 'stock_quantity': 75},
        {'name': 'Coffee Mug', 'description': 'Ceramic coffee mug', 'price': 12.99, 'category': 'Home', 'stock_quantity': 200}
    ]
    
    # Insert data
    if db_manager.insert_data('users', users_data):
        logger.info("Sample users data inserted")
    
    if db_manager.insert_data('products', products_data):
        logger.info("Sample products data inserted")


def run_sample_queries(db_manager: DatabaseManager):
    """Run sample queries to demonstrate functionality"""
    
    print("\n=== Sample Queries ===")
    
    # List all tables
    tables = db_manager.list_tables()
    print(f"Tables in database: {tables}")
    
    # Query all users
    users_df = db_manager.query_to_dataframe("SELECT * FROM users")
    print("\nUsers:")
    print(users_df)
    
    # Query all products
    products_df = db_manager.query_to_dataframe("SELECT * FROM products")
    print("\nProducts:")
    print(products_df)
    
    # Query products by category
    electronics_df = db_manager.query_to_dataframe(
        "SELECT name, price FROM products WHERE category = %s",
        ('Electronics',)
    )
    print("\nElectronics products:")
    print(electronics_df)
    
    # Get table structure
    users_info = db_manager.get_table_info('users')
    print("\nUsers table structure:")
    print(users_info)


if __name__ == "__main__":
    # Example usage
    db_manager = DatabaseManager()
    
    if db_manager.connect():
        try:
            # Create sample tables
            create_sample_tables(db_manager)
            
            # Insert sample data
            insert_sample_data(db_manager)
            
            # Run sample queries
            run_sample_queries(db_manager)
            
        finally:
            db_manager.disconnect()
    else:
        print("Failed to connect to database") 