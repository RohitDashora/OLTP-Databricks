#!/usr/bin/env python3
"""
Basic example demonstrating the DatabaseManager functionality
"""

from oltp_databricks.database_manager import DatabaseManager


def main():
    """Run basic database manager example"""
    print("🚀 OLTP-Databricks Database Manager Example")
    print("=" * 50)
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Connect to database
    if not db_manager.connect():
        print("❌ Failed to connect to database")
        print("Please check your configuration and try again.")
        return
    
    print("✅ Connected to database successfully!")
    
    try:
        # List existing tables
        print("\n📋 Existing tables:")
        tables = db_manager.list_tables()
        if tables:
            for table in tables:
                print(f"  - {table}")
        else:
            print("  No tables found")
        
        # Create a sample users table (use a unique name to avoid conflicts)
        import time
        table_name = f'sample_users_demo_{int(time.time())}'
        print(f"\n🔨 Creating sample table '{table_name}'...")
        columns = [
            {'name': 'id', 'type': 'SERIAL PRIMARY KEY'},
            {'name': 'username', 'type': 'VARCHAR(50) UNIQUE NOT NULL'},
            {'name': 'email', 'type': 'VARCHAR(100) UNIQUE NOT NULL'},
            {'name': 'created_at', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'}
        ]
        
        if db_manager.create_table(table_name, columns):
            print(f"✅ Table '{table_name}' created successfully")
        else:
            print(f"❌ Failed to create table '{table_name}'")
            return
        
        # Insert sample data
        print("\n📝 Inserting sample data...")
        sample_data = [
            {'username': 'john_doe', 'email': 'john@example.com'},
            {'username': 'jane_smith', 'email': 'jane@example.com'},
            {'username': 'bob_wilson', 'email': 'bob@example.com'}
        ]
        
        if db_manager.insert_data(table_name, sample_data):
            print("✅ Sample data inserted successfully")
        else:
            print("❌ Failed to insert sample data")
            return
        
        # Query the data
        print("\n🔍 Querying data...")
        results = db_manager.query_to_dataframe(f"SELECT * FROM {table_name} ORDER BY id")
        if results is not None:
            print("✅ Query successful")
            print("📊 Sample data:")
            print(results)
        else:
            print("❌ Failed to query data")
            return
        
        # Get table information
        print("\n📋 Table information:")
        table_info = db_manager.get_table_info(table_name)
        if table_info:
            print("✅ Table info retrieved")
            for column in table_info:
                print(f"  - {column['name']}: {column['type']}")
        else:
            print("❌ Failed to get table info")
        
        # Clean up - drop the table
        print("\n🧹 Cleaning up...")
        if db_manager.drop_table(table_name):
            print(f"✅ Table '{table_name}' dropped successfully")
        else:
            print(f"⚠️  Failed to drop table '{table_name}'")
        
        print("\n🎉 Example completed successfully!")
        print("\nYou can now try:")
        print("  python examples/example_queries.py     # Advanced queries")
        print("  python examples/insert_data.py         # Interactive data insertion")
        print("  python examples/quick_insert.py        # Quick data insertion")
        
    except Exception as e:
        print(f"❌ Error during example: {e}")
    finally:
        # Disconnect from database
        db_manager.disconnect()
        print("\n🔌 Disconnected from database")


if __name__ == "__main__":
    main() 