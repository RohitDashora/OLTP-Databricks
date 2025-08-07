"""
Script to test PostgreSQL connection with password from config
"""

from oltp_databricks.database_manager import DatabaseManager
from oltp_databricks.config import DB_CONFIG


def test_connection_with_config():
    """Test database connection with password from config"""
    print("🔐 PostgreSQL Connection Test with Config Password")
    print("=" * 50)
    
    # Check if password is available in config
    password = DB_CONFIG.get('password')
    if not password:
        print("❌ No password found in config.py")
        return False
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    if db_manager.connect():
        print("✅ Connection successful!")
        
        # Test listing tables
        tables = db_manager.list_tables()
        print(f"📋 Existing tables: {tables}")
        
        # Test simple query
        try:
            result = db_manager.execute_query("SELECT version();")
            if result:
                print(f"✅ Database version: {result[0][0]}")
        except Exception as e:
            print(f"❌ Error executing test query: {e}")
        
        db_manager.disconnect()
        return True
    else:
        print("❌ Connection failed!")
        print("\nPossible issues:")
        print("1. Incorrect password")
        print("2. User doesn't have permission to access the database")
        print("3. Network/firewall restrictions")
        return False


if __name__ == "__main__":
    test_connection_with_config() 