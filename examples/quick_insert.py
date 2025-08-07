"""
quick_insert.py

Quick and simple data insertion script for PostgreSQL database using OLTP-Databricks DatabaseManager.

Features:
- Non-interactive bulk data insertion
- Automatic table creation if they don't exist
- Quick setup of sample data for all tables
- Single function execution for rapid prototyping
- Sample data for users, products, orders, order_items, employees, departments, projects

Tables covered:
- users, products, orders, order_items
- employees, departments, projects

Note:
- Tables are automatically created if they don't exist
- If you run this script multiple times, you may see duplicate key errors for unique constraints
- To avoid this, clean up demo tables between runs (see README FAQ), or use unique data
- This script is designed for quick setup and may not handle all duplicate scenarios
"""

from datetime import datetime, date, timedelta
from oltp_databricks.database_manager import DatabaseManager
from oltp_databricks.config import DB_CONFIG
import random


def create_all_tables(db_manager: DatabaseManager):
    """Create all necessary tables if they don't exist"""
    print("🔨 Creating tables if they don't exist...")
    
    # Users table
    users_columns = [
        {'name': 'user_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'username', 'type': 'VARCHAR(50) UNIQUE NOT NULL'},
        {'name': 'email', 'type': 'VARCHAR(100) UNIQUE NOT NULL'},
        {'name': 'created_at', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'},
        {'name': 'is_active', 'type': 'BOOLEAN DEFAULT TRUE'}
    ]
    
    # Products table
    products_columns = [
        {'name': 'product_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'name', 'type': 'VARCHAR(100) NOT NULL'},
        {'name': 'description', 'type': 'TEXT'},
        {'name': 'price', 'type': 'DECIMAL(10,2) NOT NULL'},
        {'name': 'category', 'type': 'VARCHAR(50)'},
        {'name': 'stock_quantity', 'type': 'INTEGER DEFAULT 0'}
    ]
    
    # Orders table
    orders_columns = [
        {'name': 'order_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'user_id', 'type': 'INTEGER REFERENCES users(user_id)'},
        {'name': 'order_date', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'},
        {'name': 'total_amount', 'type': 'DECIMAL(10,2) NOT NULL'},
        {'name': 'status', 'type': 'VARCHAR(20) DEFAULT \'pending\''}
    ]
    
    # Order items table
    order_items_columns = [
        {'name': 'order_item_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'order_id', 'type': 'INTEGER REFERENCES orders(order_id)'},
        {'name': 'product_id', 'type': 'INTEGER REFERENCES products(product_id)'},
        {'name': 'quantity', 'type': 'INTEGER NOT NULL'},
        {'name': 'unit_price', 'type': 'DECIMAL(10,2) NOT NULL'}
    ]
    
    # Employees table
    employees_columns = [
        {'name': 'employee_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'first_name', 'type': 'VARCHAR(50) NOT NULL'},
        {'name': 'last_name', 'type': 'VARCHAR(50) NOT NULL'},
        {'name': 'email', 'type': 'VARCHAR(100) UNIQUE NOT NULL'},
        {'name': 'department', 'type': 'VARCHAR(50)'},
        {'name': 'salary', 'type': 'DECIMAL(10,2)'},
        {'name': 'hire_date', 'type': 'DATE DEFAULT CURRENT_DATE'}
    ]
    
    # Departments table
    departments_columns = [
        {'name': 'department_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'department_name', 'type': 'VARCHAR(50) UNIQUE NOT NULL'},
        {'name': 'location', 'type': 'VARCHAR(100)'},
        {'name': 'budget', 'type': 'DECIMAL(12,2)'}
    ]
    
    # Projects table
    projects_columns = [
        {'name': 'project_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'project_name', 'type': 'VARCHAR(100) NOT NULL'},
        {'name': 'description', 'type': 'TEXT'},
        {'name': 'start_date', 'type': 'DATE'},
        {'name': 'end_date', 'type': 'DATE'},
        {'name': 'status', 'type': 'VARCHAR(20) DEFAULT \'active\''}
    ]
    
    # All tables with their columns
    tables = [
        ('users', users_columns),
        ('products', products_columns),
        ('orders', orders_columns),
        ('order_items', order_items_columns),
        ('employees', employees_columns),
        ('departments', departments_columns),
        ('projects', projects_columns)
    ]
    
    for table_name, columns in tables:
        if db_manager.create_table(table_name, columns):
            print(f"✅ Table '{table_name}' created/verified")
        else:
            print(f"⚠️  Table '{table_name}' already exists or creation failed")


def quick_insert_all_data():
    """Quickly insert sample data into all tables"""
    print("🚀 Quick Data Insertion")
    print("=" * 40)
    
    # Check if password is available in config
    password = DB_CONFIG.get('password')
    if not password:
        print("❌ No password found in config.py")
        return
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    if not db_manager.connect():
        print("❌ Failed to connect to database")
        return
    
    # Create all tables first
    create_all_tables(db_manager)
    
    try:
        print("📝 Inserting sample data into all tables...")
        
        # 1. Insert users
        users_data = [
            {'username': 'john_doe', 'email': 'john@example.com'},
            {'username': 'jane_smith', 'email': 'jane@example.com'},
            {'username': 'bob_wilson', 'email': 'bob@example.com'},
            {'username': 'alice_johnson', 'email': 'alice@example.com'},
            {'username': 'charlie_brown', 'email': 'charlie@example.com'}
        ]
        if db_manager.insert_data('users', users_data):
            print("✅ Users inserted")
        
        # 2. Insert products
        products_data = [
            {'name': 'Laptop', 'description': 'High-performance laptop', 'price': 999.99, 'category': 'Electronics', 'stock_quantity': 50},
            {'name': 'Mouse', 'description': 'Wireless mouse', 'price': 29.99, 'category': 'Electronics', 'stock_quantity': 100},
            {'name': 'Keyboard', 'description': 'Mechanical keyboard', 'price': 89.99, 'category': 'Electronics', 'stock_quantity': 75},
            {'name': 'Coffee Mug', 'description': 'Ceramic coffee mug', 'price': 12.99, 'category': 'Home', 'stock_quantity': 200},
            {'name': 'Notebook', 'description': 'Spiral bound notebook', 'price': 5.99, 'category': 'Office', 'stock_quantity': 150}
        ]
        if db_manager.insert_data('products', products_data):
            print("✅ Products inserted")
        
        # 3. Insert orders (need to get user IDs first)
        users_result = db_manager.execute_query("SELECT user_id FROM users LIMIT 3")
        if users_result:
            user_ids = [row[0] for row in users_result]
            orders_data = []
            for i in range(5):
                orders_data.append({
                    'user_id': random.choice(user_ids),
                    'order_date': datetime.now() - timedelta(days=random.randint(0, 30)),
                    'total_amount': round(random.uniform(50.0, 300.0), 2),
                    'status': random.choice(['pending', 'processing', 'shipped'])
                })
            if db_manager.insert_data('orders', orders_data):
                print("✅ Orders inserted")
        
        # 4. Insert order items
        orders_result = db_manager.execute_query("SELECT order_id FROM orders LIMIT 5")
        products_result = db_manager.execute_query("SELECT product_id, price FROM products LIMIT 5")
        if orders_result and products_result:
            order_ids = [row[0] for row in orders_result]
            products = [(row[0], row[1]) for row in products_result]
            order_items_data = []
            for order_id in order_ids:
                for _ in range(random.randint(1, 3)):
                    product_id, unit_price = random.choice(products)
                    order_items_data.append({
                        'order_id': order_id,
                        'product_id': product_id,
                        'quantity': random.randint(1, 3),
                        'unit_price': unit_price
                    })
            if db_manager.insert_data('order_items', order_items_data):
                print("✅ Order items inserted")
        
        # 5. Insert employees
        employees_data = [
            {'first_name': 'Alice', 'last_name': 'Johnson', 'email': 'alice.j@company.com', 'department': 'Engineering', 'salary': 85000.00},
            {'first_name': 'Bob', 'last_name': 'Smith', 'email': 'bob.s@company.com', 'department': 'Marketing', 'salary': 65000.00},
            {'first_name': 'Carol', 'last_name': 'Davis', 'email': 'carol.d@company.com', 'department': 'Sales', 'salary': 70000.00},
            {'first_name': 'David', 'last_name': 'Wilson', 'email': 'david.w@company.com', 'department': 'Engineering', 'salary': 90000.00},
            {'first_name': 'Eva', 'last_name': 'Brown', 'email': 'eva.b@company.com', 'department': 'HR', 'salary': 55000.00}
        ]
        if db_manager.insert_data('employees', employees_data):
            print("✅ Employees inserted")
        
        # 6. Insert departments
        departments_data = [
            {'department_name': 'Engineering', 'location': 'San Francisco', 'budget': 1000000.00},
            {'department_name': 'Marketing', 'location': 'New York', 'budget': 500000.00},
            {'department_name': 'Sales', 'location': 'Chicago', 'budget': 750000.00},
            {'department_name': 'HR', 'location': 'Austin', 'budget': 300000.00}
        ]
        if db_manager.insert_data('departments', departments_data):
            print("✅ Departments inserted")
        
        # 7. Insert projects
        projects_data = [
            {'project_name': 'Website Redesign', 'description': 'Redesign company website', 'start_date': date(2024, 1, 15), 'end_date': date(2024, 6, 30), 'status': 'active'},
            {'project_name': 'Mobile App', 'description': 'Develop mobile application', 'start_date': date(2024, 2, 1), 'end_date': date(2024, 12, 31), 'status': 'active'},
            {'project_name': 'Data Migration', 'description': 'Migrate legacy data', 'start_date': date(2023, 11, 1), 'end_date': date(2024, 3, 31), 'status': 'completed'}
        ]
        if db_manager.insert_data('projects', projects_data):
            print("✅ Projects inserted")
        
        print("\n🎉 All sample data inserted successfully!")
        
        # Show summary
        print("\n📊 Data Summary:")
        tables = ['users', 'products', 'orders', 'order_items', 'employees', 'departments', 'projects']
        for table in tables:
            result = db_manager.execute_query(f"SELECT COUNT(*) FROM {table}")
            if result:
                count = result[0][0]
                print(f"  {table}: {count} rows")
    
    finally:
        db_manager.disconnect()


def insert_single_row():
    """Insert a single row into a specified table"""
    print("🎯 Single Row Insertion")
    print("=" * 30)
    
    # Check if password is available in config
    password = DB_CONFIG.get('password')
    if not password:
        print("❌ No password found in config.py")
        return
    
    db_manager = DatabaseManager()
    
    if not db_manager.connect():
        print("❌ Failed to connect to database")
        return
    
    # Create all tables first
    create_all_tables(db_manager)
    
    try:
        # Example: Insert a single user
        new_user = [{'username': 'new_user', 'email': 'newuser@example.com'}]
        if db_manager.insert_data('users', new_user):
            print("✅ Single user inserted successfully")
        
        # Example: Insert a single product
        new_product = [{
            'name': 'New Product',
            'description': 'A brand new product',
            'price': 49.99,
            'category': 'Electronics',
            'stock_quantity': 25
        }]
        if db_manager.insert_data('products', new_product):
            print("✅ Single product inserted successfully")
    
    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    print("Choose insertion method:")
    print("1. Quick insert all sample data")
    print("2. Insert single row")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == '1':
        quick_insert_all_data()
    elif choice == '2':
        insert_single_row()
    else:
        print("Invalid choice") 