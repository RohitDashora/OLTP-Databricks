"""
quick_insert.py

Quick and simple data insertion script for PostgreSQL database using OLTP-Databricks DatabaseManager.

Features:
- Non-interactive bulk data insertion
- Quick setup of sample data for all tables
- Single function execution for rapid prototyping
- Sample data for users, products, orders, order_items, employees, departments

Tables covered:
- users, products, orders, order_items
- employees, departments

Note:
- If you run this script multiple times, you may see duplicate key errors or 'table already exists' errors.
- To avoid this, clean up demo tables between runs (see README FAQ), or use unique data.
- This script is designed for quick setup and may not handle all duplicate scenarios.
"""

from datetime import datetime, date, timedelta
from oltp_databricks.database_manager import DatabaseManager
from oltp_databricks.config import DB_CONFIG
import random


def quick_insert_all_data():
    """Quickly insert sample data into all tables"""
    print("🚀 Quick Data Insertion")
    print("=" * 40)
    
    # Get password from config
    password = DB_CONFIG.get('password')
    if not password:
        print("❌ No password found in config.py")
        return
    
    # Initialize database manager
    db_manager = DatabaseManager(password=password)
    
    if not db_manager.connect():
        print("❌ Failed to connect to database")
        return
    
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
    
    # Get password from config
    password = DB_CONFIG.get('password')
    if not password:
        print("❌ No password found in config.py")
        return
    
    db_manager = DatabaseManager(password=password)
    
    if not db_manager.connect():
        print("❌ Failed to connect to database")
        return
    
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