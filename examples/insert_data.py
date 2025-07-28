"""
insert_data.py

Comprehensive data insertion script for PostgreSQL database using OLTP-Databricks DatabaseManager.

Features:
- Interactive menu-driven data insertion
- Bulk data operations for multiple tables
- Sample data generation for users, products, orders, employees, departments, projects
- Duplicate handling with insert_data_ignore_duplicates
- Custom data insertion capabilities

Tables covered:
- users, products, orders, order_items
- employees, departments, projects

Note:
- If you run this script multiple times, you may see duplicate key errors or 'table already exists' errors.
- To avoid this, clean up demo tables between runs (see README FAQ), or use unique data.
- The script includes duplicate handling for some operations but not all.
"""

from datetime import datetime, date, timedelta
from oltp_databricks.database_manager import DatabaseManager
from oltp_databricks.config import DB_CONFIG
import random


def insert_users_data(db_manager: DatabaseManager):
    """Insert new users into the users table"""
    print("\n👥 Inserting new users...")
    
    new_users = [
        {'username': 'sarah_wilson', 'email': 'sarah.w@example.com'},
        {'username': 'mike_chen', 'email': 'mike.c@example.com'},
        {'username': 'lisa_rodriguez', 'email': 'lisa.r@example.com'},
        {'username': 'alex_kumar', 'email': 'alex.k@example.com'},
        {'username': 'emma_thompson', 'email': 'emma.t@example.com'},
        {'username': 'david_lee', 'email': 'david.l@example.com'},
        {'username': 'rachel_green', 'email': 'rachel.g@example.com'},
        {'username': 'tom_anderson', 'email': 'tom.a@example.com'}
    ]
    
    inserted, skipped = db_manager.insert_data_ignore_duplicates('users', new_users)
    if inserted > 0:
        print(f"✅ Successfully inserted {inserted} new users")
    if skipped > 0:
        print(f"⏭️  Skipped {skipped} existing users (duplicates)")
    if inserted == 0 and skipped == 0:
        print("❌ Failed to insert users")


def insert_products_data(db_manager: DatabaseManager):
    """Insert new products into the products table"""
    print("\n📦 Inserting new products...")
    
    new_products = [
        {'name': 'Wireless Headphones', 'description': 'Noise-cancelling wireless headphones', 'price': 199.99, 'category': 'Electronics', 'stock_quantity': 30},
        {'name': 'Smart Watch', 'description': 'Fitness tracking smartwatch', 'price': 299.99, 'category': 'Electronics', 'stock_quantity': 25},
        {'name': 'Coffee Maker', 'description': 'Programmable coffee maker', 'price': 89.99, 'category': 'Home', 'stock_quantity': 40},
        {'name': 'Yoga Mat', 'description': 'Non-slip yoga mat', 'price': 29.99, 'category': 'Fitness', 'stock_quantity': 60},
        {'name': 'Bluetooth Speaker', 'description': 'Portable bluetooth speaker', 'price': 79.99, 'category': 'Electronics', 'stock_quantity': 35},
        {'name': 'Desk Lamp', 'description': 'LED desk lamp with adjustable brightness', 'price': 45.99, 'category': 'Home', 'stock_quantity': 50},
        {'name': 'Running Shoes', 'description': 'Comfortable running shoes', 'price': 129.99, 'category': 'Fitness', 'stock_quantity': 45},
        {'name': 'Backpack', 'description': 'Waterproof hiking backpack', 'price': 65.99, 'category': 'Outdoor', 'stock_quantity': 30}
    ]
    
    inserted, skipped = db_manager.insert_data_ignore_duplicates('products', new_products)
    if inserted > 0:
        print(f"✅ Successfully inserted {inserted} new products")
    if skipped > 0:
        print(f"⏭️  Skipped {skipped} existing products (duplicates)")
    if inserted == 0 and skipped == 0:
        print("❌ Failed to insert products")


def insert_orders_data(db_manager: DatabaseManager):
    """Insert new orders into the orders table"""
    print("\n🛒 Inserting new orders...")
    
    # First, get existing user IDs
    users_result = db_manager.execute_query("SELECT user_id FROM users LIMIT 5")
    if not users_result:
        print("❌ No users found to create orders for")
        return
    
    user_ids = [row[0] for row in users_result]
    
    # Generate orders for the last 30 days
    orders = []
    for i in range(10):
        user_id = random.choice(user_ids)
        order_date = datetime.now() - timedelta(days=random.randint(0, 30))
        total_amount = round(random.uniform(50.0, 500.0), 2)
        status = random.choice(['pending', 'processing', 'shipped', 'delivered'])
        
        orders.append({
            'user_id': user_id,
            'order_date': order_date,
            'total_amount': total_amount,
            'status': status
        })
    
    if db_manager.insert_data('orders', orders):
        print(f"✅ Successfully inserted {len(orders)} new orders")
    else:
        print("❌ Failed to insert orders")


def insert_order_items_data(db_manager: DatabaseManager):
    """Insert new order items into the order_items table"""
    print("\n📋 Inserting new order items...")
    
    # Get existing order IDs and product IDs
    orders_result = db_manager.execute_query("SELECT order_id FROM orders LIMIT 8")
    products_result = db_manager.execute_query("SELECT product_id, price FROM products LIMIT 10")
    
    if not orders_result or not products_result:
        print("❌ No orders or products found to create order items for")
        return
    
    order_ids = [row[0] for row in orders_result]
    products = [(row[0], row[1]) for row in products_result]
    
    order_items = []
    for order_id in order_ids:
        # Add 1-3 items per order
        num_items = random.randint(1, 3)
        for _ in range(num_items):
            product_id, unit_price = random.choice(products)
            quantity = random.randint(1, 5)
            
            order_items.append({
                'order_id': order_id,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price
            })
    
    if db_manager.insert_data('order_items', order_items):
        print(f"✅ Successfully inserted {len(order_items)} new order items")
    else:
        print("❌ Failed to insert order items")


def insert_employees_data(db_manager: DatabaseManager):
    """Insert new employees into the employees table"""
    print("\n👨‍💼 Inserting new employees...")
    
    new_employees = [
        {'first_name': 'Grace', 'last_name': 'Taylor', 'email': 'grace.t@company.com', 'department': 'Engineering', 'salary': 95000.00},
        {'first_name': 'James', 'last_name': 'Martinez', 'email': 'james.m@company.com', 'department': 'Sales', 'salary': 75000.00},
        {'first_name': 'Sophia', 'last_name': 'Garcia', 'email': 'sophia.g@company.com', 'department': 'Marketing', 'salary': 70000.00},
        {'first_name': 'Daniel', 'last_name': 'Brown', 'email': 'daniel.b@company.com', 'department': 'Engineering', 'salary': 88000.00},
        {'first_name': 'Olivia', 'last_name': 'Davis', 'email': 'olivia.d@company.com', 'department': 'HR', 'salary': 60000.00},
        {'first_name': 'William', 'last_name': 'Miller', 'email': 'william.m@company.com', 'department': 'Sales', 'salary': 72000.00},
        {'first_name': 'Ava', 'last_name': 'Wilson', 'email': 'ava.w@company.com', 'department': 'Marketing', 'salary': 68000.00},
        {'first_name': 'Ethan', 'last_name': 'Moore', 'email': 'ethan.m@company.com', 'department': 'Engineering', 'salary': 92000.00}
    ]
    
    if db_manager.insert_data('employees', new_employees):
        print(f"✅ Successfully inserted {len(new_employees)} new employees")
    else:
        print("❌ Failed to insert employees")


def insert_departments_data(db_manager: DatabaseManager):
    """Insert new departments into the departments table"""
    print("\n🏢 Inserting new departments...")
    
    new_departments = [
        {'department_name': 'Research & Development', 'location': 'Boston', 'budget': 1200000.00},
        {'department_name': 'Customer Support', 'location': 'Denver', 'budget': 400000.00},
        {'department_name': 'Finance', 'location': 'Seattle', 'budget': 600000.00},
        {'department_name': 'Legal', 'location': 'Los Angeles', 'budget': 350000.00}
    ]
    
    if db_manager.insert_data('departments', new_departments):
        print(f"✅ Successfully inserted {len(new_departments)} new departments")
    else:
        print("❌ Failed to insert departments")


def insert_projects_data(db_manager: DatabaseManager):
    """Insert new projects into the projects table"""
    print("\n📊 Inserting new projects...")
    
    new_projects = [
        {'project_name': 'Cloud Migration', 'description': 'Migrate infrastructure to cloud platform', 'start_date': date(2024, 3, 1), 'end_date': date(2024, 9, 30), 'status': 'active'},
        {'project_name': 'Mobile App v2.0', 'description': 'Develop next generation mobile application', 'start_date': date(2024, 4, 15), 'end_date': date(2024, 12, 31), 'status': 'active'},
        {'project_name': 'Security Audit', 'description': 'Comprehensive security assessment', 'start_date': date(2024, 2, 1), 'end_date': date(2024, 5, 31), 'status': 'active'},
        {'project_name': 'API Integration', 'description': 'Integrate third-party APIs', 'start_date': date(2024, 1, 10), 'end_date': date(2024, 6, 30), 'status': 'active'},
        {'project_name': 'Database Optimization', 'description': 'Optimize database performance', 'start_date': date(2024, 5, 1), 'end_date': date(2024, 8, 31), 'status': 'planning'}
    ]
    
    if db_manager.insert_data('projects', new_projects):
        print(f"✅ Successfully inserted {len(new_projects)} new projects")
    else:
        print("❌ Failed to insert projects")


def bulk_insert_sample_data(db_manager: DatabaseManager):
    """Insert large amounts of sample data for testing"""
    print("\n🚀 Performing bulk data insertion...")
    
    # Bulk insert users
    bulk_users = []
    for i in range(50):
        bulk_users.append({
            'username': f'user_{i+1:03d}',
            'email': f'user_{i+1:03d}@example.com'
        })
    
    inserted, skipped = db_manager.insert_data_ignore_duplicates('users', bulk_users)
    if inserted > 0:
        print(f"✅ Successfully inserted {inserted} bulk users")
    if skipped > 0:
        print(f"⏭️  Skipped {skipped} existing users (duplicates)")
    
    # Bulk insert products
    categories = ['Electronics', 'Home', 'Fitness', 'Outdoor', 'Books', 'Clothing']
    bulk_products = []
    for i in range(100):
        category = random.choice(categories)
        price = round(random.uniform(10.0, 1000.0), 2)
        bulk_products.append({
            'name': f'Product {i+1:03d}',
            'description': f'Sample product {i+1} in {category} category',
            'price': price,
            'category': category,
            'stock_quantity': random.randint(10, 200)
        })
    
    inserted, skipped = db_manager.insert_data_ignore_duplicates('products', bulk_products)
    if inserted > 0:
        print(f"✅ Successfully inserted {inserted} bulk products")
    if skipped > 0:
        print(f"⏭️  Skipped {skipped} existing products (duplicates)")


def insert_custom_data(db_manager: DatabaseManager):
    """Insert custom data based on user input"""
    print("\n🎯 Custom Data Insertion")
    print("=" * 40)
    
    # Custom user insertion
    print("\nAdd a custom user:")
    username = input("Username: ").strip()
    email = input("Email: ").strip()
    
    if username and email:
        custom_user = [{'username': username, 'email': email}]
        inserted, skipped = db_manager.insert_data_ignore_duplicates('users', custom_user)
        if inserted > 0:
            print("✅ Custom user inserted successfully")
        elif skipped > 0:
            print("⏭️  User already exists (duplicate)")
        else:
            print("❌ Failed to insert custom user")
    
    # Custom product insertion
    print("\nAdd a custom product:")
    name = input("Product name: ").strip()
    description = input("Description: ").strip()
    price_str = input("Price: ").strip()
    category = input("Category: ").strip()
    
    if name and price_str:
        try:
            price = float(price_str)
            custom_product = [{
                'name': name,
                'description': description,
                'price': price,
                'category': category,
                'stock_quantity': 10
            }]
            inserted, skipped = db_manager.insert_data_ignore_duplicates('products', custom_product)
            if inserted > 0:
                print("✅ Custom product inserted successfully")
            elif skipped > 0:
                print("⏭️  Product already exists (duplicate)")
            else:
                print("❌ Failed to insert custom product")
        except ValueError:
            print("❌ Invalid price format")


def show_insertion_menu():
    """Display the insertion menu"""
    print("\n📝 Data Insertion Menu")
    print("=" * 30)
    print("1. Insert new users")
    print("2. Insert new products")
    print("3. Insert new orders")
    print("4. Insert new order items")
    print("5. Insert new employees")
    print("6. Insert new departments")
    print("7. Insert new projects")
    print("8. Bulk insert sample data")
    print("9. Custom data insertion")
    print("10. Insert all sample data")
    print("0. Exit")
    print("=" * 30)


def main():
    """Main function to run the data insertion script"""
    print("🚀 PostgreSQL Data Insertion Tool")
    print("=" * 50)
    
    # Get password from config
    password = DB_CONFIG.get('password')
    if not password:
        print("❌ No password found in config.py")
        return
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    if not db_manager.connect():
        print("❌ Failed to connect to database")
        return
    
    try:
        while True:
            show_insertion_menu()
            choice = input("\nEnter your choice (0-10): ").strip()
            
            if choice == '0':
                print("👋 Goodbye!")
                break
            elif choice == '1':
                insert_users_data(db_manager)
            elif choice == '2':
                insert_products_data(db_manager)
            elif choice == '3':
                insert_orders_data(db_manager)
            elif choice == '4':
                insert_order_items_data(db_manager)
            elif choice == '5':
                insert_employees_data(db_manager)
            elif choice == '6':
                insert_departments_data(db_manager)
            elif choice == '7':
                insert_projects_data(db_manager)
            elif choice == '8':
                bulk_insert_sample_data(db_manager)
            elif choice == '9':
                insert_custom_data(db_manager)
            elif choice == '10':
                print("\n🔄 Inserting all sample data...")
                insert_users_data(db_manager)
                insert_products_data(db_manager)
                insert_orders_data(db_manager)
                insert_order_items_data(db_manager)
                insert_employees_data(db_manager)
                insert_departments_data(db_manager)
                insert_projects_data(db_manager)
                print("\n✅ All sample data inserted successfully!")
            else:
                print("❌ Invalid choice. Please try again.")
            
            input("\nPress Enter to continue...")
    
    finally:
        db_manager.disconnect()


if __name__ == "__main__":
    main() 