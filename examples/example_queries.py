"""
example_queries.py

Demonstrates advanced SQL queries and data manipulation using OLTP-Databricks DatabaseManager.

- Creates and populates demo tables (employees, departments, projects)
- Runs advanced analytics queries
- Shows data manipulation (update, delete, insert)

Note:
- If you run this script multiple times, you may see duplicate key errors or 'table already exists' errors.
- To avoid this, clean up demo tables between runs (see README FAQ), or use unique data.
"""

from oltp_databricks.database_manager import DatabaseManager
import pandas as pd


def run_advanced_queries(db_manager: DatabaseManager):
    """Run advanced queries to demonstrate complex operations"""
    
    print("\n=== Advanced Queries ===")
    
    # 1. Complex JOIN query
    print("\n1. Users with their order counts:")
    user_orders_query = """
    SELECT u.username, u.email, COUNT(o.order_id) as order_count
    FROM users u
    LEFT JOIN orders o ON u.user_id = o.user_id
    GROUP BY u.user_id, u.username, u.email
    ORDER BY order_count DESC;
    """
    user_orders_df = db_manager.query_to_dataframe(user_orders_query)
    print(user_orders_df)
    
    # 2. Aggregation with HAVING
    print("\n2. Products with stock quantity > 50:")
    stock_query = """
    SELECT category, COUNT(*) as product_count, AVG(price) as avg_price
    FROM products
    GROUP BY category
    HAVING COUNT(*) > 0
    ORDER BY avg_price DESC;
    """
    stock_df = db_manager.query_to_dataframe(stock_query)
    print(stock_df)
    
    # 3. Window functions
    print("\n3. Products ranked by price within category:")
    window_query = """
    SELECT name, category, price,
           RANK() OVER (PARTITION BY category ORDER BY price DESC) as price_rank,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as row_num
    FROM products
    ORDER BY category, price DESC;
    """
    window_df = db_manager.query_to_dataframe(window_query)
    print(window_df)
    
    # 4. Subquery example
    print("\n4. Products priced above average:")
    subquery = """
    SELECT name, price, category
    FROM products
    WHERE price > (SELECT AVG(price) FROM products)
    ORDER BY price DESC;
    """
    subquery_df = db_manager.query_to_dataframe(subquery)
    print(subquery_df)
    
    # 5. Date/time operations
    print("\n5. Users created in the last 30 days:")
    date_query = """
    SELECT username, email, created_at
    FROM users
    WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
    ORDER BY created_at DESC;
    """
    date_df = db_manager.query_to_dataframe(date_query)
    print(date_df)


def create_custom_tables(db_manager: DatabaseManager):
    """Create additional custom tables"""
    
    # Create employees table
    employees_columns = [
        {'name': 'employee_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'first_name', 'type': 'VARCHAR(50) NOT NULL'},
        {'name': 'last_name', 'type': 'VARCHAR(50) NOT NULL'},
        {'name': 'email', 'type': 'VARCHAR(100) UNIQUE NOT NULL'},
        {'name': 'department', 'type': 'VARCHAR(50)'},
        {'name': 'salary', 'type': 'DECIMAL(10,2)'},
        {'name': 'hire_date', 'type': 'DATE DEFAULT CURRENT_DATE'}
    ]
    
    # Create departments table
    departments_columns = [
        {'name': 'department_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'department_name', 'type': 'VARCHAR(50) UNIQUE NOT NULL'},
        {'name': 'location', 'type': 'VARCHAR(100)'},
        {'name': 'budget', 'type': 'DECIMAL(12,2)'}
    ]
    
    # Create projects table
    projects_columns = [
        {'name': 'project_id', 'type': 'SERIAL PRIMARY KEY'},
        {'name': 'project_name', 'type': 'VARCHAR(100) NOT NULL'},
        {'name': 'description', 'type': 'TEXT'},
        {'name': 'start_date', 'type': 'DATE'},
        {'name': 'end_date', 'type': 'DATE'},
        {'name': 'status', 'type': 'VARCHAR(20) DEFAULT \'active\''}
    ]
    
    tables = [
        ('employees', employees_columns),
        ('departments', departments_columns),
        ('projects', projects_columns)
    ]
    
    for table_name, columns in tables:
        if db_manager.create_table(table_name, columns):
            print(f"Table '{table_name}' created successfully")
        else:
            print(f"Failed to create table '{table_name}'")


def insert_custom_data(db_manager: DatabaseManager):
    """Insert custom data into the new tables"""
    
    # Insert departments
    departments_data = [
        {'department_name': 'Engineering', 'location': 'San Francisco', 'budget': 1000000.00},
        {'department_name': 'Marketing', 'location': 'New York', 'budget': 500000.00},
        {'department_name': 'Sales', 'location': 'Chicago', 'budget': 750000.00},
        {'department_name': 'HR', 'location': 'Austin', 'budget': 300000.00}
    ]
    
    # Insert employees
    employees_data = [
        {'first_name': 'Alice', 'last_name': 'Johnson', 'email': 'alice.j@company.com', 'department': 'Engineering', 'salary': 85000.00},
        {'first_name': 'Bob', 'last_name': 'Smith', 'email': 'bob.s@company.com', 'department': 'Marketing', 'salary': 65000.00},
        {'first_name': 'Carol', 'last_name': 'Davis', 'email': 'carol.d@company.com', 'department': 'Sales', 'salary': 70000.00},
        {'first_name': 'David', 'last_name': 'Wilson', 'email': 'david.w@company.com', 'department': 'Engineering', 'salary': 90000.00},
        {'first_name': 'Eva', 'last_name': 'Brown', 'email': 'eva.b@company.com', 'department': 'HR', 'salary': 55000.00}
    ]
    
    # Insert projects
    projects_data = [
        {'project_name': 'Website Redesign', 'description': 'Redesign company website', 'start_date': '2024-01-15', 'end_date': '2024-06-30', 'status': 'active'},
        {'project_name': 'Mobile App', 'description': 'Develop mobile application', 'start_date': '2024-02-01', 'end_date': '2024-12-31', 'status': 'active'},
        {'project_name': 'Data Migration', 'description': 'Migrate legacy data', 'start_date': '2023-11-01', 'end_date': '2024-03-31', 'status': 'completed'}
    ]
    
    # Insert data
    if db_manager.insert_data('departments', departments_data):
        print("Departments data inserted")
    
    if db_manager.insert_data('employees', employees_data):
        print("Employees data inserted")
    
    if db_manager.insert_data('projects', projects_data):
        print("Projects data inserted")


def run_custom_queries(db_manager: DatabaseManager):
    """Run queries on the custom tables"""
    
    print("\n=== Custom Table Queries ===")
    
    # 1. Department budget analysis
    print("\n1. Department budget analysis:")
    budget_query = """
    SELECT department_name, budget,
           ROUND(budget / SUM(budget) OVER() * 100, 2) as budget_percentage
    FROM departments
    ORDER BY budget DESC;
    """
    budget_df = db_manager.query_to_dataframe(budget_query)
    print(budget_df)
    
    # 2. Employee salary statistics by department
    print("\n2. Employee salary statistics by department:")
    salary_query = """
    SELECT department,
           COUNT(*) as employee_count,
           ROUND(AVG(salary), 2) as avg_salary,
           MIN(salary) as min_salary,
           MAX(salary) as max_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC;
    """
    salary_df = db_manager.query_to_dataframe(salary_query)
    print(salary_df)
    
    # 3. Project timeline analysis
    print("\n3. Project timeline analysis:")
    timeline_query = """
    SELECT project_name,
           start_date,
           end_date,
           (end_date - start_date) as duration_days,
           CASE 
               WHEN end_date < CURRENT_DATE THEN 'Completed'
               WHEN start_date > CURRENT_DATE THEN 'Not Started'
               ELSE 'In Progress'
           END as project_status
    FROM projects
    ORDER BY start_date;
    """
    timeline_df = db_manager.query_to_dataframe(timeline_query)
    print(timeline_df)
    
    # 4. Complex analysis query
    print("\n4. Department efficiency analysis:")
    efficiency_query = """
    WITH dept_stats AS (
        SELECT 
            department,
            COUNT(*) as emp_count,
            AVG(salary) as avg_salary,
            SUM(salary) as total_salary
        FROM employees
        GROUP BY department
    )
    SELECT 
        d.department_name,
        COALESCE(ds.emp_count, 0) as employee_count,
        d.budget,
        COALESCE(ds.total_salary, 0) as salary_expense,
        ROUND(COALESCE(ds.total_salary, 0) / d.budget * 100, 2) as salary_budget_ratio
    FROM departments d
    LEFT JOIN dept_stats ds ON d.department_name = ds.department
    ORDER BY salary_budget_ratio DESC;
    """
    efficiency_df = db_manager.query_to_dataframe(efficiency_query)
    print(efficiency_df)


def demonstrate_data_manipulation(db_manager: DatabaseManager):
    """Demonstrate data manipulation operations"""
    
    print("\n=== Data Manipulation Examples ===")
    
    # 1. Update operation
    print("\n1. Updating employee salary:")
    update_query = """
    UPDATE employees 
    SET salary = salary * 1.05 
    WHERE department = 'Engineering';
    """
    db_manager.execute_query(update_query)
    
    # Show updated data
    updated_salaries = db_manager.query_to_dataframe(
        "SELECT first_name, last_name, department, salary FROM employees WHERE department = 'Engineering'"
    )
    print("Updated Engineering salaries:")
    print(updated_salaries)
    
    # 2. Delete operation (safe delete)
    print("\n2. Deleting completed projects:")
    delete_query = """
    DELETE FROM projects WHERE status = 'completed';
    """
    db_manager.execute_query(delete_query)
    
    # Show remaining projects
    remaining_projects = db_manager.query_to_dataframe("SELECT * FROM projects")
    print("Remaining projects:")
    print(remaining_projects)
    
    # 3. Insert with RETURNING
    print("\n3. Inserting new employee with RETURNING:")
    insert_returning_query = """
    INSERT INTO employees (first_name, last_name, email, department, salary)
    VALUES ('Frank', 'Miller', 'frank.m@company.com', 'Sales', 72000.00)
    RETURNING employee_id, first_name, last_name, email;
    """
    result = db_manager.execute_query(insert_returning_query)
    if result:
        print("New employee inserted:")
        for row in result:
            print(f"ID: {row[0]}, Name: {row[1]} {row[2]}, Email: {row[3]}")


if __name__ == "__main__":
    # Example usage with custom tables
    db_manager = DatabaseManager()
    
    if db_manager.connect():
        try:
            # Create custom tables
            create_custom_tables(db_manager)
            
            # Insert custom data
            insert_custom_data(db_manager)
            
            # Run custom queries
            run_custom_queries(db_manager)
            
            # Demonstrate data manipulation
            demonstrate_data_manipulation(db_manager)
            
        finally:
            db_manager.disconnect()
    else:
        print("Failed to connect to database") 