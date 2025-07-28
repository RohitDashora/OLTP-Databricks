# OLTP-Databricks API Documentation

## DatabaseManager Class

The main class for managing PostgreSQL database operations in Databricks.

### Initialization

```python
from oltp_databricks.database_manager import DatabaseManager

# Initialize with password
db_manager = DatabaseManager(password="your-password")

# Initialize with full config
db_manager = DatabaseManager(
    host="your-host",
    user="your-user", 
    password="your-password",
    database="your-database",
    port=5432,
    sslmode="require"
)
```

### Connection Methods

#### `connect() -> bool`
Establishes a connection to the PostgreSQL database.

**Returns:** `True` if connection successful, `False` otherwise.

**Example:**
```python
if db_manager.connect():
    print("✅ Connected to database")
else:
    print("❌ Connection failed")
```

#### `disconnect()`
Closes the database connection.

**Example:**
```python
db_manager.disconnect()
```

#### `is_connected() -> bool`
Checks if the database connection is active.

**Returns:** `True` if connected, `False` otherwise.

### Table Management

#### `create_table(table_name: str, columns: dict) -> bool`
Creates a new table in the database.

**Parameters:**
- `table_name`: Name of the table to create
- `columns`: Dictionary defining column names and their SQL types

**Example:**
```python
columns = {
    'id': 'SERIAL PRIMARY KEY',
    'name': 'VARCHAR(100) NOT NULL',
    'email': 'VARCHAR(255) UNIQUE',
    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
}
db_manager.create_table('users', columns)
```

#### `drop_table(table_name: str) -> bool`
Drops a table from the database.

**Parameters:**
- `table_name`: Name of the table to drop

**Example:**
```python
db_manager.drop_table('users')
```

#### `table_exists(table_name: str) -> bool`
Checks if a table exists in the database.

**Parameters:**
- `table_name`: Name of the table to check

**Returns:** `True` if table exists, `False` otherwise.

### Data Operations

#### `insert_data(table_name: str, data: list) -> bool`
Inserts data into a table.

**Parameters:**
- `table_name`: Name of the target table
- `data`: List of dictionaries containing the data to insert

**Returns:** `True` if insertion successful, `False` otherwise.

**Example:**
```python
users_data = [
    {'username': 'john_doe', 'email': 'john@example.com'},
    {'username': 'jane_smith', 'email': 'jane@example.com'}
]
db_manager.insert_data('users', users_data)
```

#### `insert_data_ignore_duplicates(table_name: str, data: list) -> tuple[int, int]`
Inserts data while ignoring duplicates.

**Parameters:**
- `table_name`: Name of the target table
- `data`: List of dictionaries containing the data to insert

**Returns:** Tuple of (inserted_count, skipped_count)

**Example:**
```python
inserted, skipped = db_manager.insert_data_ignore_duplicates('users', users_data)
print(f"Inserted: {inserted}, Skipped: {skipped}")
```

#### `execute_query(query: str, params: tuple = None) -> list`
Executes a SQL query and returns results.

**Parameters:**
- `query`: SQL query string
- `params`: Optional tuple of parameters for parameterized queries

**Returns:** List of tuples containing query results

**Example:**
```python
# Simple query
results = db_manager.execute_query("SELECT * FROM users")

# Parameterized query
results = db_manager.execute_query(
    "SELECT * FROM users WHERE email = %s", 
    ('john@example.com',)
)
```

#### `execute_query_pandas(query: str, params: tuple = None) -> pd.DataFrame`
Executes a SQL query and returns results as a pandas DataFrame.

**Parameters:**
- `query`: SQL query string
- `params`: Optional tuple of parameters for parameterized queries

**Returns:** Pandas DataFrame containing query results

**Example:**
```python
import pandas as pd

df = db_manager.execute_query_pandas("SELECT * FROM users")
print(df.head())
```

#### `update_data(table_name: str, data: dict, condition: str, params: tuple = None) -> bool`
Updates data in a table.

**Parameters:**
- `table_name`: Name of the target table
- `data`: Dictionary of column names and new values
- `condition`: WHERE clause condition
- `params`: Optional tuple of parameters for the condition

**Returns:** `True` if update successful, `False` otherwise.

**Example:**
```python
update_data = {'email': 'newemail@example.com'}
db_manager.update_data('users', update_data, 'username = %s', ('john_doe',))
```

#### `delete_data(table_name: str, condition: str, params: tuple = None) -> bool`
Deletes data from a table.

**Parameters:**
- `table_name`: Name of the target table
- `condition`: WHERE clause condition
- `params`: Optional tuple of parameters for the condition

**Returns:** `True` if deletion successful, `False` otherwise.

**Example:**
```python
db_manager.delete_data('users', 'email = %s', ('john@example.com',))
```

### Advanced Operations

#### `execute_transaction(queries: list) -> bool`
Executes multiple queries in a transaction.

**Parameters:**
- `queries`: List of SQL query strings

**Returns:** `True` if all queries successful, `False` otherwise.

**Example:**
```python
queries = [
    "INSERT INTO users (username, email) VALUES ('user1', 'user1@example.com')",
    "INSERT INTO users (username, email) VALUES ('user2', 'user2@example.com')"
]
db_manager.execute_transaction(queries)
```

#### `get_table_info(table_name: str) -> dict`
Gets information about a table's structure.

**Parameters:**
- `table_name`: Name of the table

**Returns:** Dictionary containing table information

**Example:**
```python
info = db_manager.get_table_info('users')
print(info)
```

### Error Handling

The DatabaseManager includes comprehensive error handling:

- Connection errors are logged and handled gracefully
- SQL errors are captured and logged
- Transaction rollback on errors
- Detailed error messages for debugging

### Best Practices

1. **Always check connection status:**
   ```python
   if not db_manager.is_connected():
       db_manager.connect()
   ```

2. **Use parameterized queries to prevent SQL injection:**
   ```python
   # Good
   db_manager.execute_query("SELECT * FROM users WHERE email = %s", (email,))
   
   # Bad
   db_manager.execute_query(f"SELECT * FROM users WHERE email = '{email}'")
   ```

3. **Handle transactions properly:**
   ```python
   try:
       db_manager.execute_transaction(queries)
   except Exception as e:
       print(f"Transaction failed: {e}")
   ```

4. **Close connections when done:**
   ```python
   try:
       # Your database operations
       pass
   finally:
       db_manager.disconnect()
   ```

### Configuration

The DatabaseManager uses the following configuration from `config.py`:

- `DATABRICKS_DB_HOST`: Database host
- `DATABRICKS_DB_USER`: Database username
- `DATABRICKS_DB_NAME`: Database name
- `DATABRICKS_DB_PORT`: Database port (default: 5432)
- `DATABRICKS_DB_SSLMODE`: SSL mode (default: "require")
- `DATABRICKS_DB_PASSWORD`: Database password

See `ENVIRONMENT_SETUP.md` for configuration details. 