# Troubleshooting Guide

This guide helps you resolve common issues when using OLTP-Databricks.

## Connection Issues

### ❌ "Connection failed" or "Unable to connect to database"

**Possible Causes:**
1. Incorrect database credentials
2. Network connectivity issues
3. Firewall blocking connection
4. SSL configuration problems

**Solutions:**
1. **Verify your credentials:**
   ```bash
   python -m oltp_databricks.test_env_config
   ```

2. **Check your environment variables:**
   ```bash
   echo $DATABRICKS_DB_HOST
   echo $DATABRICKS_DB_USER
   echo $DATABRICKS_DB_NAME
   ```

3. **Test network connectivity:**
   ```bash
   # Test if you can reach the host
   ping your-instance.database.cloud.databricks.com
   
   # Test port connectivity
   telnet your-instance.database.cloud.databricks.com 5432
   ```

4. **Verify SSL settings:**
   - Ensure `DATABRICKS_DB_SSLMODE=require`
   - Check if your Databricks instance requires specific SSL certificates

### ❌ "SSL connection required"

**Solution:**
Make sure your `export_my_env.sh` includes:
```bash
export DATABRICKS_DB_SSLMODE="require"
```

### ❌ "Authentication failed"

**Possible Causes:**
1. Incorrect username/password
2. Expired JWT token
3. User doesn't have database access

**Solutions:**
1. **Regenerate your JWT token** in Databricks:
   - Go to User Settings → Access Tokens
   - Create a new token
   - Update your `export_my_env.sh`

2. **Verify your username format:**
   - Should be your email address: `user@databricks.com`
   - Not your display name

3. **Check database permissions:**
   - Ensure your user has access to the specified database
   - Contact your Databricks admin if needed

## Environment Issues

### ❌ "ModuleNotFoundError: No module named 'oltp_databricks'"

**Solution:**
1. **Activate your environment:**
   ```bash
   source scripts/activate_env.sh
   ```

2. **Install the package in development mode:**
   ```bash
   pip install -e .
   ```

3. **Verify installation:**
   ```bash
   python -c "import oltp_databricks; print('Package installed successfully')"
   ```

### ❌ "Environment variables not found"

**Solution:**
1. **Create your environment file:**
   ```bash
   cp scripts/export_env.sh scripts/export_my_env.sh
   # Edit with your credentials
   ```

2. **Activate environment:**
   ```bash
   source scripts/activate_env.sh
   ```

3. **Verify variables are set:**
   ```bash
   python -m oltp_databricks.test_env_config
   ```

## Data Operation Issues

### ❌ "Duplicate key value violates unique constraint"

**Cause:** Trying to insert data that violates unique constraints (e.g., duplicate emails, primary keys).

**Solutions:**
1. **Use `insert_data_ignore_duplicates`:**
   ```python
   inserted, skipped = db_manager.insert_data_ignore_duplicates('users', users_data)
   ```

2. **Clean up existing data:**
   ```sql
   DROP TABLE IF EXISTS users CASCADE;
   -- Recreate table and insert fresh data
   ```

3. **Use unique data for each run:**
   ```python
   # Add timestamp or random suffix to make data unique
   username = f"user_{int(time.time())}"
   ```

### ❌ "Table already exists"

**Cause:** Trying to create a table that already exists.

**Solutions:**
1. **Check if table exists before creating:**
   ```python
   if not db_manager.table_exists('users'):
       db_manager.create_table('users', columns)
   ```

2. **Drop existing table:**
   ```python
   db_manager.drop_table('users')
   db_manager.create_table('users', columns)
   ```

3. **Use unique table names:**
   ```python
   table_name = f"users_{int(time.time())}"
   ```

### ❌ "Column does not exist"

**Cause:** Trying to insert data with columns that don't exist in the table.

**Solutions:**
1. **Check table structure:**
   ```python
   info = db_manager.get_table_info('users')
   print(info)
   ```

2. **Verify column names match exactly:**
   - Check for typos
   - Ensure case sensitivity matches
   - Verify column names in your data match the table schema

### ❌ "Data type mismatch"

**Cause:** Inserting data with wrong data types.

**Solutions:**
1. **Check data types:**
   ```python
   # Ensure numeric fields are numbers, not strings
   data = {'price': 99.99, 'quantity': 5}  # Not '99.99', '5'
   ```

2. **Convert data types:**
   ```python
   import datetime
   data = {
       'created_at': datetime.datetime.now(),
       'price': float(price_string),
       'quantity': int(quantity_string)
   }
   ```

## Performance Issues

### ❌ "Slow query performance"

**Solutions:**
1. **Use indexes on frequently queried columns:**
   ```sql
   CREATE INDEX idx_users_email ON users(email);
   ```

2. **Limit result sets:**
   ```python
   results = db_manager.execute_query("SELECT * FROM users LIMIT 1000")
   ```

3. **Use parameterized queries:**
   ```python
   # More efficient than string formatting
   results = db_manager.execute_query("SELECT * FROM users WHERE email = %s", (email,))
   ```

### ❌ "Connection timeout"

**Solutions:**
1. **Check connection pooling settings**
2. **Close connections properly:**
   ```python
   try:
       # Your operations
       pass
   finally:
       db_manager.disconnect()
   ```

3. **Implement retry logic:**
   ```python
   import time
   
   def execute_with_retry(db_manager, query, max_retries=3):
       for attempt in range(max_retries):
           try:
               return db_manager.execute_query(query)
           except Exception as e:
               if attempt == max_retries - 1:
                   raise e
               time.sleep(2 ** attempt)  # Exponential backoff
   ```

## Development Issues

### ❌ "Pre-commit hooks failing"

**Solutions:**
1. **Install pre-commit:**
   ```bash
   pip install pre-commit
   pre-commit install
   ```

2. **Run formatting manually:**
   ```bash
   black src/ tests/
   isort src/ tests/
   flake8 src/ tests/
   ```

3. **Skip pre-commit (not recommended):**
   ```bash
   git commit --no-verify
   ```

### ❌ "Tests failing"

**Solutions:**
1. **Run tests with verbose output:**
   ```bash
   pytest -v
   ```

2. **Check test database configuration:**
   - Ensure test database exists
   - Verify test credentials are correct

3. **Run specific test:**
   ```bash
   pytest tests/test_database_manager.py::test_specific_function -v
   ```

## Getting Help

If you're still experiencing issues:

1. **Check the logs:** Look for detailed error messages in the console output
2. **Search existing issues:** Check the GitHub issues page for similar problems
3. **Create a new issue:** Include:
   - Error message and stack trace
   - Your environment details (OS, Python version)
   - Steps to reproduce the issue
   - Your configuration (without sensitive data)

## Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `psycopg2.OperationalError: connection to server failed` | Network/connection issue | Check credentials and network connectivity |
| `psycopg2.IntegrityError: duplicate key value` | Duplicate data | Use `insert_data_ignore_duplicates` or clean existing data |
| `psycopg2.ProgrammingError: relation "table" does not exist` | Table doesn't exist | Create table first or check table name |
| `psycopg2.DataError: invalid input syntax` | Wrong data type | Convert data to correct type |
| `ModuleNotFoundError` | Package not installed | Run `pip install -e .` in activated environment | 