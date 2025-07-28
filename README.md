# OLTP-Databricks

[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

> **⚠️ Demo Code Warning**: This is a **demonstration project** for educational and testing purposes. It is **not intended for production use** or distribution as a Python package/wheel.

A comprehensive Python solution for connecting to and managing PostgreSQL databases hosted on Databricks. This project provides a robust database manager class with table creation, data insertion, querying, and advanced SQL operations.

**Purpose**: This repository serves as a learning resource and proof-of-concept for PostgreSQL integration with Databricks, demonstrating best practices for database management in Python.

## 🚀 Features

- **Secure Connection**: SSL-enabled PostgreSQL connection to Databricks
- **Table Management**: Create, drop, and manage database tables
- **Data Operations**: Insert, update, delete, and query data
- **Pandas Integration**: Query results returned as pandas DataFrames
- **Advanced Queries**: Support for complex SQL operations including JOINs, window functions, and aggregations
- **Error Handling**: Comprehensive error handling and logging
- **Connection Pooling**: Efficient connection management
- **Environment-based Configuration**: Secure credential management
- **Demo Structure**: Educational project structure with examples and documentation

## ⚡ Quick Start

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/OLTP-Databricks.git
   cd OLTP-Databricks
   ```

2. **Set up your environment:**
   ```bash
   # Activate virtual environment and install dependencies
   source scripts/activate_env.sh
   ```
   This will:
   - Create and activate a Python virtual environment
   - Install all required dependencies from `requirements.txt`
   - Set up your database configuration

3. **Configure your credentials:**
   ```bash
   cp scripts/export_env.sh scripts/export_my_env.sh
   # Edit scripts/export_my_env.sh and add your real credentials
   source scripts/activate_env.sh
   ```

4. **Test your environment:**
   ```bash
   python -m oltp_databricks.test_env_config
   ```

5. **Run an example:**
   ```bash
   python examples/database_manager.py
   ```

## 📜 Available Scripts

- `python -m oltp_databricks.test_env_config`  — Test environment configuration
- `python examples/test_connection.py`         — Test connection
- `python examples/connect_with_password.py`   — Test connection with password
- `python examples/database_manager.py`        — Run basic example (uses a unique table name)
- `python examples/example_queries.py`         — Run advanced queries
- `python examples/insert_data.py`             — Interactive data insertion
- `python examples/quick_insert.py`            — Quick data insertion

## 🗂️ Project Structure

```
OLTP-Databricks/
├── src/oltp_databricks/          # Main package
├── examples/                     # Example scripts
├── scripts/                      # Utility scripts
├── docs/                         # Documentation
├── tests/                        # Test suite
├── requirements.txt              # Dependencies
├── setup.py                      # Demo project setup (not for distribution)
├── .pre-commit-config.yaml       # Code quality hooks
├── LICENSE                       # MIT License
├── CONTRIBUTING.md               # Contributing guidelines
├── CHANGELOG.md                  # Version history
└── README.md                     # This file
```

## 🧪 Running All Examples

You can run all example scripts from the `examples/` directory. Each script demonstrates a different feature of the demo project. For example:

```bash
python examples/database_manager.py
python examples/example_queries.py
python examples/insert_data.py
python examples/quick_insert.py
```

> **Note:** The `database_manager.py` example now uses a unique table name for each run to avoid conflicts. Remember, this is demo code for learning purposes.

## 🧹 Cleaning Up Demo Tables

If you want to remove demo tables created by the examples, you can do so manually using a SQL client or by running a script that drops tables with a specific prefix (e.g., `sample_users_demo_*`).

## 🔄 Updating Your Environment Variables

If you need to update your credentials, simply edit `scripts/export_my_env.sh` and re-run:
```bash
source scripts/activate_env.sh
```

## 🛠️ Troubleshooting

### Common Issues

- **Table already exists:** The examples now use unique table names, but if you see this error, clean up old tables as described above.
- **Connection failed:** Double-check your credentials and network access. Use `python -m oltp_databricks.test_env_config` to verify your environment.
- **Environment variables not set:** Make sure you have sourced `scripts/activate_env.sh` and that `scripts/export_my_env.sh` contains your credentials.
- **Permission errors:** Ensure your database user has the necessary privileges.

### Debug Mode

Enable detailed logging by modifying the logging level in `src/oltp_databricks/database_manager.py`:
```python
logging.basicConfig(level=logging.DEBUG)
```

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## ❓ FAQ

**Q: Why do I see duplicate key errors or 'table already exists' errors when running examples?**
A: This means the example data or tables already exist in your database from a previous run. You can:
- Clean up demo tables manually using a SQL client (e.g., DROP TABLE ...)
- Use unique table names or data for each run (the basic example does this automatically)
- Reset your environment (see below)

**Q: How do I clean up demo tables?**
A: Connect to your database and run:
```sql
DROP TABLE IF EXISTS sample_users_demo CASCADE;
DROP TABLE IF EXISTS employees CASCADE;
DROP TABLE IF EXISTS departments CASCADE;
DROP TABLE IF EXISTS projects CASCADE;
-- Add more as needed
```
Or use a SQL client/GUI to drop tables.

**Q: How do I reset my environment?**
A:
```bash
# Remove and recreate your virtual environment
rm -rf pgconnect/
python -m venv pgconnect
source scripts/activate_env.sh
```

**Q: How do I contribute a new example?**
A:
- Add your script to the `examples/` directory.
- Use the import style: `from oltp_databricks.database_manager import DatabaseManager`
- Add a docstring at the top explaining what your script does.
- Test your script and submit a pull request!

**Q: Can I use this in production?**
A: **No, this is demo code only.** This project is designed for educational purposes and learning PostgreSQL integration with Databricks. For production use, you should implement proper error handling, security measures, and follow your organization's coding standards.

**Q: Can I package this as a Python wheel?**
A: **No, this is not intended for distribution.** While the project has a `setup.py` and `pyproject.toml`, these are for demonstration purposes only. This is educational code meant to be cloned and run locally for learning.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 

## 📚 Documentation

- **[README.md](README.md)** - This file, with setup and usage instructions
- **[docs/ENVIRONMENT_SETUP.md](docs/ENVIRONMENT_SETUP.md)** - Detailed environment configuration guide
- **[docs/API.md](docs/API.md)** - Complete API documentation for DatabaseManager
- **[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)** - Troubleshooting guide for common issues
- **[CONTRIBUTING.md](CONTRIBUTING.md)** - Guidelines for contributors
- **[CHANGELOG.md](CHANGELOG.md)** - Project changelog and version history 