"""
Setup script for OLTP-Databricks demo project

⚠️  DEMO CODE WARNING: This is a demonstration project for educational purposes only.
It is NOT intended for production use or distribution as a Python package/wheel.

This setup.py exists solely for educational purposes to demonstrate proper Python
project structure and is not meant to be installed via pip or distributed.

Purpose: Learning resource for PostgreSQL integration with Databricks
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# Read requirements
def read_requirements():
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="oltp-databricks-demo",  # Changed to indicate this is demo code
    version="0.1.0-demo",  # Changed to indicate demo version
    author="Rohit Dashora",
    author_email="rohit.dashora@databricks.com",
    description="Demo: PostgreSQL Database Manager for Databricks (Educational Project)",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/RohitDashora/OLTP-Databricks",
    project_urls={
        "Bug Reports": "https://github.com/RohitDashora/OLTP-Databricks/issues",
        "Source": "https://github.com/RohitDashora/OLTP-Databricks",
        "Documentation": "https://github.com/RohitDashora/OLTP-Databricks/docs",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 1 - Planning",  # Changed to indicate demo status
        "Intended Audience :: Education",  # Changed to indicate educational purpose
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Education",  # Added education topic
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Documentation",  # Added documentation topic
    ],
    python_requires=">=3.7",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.800",
        ],
    },
    # Removed entry_points since this is demo code
    include_package_data=True,
    zip_safe=False,
    keywords="databricks, postgresql, database, oltp, sql, demo, educational, learning",
) 