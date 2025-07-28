"""
Setup script for OLTP-Databricks package
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
    name="oltp-databricks",
    version="1.0.0",
    author="Rohit Dashora",
    author_email="rohit.dashora@databricks.com",
    description="PostgreSQL Database Manager for Databricks",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/OLTP-Databricks",
    project_urls={
        "Bug Reports": "https://github.com/yourusername/OLTP-Databricks/issues",
        "Source": "https://github.com/yourusername/OLTP-Databricks",
        "Documentation": "https://github.com/yourusername/OLTP-Databricks/docs",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
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
        "Topic :: Software Development :: Libraries :: Python Modules",
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
    entry_points={
        "console_scripts": [
            "oltp-databricks=oltp_databricks.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
    keywords="databricks, postgresql, database, oltp, sql",
) 