"""Setup script for the Alteryx to PySpark Converter."""

from setuptools import setup, find_packages

setup(
    name="alteryx-pyspark-converter",
    version="1.0.0",
    description="Generic Alteryx to PySpark/Databricks Converter Framework",
    long_description=open("../README.md").read() if __import__("os").path.exists("../README.md") else "",
    long_description_content_type="text/markdown",
    author="Alteryx Converter Team",
    python_requires=">=3.10",
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "pyyaml>=6.0",
        "jinja2>=3.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "alteryx-parse=alteryx_pyspark_converter.phase1_parser.cli:main",
            "alteryx-generate=alteryx_pyspark_converter.phase2_generator.cli:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
