# Data Engineering Assessment - Analysis & Implementation

## Assignment Overview

This assessment evaluates core **data engineering** skills focusing on:
- **SQL**: Relational modeling, normalization, DDL/DML scripting
- **Python ETL**: Data ingestion, cleaning, transformation, & loading (ELT/ETL)

## Problem Statement

The assignment involves normalizing a raw JSON file containing property records where:
- Each row relates to a property
- Each row mixes many unrelated attributes (property details, HOA data, rehab estimates, valuations, etc.)
- The database is not normalized and lacks relational structure
- Use the supplied Field Config.xlsx (in data/) to understand business semantics

## Project File Structure & Purpose

### **Core ETL Files**

#### `scripts/advanced_etl_pipeline.py` (Main ETL Script)
**Purpose**: Primary ETL pipeline that orchestrates the entire data normalization process
**What it does**:
- Extracts 10,000 records from `fake_property_data.json`
- Cleans and validates the data (handles missing values, converts data types)
- Transforms nested JSON structures into normalized relational format
- Loads data into 7 normalized MySQL tables in proper dependency order
- Handles complex one-to-many relationships (multiple valuations, HOA records, rehab estimates per property)
- Provides comprehensive logging and error handling

#### `scripts/database.py` (Database Connection Manager)
**Purpose**: Manages MySQL database connections and SQL script execution
**What it does**:
- Establishes connection to MySQL database using Docker credentials
- Provides methods to execute SQL scripts from files
- Handles connection errors and fallback authentication
- Manages transactions (commit/rollback)
- **Note**: This is a utility module - you don't run it directly, it's imported by other scripts

#### `sql/create_final_schema.sql` (Database Schema)
**Purpose**: Defines the normalized database structure
**What it does**:
- Creates 7 normalized tables with proper relationships
- Defines primary keys, foreign keys, and constraints
- Sets up indexes for performance optimization
- Handles one-to-many relationships with sequence numbers
- **Note**: This is executed automatically by the ETL pipeline

### **Data Files**

#### `data/fake_property_data.json` (Source Data)
**Purpose**: Raw denormalized property data (10,000 records)
**What it contains**:
- Property details (address, characteristics, location)
- Lead management information
- Tax data
- Multiple HOA records per property (nested arrays)
- Multiple valuation records per property (nested arrays)
- Multiple rehab estimates per property (nested arrays)

#### `data/Field Config.xlsx` (Business Logic Reference)
**Purpose**: Defines business semantics and field-to-table mapping
**What it contains**:
- Field names and their target tables
- Business logic for data relationships
- Field categorization (property, leads, taxes, HOA, valuation, rehab)

### **Validation & Analysis Files**

#### `scripts/advanced_validation.py` (Data Quality Validation)
**Purpose**: Validates the loaded data and checks data quality
**What it does**:
- Verifies record counts across all tables
- Checks for properties with multiple related records
- Validates data quality (missing critical data, value ranges)
- Confirms referential integrity
- Provides summary statistics

#### `scripts/analyze_complex_data.py` (Data Analysis)
**Purpose**: Analyzes the complex nested data structures
**What it does**:
- Examines the structure of nested JSON arrays
- Extracts unique keys from complex columns
- Maps fields according to Field Config.xlsx
- Provides insights into data complexity
- Helps understand the normalization requirements

#### `scripts/data_exploration.py` (Data Exploration)
**Purpose**: Initial data exploration and understanding
**What it does**:
- Analyzes the raw JSON data structure
- Shows data types and missing values
- Provides sample records for understanding
- Maps fields to target tables based on Field Config

### **Configuration Files**

#### `requirements.txt` (Python Dependencies)
**Purpose**: Lists all required Python packages
**What it contains**:
- pandas>=1.5.0 (data manipulation)
- mysql-connector-python>=8.0.0 (MySQL connectivity)
- openpyxl>=3.0.0 (Excel file reading)
- python-dotenv>=0.19.0 (environment management)

#### `docker-compose.initial.yml` (Database Setup)
**Purpose**: Defines MySQL database container configuration
**What it does**:
- Sets up MySQL 8 container
- Configures database credentials
- Maps port 3306 for database access
- Creates persistent volume for data storage

#### `Dockerfile.initial_db` (Database Image)
**Purpose**: Custom MySQL Docker image configuration
**What it does**:
- Extends official MySQL 8 image
- Sets up initial database configuration
- Configures character sets and collations

## Requirements Analysis

###  **Technology Stack Compliance**
- **Python ≥ 3.8**:  Used Python 3.11
- **MySQL 8**:  Using MySQL 8 in Docker container
- **Lightweight libraries**:  Only using pandas, mysql-connector-python, openpyxl, python-dotenv
- **No ORMs**:  All SQL written by hand
- **requirements.txt**:  Properly documented dependencies

###  **Database Design & Normalization**

The implementation successfully normalizes the denormalized JSON data into **7 properly related tables**:

1. **properties** (Main entity table)
   - Primary key: `property_id` (AUTO_INCREMENT)
   - Contains all property-specific fields (address, characteristics, location, etc.)
   - 10,000 records loaded

2. **leads** (Lead management and investment metrics)
   - Foreign key: `property_id` → properties(property_id)
   - Contains lead status, source, occupancy, financial metrics
   - 10,000 records loaded

3. **taxes** (Tax information)
   - Foreign key: `property_id` → properties(property_id)
   - Contains tax amounts and years
   - 10,000 records loaded

4. **hoa_details** (Multiple HOA records per property)
   - Foreign key: `property_id` → properties(property_id)
   - Sequence number for multiple HOA records
   - 9,993 records loaded (some properties have no HOA)

5. **valuation_details** (Multiple valuations per property)
   - Foreign key: `property_id` → properties(property_id)
   - Sequence number for multiple valuations
   - 24,705 records loaded (average ~2.5 valuations per property)

6. **rehab_estimates** (Multiple rehab estimates per property)
   - Foreign key: `property_id` → properties(property_id)
   - Sequence number for multiple estimates
   - 20,024 records loaded (average ~2 estimates per property)

7. **rehab_details** (Detailed rehab breakdown for each estimate)
   - Foreign key: `rehab_estimate_id` → rehab_estimates(rehab_estimate_id)
   - Contains detailed flags for various rehab components
   - 20,024 records loaded

###  **ETL Implementation**

The ETL pipeline (`advanced_etl_pipeline.py`) successfully:

1. **Extract**: Reads 10,000 records from `fake_property_data.json`
2. **Transform**: 
   - Cleans and validates data
   - Handles missing values appropriately
   - Parses nested JSON structures (HOA, Valuation, Rehab arrays)
   - Converts data types (numeric, date, etc.)
3. **Load**: 
   - Creates normalized schema
   - Loads data in proper dependency order
   - Maintains referential integrity
   - Uses bulk inserts for performance

###  **Data Quality & Validation**

The validation script (`advanced_validation.py`) confirms:
- **Record counts**: All expected records loaded
- **Multiple records**: Successfully handles properties with multiple valuations, HOA records, and rehab estimates
- **Data quality**: No missing critical data (city/state), reasonable value ranges
- **Referential integrity**: All foreign key relationships maintained

## Implementation Strengths

### 1. **Robust Database Schema**
- Proper normalization following 3NF principles
- Appropriate use of primary and foreign keys
- Indexes on frequently queried columns
- Handles one-to-many relationships correctly

### 2. **Advanced ETL Features**
- Handles complex nested JSON structures
- Supports multiple records per property (valuations, HOA, rehab)
- Comprehensive data cleaning and validation
- Error handling and logging
- Transaction management

### 3. **Data Analysis Capabilities**
- Validation scripts for data quality checks
- Analysis scripts for business insights
- Support for complex queries across normalized tables

### 4. **Production-Ready Code**
- Proper error handling and logging
- Database connection management
- Modular design with separate concerns
- Comprehensive documentation

## Data Loading Summary

| Table | Records | Description |
|-------|---------|-------------|
| properties | 10,000 | Main property records |
| leads | 10,000 | Lead management data |
| taxes | 10,000 | Tax information |
| hoa_details | 9,993 | HOA records (some properties have no HOA) |
| valuation_details | 24,705 | Multiple valuations per property |
| rehab_estimates | 20,024 | Multiple rehab estimates per property |
| rehab_details | 20,024 | Detailed rehab breakdowns |

## How to Run the Solution

### Prerequisites
1. Docker installed and running
2. Python 3.8+ installed
3. Git (to clone the repository)

### Setup Instructions

1. **Start the MySQL Database**:
   ```bash
   docker-compose -f docker-compose.initial.yml up --build -d
   ```

2. **Install Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Complete ETL Pipeline** (This is the main script):
   ```bash
   python scripts/advanced_etl_pipeline.py
   ```
   **What this does**:
   - Automatically imports and uses `database.py` for database connections
   - Executes `sql/create_final_schema.sql` to create tables
   - Runs the complete ETL process (extract, clean, transform, load)
   - Loads all 10,000 records into normalized tables

4. **Validate the Data** (Optional - for verification):
   ```bash
   python scripts/advanced_validation.py
   ```

5. **Analyze the Data** (Optional - for insights):
   ```bash
   python scripts/analyze_complex_data.py
   ```

6. **Explore Raw Data** (Optional - for understanding):
   ```bash
   python scripts/data_exploration.py
   ```

### Important Notes

- **`database.py` is NOT run directly** - it's a utility module imported by other scripts
- **`sql/create_final_schema.sql` is executed automatically** by the ETL pipeline
- **The main entry point is `advanced_etl_pipeline.py`** - this orchestrates everything
- **All scripts are independent** and can be run in any order after the ETL is complete

### Database Connection Details
- **Host**: localhost
- **Port**: 3306
- **Database**: home_db
- **User**: root
- **Password**: 6equj5_root

## Key Features Demonstrated

### 1. **Complex Data Handling**
- Successfully parses and normalizes nested JSON arrays
- Handles multiple records per property entity
- Maintains data integrity across relationships

### 2. **Performance Optimization**
- Bulk inserts for large datasets
- Proper indexing strategy
- Efficient data processing

### 3. **Data Quality Assurance**
- Comprehensive validation checks
- Error handling and logging
- Data type validation and conversion

### 4. **Scalability Considerations**
- Modular ETL design
- Configurable database connections
- Reusable components

## Business Value

The normalized database structure provides:

1. **Data Integrity**: Eliminates redundancy and ensures consistency
2. **Query Performance**: Optimized for complex analytical queries
3. **Flexibility**: Easy to add new fields or modify relationships
4. **Maintainability**: Clear separation of concerns and modular design
5. **Analytics Ready**: Structured for business intelligence and reporting

## Conclusion

This implementation successfully meets all assignment requirements:

 **Technology compliance**: Uses only allowed technologies  
 **Proper normalization**: Well-designed relational schema  
 **Complete ETL**: End-to-end data processing pipeline  
 **Data quality**: Comprehensive validation and error handling  
 **Documentation**: Clear instructions and code comments  
 **Reproducibility**: Fully automated setup and execution  

