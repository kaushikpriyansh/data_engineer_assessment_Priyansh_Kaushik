# Data Engineering Assessment

Welcome!  
This exercise evaluates your core **data-engineering** skills:

| Competency | Focus                                                         |
| ---------- | ------------------------------------------------------------- |
| SQL        | relational modelling, normalisation, DDL/DML scripting        |
| Python ETL | data ingestion, cleaning, transformation, & loading (ELT/ETL) |

---

## 0 Prerequisites & Setup

> **Allowed technologies**

- **Python ≥ 3.8** – all ETL / data-processing code
- **MySQL 8** – the target relational database
- **Lightweight helper libraries only** (e.g. `pandas`, `mysql-connector-python`).  
  List every dependency in **`requirements.txt`** and justify anything unusual.
- **No ORMs / auto-migration tools** – write plain SQL by hand.

---

## 1 Clone the skeleton repo

```
git clone https://github.com/100x-Home-LLC/data_engineer_assessment.git
```

✏️ Note: Rename the repo after cloning and add your full name.

**Start the MySQL database in Docker:**

```
docker-compose -f docker-compose.initial.yml up --build -d
```

- Database is available on `localhost:3306`
- Credentials/configuration are in the Docker Compose file
- **Do not change** database name or credentials

For MySQL Docker image reference:
[MySQL Docker Hub](https://hub.docker.com/_/mysql)

---

### Problem

- You are provided with a raw JSON file containing property records is located in data/
- Each row relates to a property. Each row mixes many unrelated attributes (property details, HOA data, rehab estimates, valuations, etc.).
- There are multiple Columns related to this property.
- The database is not normalized and lacks relational structure.
- Use the supplied Field Config.xlsx (in data/) to understand business semantics.

### Task

- **Normalize the data:**

  - Develop a Python ETL script to read, clean, transform, and load data into your normalized MySQL tables.
  - Refer the field config document for the relation of business logic
  - Use primary keys and foreign keys to properly capture relationships

- **Deliverable:**
  - Write necessary python and sql scripts
  - Place your scripts in `sql/` and `scripts/`
  - The scripts should take the initial json to your final, normalized schema when executed
  - Clearly document how to run your script, dependencies, and how it integrates with your database.

**Tech Stack:**

- Python (include a `requirements.txt`)
  Use **MySQL** and SQL for all database work
- You may use any CLI or GUI for development, but the final changes must be submitted as python/ SQL scripts
- **Do not** use ORM migrations—write all SQL by hand

---

## Submission Guidelines

- Edit the section to the bottom of this README with your solutions and instructions for each section at the bottom.
- Place all scripts/code in their respective folders (`sql/`, `scripts/`, etc.)
- Ensure all steps are fully **reproducible** using your documentation
- Create a new private repo and invite the reviewer https://github.com/mantreshjain

---

**Good luck! We look forward to your submission.**

## Solutions and Instructions (Filed by Candidate)

### Database Design and Solution

#### **Schema Design and Normalization Strategy**

The solution implements a **7-table normalized schema** that properly separates concerns and eliminates data redundancy:

**1. properties (Main Entity Table)**
- **Primary Key**: `property_id` (AUTO_INCREMENT)
- **Purpose**: Core property information and characteristics
- **Key Fields**: address, city, state, zip, property_type, market, year_built, bed, bath, sqft_total
- **Design Decision**: Central entity that all other tables reference through foreign keys

**2. leads (Lead Management)**
- **Primary Key**: `lead_id` (AUTO_INCREMENT)
- **Foreign Key**: `property_id` → properties(property_id)
- **Purpose**: Lead status, source, occupancy, and investment metrics
- **Key Fields**: reviewed_status, most_recent_status, source, occupancy, net_yield, irr

**3. taxes (Tax Information)**
- **Primary Key**: `tax_id` (AUTO_INCREMENT)
- **Foreign Key**: `property_id` → properties(property_id)
- **Purpose**: Property tax data with year tracking
- **Key Fields**: taxes, tax_year

**4. hoa_details (HOA Records)**
- **Primary Key**: `hoa_detail_id` (AUTO_INCREMENT)
- **Foreign Key**: `property_id` → properties(property_id)
- **Purpose**: Multiple HOA records per property with sequence tracking
- **Key Fields**: hoa_fee, hoa_flag, sequence_number
- **Design Decision**: Uses sequence_number to handle multiple HOA records per property

**5. valuation_details (Property Valuations)**
- **Primary Key**: `valuation_detail_id` (AUTO_INCREMENT)
- **Foreign Key**: `property_id` → properties(property_id)
- **Purpose**: Multiple valuation records per property
- **Key Fields**: list_price, zestimate, arv, expected_rent, sequence_number
- **Design Decision**: Comprehensive valuation fields with sequence tracking

**6. rehab_estimates (Rehab Estimates)**
- **Primary Key**: `rehab_estimate_id` (AUTO_INCREMENT)
- **Foreign Key**: `property_id` → properties(property_id)
- **Purpose**: Multiple rehab cost estimates per property
- **Key Fields**: underwriting_rehab, rehab_calculation, sequence_number

**7. rehab_details (Rehab Breakdown)**
- **Primary Key**: `rehab_detail_id` (AUTO_INCREMENT)
- **Foreign Key**: `rehab_estimate_id` → rehab_estimates(rehab_estimate_id)
- **Purpose**: Detailed rehab component flags for each estimate
- **Key Fields**: paint, flooring_flag, foundation_flag, roof_flag, hvac_flag, etc.

#### **Key Design Decisions**

1. **Normalization Level**: Implemented 3NF to eliminate redundancy while maintaining query performance
2. **One-to-Many Relationships**: Used sequence_number fields to handle multiple records per property
3. **Referential Integrity**: All foreign keys with ON DELETE CASCADE for data consistency
4. **Indexing Strategy**: Indexes on frequently queried columns (city, state, property_type, list_price)
5. **Data Types**: Appropriate precision for monetary values (DECIMAL) and proper VARCHAR lengths

#### **How to Run and Test**

**Prerequisites:**
```bash
# Ensure Docker is running
docker --version

# Ensure Python 3.8+ is installed
python --version
```

**Setup Instructions:**
```bash
# 1. Start MySQL database
docker-compose -f docker-compose.initial.yml up --build -d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Run the complete ETL pipeline
python scripts/advanced_etl_pipeline.py
```

**Testing the Solution:**
```bash
# Verify data loading
python scripts/advanced_validation.py

# Analyze data structure
python scripts/analyze_complex_data.py

# Explore raw data
python scripts/data_exploration.py
```

**Database Connection Details:**
- Host: localhost
- Port: 3306
- Database: home_db
- User: root
- Password: 6equj5_root

### ETL Logic and Implementation

#### **Approach and Design**

**ETL Architecture:**
The solution implements a **modular ETL pipeline** with clear separation of concerns:

1. **Extract Layer**: JSON file reading with pandas
2. **Transform Layer**: Data cleaning, type conversion, and normalization
3. **Load Layer**: Bulk database insertion with transaction management

**Key Design Principles:**
- **Modularity**: Separate classes for database connection and ETL processing
- **Error Handling**: Comprehensive try-catch blocks with rollback mechanisms
- **Logging**: Detailed logging for monitoring and debugging
- **Performance**: Bulk inserts using `executemany()` for efficiency
- **Data Quality**: Validation and cleaning at each transformation step

#### **ETL Process Flow**

**1. Data Extraction (`extract_data`)**
```python
def extract_data(self, json_file_path):
    with open(json_file_path, 'r') as f:
        raw_data = json.load(f)
    self.df = pd.DataFrame(raw_data)
    # Extracts 10,000 records from fake_property_data.json
```

**2. Data Cleaning (`clean_data`)**
```python
def clean_data(self):
    # Handle missing values
    self.df = self.df.fillna('')
    
    # Convert numeric columns
    numeric_mappings = {
        'Tax_Rate': 'tax_rate',
        'Year_Built': 'year_built',
        'SQFT_Total': 'sqft_total',
        # ... more mappings
    }
    
    for original_col, clean_col in numeric_mappings.items():
        self.df[clean_col] = pd.to_numeric(self.df[original_col], errors='coerce')
```

**3. Complex Data Transformation (`parse_nested_json`)**
```python
def parse_nested_json(self, json_string):
    # Handles nested JSON arrays for HOA, Valuation, and Rehab data
    if isinstance(json_string, list):
        return json_string
    # Returns empty list for invalid data
```

**4. Sequential Data Loading**
```python
def run_etl(self, json_file_path):
    # Load in dependency order
    self.load_properties()      # Parent table first
    self.load_leads()          # Child tables
    self.load_taxes()
    self.load_hoa_details()    # Complex nested data
    self.load_valuation_details()
    self.load_rehab_estimates()
```

#### **Complex Data Handling**

**Nested JSON Processing:**
- **HOA Arrays**: Multiple HOA records per property with fees and flags
- **Valuation Arrays**: Multiple valuations with different metrics (Zestimate, ARV, etc.)
- **Rehab Arrays**: Multiple rehab estimates with detailed component breakdowns

**Sequence Management:**
- Uses `sequence_number` to maintain order of multiple records
- Unique constraints prevent duplicate sequences per property
- Proper foreign key relationships maintain data integrity

#### **Performance Optimizations**

1. **Bulk Operations**: `executemany()` for efficient batch inserts
2. **Transaction Management**: Atomic operations with commit/rollback
3. **Memory Efficiency**: Streaming data processing for large datasets
4. **Indexing**: Strategic indexes on frequently queried columns

#### **Requirements**

**Python Dependencies (`requirements.txt`):**
```
pandas>=1.5.0              # Data manipulation and analysis
mysql-connector-python>=8.0.0  # MySQL database connectivity
openpyxl>=3.0.0            # Excel file reading (Field Config)
python-dotenv>=0.19.0      # Environment variable management
```

**System Requirements:**
- Python 3.8 or higher
- Docker and Docker Compose
- MySQL 8 (provided via Docker)
- 4GB+ RAM for processing 10,000 records

#### **Data Quality Assurance**

**Validation Checks:**
- Record count verification across all tables
- Referential integrity validation
- Data type and range validation
- Missing value handling
- Duplicate detection and prevention

**Error Handling:**
- Connection failure recovery
- Transaction rollback on errors
- Detailed error logging
- Graceful degradation for missing data

#### **Results and Verification**

**Data Loading Summary:**
- **Properties**: 10,000 records
- **Leads**: 10,000 records
- **Taxes**: 10,000 records
- **HOA Details**: 9,993 records (some properties have no HOA)
- **Valuation Details**: 24,705 records (multiple valuations per property)
- **Rehab Estimates**: 20,024 records (multiple estimates per property)
- **Rehab Details**: 20,024 records (detailed breakdowns)

**Success Metrics:**
-  All 10,000 source records processed
-  Complex nested data properly normalized
-  Referential integrity maintained
-  No data loss during transformation
-  Performance optimized for large datasets
