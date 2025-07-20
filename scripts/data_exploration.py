# scripts/data_exploration.py
import pandas as pd
import json
import mysql.connector
from pathlib import Path


def explore_json_data():
    """Explore the raw JSON property data"""
    with open('C:/Users/grant/Desktop/Assignmnet 2/data_engineer_assessment_Priyansh_Kaushik/data/fake_property_data.json', 'r') as f:
        data = json.load(f)

    # Convert to DataFrame for analysis
    df = pd.DataFrame(data)

    print(f"Dataset shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"\nData types:\n{df.dtypes}")
    print(f"\nSample records:\n{df.head()}")

    # Check for missing values
    print(f"\nMissing values:\n{df.isnull().sum()}")

    return df


def analyze_field_config():
    """Analyze the field configuration Excel file"""
    config_df = pd.read_excel('C:/Users/grant/Desktop/Assignmnet 2/data_engineer_assessment_Priyansh_Kaushik/data/Field Config.xlsx')
    print("Field Configuration:")
    print(config_df.head(10))
    return config_df


if __name__ == "__main__":
    property_df = explore_json_data()
    config_df = analyze_field_config()
