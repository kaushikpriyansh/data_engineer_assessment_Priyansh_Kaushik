# scripts/analyze_complex_data.py
import pandas as pd
import json
import ast
from collections import Counter


def analyze_nested_columns():
    """Analyze the nested JSON structures in complex columns"""

    # Load the data
    with open('C:/Users/grant/Desktop/Assignmnet 2/data_engineer_assessment_Priyansh_Kaushik/data/fake_property_data.json', 'r') as f:
        data = json.load(f)
    df = pd.DataFrame(data)

    print("=== ANALYZING COMPLEX COLUMNS ===\n")

    # Analyze Valuation column
    print("1. VALUATION COLUMN ANALYSIS:")
    print("-" * 40)
    sample_valuations = []
    for i in range(5):  # Check first 5 records
        val = df.loc[i, 'Valuation']
        if isinstance(val, str):
            try:
                val_parsed = ast.literal_eval(val)
                sample_valuations.append(val_parsed)
                print(f"Record {i}: {val_parsed}")
            except:
                print(f"Record {i}: {val}")
        else:
            print(f"Record {i}: {val}")

    # Analyze HOA column
    print(f"\n2. HOA COLUMN ANALYSIS:")
    print("-" * 40)
    sample_hoa = []
    for i in range(5):
        hoa = df.loc[i, 'HOA']
        if isinstance(hoa, str):
            try:
                hoa_parsed = ast.literal_eval(hoa)
                sample_hoa.append(hoa_parsed)
                print(f"Record {i}: {hoa_parsed}")
            except:
                print(f"Record {i}: {hoa}")
        else:
            print(f"Record {i}: {hoa}")

    # Analyze Rehab column
    print(f"\n3. REHAB COLUMN ANALYSIS:")
    print("-" * 40)
    sample_rehab = []
    for i in range(5):
        rehab = df.loc[i, 'Rehab']
        if isinstance(rehab, str):
            try:
                rehab_parsed = ast.literal_eval(rehab)
                sample_rehab.append(rehab_parsed)
                print(f"Record {i}: {rehab_parsed}")
            except:
                print(f"Record {i}: {rehab}")
        else:
            print(f"Record {i}: {rehab}")

    # Get all unique keys from each nested structure
    print(f"\n4. EXTRACTING UNIQUE KEYS:")
    print("-" * 40)

    # Valuation keys
    val_keys = set()
    for _, row in df.iterrows():
        try:
            val_data = ast.literal_eval(row['Valuation']) if isinstance(row['Valuation'], str) else row['Valuation']
            if isinstance(val_data, list):
                for item in val_data:
                    if isinstance(item, dict):
                        val_keys.update(item.keys())
            elif isinstance(val_data, dict):
                val_keys.update(val_data.keys())
        except:
            continue
    print(f"Valuation keys: {sorted(val_keys)}")

    # HOA keys
    hoa_keys = set()
    for _, row in df.iterrows():
        try:
            hoa_data = ast.literal_eval(row['HOA']) if isinstance(row['HOA'], str) else row['HOA']
            if isinstance(hoa_data, list):
                for item in hoa_data:
                    if isinstance(item, dict):
                        hoa_keys.update(item.keys())
            elif isinstance(hoa_data, dict):
                hoa_keys.update(hoa_data.keys())
        except:
            continue
    print(f"HOA keys: {sorted(hoa_keys)}")

    # Rehab keys
    rehab_keys = set()
    for _, row in df.iterrows():
        try:
            rehab_data = ast.literal_eval(row['Rehab']) if isinstance(row['Rehab'], str) else row['Rehab']
            if isinstance(rehab_data, list):
                for item in rehab_data:
                    if isinstance(item, dict):
                        rehab_keys.update(item.keys())
            elif isinstance(rehab_data, dict):
                rehab_keys.update(rehab_data.keys())
        except:
            continue
    print(f"Rehab keys: {sorted(rehab_keys)}")

    return val_keys, hoa_keys, rehab_keys


def analyze_field_config_detailed():
    """Get detailed field configuration mapping"""
    config_df = pd.read_excel('C:/Users/grant/Desktop/Assignmnet 2/data_engineer_assessment_Priyansh_Kaushik/data/Field Config.xlsx')

    print(f"\n5. FIELD CONFIGURATION MAPPING:")
    print("-" * 40)
    print(f"Total fields in config: {len(config_df)}")

    # Group by target table
    table_mapping = config_df.groupby('Target Table')['Column Name'].apply(list).to_dict()

    for table, columns in table_mapping.items():
        print(f"\n{table.upper()} TABLE:")
        for col in columns:
            print(f"  - {col}")

    return table_mapping


if __name__ == "__main__":
    val_keys, hoa_keys, rehab_keys = analyze_nested_columns()
    table_mapping = analyze_field_config_detailed()
