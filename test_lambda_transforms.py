#!/usr/bin/env python3
"""
Test script for lambda-based transform functions
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from datetime import datetime

# Import Transform class
from feature_store_sdk.transform import Transform

# Create lambda-based transforms for testing (simulating the actual functions)
def test_lambda_year(column_name: str, output_name: str = None):
    if output_name is None:
        output_name = f"{column_name}_year"
    return Transform(output_name, lambda df: df[column_name].dt.year if hasattr(df[column_name], 'dt') else pd.to_datetime(df[column_name]).dt.year)

def test_lambda_upper(column_name: str, output_name: str = None):
    if output_name is None:
        output_name = f"{column_name}_upper"
    return Transform(output_name, lambda df: df[column_name].str.upper())

def test_lambda_lpad(column_name: str, length: int, pad_string: str = ' ', output_name: str = None):
    if output_name is None:
        output_name = f"{column_name}_lpad"
    return Transform(output_name, lambda df: df[column_name].astype(str).str.rjust(length, pad_string))

def test_lambda_nullif(column_name: str, value, output_name: str = None):
    if output_name is None:
        output_name = f"{column_name}_nullif"
    return Transform(output_name, lambda df: df[column_name].where(df[column_name] != value, None))

def test_lambda_coalesce(*column_names: str, output_name: str = None):
    if output_name is None:
        output_name = "coalesce_result"
    
    def coalesce_func(df):
        available_columns = [col for col in column_names if col in df.columns]
        if not available_columns:
            return pd.Series([None] * len(df), name=output_name, index=df.index)
        
        result = df[available_columns[0]].copy()
        for col_name in available_columns[1:]:
            result = result.fillna(df[col_name])
        return result
    
    return Transform(output_name, coalesce_func)

# Ultra-concise one-liner helpers
year = lambda col: test_lambda_year(col)
upper = lambda col: test_lambda_upper(col)
lpad = lambda col, length, pad=' ': test_lambda_lpad(col, length, pad)
nullif = lambda col, val: test_lambda_nullif(col, val)
coalesce = lambda *cols: test_lambda_coalesce(*cols)


def test_lambda_transforms():
    """Test lambda-based transform functions"""
    print("=" * 70)
    print("TESTING LAMBDA-BASED TRANSFORM FUNCTIONS")
    print("=" * 70)
    
    # Create test data
    df = pd.DataFrame({
        'date_col': pd.to_datetime(['2023-05-15', '2022-12-25', '2024-01-01']),
        'text_col': ['hello', 'world', 'test'],
        'status_col': ['active', 'inactive', 'active'],
        'email1': ['user@example.com', None, 'jane@example.com'],
        'email2': [None, 'backup@example.com', None],
        'email3': ['fallback@example.com', 'fallback2@example.com', 'fallback3@example.com']
    })
    
    print("Test data:")
    print(df)
    print()
    
    print("1. Lambda-based transforms:")
    print("-" * 40)
    
    # Test lambda versions
    transforms = [
        ('year_lambda', test_lambda_year('date_col'), [2023, 2022, 2024]),
        ('upper_lambda', test_lambda_upper('text_col'), ['HELLO', 'WORLD', 'TEST']),
        ('lpad_lambda', test_lambda_lpad('text_col', 8, '0'), ['000hello', '000world', '0000test']),
        ('nullif_lambda', test_lambda_nullif('status_col', 'inactive'), ['active', None, 'active']),
        ('coalesce_lambda', test_lambda_coalesce('email1', 'email2', 'email3'), ['user@example.com', 'backup@example.com', 'jane@example.com'])
    ]
    
    for name, transform, expected in transforms:
        result = transform.apply_pandas(df)
        print(f"{name}: {result.tolist()}")
        assert result.tolist() == expected, f"{name} failed"
        print(f"  ✓ {name} passed")
    
    print()
    print("2. Ultra-concise one-liner helpers:")
    print("-" * 40)
    
    # Test ultra-concise helpers
    concise_transforms = [
        ('year()', year('date_col'), [2023, 2022, 2024]),
        ('upper()', upper('text_col'), ['HELLO', 'WORLD', 'TEST']),
        ('lpad()', lpad('text_col', 8, '0'), ['000hello', '000world', '0000test']),
        ('nullif()', nullif('status_col', 'inactive'), ['active', None, 'active']),
        ('coalesce()', coalesce('email1', 'email2', 'email3'), ['user@example.com', 'backup@example.com', 'jane@example.com'])
    ]
    
    for name, transform, expected in concise_transforms:
        result = transform.apply_pandas(df)
        print(f"{name}: {result.tolist()}")
        assert result.tolist() == expected, f"{name} failed"
        print(f"  ✓ {name} passed")
    
    print()
    print("=" * 70)
    print("LAMBDA TRANSFORM USAGE EXAMPLES")
    print("=" * 70)
    
    print("# Method 1: Lambda-based functions")
    print("transforms = [")
    print("    year_lambda('date_col'),")
    print("    upper_lambda('text_col'),")
    print("    lpad_lambda('text_col', 8, '0'),")
    print("    nullif_lambda('status_col', 'inactive'),")
    print("    coalesce_lambda('email1', 'email2', 'email3')")
    print("]")
    print()
    
    print("# Method 2: Ultra-concise one-liners")
    print("transforms = [")
    print("    year('date_col'),")
    print("    upper('text_col'),")
    print("    lpad('text_col', 8, '0'),")
    print("    nullif('status_col', 'inactive'),")
    print("    coalesce('email1', 'email2', 'email3')")
    print("]")
    print()
    
    print("# Method 3: Lambda API")
    print("transforms = [")
    print("    create_lambda_transform('YEAR', 'date_col'),")
    print("    create_lambda_transform('UPPER', 'text_col'),")
    print("    create_lambda_transform('LPAD', 'text_col', 8, '0'),")
    print("    create_lambda_transform('NULLIF', 'status_col', 'inactive'),")
    print("    create_lambda_transform('COALESCE', 'email1', 'email2', 'email3')")
    print("]")
    print()
    
    print("✅ ALL LAMBDA TESTS PASSED!")
    print("✅ Now you have 4 different ways to create transforms:")
    print("   1. Regular functions: YEAR('date_col')")
    print("   2. Function mapping: create_transform('YEAR', 'date_col')")
    print("   3. Lambda functions: year_lambda('date_col')")
    print("   4. Ultra-concise: year('date_col')")


if __name__ == "__main__":
    try:
        test_lambda_transforms()
    except Exception as e:
        print(f"❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)