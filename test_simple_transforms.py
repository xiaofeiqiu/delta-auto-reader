#!/usr/bin/env python3
"""
Simple test script for transform functions without full framework dependencies
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from datetime import datetime

# Test just the transform creation and function logic directly
sys.path.insert(0, '/Users/dev/workspace/delta-auto-reader')

# Import Transform class directly
from feature_store_sdk.transform import Transform

# Import datetime for date functions
import datetime as dt


def create_year_transform(column_name: str, output_name: str = None):
    """Create YEAR transform function"""
    if output_name is None:
        output_name = f"{column_name}_year"
    
    def year_transform(df):
        import pandas as pd
        if hasattr(df, 'columns') and column_name in df.columns:
            series = df[column_name]
            if hasattr(series, 'dt'):
                return series.dt.year
            else:
                return pd.to_datetime(series).dt.year
        return None
    
    return Transform(output_name, year_transform)


def create_month_transform(column_name: str, output_name: str = None):
    """Create MONTH transform function"""
    if output_name is None:
        output_name = f"{column_name}_month"
    
    def month_transform(df):
        import pandas as pd
        if hasattr(df, 'columns') and column_name in df.columns:
            series = df[column_name]
            if hasattr(series, 'dt'):
                return series.dt.month
            else:
                return pd.to_datetime(series).dt.month
        return None
    
    return Transform(output_name, month_transform)


def create_day_transform(column_name: str, output_name: str = None):
    """Create DAY transform function"""
    if output_name is None:
        output_name = f"{column_name}_day"
    
    def day_transform(df):
        import pandas as pd
        if hasattr(df, 'columns') and column_name in df.columns:
            series = df[column_name]
            if hasattr(series, 'dt'):
                return series.dt.day
            else:
                return pd.to_datetime(series).dt.day
        return None
    
    return Transform(output_name, day_transform)


def create_lpad_transform(column_name: str, length: int, pad_string: str = ' ', output_name: str = None):
    """Create LPAD transform function"""
    if output_name is None:
        output_name = f"{column_name}_lpad"
    
    def lpad_transform(df):
        import pandas as pd
        if hasattr(df, 'columns') and column_name in df.columns:
            series = df[column_name].astype(str)
            return series.str.rjust(length, pad_string)
        return None
    
    return Transform(output_name, lpad_transform)


def create_nullif_transform(column_name: str, value, output_name: str = None):
    """Create NULLIF transform function"""
    if output_name is None:
        output_name = f"{column_name}_nullif"
    
    def nullif_transform(df):
        import pandas as pd
        if hasattr(df, 'columns') and column_name in df.columns:
            series = df[column_name]
            return series.where(series != value, None)
        return None
    
    return Transform(output_name, nullif_transform)


def create_upper_transform(column_name: str, output_name: str = None):
    """Create UPPER transform function"""
    if output_name is None:
        output_name = f"{column_name}_upper"
    
    def upper_transform(df):
        import pandas as pd
        if hasattr(df, 'columns') and column_name in df.columns:
            series = df[column_name]
            return series.str.upper()
        return None
    
    return Transform(output_name, upper_transform)


def create_coalesce_transform(*column_names, output_name: str = None):
    """Create COALESCE transform function"""
    if output_name is None:
        output_name = "coalesce_result"
    
    def coalesce_transform(df):
        import pandas as pd
        if hasattr(df, 'columns'):
            available_columns = [col for col in column_names if col in df.columns]
            if not available_columns:
                return pd.Series([None] * len(df), name=output_name, index=df.index)
            
            result = df[available_columns[0]].copy()
            for col_name in available_columns[1:]:
                result = result.fillna(df[col_name])
            
            return result
        return None
    
    return Transform(output_name, coalesce_transform)


def test_date_functions():
    """Test YEAR, MONTH, DAY functions"""
    print("Testing date extraction functions...")
    
    # Create test DataFrame
    df = pd.DataFrame({
        'date_col': pd.to_datetime(['2023-05-15', '2022-12-25', '2024-01-01', '2023-07-04'])
    })
    
    # Test YEAR function
    year_transform = create_year_transform('date_col', 'extracted_year')
    year_result = year_transform.apply_pandas(df)
    expected_years = [2023, 2022, 2024, 2023]
    
    print(f"YEAR function test:")
    print(f"  Expected: {expected_years}")
    print(f"  Got: {year_result.tolist()}")
    assert year_result.tolist() == expected_years
    print("  ✓ YEAR function passed")
    
    # Test MONTH function
    month_transform = create_month_transform('date_col', 'extracted_month')
    month_result = month_transform.apply_pandas(df)
    expected_months = [5, 12, 1, 7]
    
    print(f"MONTH function test:")
    print(f"  Expected: {expected_months}")
    print(f"  Got: {month_result.tolist()}")
    assert month_result.tolist() == expected_months
    print("  ✓ MONTH function passed")
    
    # Test DAY function
    day_transform = create_day_transform('date_col', 'extracted_day')
    day_result = day_transform.apply_pandas(df)
    expected_days = [15, 25, 1, 4]
    
    print(f"DAY function test:")
    print(f"  Expected: {expected_days}")
    print(f"  Got: {day_result.tolist()}")
    assert day_result.tolist() == expected_days
    print("  ✓ DAY function passed")


def test_string_functions():
    """Test LPAD and UPPER functions"""
    print("\nTesting string functions...")
    
    # Create test DataFrame
    df = pd.DataFrame({
        'text_col': ['hello', 'world', 'test', 'data'],
        'number_col': [1, 22, 333, 4444]
    })
    
    # Test LPAD function
    lpad_transform = create_lpad_transform('text_col', 10, '0', 'padded_text')
    lpad_result = lpad_transform.apply_pandas(df)
    expected_lpad = ['00000hello', '00000world', '000000test', '000000data']
    
    print(f"LPAD function test:")
    print(f"  Expected: {expected_lpad}")
    print(f"  Got: {lpad_result.tolist()}")
    assert lpad_result.tolist() == expected_lpad
    print("  ✓ LPAD function passed")
    
    # Test UPPER function
    upper_transform = create_upper_transform('text_col', 'upper_text')
    upper_result = upper_transform.apply_pandas(df)
    expected_upper = ['HELLO', 'WORLD', 'TEST', 'DATA']
    
    print(f"UPPER function test:")
    print(f"  Expected: {expected_upper}")
    print(f"  Got: {upper_result.tolist()}")
    assert upper_result.tolist() == expected_upper
    print("  ✓ UPPER function passed")


def test_conditional_functions():
    """Test NULLIF and COALESCE functions"""
    print("\nTesting conditional functions...")
    
    # Create test DataFrame with nulls
    df = pd.DataFrame({
        'status': ['active', 'inactive', 'active', 'disabled'],
        'primary_email': ['john@example.com', None, 'jane@example.com', None],
        'backup_email': [None, 'backup@example.com', None, 'backup2@example.com'],
        'fallback_email': ['fallback@example.com', 'fallback2@example.com', 'fallback3@example.com', None]
    })
    
    # Test NULLIF function
    nullif_transform = create_nullif_transform('status', 'inactive', 'status_nullif')
    nullif_result = nullif_transform.apply_pandas(df)
    expected_nullif = ['active', None, 'active', 'disabled']
    
    print(f"NULLIF function test:")
    print(f"  Expected: {expected_nullif}")
    print(f"  Got: {nullif_result.tolist()}")
    assert nullif_result.tolist() == expected_nullif
    print("  ✓ NULLIF function passed")
    
    # Test COALESCE function
    coalesce_transform = create_coalesce_transform('primary_email', 'backup_email', 'fallback_email', output_name='coalesced_email')
    coalesce_result = coalesce_transform.apply_pandas(df)
    expected_coalesce = ['john@example.com', 'backup@example.com', 'jane@example.com', 'backup2@example.com']
    
    print(f"COALESCE function test:")
    print(f"  Expected: {expected_coalesce}")
    print(f"  Got: {coalesce_result.tolist()}")
    assert coalesce_result.tolist() == expected_coalesce
    print("  ✓ COALESCE function passed")


def run_all_tests():
    """Run all test functions"""
    print("=" * 60)
    print("TESTING TRANSFORM FUNCTIONS (SIMPLIFIED)")
    print("=" * 60)
    
    try:
        test_date_functions()
        test_string_functions()
        test_conditional_functions()
        
        print("\n" + "=" * 60)
        print("ALL TESTS PASSED! ✓")
        print("=" * 60)
        
        print("\nFunction usage examples:")
        print("  YEAR('date_col') - Extract year from date column")
        print("  MONTH('date_col') - Extract month from date column") 
        print("  DAY('date_col') - Extract day from date column")
        print("  LPAD('text_col', 10, '0') - Left pad with zeros to length 10")
        print("  NULLIF('status_col', 'inactive') - Return NULL if value equals 'inactive'")
        print("  UPPER('text_col') - Convert to uppercase")
        print("  COALESCE('col1', 'col2', 'col3') - Return first non-null value")
        
        return True
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)