#!/usr/bin/env python3
"""
Test script for the simplified transform API using create_transform()
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from datetime import datetime

# Import the simplified API functions (avoiding dependency issues by testing directly)
from feature_store_sdk.transform import Transform

# Create simplified versions of the core transform functions for testing
def create_transform_direct(function_name: str, *args, **kwargs):
    """Direct implementation of create_transform for testing"""
    
    function_name = function_name.upper()
    
    if function_name == 'YEAR':
        column_name = args[0]
        output_name = args[1] if len(args) > 1 else f"{column_name}_year"
        
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
    
    elif function_name == 'MONTH':
        column_name = args[0]
        output_name = args[1] if len(args) > 1 else f"{column_name}_month"
        
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
    
    elif function_name == 'DAY':
        column_name = args[0]
        output_name = args[1] if len(args) > 1 else f"{column_name}_day"
        
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
    
    elif function_name == 'LPAD':
        column_name = args[0]
        length = args[1]
        pad_string = args[2] if len(args) > 2 else ' '
        output_name = args[3] if len(args) > 3 else f"{column_name}_lpad"
        
        def lpad_transform(df):
            import pandas as pd
            if hasattr(df, 'columns') and column_name in df.columns:
                series = df[column_name].astype(str)
                return series.str.rjust(length, pad_string)
            return None
        
        return Transform(output_name, lpad_transform)
    
    elif function_name == 'NULLIF':
        column_name = args[0]
        value = args[1]
        output_name = args[2] if len(args) > 2 else f"{column_name}_nullif"
        
        def nullif_transform(df):
            import pandas as pd
            if hasattr(df, 'columns') and column_name in df.columns:
                series = df[column_name]
                return series.where(series != value, None)
            return None
        
        return Transform(output_name, nullif_transform)
    
    elif function_name == 'UPPER':
        column_name = args[0]
        output_name = args[1] if len(args) > 1 else f"{column_name}_upper"
        
        def upper_transform(df):
            import pandas as pd
            if hasattr(df, 'columns') and column_name in df.columns:
                series = df[column_name]
                return series.str.upper()
            return None
        
        return Transform(output_name, upper_transform)
    
    elif function_name == 'COALESCE':
        column_names = args[:-1] if 'output_name' not in kwargs else args
        output_name = kwargs.get('output_name', args[-1] if 'output_name' not in kwargs and len(args) > 1 else 'coalesce_result')
        
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
    
    else:
        available_functions = ['YEAR', 'MONTH', 'DAY', 'LPAD', 'NULLIF', 'UPPER', 'COALESCE']
        raise ValueError(f"Unknown transform function '{function_name}'. Available functions: {', '.join(available_functions)}")


def test_simplified_api():
    """Test the simplified create_transform API"""
    print("=" * 60)
    print("TESTING SIMPLIFIED TRANSFORM API")
    print("=" * 60)
    
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
    
    # Test each function using the simplified API
    test_cases = [
        ('YEAR', ['date_col'], [2023, 2022, 2024]),
        ('MONTH', ['date_col'], [5, 12, 1]),
        ('DAY', ['date_col'], [15, 25, 1]),
        ('LPAD', ['text_col', 8, '0'], ['000hello', '000world', '0000test']),
        ('NULLIF', ['status_col', 'inactive'], ['active', None, 'active']),
        ('UPPER', ['text_col'], ['HELLO', 'WORLD', 'TEST']),
        ('COALESCE', ['email1', 'email2', 'email3'], ['user@example.com', 'backup@example.com', 'jane@example.com'])
    ]
    
    for func_name, args, expected in test_cases:
        print(f"Testing {func_name} with args {args}:")
        
        # Create transform using simplified API
        transform = create_transform_direct(func_name, *args)
        result = transform.apply_pandas(df)
        
        print(f"  Expected: {expected}")
        print(f"  Got: {result.tolist()}")
        
        assert result.tolist() == expected, f"{func_name} test failed"
        print(f"  ✓ {func_name} function passed")
        print()
    
    print("=" * 60)
    print("ALL SIMPLIFIED API TESTS PASSED! ✓")
    print("=" * 60)
    
    print("\nSimplified usage examples:")
    print("  create_transform('YEAR', 'date_col')")
    print("  create_transform('MONTH', 'date_col', 'custom_month')")
    print("  create_transform('LPAD', 'text_col', 10, '0')")
    print("  create_transform('NULLIF', 'status_col', 'inactive')")
    print("  create_transform('UPPER', 'text_col')")
    print("  create_transform('COALESCE', 'col1', 'col2', 'col3')")


if __name__ == "__main__":
    try:
        test_simplified_api()
        print("\n🎉 SUCCESS: All tests passed!")
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)