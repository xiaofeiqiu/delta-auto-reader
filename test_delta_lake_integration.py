#!/usr/bin/env python3
"""
Integration test script to verify the to_delta_table_filter functionality 
works with actual Delta Lake tables and data fetching.
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from feature_store_sdk.filters import c, condition, and_, or_, not_, DELTALAKE_AVAILABLE

# Try to import required packages
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

if DELTALAKE_AVAILABLE:
    from deltalake import DeltaTable, write_deltalake

# ANSI color codes for better output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    YELLOW = '\033[93m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_test_header(test_name: str):
    """Print a formatted test header"""
    print(f"\n{Colors.BLUE}{Colors.BOLD}=== {test_name} ==={Colors.END}")

def print_success(message: str):
    """Print a success message"""
    print(f"{Colors.GREEN}✓ {message}{Colors.END}")

def print_error(message: str):
    """Print an error message"""
    print(f"{Colors.RED}❌ {message}{Colors.END}")

def print_info(message: str):
    """Print an info message"""
    print(f"{Colors.YELLOW}ℹ {message}{Colors.END}")

def check_dependencies():
    """Check if all required dependencies are available"""
    print_test_header("Checking Dependencies")
    
    if not HAS_PANDAS:
        print_error("pandas is required but not available")
        return False
    else:
        print_success("pandas is available")
    
    if not HAS_PYARROW:
        print_error("pyarrow is required but not available")
        return False
    else:
        print_success("pyarrow is available")
    
    if not DELTALAKE_AVAILABLE:
        print_error("deltalake is required but not available")
        print_info("Install with: pip install deltalake")
        return False
    else:
        print_success("deltalake is available")
    
    return True

def create_test_data():
    """Create test data for Delta table"""
    data = {
        'id': list(range(1, 101)),
        'name': [f'User_{i:03d}' for i in range(1, 101)],
        'age': [20 + (i % 50) for i in range(100)],
        'country': ['US', 'UK', 'CA', 'DE', 'FR'] * 20,
        'status': ['active', 'inactive', 'premium', 'trial', 'banned'] * 20,
        'score': [50 + (i % 50) for i in range(100)],
        'is_vip': [i % 5 == 0 for i in range(100)],
        'balance': [100.0 + (i * 10.5) for i in range(100)],
        'last_login': pd.date_range('2024-01-01', periods=100, freq='D')[:100]
    }
    return pd.DataFrame(data)

def test_filter_type_format():
    """Test that our filters return the correct FilterType format"""
    print_test_header("Testing FilterType Format")
    
    # Test simple condition
    simple_filter = c("age", ">", 25).to_delta_table_filter()
    print_info(f"Simple filter: {simple_filter}")
    
    # Should be a list of tuples (FilterConjunctionType)
    if not isinstance(simple_filter, list):
        print_error("Filter should be a list")
        return False
    
    if not all(isinstance(f, tuple) and len(f) == 3 for f in simple_filter):
        print_error("Filter should be a list of 3-tuples")
        return False
    
    print_success("Simple filter format is correct")
    
    # Test AND condition
    and_filter = (c("age", ">", 25) & c("country", "==", "US")).to_delta_table_filter()
    print_info(f"AND filter: {and_filter}")
    
    if not isinstance(and_filter, list):
        print_error("AND filter should be a list")
        return False
    
    print_success("AND filter format is correct")
    
    # Test OR condition
    or_filter = (c("country", "==", "US") | c("country", "==", "UK")).to_delta_table_filter()
    print_info(f"OR filter: {or_filter}")
    
    if not isinstance(or_filter, list):
        print_error("OR filter should be a list")
        return False
    
    print_success("OR filter format is correct")
    
    return True

def test_delta_table_creation_and_filtering(temp_dir):
    """Test creating a Delta table and filtering with our filters"""
    print_test_header("Testing Delta Table Creation and Filtering")
    
    # Create test data
    df = create_test_data()
    table_path = os.path.join(temp_dir, "test_table")
    
    print_info(f"Creating Delta table at {table_path}")
    print_info(f"Test data shape: {df.shape}")
    
    # Write Delta table
    try:
        write_deltalake(table_path, df)
        print_success("Delta table created successfully")
    except Exception as e:
        print_error(f"Failed to create Delta table: {e}")
        return False
    
    # Load the table
    try:
        dt = DeltaTable(table_path)
        print_success("Delta table loaded successfully")
        print_info(f"Table schema: {dt.schema()}")
    except Exception as e:
        print_error(f"Failed to load Delta table: {e}")
        return False
    
    # Test various filters
    test_cases = [
        {
            "name": "Simple equality filter",
            "condition": c("country", "==", "US"),
            "expected_count": 20  # 5 countries * 20 repetitions = 100, so 20 for US
        },
        {
            "name": "Greater than filter",
            "condition": c("age", ">", 40),
            "expected_rows": lambda df: len(df[df['age'] > 40])
        },
        {
            "name": "IN filter",
            "condition": c("status", "in", ["active", "premium"]),
            "expected_rows": lambda df: len(df[df['status'].isin(["active", "premium"])])
        },
        {
            "name": "AND filter",
            "condition": c("age", ">", 30) & c("country", "==", "US"),
            "expected_rows": lambda df: len(df[(df['age'] > 30) & (df['country'] == "US")])
        },
        {
            "name": "OR filter", 
            "condition": c("country", "==", "US") | c("country", "==", "UK"),
            "expected_rows": lambda df: len(df[df['country'].isin(["US", "UK"])])
        },
        {
            "name": "NOT filter (simple)",
            "condition": ~c("status", "==", "banned"),
            "expected_rows": lambda df: len(df[df['status'] != "banned"])
        },
        {
            "name": "BETWEEN equivalent (age >= 25 AND age <= 45)",
            "condition": c("age", "between", (25, 45)),
            "expected_rows": lambda df: len(df[(df['age'] >= 25) & (df['age'] <= 45)])
        }
    ]
    
    all_passed = True
    
    for test_case in test_cases:
        try:
            print_info(f"\nTesting: {test_case['name']}")
            
            # Get the filter
            filter_condition = test_case["condition"].to_delta_table_filter()
            print_info(f"Filter: {filter_condition}")
            
            # Apply filter to Delta table
            filtered_df = dt.to_pandas(filters=filter_condition)
            actual_count = len(filtered_df)
            
            print_info(f"Filtered result count: {actual_count}")
            
            # Calculate expected count
            if "expected_count" in test_case:
                expected_count = test_case["expected_count"]
            else:
                expected_count = test_case["expected_rows"](df)
            
            print_info(f"Expected count: {expected_count}")
            
            if actual_count == expected_count:
                print_success(f"✓ {test_case['name']}: {actual_count} rows (correct)")
            else:
                print_error(f"✗ {test_case['name']}: got {actual_count} rows, expected {expected_count}")
                all_passed = False
                
        except Exception as e:
            print_error(f"✗ {test_case['name']}: Exception - {e}")
            all_passed = False
    
    return all_passed

def test_unsupported_operations():
    """Test operations that are not supported by Delta Lake filters"""
    print_test_header("Testing Unsupported Operations")
    
    unsupported_cases = [
        ("String starts_with", c("name", "starts_with", "User_0")),
        ("String ends_with", c("name", "ends_with", "_001")),
        ("String contains", c("name", "contains", "ser_0")),
    ]
    
    for name, condition in unsupported_cases:
        try:
            filter_result = condition.to_delta_table_filter()
            print_error(f"{name}: Expected exception but got {filter_result}")
            return False
        except ValueError as e:
            print_success(f"{name}: Correctly raised ValueError - {e}")
        except Exception as e:
            print_error(f"{name}: Unexpected exception - {e}")
            return False
    
    return True

def test_complex_conditions():
    """Test complex nested conditions"""
    print_test_header("Testing Complex Conditions with Delta Lake")
    
    if not DELTALAKE_AVAILABLE:
        print_info("Skipping complex conditions test - deltalake not available")
        return True
    
    # Create a temporary table for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        df = create_test_data()
        table_path = os.path.join(temp_dir, "complex_test_table")
        
        try:
            write_deltalake(table_path, df)
            dt = DeltaTable(table_path)
        except Exception as e:
            print_error(f"Failed to create test table: {e}")
            return False
        
        # Test complex condition
        complex_condition = (
            (c("age", ">=", 25) & c("country", "in", ["US", "UK"])) |
            (c("status", "==", "premium") & c("score", ">", 80))
        )
        
        try:
            filter_condition = complex_condition.to_delta_table_filter()
            print_info(f"Complex filter: {filter_condition}")
            
            filtered_df = dt.to_pandas(filters=filter_condition)
            print_success(f"Complex condition filtered to {len(filtered_df)} rows")
            
            # Verify the filter worked by checking some conditions manually
            manual_filter = (
                ((df['age'] >= 25) & (df['country'].isin(["US", "UK"]))) |
                ((df['status'] == "premium") & (df['score'] > 80))
            )
            expected_count = len(df[manual_filter])
            actual_count = len(filtered_df)
            
            if actual_count == expected_count:
                print_success(f"Complex condition result matches expected: {actual_count} rows")
                return True
            else:
                print_error(f"Complex condition mismatch: got {actual_count}, expected {expected_count}")
                return False
                
        except Exception as e:
            print_error(f"Complex condition failed: {e}")
            return False

def main():
    """Run all integration tests"""
    print(f"{Colors.BOLD}🧪 Delta Lake Integration Test Suite{Colors.END}")
    print("=" * 60)
    
    # Check dependencies first
    if not check_dependencies():
        print_error("Required dependencies not available. Exiting.")
        return False
    
    test_functions = [
        test_filter_type_format,
        test_unsupported_operations,
        test_complex_conditions,
    ]
    
    # Test with actual Delta table creation and filtering
    if DELTALAKE_AVAILABLE and HAS_PANDAS:
        with tempfile.TemporaryDirectory() as temp_dir:
            print_info(f"Using temporary directory: {temp_dir}")
            test_functions.insert(-1, lambda: test_delta_table_creation_and_filtering(temp_dir))
    
    passed_tests = 0
    total_tests = len(test_functions)
    
    for test_func in test_functions:
        try:
            if test_func():
                passed_tests += 1
            else:
                print_error(f"Test {test_func.__name__} failed!")
        except Exception as e:
            print_error(f"Test {test_func.__name__} raised exception: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{Colors.BOLD}Integration Test Results:{Colors.END}")
    print("=" * 40)
    
    if passed_tests == total_tests:
        print(f"{Colors.GREEN}{Colors.BOLD}🎉 ALL INTEGRATION TESTS PASSED! ({passed_tests}/{total_tests}){Colors.END}")
        print(f"{Colors.GREEN}The to_delta_table_filter implementation works with real Delta Lake tables!{Colors.END}")
        return True
    else:
        print(f"{Colors.RED}{Colors.BOLD}❌ SOME TESTS FAILED! ({passed_tests}/{total_tests}){Colors.END}")
        print(f"{Colors.RED}Please check the failed tests above.{Colors.END}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)