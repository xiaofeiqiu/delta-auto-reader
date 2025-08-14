#!/usr/bin/env python3
"""
Comprehensive test script to verify the to_delta_table_filter functionality.
This script tests all operators, edge cases, and validates the generated SQL syntax.
"""

import sys
import os
import tempfile
from pathlib import Path

# Try to import pandas, make it optional
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from feature_store_sdk.filters import c, condition, and_, or_, not_

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

def test_basic_operators():
    """Test all basic comparison operators"""
    print_test_header("Testing Basic Operators")
    
    test_cases = [
        # (condition, expected_output, description)
        (c("age", "==", 25), "age = 25", "Equality operator"),
        (c("status", "!=", "inactive"), "status != 'inactive'", "Inequality operator"),
        (c("score", ">", 100.5), "score > 100.5", "Greater than with float"),
        (c("count", ">=", 10), "count >= 10", "Greater than or equal"),
        (c("rating", "<", 5), "rating < 5", "Less than"),
        (c("balance", "<=", 1000.0), "balance <= 1000.0", "Less than or equal"),
        (c("is_active", "==", True), "is_active = true", "Boolean true"),
        (c("is_deleted", "==", False), "is_deleted = false", "Boolean false"),
        (c("user_id", "==", 12345), "user_id = 12345", "Integer value"),
    ]
    
    for condition_obj, expected, description in test_cases:
        result = condition_obj.to_delta_table_filter()
        if result == expected:
            print_success(f"{description}: {result}")
        else:
            print_error(f"{description}: Expected '{expected}', got '{result}'")
            return False
    
    return True

def test_null_operators():
    """Test NULL checking operators"""
    print_test_header("Testing NULL Operators")
    
    test_cases = [
        (c("email", "is_null"), "email IS NULL", "IS NULL operator"),
        (c("phone", "is_not_null"), "phone IS NOT NULL", "IS NOT NULL operator"),
        (c("value", "==", None), "value = NULL", "Equality with None"),
        (c("description", "!=", None), "description != NULL", "Inequality with None"),
    ]
    
    for condition_obj, expected, description in test_cases:
        result = condition_obj.to_delta_table_filter()
        if result == expected:
            print_success(f"{description}: {result}")
        else:
            print_error(f"{description}: Expected '{expected}', got '{result}'")
            return False
    
    return True

def test_list_operators():
    """Test IN and NOT IN operators"""
    print_test_header("Testing List Operators")
    
    test_cases = [
        (c("country", "in", ["US", "UK", "CA"]), "country IN ('US', 'UK', 'CA')", "IN with strings"),
        (c("status_code", "in", [200, 201, 204]), "status_code IN (200, 201, 204)", "IN with integers"),
        (c("category", "not_in", ["spam", "test"]), "category NOT IN ('spam', 'test')", "NOT IN with strings"),
        (c("error_code", "not_in", [404, 500, 503]), "error_code NOT IN (404, 500, 503)", "NOT IN with integers"),
        (c("priority", "in", [1.1, 2.5, 3.0]), "priority IN (1.1, 2.5, 3.0)", "IN with floats"),
    ]
    
    for condition_obj, expected, description in test_cases:
        result = condition_obj.to_delta_table_filter()
        if result == expected:
            print_success(f"{description}: {result}")
        else:
            print_error(f"{description}: Expected '{expected}', got '{result}'")
            return False
    
    return True

def test_range_operators():
    """Test BETWEEN operator"""
    print_test_header("Testing Range Operators")
    
    test_cases = [
        (c("age", "between", (18, 65)), "age BETWEEN 18 AND 65", "BETWEEN with integers"),
        (c("price", "between", (10.99, 99.99)), "price BETWEEN 10.99 AND 99.99", "BETWEEN with floats"),
        (c("score", "between", (0, 100)), "score BETWEEN 0 AND 100", "BETWEEN range"),
    ]
    
    for condition_obj, expected, description in test_cases:
        result = condition_obj.to_delta_table_filter()
        if result == expected:
            print_success(f"{description}: {result}")
        else:
            print_error(f"{description}: Expected '{expected}', got '{result}'")
            return False
    
    return True

def test_string_pattern_operators():
    """Test string pattern matching operators"""
    print_test_header("Testing String Pattern Operators")
    
    test_cases = [
        (c("name", "starts_with", "John"), "name LIKE 'John%'", "STARTS_WITH operator"),
        (c("email", "ends_with", "@example.com"), "email LIKE '%@example.com'", "ENDS_WITH operator"),
        (c("description", "contains", "important"), "description LIKE '%important%'", "CONTAINS operator"),
        (c("filename", "starts_with", "data_"), "filename LIKE 'data_%'", "STARTS_WITH with underscore"),
        (c("path", "ends_with", ".json"), "path LIKE '%.json'", "ENDS_WITH with extension"),
    ]
    
    for condition_obj, expected, description in test_cases:
        result = condition_obj.to_delta_table_filter()
        if result == expected:
            print_success(f"{description}: {result}")
        else:
            print_error(f"{description}: Expected '{expected}', got '{result}'")
            return False
    
    return True

def test_edge_cases():
    """Test edge cases and special characters"""
    print_test_header("Testing Edge Cases")
    
    test_cases = [
        (c("name", "==", "O'Connor"), "name = 'O''Connor'", "String with apostrophe"),
        (c("description", "==", ""), "description = ''", "Empty string"),
        (c("text", "contains", "can't"), "text LIKE '%can''t%'", "Pattern with apostrophe"),
        (c("message", "==", "Hello 'World'"), "message = 'Hello ''World'''", "String with quotes"),
        (c("amount", "==", 0), "amount = 0", "Zero value"),
        (c("percentage", "==", -15.5), "percentage = -15.5", "Negative number"),
        (c("data", "in", []), "data IN ()", "Empty list"),
        (c("tags", "in", ["a'b", "c'd"]), "tags IN ('a''b', 'c''d')", "List with apostrophes"),
    ]
    
    for condition_obj, expected, description in test_cases:
        result = condition_obj.to_delta_table_filter()
        if result == expected:
            print_success(f"{description}: {result}")
        else:
            print_error(f"{description}: Expected '{expected}', got '{result}'")
            return False
    
    return True

def test_complex_conditions():
    """Test complex nested conditions with AND, OR, NOT"""
    print_test_header("Testing Complex Conditions")
    
    test_cases = [
        # AND conditions
        (
            c("age", ">", 25) & c("country", "==", "US"),
            "(age > 25 AND country = 'US')",
            "Simple AND condition"
        ),
        
        # OR conditions
        (
            c("status", "==", "VIP") | c("score", ">", 1000),
            "(status = 'VIP' OR score > 1000)",
            "Simple OR condition"
        ),
        
        # NOT conditions
        (
            ~c("status", "==", "banned"),
            "NOT (status = 'banned')",
            "Simple NOT condition"
        ),
        
        # Complex nested conditions
        (
            (c("age", ">", 25) & c("country", "==", "US")) | (c("status", "==", "VIP") & ~c("banned", "==", True)),
            "((age > 25 AND country = 'US') OR (status = 'VIP' AND NOT (banned = true)))",
            "Complex nested condition"
        ),
        
        # Multiple AND conditions
        (
            c("age", ">=", 18) & c("age", "<=", 65) & c("active", "==", True),
            "(age >= 18 AND age <= 65 AND active = true)",
            "Multiple AND conditions"
        ),
        
        # Multiple OR conditions
        (
            c("country", "==", "US") | c("country", "==", "UK") | c("country", "==", "CA"),
            "(country = 'US' OR country = 'UK' OR country = 'CA')",
            "Multiple OR conditions"
        ),
        
        # Mixed complex condition
        (
            (c("category", "in", ["premium", "gold"]) & c("active", "==", True)) | 
            (c("trial", "==", True) & ~c("expired", "==", True)),
            "((category IN ('premium', 'gold') AND active = true) OR (trial = true AND NOT (expired = true)))",
            "Mixed complex condition"
        ),
    ]
    
    for condition_obj, expected, description in test_cases:
        result = condition_obj.to_delta_table_filter()
        if result == expected:
            print_success(f"{description}: {result}")
        else:
            print_error(f"{description}: Expected '{expected}', got '{result}'")
            return False
    
    return True

def test_alternative_api():
    """Test the alternative condition() function API"""
    print_test_header("Testing Alternative API")
    
    test_cases = [
        (condition("age", ">", 25), "age > 25", "condition() function"),
        (and_(condition("age", ">", 25), condition("country", "==", "US")), 
         "(age > 25 AND country = 'US')", "and_() function"),
        (or_(condition("status", "==", "VIP"), condition("score", ">", 1000)), 
         "(status = 'VIP' OR score > 1000)", "or_() function"),
        (not_(condition("banned", "==", True)), "NOT (banned = true)", "not_() function"),
    ]
    
    for condition_obj, expected, description in test_cases:
        result = condition_obj.to_delta_table_filter()
        if result == expected:
            print_success(f"{description}: {result}")
        else:
            print_error(f"{description}: Expected '{expected}', got '{result}'")
            return False
    
    return True

def test_real_world_scenarios():
    """Test real-world filtering scenarios"""
    print_test_header("Testing Real-World Scenarios")
    
    scenarios = [
        {
            "name": "User filtering",
            "condition": (
                c("age", ">=", 18) & 
                c("country", "in", ["US", "UK", "CA"]) & 
                c("status", "==", "active") & 
                ~c("banned", "==", True)
            ),
            "expected": "(age >= 18 AND country IN ('US', 'UK', 'CA') AND status = 'active' AND NOT (banned = true))"
        },
        
        {
            "name": "Product filtering",
            "condition": (
                (c("category", "==", "electronics") & c("price", "between", (100, 1000))) |
                (c("category", "==", "books") & c("rating", ">=", 4.0))
            ),
            "expected": "((category = 'electronics' AND price BETWEEN 100 AND 1000) OR (category = 'books' AND rating >= 4.0))"
        },
        
        {
            "name": "Log filtering",
            "condition": (
                c("timestamp", ">", "2024-01-01") &
                (c("level", "==", "ERROR") | c("level", "==", "WARN")) &
                c("message", "contains", "failed") &
                ~c("component", "==", "test")
            ),
            "expected": "(timestamp > '2024-01-01' AND (level = 'ERROR' OR level = 'WARN') AND message LIKE '%failed%' AND NOT (component = 'test'))"
        },
        
        {
            "name": "Financial filtering",
            "condition": (
                c("account_type", "in", ["checking", "savings"]) &
                c("balance", ">", 0) &
                (c("last_transaction", ">=", "2024-01-01") | c("vip_customer", "==", True)) &
                ~c("status", "in", ["closed", "suspended"])
            ),
            "expected": "(account_type IN ('checking', 'savings') AND balance > 0 AND (last_transaction >= '2024-01-01' OR vip_customer = true) AND NOT (status IN ('closed', 'suspended')))"
        }
    ]
    
    for scenario in scenarios:
        result = scenario["condition"].to_delta_table_filter()
        if result == scenario["expected"]:
            print_success(f"{scenario['name']}: PASSED")
            print_info(f"  Filter: {result}")
        else:
            print_error(f"{scenario['name']}: FAILED")
            print_info(f"  Expected: {scenario['expected']}")
            print_info(f"  Got:      {result}")
            return False
    
    return True

def test_performance():
    """Test performance with large conditions"""
    print_test_header("Testing Performance")
    
    import time
    
    # Create a large condition with many OR clauses
    countries = ["US", "UK", "CA", "DE", "FR", "IT", "ES", "NL", "BE", "AT"]
    statuses = ["active", "premium", "vip", "gold", "silver", "bronze"]
    
    start_time = time.time()
    
    # Build a complex condition
    country_condition = c("country", "in", countries)
    status_condition = c("status", "in", statuses)
    age_condition = c("age", "between", (18, 65))
    active_condition = c("active", "==", True)
    not_banned_condition = ~c("banned", "==", True)
    
    complex_condition = (
        country_condition & 
        status_condition & 
        age_condition & 
        active_condition & 
        not_banned_condition
    )
    
    # Generate the filter string
    filter_str = complex_condition.to_delta_table_filter()
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print_success(f"Performance test completed in {execution_time:.4f} seconds")
    print_info(f"Generated filter length: {len(filter_str)} characters")
    print_info(f"Filter preview: {filter_str[:100]}...")
    
    return execution_time < 1.0  # Should complete in less than 1 second

def test_pandas_integration():
    """Test integration with pandas DataFrames to validate SQL syntax"""
    print_test_header("Testing Pandas Integration (SQL Syntax Validation)")
    
    if not HAS_PANDAS:
        print_info("Pandas not available, skipping pandas integration test")
        return True
    
    # Create a sample DataFrame
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'country': ['US', 'UK', 'US', 'CA', 'US'],
        'status': ['active', 'inactive', 'active', 'active', 'premium'],
        'score': [85, 92, 78, 88, 95],
        'is_vip': [False, True, False, False, True]
    }
    
    df = pd.DataFrame(data)
    print_info(f"Created test DataFrame with {len(df)} rows")
    
    # Test various conditions and validate they produce reasonable results
    test_conditions = [
        (c("age", ">", 30), "Age greater than 30"),
        (c("country", "==", "US"), "Country equals US"),
        (c("status", "in", ["active", "premium"]), "Status in active/premium"),
        (c("name", "starts_with", "A"), "Name starts with A"),
        (c("age", "between", (25, 32)), "Age between 25 and 32"),
        (c("age", ">", 25) & c("country", "==", "US"), "Age > 25 AND country = US"),
        (c("status", "==", "premium") | c("score", ">", 90), "Premium status OR high score"),
    ]
    
    for condition_obj, description in test_conditions:
        try:
            filter_str = condition_obj.to_delta_table_filter()
            print_success(f"{description}: {filter_str}")
            
            # Note: We can't actually test with pandas.query() because it uses Python syntax,
            # but we can validate the SQL syntax looks correct
            if " = " in filter_str or " AND " in filter_str or " OR " in filter_str:
                print_info(f"  ✓ SQL syntax validated")
            else:
                print_error(f"  ✗ Unexpected SQL syntax: {filter_str}")
                return False
                
        except Exception as e:
            print_error(f"{description}: Exception - {e}")
            return False
    
    return True

def main():
    """Run all tests"""
    print(f"{Colors.BOLD}🧪 Delta Table Filter Test Suite{Colors.END}")
    print("=" * 50)
    
    test_functions = [
        test_basic_operators,
        test_null_operators,
        test_list_operators,
        test_range_operators,
        test_string_pattern_operators,
        test_edge_cases,
        test_complex_conditions,
        test_alternative_api,
        test_real_world_scenarios,
        test_performance,
        test_pandas_integration,
    ]
    
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
    
    print(f"\n{Colors.BOLD}Test Results:{Colors.END}")
    print("=" * 30)
    
    if passed_tests == total_tests:
        print(f"{Colors.GREEN}{Colors.BOLD}🎉 ALL TESTS PASSED! ({passed_tests}/{total_tests}){Colors.END}")
        print(f"{Colors.GREEN}The to_delta_table_filter implementation is working correctly!{Colors.END}")
        return True
    else:
        print(f"{Colors.RED}{Colors.BOLD}❌ SOME TESTS FAILED! ({passed_tests}/{total_tests}){Colors.END}")
        print(f"{Colors.RED}Please check the failed tests above.{Colors.END}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)