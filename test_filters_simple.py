"""
Simple test script for the Enhanced Filter DSL without external dependencies.

This tests the core condition building and logical operators.
"""

import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from feature_store_sdk.filters import (
    Condition, AndCondition, OrCondition, NotCondition,
    FilterParser, condition, and_, or_, not_
)

def test_condition_creation():
    """Test basic condition creation"""
    print("🧪 Testing Condition Creation...")
    
    # Basic condition
    age_cond = Condition("age", ">", 25)
    print(f"✅ Age condition: {age_cond}")
    
    # In condition
    country_cond = Condition("country", "in", ["US", "UK"])
    print(f"✅ Country condition: {country_cond}")
    
    # Null check
    email_cond = Condition("email", "is_not_null")
    print(f"✅ Email condition: {email_cond}")
    
    # Between condition
    income_cond = Condition("income", "between", [50000, 100000])
    print(f"✅ Income condition: {income_cond}")
    
    print("✅ Condition creation test passed!\n")

def test_logical_operators():
    """Test logical operators (&, |, ~)"""
    print("🧪 Testing Logical Operators...")
    
    age_cond = Condition("age", ">", 25)
    country_cond = Condition("country", "==", "US")
    segment_cond = Condition("segment", "==", "PREMIUM")
    status_cond = Condition("status", "==", "BANNED")
    
    # AND operator
    and_cond = age_cond & country_cond
    print(f"✅ AND condition: {and_cond}")
    assert isinstance(and_cond, AndCondition)
    
    # OR operator
    or_cond = country_cond | segment_cond
    print(f"✅ OR condition: {or_cond}")
    assert isinstance(or_cond, OrCondition)
    
    # NOT operator
    not_cond = ~status_cond
    print(f"✅ NOT condition: {not_cond}")
    assert isinstance(not_cond, NotCondition)
    
    # Complex combination
    complex_cond = (age_cond & country_cond) | (segment_cond & ~status_cond)
    print(f"✅ Complex condition: {complex_cond}")
    
    print("✅ Logical operators test passed!\n")

def test_convenience_functions():
    """Test convenience functions"""
    print("🧪 Testing Convenience Functions...")
    
    # Basic conditions using functions
    cond1 = condition("age", ">", 25)
    cond2 = condition("country", "==", "US")
    cond3 = condition("segment", "==", "PREMIUM")
    
    # Using and_ function
    and_result = and_(cond1, cond2, cond3)
    print(f"✅ and_() function: {and_result}")
    assert isinstance(and_result, AndCondition)
    assert len(and_result.conditions) == 3
    
    # Using or_ function
    or_result = or_(cond1, cond2)
    print(f"✅ or_() function: {or_result}")
    assert isinstance(or_result, OrCondition)
    
    # Using not_ function
    not_result = not_(cond1)
    print(f"✅ not_() function: {not_result}")
    assert isinstance(not_result, NotCondition)
    
    print("✅ Convenience functions test passed!\n")

def test_filter_parser():
    """Test FilterParser functionality"""
    print("🧪 Testing Filter Parser...")
    
    # Parse list of tuples
    where_list = [
        ("age", ">", 25),
        ("status", "==", "ACTIVE")
    ]
    parsed = FilterParser.parse_where_conditions(where_list)
    print(f"✅ Parsed list: {parsed}")
    assert isinstance(parsed, AndCondition)
    
    # Parse single tuple
    where_tuple = ("country", "in", ["US", "UK"])
    parsed = FilterParser.parse_where_conditions(where_tuple)
    print(f"✅ Parsed tuple: {parsed}")
    assert isinstance(parsed, Condition)
    
    # Parse None
    parsed = FilterParser.parse_where_conditions(None)
    print(f"✅ Parsed None: {parsed}")
    assert parsed is None
    
    # Parse existing condition
    existing = condition("age", ">", 30)
    parsed = FilterParser.parse_where_conditions(existing)
    print(f"✅ Parsed existing: {parsed}")
    assert parsed is existing
    
    print("✅ Filter parser test passed!\n")

def test_complex_scenarios():
    """Test complex real-world scenarios"""
    print("🧪 Testing Complex Scenarios...")
    
    # High-value customer filter
    # (age > 30 AND country = US) OR (segment = PREMIUM AND income = HIGH)
    high_value = (
        (condition("age", ">", 30) & condition("country", "==", "US")) |
        (condition("segment", "==", "PREMIUM") & condition("income_bracket", "==", "HIGH"))
    )
    print(f"✅ High-value customers: {high_value}")
    
    # Risk exclusion filter
    # NOT (fraud_score > 0.5 OR (credit_score < 500 AND age < 21))
    safe_users = ~(
        condition("fraud_score", ">", 0.5) |
        (condition("credit_score", "<", 500) & condition("age", "<", 21))
    )
    print(f"✅ Safe users: {safe_users}")
    
    # Marketing target
    # Active AND ((young US users) OR (European premium users))
    marketing_target = (
        condition("status", "==", "ACTIVE") &
        (
            (condition("age", "<", 35) & condition("country", "==", "US")) |
            (condition("country", "in", ["UK", "DE"]) & condition("segment", "==", "PREMIUM"))
        )
    )
    print(f"✅ Marketing target: {marketing_target}")
    
    print("✅ Complex scenarios test passed!\n")

def test_error_handling():
    """Test error handling"""
    print("🧪 Testing Error Handling...")
    
    # Invalid operator
    try:
        Condition("age", "invalid_op", 25)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        print(f"✅ Caught invalid operator error: {e}")
    
    # Missing value for operator that requires it
    try:
        Condition("age", ">", None)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        print(f"✅ Caught missing value error: {e}")
    
    # Valid null check (no value needed)
    try:
        null_cond = Condition("email", "is_null")
        print(f"✅ Valid null condition: {null_cond}")
    except ValueError:
        assert False, "Should not raise error for null check"
    
    # Invalid where format in parser
    try:
        FilterParser.parse_where_conditions({"invalid": "format"})
        assert False, "Should have raised ValueError"
    except ValueError as e:
        print(f"✅ Caught invalid format error: {e}")
    
    print("✅ Error handling test passed!\n")

def test_operator_precedence():
    """Test that operator precedence works as expected"""
    print("🧪 Testing Operator Precedence...")
    
    # Test that parentheses work correctly
    cond1 = condition("age", ">", 25)
    cond2 = condition("country", "==", "US")
    cond3 = condition("segment", "==", "PREMIUM")
    
    # (cond1 & cond2) | cond3
    expr1 = (cond1 & cond2) | cond3
    print(f"✅ (age > 25 AND country = US) OR segment = PREMIUM: {expr1}")
    
    # cond1 & (cond2 | cond3)
    expr2 = cond1 & (cond2 | cond3)
    print(f"✅ age > 25 AND (country = US OR segment = PREMIUM): {expr2}")
    
    # Verify they're different structures
    assert isinstance(expr1, OrCondition)
    assert isinstance(expr2, AndCondition)
    
    print("✅ Operator precedence test passed!\n")

def main():
    """Run all tests"""
    print("🚀 Testing Enhanced Filter DSL (Core Logic)")
    print("=" * 50)
    
    test_condition_creation()
    test_logical_operators()
    test_convenience_functions()
    test_filter_parser()
    test_complex_scenarios()
    test_error_handling()
    test_operator_precedence()
    
    print("🎉 All core tests passed! Enhanced Filter DSL is working correctly.")
    print("\n📚 Quick Usage Guide:")
    print("# Simple conditions")
    print('condition("age", ">", 25)')
    print('condition("country", "in", ["US", "UK"])')
    print('condition("email", "is_not_null")')
    print()
    print("# Logical combinations")
    print('condition("age", ">", 25) & condition("country", "==", "US")  # AND')
    print('condition("country", "==", "US") | condition("segment", "==", "PREMIUM")  # OR')
    print('~condition("status", "==", "BANNED")  # NOT')
    print()
    print("# Complex expressions")
    print('(condition("age", ">", 25) & condition("country", "==", "US")) | condition("segment", "==", "PREMIUM")')
    print()
    print("# In projection where clause:")
    print('where=[')
    print('    condition("age", ">", 25),')
    print('    condition("country", "==", "US") | condition("country", "==", "UK"),')
    print('    ~condition("status", "==", "BANNED")')
    print(']')

if __name__ == "__main__":
    main()