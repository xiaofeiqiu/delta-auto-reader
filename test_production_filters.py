#!/usr/bin/env python3
"""
Test script for the production-ready Enhanced Filter DSL.

This tests the new ConditionTuple class and c() helper function.
"""

import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from feature_store_sdk.filters import (
    Condition, AndCondition, OrCondition, NotCondition,
    ConditionTuple, FilterParser, condition, and_, or_, not_, c
)

def test_condition_tuple_creation():
    """Test ConditionTuple creation and basic functionality"""
    print("🧪 Testing ConditionTuple Creation...")
    
    # Create condition tuples using c() helper
    age_tuple = c("age", ">", 25)
    country_tuple = c("country", "==", "US")
    null_tuple = c("email", "is_not_null")
    
    print(f"✅ Age tuple: {age_tuple}")
    print(f"✅ Country tuple: {country_tuple}")
    print(f"✅ Null check tuple: {null_tuple}")
    
    # Verify they are ConditionTuple instances
    assert isinstance(age_tuple, ConditionTuple)
    assert isinstance(country_tuple, ConditionTuple)
    assert isinstance(null_tuple, ConditionTuple)
    
    print("✅ ConditionTuple creation test passed!\n")

def test_condition_tuple_operators():
    """Test logical operators with ConditionTuple"""
    print("🧪 Testing ConditionTuple Operators...")
    
    age_tuple = c("age", ">", 25)
    country_tuple = c("country", "==", "US")
    segment_tuple = c("segment", "==", "PREMIUM")
    status_tuple = c("status", "==", "BANNED")
    
    # Test AND operator
    and_result = age_tuple & country_tuple
    print(f"✅ AND operation: {and_result}")
    assert isinstance(and_result, AndCondition)
    
    # Test OR operator
    or_result = country_tuple | segment_tuple
    print(f"✅ OR operation: {or_result}")
    assert isinstance(or_result, OrCondition)
    
    # Test NOT operator
    not_result = ~status_tuple
    print(f"✅ NOT operation: {not_result}")
    assert isinstance(not_result, NotCondition)
    
    # Test complex combination
    complex_result = (age_tuple & country_tuple) | (segment_tuple & ~status_tuple)
    print(f"✅ Complex operation: {complex_result}")
    assert isinstance(complex_result, OrCondition)
    
    print("✅ ConditionTuple operators test passed!\n")

def test_mixed_operand_types():
    """Test mixing ConditionTuple with other condition types"""
    print("🧪 Testing Mixed Operand Types...")
    
    # Mix ConditionTuple with Condition objects
    tuple_cond = c("age", ">", 25)
    obj_cond = condition("country", "==", "US")
    
    # Test mixing different types
    mixed_and = tuple_cond & obj_cond
    mixed_or = obj_cond | tuple_cond
    
    print(f"✅ Mixed AND: {mixed_and}")
    print(f"✅ Mixed OR: {mixed_or}")
    
    assert isinstance(mixed_and, AndCondition)
    assert isinstance(mixed_or, OrCondition)
    
    # Test with regular tuples (should work via make_condition)
    regular_tuple = ("segment", "==", "PREMIUM")
    mixed_with_regular = tuple_cond & regular_tuple
    print(f"✅ Mixed with regular tuple: {mixed_with_regular}")
    assert isinstance(mixed_with_regular, AndCondition)
    
    print("✅ Mixed operand types test passed!\n")

def test_filter_parser_compatibility():
    """Test that FilterParser still works with the new approach"""
    print("🧪 Testing FilterParser Compatibility...")
    
    # Test parsing different input types
    test_cases = [
        # Regular tuples (legacy support)
        [("age", ">", 25), ("status", "==", "ACTIVE")],
        
        # ConditionTuple objects
        [c("age", ">", 25), c("status", "==", "ACTIVE")],
        
        # Mixed types
        [c("age", ">", 25), ("status", "==", "ACTIVE")],
        
        # Complex conditions with operators
        [c("age", ">", 25) & c("country", "==", "US")],
        
        # Single ConditionTuple
        c("age", ">", 30),
        
        # Single regular tuple
        ("country", "in", ["US", "UK"])
    ]
    
    for i, test_case in enumerate(test_cases):
        try:
            result = FilterParser.parse_where_conditions(test_case)
            print(f"✅ Test case {i+1}: {result}")
        except Exception as e:
            print(f"❌ Test case {i+1} failed: {e}")
            raise
    
    print("✅ FilterParser compatibility test passed!\n")

def test_real_world_scenarios():
    """Test production-ready real-world scenarios"""
    print("🧪 Testing Real-World Scenarios...")
    
    # Scenario 1: High-value customer targeting
    high_value_customers = (
        (c("age", ">", 30) & c("country", "==", "US")) |
        (c("segment", "==", "PREMIUM") & c("income_bracket", "==", "HIGH"))
    )
    print(f"✅ High-value customers: {high_value_customers}")
    
    # Scenario 2: Risk exclusion using ConditionTuple
    safe_users = ~(
        c("fraud_score", ">", 0.5) |
        (c("credit_score", "<", 500) & c("age", "<", 21))
    )
    print(f"✅ Safe users: {safe_users}")
    
    # Scenario 3: Marketing segmentation with mixed approaches
    marketing_target = (
        condition("status", "==", "ACTIVE") &
        (
            (c("age", "<", 35) & c("country", "==", "US")) |
            (c("country", "in", ["UK", "DE"]) & condition("segment", "==", "PREMIUM"))
        )
    )
    print(f"✅ Marketing target: {marketing_target}")
    
    # Scenario 4: Where clause for projection (production usage)
    where_clause = [
        c("age", ">", 25),
        c("country", "==", "US") | c("country", "==", "UK"),
        ~c("status", "==", "BANNED"),
        condition("email", "is_not_null")  # Mix with condition objects
    ]
    
    parsed_where = FilterParser.parse_where_conditions(where_clause)
    print(f"✅ Projection where clause: {parsed_where}")
    
    print("✅ Real-world scenarios test passed!\n")

def test_error_handling():
    """Test error handling in production scenarios"""
    print("🧪 Testing Error Handling...")
    
    # Test invalid operand types
    try:
        result = c("age", ">", 25) & "invalid"
        assert False, "Should have raised TypeError"
    except TypeError as e:
        print(f"✅ Caught invalid operand error: {e}")
    
    # Test invalid condition creation
    try:
        invalid_tuple = c("age", "invalid_op", 25)
        # This should raise when converted to condition
        invalid_tuple._to_condition()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        print(f"✅ Caught invalid operator error: {e}")
    
    print("✅ Error handling test passed!\n")

def test_backwards_compatibility():
    """Ensure backwards compatibility with existing code"""
    print("🧪 Testing Backwards Compatibility...")
    
    # Test that existing tuple-based syntax still works
    legacy_where = [
        ("age", ">", 25),
        ("status", "==", "ACTIVE"),
        ("country", "in", ["US", "UK"])
    ]
    
    parsed_legacy = FilterParser.parse_where_conditions(legacy_where)
    print(f"✅ Legacy syntax: {parsed_legacy}")
    assert isinstance(parsed_legacy, AndCondition)
    
    # Test that condition() function still works
    condition_obj = condition("age", ">", 25)
    print(f"✅ Condition function: {condition_obj}")
    assert isinstance(condition_obj, Condition)
    
    # Test functional API
    functional_result = and_(
        condition("age", ">", 25),
        or_(
            condition("country", "==", "US"),
            condition("segment", "==", "PREMIUM")
        )
    )
    print(f"✅ Functional API: {functional_result}")
    assert isinstance(functional_result, AndCondition)
    
    print("✅ Backwards compatibility test passed!\n")

def main():
    """Run all production tests"""
    print("🚀 Testing Production-Ready Enhanced Filter DSL")
    print("=" * 60)
    
    test_condition_tuple_creation()
    test_condition_tuple_operators()
    test_mixed_operand_types()
    test_filter_parser_compatibility()
    test_real_world_scenarios()
    test_error_handling()
    test_backwards_compatibility()
    
    print("🎉 All production tests passed! The Enhanced Filter DSL is production-ready.")
    print("\n📚 Production Usage Guide:")
    print("# Recommended approach using c() helper")
    print('from feature_store_sdk.filters import c')
    print('where = [')
    print('    c("age", ">", 25) & c("country", "==", "US"),')
    print('    c("segment", "==", "PREMIUM") | c("income", ">", 50000),')
    print('    ~c("status", "==", "BANNED")')
    print(']')
    print()
    print("# Alternative: Direct condition objects")
    print('from feature_store_sdk.filters import condition')
    print('where = [')
    print('    condition("age", ">", 25) & condition("country", "==", "US")')
    print(']')
    print()
    print("# Backwards compatible: Legacy tuples")
    print('where = [("age", ">", 25), ("status", "==", "ACTIVE")]')

if __name__ == "__main__":
    main()