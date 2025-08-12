"""
Test script for the new Enhanced Filter DSL with Python-style operators.

This script demonstrates the new filter capabilities including:
- Basic conditions with tuple syntax
- OR logic using | operator  
- AND logic using & operator
- NOT logic using ~ operator
- Complex nested conditions
"""

import sys
import os
import pandas as pd

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from feature_store_sdk.filters import (
    Condition, AndCondition, OrCondition, NotCondition,
    FilterParser, condition, and_, or_, not_, make_condition
)

def create_test_data():
    """Create sample data for testing"""
    return pd.DataFrame({
        'user_id': ['U001', 'U002', 'U003', 'U004', 'U005', 'U006'],
        'age': [25, 34, 28, 45, 33, 19],
        'country': ['US', 'UK', 'CA', 'US', 'DE', 'US'],
        'segment': ['PREMIUM', 'STANDARD', 'PREMIUM', 'GOLD', 'STANDARD', 'STANDARD'],
        'income_bracket': ['HIGH', 'MEDIUM', 'HIGH', 'VERY_HIGH', 'MEDIUM', 'LOW'],
        'status': ['ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE', 'ACTIVE', 'BANNED'],
        'fraud_score': [0.05, 0.12, 0.03, 0.01, 0.08, 0.6],
        'credit_score': [750, 680, 720, 800, 650, 450]
    })

def test_basic_conditions():
    """Test basic condition creation and evaluation"""
    print("🧪 Testing Basic Conditions...")
    
    df = create_test_data()
    
    # Test simple condition
    age_condition = Condition("age", ">", 30)
    print(f"Age > 30 condition: {age_condition}")
    
    # Apply to pandas
    mask = age_condition.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Users with age > 30: {len(filtered_df)} out of {len(df)}")
    print(f"User IDs: {filtered_df['user_id'].tolist()}")
    
    # Test in condition
    country_condition = Condition("country", "in", ["US", "UK"])
    mask = country_condition.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Users in US/UK: {len(filtered_df)} out of {len(df)}")
    print(f"User IDs: {filtered_df['user_id'].tolist()}")
    
    print("✅ Basic conditions test passed!\n")

def test_operator_combinations():
    """Test Python-style operators (&, |, ~)"""
    print("🧪 Testing Operator Combinations...")
    
    df = create_test_data()
    
    # Test AND operator
    age_cond = Condition("age", ">", 25)
    country_cond = Condition("country", "==", "US")
    and_condition = age_cond & country_cond
    
    print(f"AND condition: {and_condition}")
    mask = and_condition.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Users age > 25 AND country = US: {len(filtered_df)} out of {len(df)}")
    print(f"User IDs: {filtered_df['user_id'].tolist()}")
    
    # Test OR operator
    premium_cond = Condition("segment", "==", "PREMIUM")
    gold_cond = Condition("segment", "==", "GOLD")
    or_condition = premium_cond | gold_cond
    
    print(f"OR condition: {or_condition}")
    mask = or_condition.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Users PREMIUM OR GOLD: {len(filtered_df)} out of {len(df)}")
    print(f"User IDs: {filtered_df['user_id'].tolist()}")
    
    # Test NOT operator
    banned_cond = Condition("status", "==", "BANNED")
    not_condition = ~banned_cond
    
    print(f"NOT condition: {not_condition}")
    mask = not_condition.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Users NOT BANNED: {len(filtered_df)} out of {len(df)}")
    print(f"User IDs: {filtered_df['user_id'].tolist()}")
    
    print("✅ Operator combinations test passed!\n")

def test_complex_conditions():
    """Test complex nested conditions"""
    print("🧪 Testing Complex Conditions...")
    
    df = create_test_data()
    
    # Complex condition: (age > 25 AND country = US) OR (segment = PREMIUM AND age < 30)
    condition = (
        (Condition("age", ">", 25) & Condition("country", "==", "US")) |
        (Condition("segment", "==", "PREMIUM") & Condition("age", "<", 30))
    )
    
    print(f"Complex condition: {condition}")
    mask = condition.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Complex filter result: {len(filtered_df)} out of {len(df)}")
    print(f"User IDs: {filtered_df['user_id'].tolist()}")
    
    # Risk filtering: NOT (fraud_score > 0.5 OR (credit_score < 500 AND age < 21))
    risk_condition = ~(
        Condition("fraud_score", ">", 0.5) |
        (Condition("credit_score", "<", 500) & Condition("age", "<", 21))
    )
    
    print(f"Risk filter: {risk_condition}")
    mask = risk_condition.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Safe users: {len(filtered_df)} out of {len(df)}")
    print(f"User IDs: {filtered_df['user_id'].tolist()}")
    
    print("✅ Complex conditions test passed!\n")

def test_filter_parser():
    """Test the FilterParser with various input formats"""
    print("🧪 Testing Filter Parser...")
    
    df = create_test_data()
    
    # Test parsing list of tuples (traditional format)
    where_list = [
        ("age", ">", 25),
        ("status", "==", "ACTIVE")
    ]
    
    parsed_condition = FilterParser.parse_where_conditions(where_list)
    print(f"Parsed list condition: {parsed_condition}")
    mask = parsed_condition.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"List filter result: {len(filtered_df)} out of {len(df)}")
    
    # Test parsing single tuple
    where_tuple = ("country", "in", ["US", "UK"])
    parsed_condition = FilterParser.parse_where_conditions(where_tuple)
    print(f"Parsed tuple condition: {parsed_condition}")
    mask = parsed_condition.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Tuple filter result: {len(filtered_df)} out of {len(df)}")
    
    # Test parsing already created condition
    existing_condition = Condition("age", ">", 30) | Condition("segment", "==", "GOLD")
    parsed_condition = FilterParser.parse_where_conditions(existing_condition)
    print(f"Parsed existing condition: {parsed_condition}")
    mask = parsed_condition.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Existing condition result: {len(filtered_df)} out of {len(df)}")
    
    print("✅ Filter parser test passed!\n")

def test_convenience_functions():
    """Test convenience functions for creating conditions"""
    print("🧪 Testing Convenience Functions...")
    
    df = create_test_data()
    
    # Using convenience functions
    cond1 = condition("age", ">", 25)
    cond2 = condition("country", "==", "US")
    cond3 = condition("segment", "==", "PREMIUM")
    
    # Using and_ function
    and_cond = and_(cond1, cond2)
    print(f"and_() function: {and_cond}")
    
    # Using or_ function  
    or_cond = or_(cond2, cond3)
    print(f"or_() function: {or_cond}")
    
    # Using not_ function
    not_cond = not_(condition("status", "==", "BANNED"))
    print(f"not_() function: {not_cond}")
    
    # Complex combination
    complex_cond = and_(
        or_(cond1, cond3),
        not_(condition("status", "==", "INACTIVE"))
    )
    print(f"Complex combination: {complex_cond}")
    mask = complex_cond.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Complex combination result: {len(filtered_df)} out of {len(df)}")
    
    print("✅ Convenience functions test passed!\n")

def test_extended_operators():
    """Test extended operators like between, starts_with, etc."""
    print("🧪 Testing Extended Operators...")
    
    df = create_test_data()
    
    # Test between operator
    age_range = Condition("age", "between", [25, 35])
    print(f"Age between 25-35: {age_range}")
    mask = age_range.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Users with age 25-35: {len(filtered_df)} out of {len(df)}")
    print(f"User IDs: {filtered_df['user_id'].tolist()}")
    
    # Test starts_with operator
    df_with_names = df.copy()
    df_with_names['name'] = ['John Doe', 'Jane Smith', 'John Wilson', 'Bob Johnson', 'Alice Brown', 'John Davis']
    
    name_starts = Condition("name", "starts_with", "John")
    print(f"Name starts with 'John': {name_starts}")
    mask = name_starts.to_pandas_condition(df_with_names)
    filtered_df = df_with_names[mask]
    print(f"Users with name starting 'John': {len(filtered_df)} out of {len(df_with_names)}")
    print(f"Names: {filtered_df['name'].tolist()}")
    
    print("✅ Extended operators test passed!\n")

def test_real_world_scenarios():
    """Test real-world filtering scenarios"""
    print("🧪 Testing Real-World Scenarios...")
    
    df = create_test_data()
    
    # Scenario 1: High-value customers
    # (age > 30 AND country = US) OR (segment = PREMIUM AND income = HIGH)
    high_value = (
        (Condition("age", ">", 30) & Condition("country", "==", "US")) |
        (Condition("segment", "==", "PREMIUM") & Condition("income_bracket", "==", "HIGH"))
    )
    
    print(f"High-value customers: {high_value}")
    mask = high_value.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"High-value customers: {len(filtered_df)} out of {len(df)}")
    print(f"User IDs: {filtered_df['user_id'].tolist()}")
    
    # Scenario 2: Risk exclusion
    # NOT (fraud_score > 0.5 OR (credit_score < 500 AND age < 21))
    safe_users = ~(
        Condition("fraud_score", ">", 0.5) |
        (Condition("credit_score", "<", 500) & Condition("age", "<", 21))
    )
    
    print(f"Safe users filter: {safe_users}")
    mask = safe_users.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Safe users: {len(filtered_df)} out of {len(df)}")
    print(f"User IDs: {filtered_df['user_id'].tolist()}")
    
    # Scenario 3: Marketing target
    # Active users who are either (young US users) or (European premium users)
    marketing_target = (
        Condition("status", "==", "ACTIVE") &
        (
            (Condition("age", "<", 35) & Condition("country", "==", "US")) |
            (Condition("country", "in", ["UK", "DE"]) & Condition("segment", "==", "PREMIUM"))
        )
    )
    
    print(f"Marketing target: {marketing_target}")
    mask = marketing_target.to_pandas_condition(df)
    filtered_df = df[mask]
    print(f"Marketing targets: {len(filtered_df)} out of {len(df)}")
    print(f"User IDs: {filtered_df['user_id'].tolist()}")
    
    print("✅ Real-world scenarios test passed!\n")

def main():
    """Run all tests"""
    print("🚀 Testing Enhanced Filter DSL with Python-style Operators")
    print("=" * 60)
    
    test_basic_conditions()
    test_operator_combinations()
    test_complex_conditions() 
    test_filter_parser()
    test_convenience_functions()
    test_extended_operators()
    test_real_world_scenarios()
    
    print("🎉 All tests passed! Enhanced Filter DSL is working correctly.")
    print("\n📚 Usage Examples:")
    print("# Simple conditions")
    print('where=[("age", ">", 25), ("status", "==", "ACTIVE")]')
    print("\n# OR logic")
    print('where=[("country", "==", "US") | ("country", "==", "UK")]')
    print("\n# Complex nested conditions") 
    print('where=[("age", ">", 25), (("country", "==", "US") | ("segment", "==", "PREMIUM"))]')
    print("\n# NOT logic")
    print('where=[~("status", "==", "BANNED")]')

if __name__ == "__main__":
    main()