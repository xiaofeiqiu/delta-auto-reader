#!/usr/bin/env python3
"""
Test cases for ConditionTuple serialize/deserialize functionality.

This module tests the new string-based serialization and deserialization
methods for ConditionTuple objects.
"""

from feature_store_sdk.filters import ConditionTuple, c


def test_basic_serialization():
    """Test basic serialization of ConditionTuple objects."""
    # Test 3-argument condition
    ct1 = c("age", ">", 25)
    serialized = ct1.serialize()
    assert serialized == "('age', '>', 25)"
    
    # Test string value
    ct2 = c("name", "==", "John")
    serialized = ct2.serialize()
    assert serialized == "('name', '==', 'John')"
    
    # Test null check (2 arguments)
    ct3 = c("email", "is_null")
    serialized = ct3.serialize()
    assert serialized == "('email', 'is_null')"


def test_basic_deserialization():
    """Test basic deserialization of ConditionTuple objects."""
    # Test 3-argument condition
    ct1 = ConditionTuple.deserialize("('age', '>', 25)")
    assert len(ct1) == 3
    assert ct1[0] == "age"
    assert ct1[1] == ">"
    assert ct1[2] == 25
    
    # Test string value
    ct2 = ConditionTuple.deserialize("('name', '==', 'John')")
    assert len(ct2) == 3
    assert ct2[0] == "name"
    assert ct2[1] == "=="
    assert ct2[2] == "John"
    
    # Test null check (2 arguments)
    ct3 = ConditionTuple.deserialize("('email', 'is_null')")
    assert len(ct3) == 2
    assert ct3[0] == "email"
    assert ct3[1] == "is_null"


def test_round_trip_serialization():
    """Test that serialize/deserialize is a perfect round trip."""
    test_cases = [
        c("age", ">", 25),
        c("name", "==", "John Doe"),
        c("email", "is_null"),
        c("status", "is_not_null"),
        c("score", ">=", 90.5),
        c("category", "in", ["A", "B", "C"]),
        c("count", "between", [1, 100]),
    ]
    
    for original in test_cases:
        # Serialize then deserialize
        serialized = original.serialize()
        deserialized = ConditionTuple.deserialize(serialized)
        
        # Should be equal
        assert original == deserialized
        assert len(original) == len(deserialized)
        for i in range(len(original)):
            assert original[i] == deserialized[i]


def test_complex_values():
    """Test serialization/deserialization with complex values."""
    # List values
    ct1 = c("status", "in", ["ACTIVE", "PENDING"])
    serialized = ct1.serialize()
    deserialized = ConditionTuple.deserialize(serialized)
    assert deserialized == ct1
    
    # Tuple values (for between operator)
    ct2 = c("age", "between", (18, 65))
    serialized = ct2.serialize()
    deserialized = ConditionTuple.deserialize(serialized)
    assert deserialized == ct2
    
    # Numeric values
    ct3 = c("price", "<=", 99.99)
    serialized = ct3.serialize()
    deserialized = ConditionTuple.deserialize(serialized)
    assert deserialized == ct3
    
    # Boolean values
    ct4 = c("is_active", "==", True)
    serialized = ct4.serialize()
    deserialized = ConditionTuple.deserialize(serialized)
    assert deserialized == ct4


def test_edge_cases():
    """Test edge cases and special characters."""
    # Empty string value
    ct1 = c("description", "==", "")
    serialized = ct1.serialize()
    deserialized = ConditionTuple.deserialize(serialized)
    assert deserialized == ct1
    
    # String with quotes
    ct2 = c("text", "contains", "It's a 'test'")
    serialized = ct2.serialize()
    deserialized = ConditionTuple.deserialize(serialized)
    assert deserialized == ct2
    
    # String with special characters
    ct3 = c("path", "starts_with", "/usr/local/bin")
    serialized = ct3.serialize()
    deserialized = ConditionTuple.deserialize(serialized)
    assert deserialized == ct3
    
    # None value (should work for certain operators)
    ct4 = ConditionTuple("optional_field", "==", None)
    serialized = ct4.serialize()
    deserialized = ConditionTuple.deserialize(serialized)
    assert deserialized == ct4


def test_deserialization_error_cases():
    """Test error handling in deserialization."""
    # Empty string
    try:
        ConditionTuple.deserialize("")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Empty string cannot be deserialized" in str(e)
    
    # Invalid format - missing parentheses
    try:
        ConditionTuple.deserialize("'age', '>', 25")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Invalid ConditionTuple string format" in str(e)
    
    # Invalid format - not matching pattern
    try:
        ConditionTuple.deserialize("invalid_format")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Invalid ConditionTuple string format" in str(e)
    
    # Invalid syntax inside parentheses
    try:
        ConditionTuple.deserialize("('age', '>', 25])")  # Mismatched brackets
        assert False, "Should have raised ValueError"  
    except ValueError as e:
        assert "Failed to parse ConditionTuple arguments" in str(e)
    
    # Malformed arguments
    try:
        ConditionTuple.deserialize("(age, >, 25)")  # Missing quotes
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Failed to parse ConditionTuple arguments" in str(e)


def test_whitespace_handling():
    """Test that deserialization handles whitespace correctly."""
    # Extra spaces around the string
    ct1 = ConditionTuple.deserialize("  ('age', '>', 25)  ")
    assert ct1 == c("age", ">", 25)
    
    # Spaces inside the arguments (should still work with literal_eval)
    ct2 = ConditionTuple.deserialize("( 'name' , '==' , 'John' )")
    assert ct2 == c("name", "==", "John")


def test_comparison_with_to_dict():
    """Test that serialize method works differently from to_dict."""
    ct = c("age", ">", 25)
    
    # String serialization
    string_serialized = ct.serialize()
    assert isinstance(string_serialized, str)
    assert string_serialized.startswith("(")
    
    # Dict serialization (existing method)
    dict_serialized = ct.to_dict()
    assert isinstance(dict_serialized, dict)
    assert "type" in dict_serialized
    
    # They should be different
    assert string_serialized != str(dict_serialized)


def test_functional_equivalence():
    """Test that deserialized ConditionTuple works the same as original."""
    original = c("age", ">", 25)
    deserialized = ConditionTuple.deserialize(original.serialize())
    
    # Should work the same in logical operations
    condition1 = original & c("status", "==", "ACTIVE")
    condition2 = deserialized & c("status", "==", "ACTIVE")
    
    # Both should be AndCondition objects with same structure
    assert type(condition1) == type(condition2)
    assert str(condition1) == str(condition2)
    
    # Should work the same when converted to actual conditions
    original_condition = original._to_condition()
    deserialized_condition = deserialized._to_condition()
    
    assert type(original_condition) == type(deserialized_condition)
    assert str(original_condition) == str(deserialized_condition)


def test_nested_conditions():
    """Test serialization/deserialization with nested conditions and logical operations."""
    # Test AND condition
    ct1 = c("age", ">", 25)
    ct2 = c("status", "==", "ACTIVE")
    and_condition = ct1 & ct2
    
    # Individual tuples should serialize/deserialize properly
    ct1_serialized = ct1.serialize()
    ct1_deserialized = ConditionTuple.deserialize(ct1_serialized)
    assert ct1 == ct1_deserialized
    
    ct2_serialized = ct2.serialize()
    ct2_deserialized = ConditionTuple.deserialize(ct2_serialized)
    assert ct2 == ct2_deserialized
    
    # Recreate the AND condition with deserialized tuples
    recreated_and = ct1_deserialized & ct2_deserialized
    assert str(and_condition) == str(recreated_and)


def test_complex_nested_scenarios():
    """Test complex scenarios with multiple levels of nesting."""
    # Create complex nested condition: (age > 25 AND status == "ACTIVE") OR (country == "US" AND NOT banned)
    base1 = c("age", ">", 25)
    base2 = c("status", "==", "ACTIVE")
    base3 = c("country", "==", "US")
    base4 = c("banned", "==", True)
    
    # Create nested structure
    left_side = base1 & base2  # (age > 25 AND status == "ACTIVE")
    right_side = base3 & (~base4)  # (country == "US" AND NOT banned)
    complex_condition = left_side | right_side  # Combined with OR
    
    # Test that all individual parts can be serialized/deserialized
    test_cases = [base1, base2, base3, base4]
    
    for original in test_cases:
        serialized = original.serialize()
        deserialized = ConditionTuple.deserialize(serialized)
        assert original == deserialized
        
        # Test that they work the same in logical operations
        test_and = original & c("test", "==", "value")
        test_and_recreated = deserialized & c("test", "==", "value")
        assert str(test_and) == str(test_and_recreated)


def test_all_operators_serialization():
    """Test serialization with all supported operators."""
    operator_test_cases = [
        c("age", "==", 25),
        c("age", "!=", 25), 
        c("age", ">", 25),
        c("age", ">=", 25),
        c("age", "<", 25),
        c("age", "<=", 25),
        c("status", "in", ["ACTIVE", "PENDING"]),
        c("status", "not_in", ["BANNED", "INACTIVE"]),
        c("email", "is_null"),
        c("email", "is_not_null"),
        c("age", "between", (18, 65)),
        c("name", "starts_with", "John"),
        c("name", "ends_with", "Smith"),
        c("description", "contains", "test"),
    ]
    
    for original in operator_test_cases:
        # Test round trip
        serialized = original.serialize()
        deserialized = ConditionTuple.deserialize(serialized)
        assert original == deserialized
        
        # Test that deserialized version works in conditions
        condition_original = original._to_condition()
        condition_deserialized = deserialized._to_condition()
        
        assert str(condition_original) == str(condition_deserialized)
        assert type(condition_original) == type(condition_deserialized)


def test_chained_operations_serialization():
    """Test serialization with chained logical operations."""
    # Create a chain: ct1 & ct2 | ct3 & ~ct4
    ct1 = c("age", ">", 18)
    ct2 = c("age", "<", 65)  
    ct3 = c("country", "==", "US")
    ct4 = c("status", "==", "BANNED")
    
    # Test individual serialization
    tuples = [ct1, ct2, ct3, ct4]
    deserialized_tuples = []
    
    for ct in tuples:
        serialized = ct.serialize()
        deserialized = ConditionTuple.deserialize(serialized)
        deserialized_tuples.append(deserialized)
        assert ct == deserialized
    
    # Create the same complex condition with both original and deserialized
    original_complex = ct1 & ct2 | ct3 & ~ct4
    deserialized_complex = ConditionTuple.deserialize(original_complex.serialize())

    assert str(original_complex) == str(deserialized_complex)
    assert type(original_complex) == type(deserialized_complex)


def test_null_scenarios_comprehensive():
    """Test comprehensive null and not_null scenarios."""
    # Test all variations of is_null operator
    null_test_cases = [
        # Different column names
        c("email", "is_null"),
        c("phone", "is_null"),
        c("address", "is_null"),
        c("optional_field", "is_null"),
        c("nullable_column", "is_null"),
        
        # Different column name types (if they contain special chars)
        c("user_email", "is_null"),
        c("contact.phone", "is_null"),
        c("data_field_123", "is_null"),
    ]
    
    # Test all variations of is_not_null operator
    not_null_test_cases = [
        # Different column names
        c("email", "is_not_null"),
        c("phone", "is_not_null"), 
        c("address", "is_not_null"),
        c("required_field", "is_not_null"),
        c("primary_key", "is_not_null"),
        
        # Different column name types
        c("user_email", "is_not_null"),
        c("contact.phone", "is_not_null"),
        c("data_field_123", "is_not_null"),
    ]
    
    print("Testing is_null scenarios...")
    for original in null_test_cases:
        # Test serialization format
        serialized = original.serialize()
        expected_format = f"('{original[0]}', 'is_null')"
        assert serialized == expected_format, f"Expected {expected_format}, got {serialized}"
        
        # Test round-trip
        deserialized = ConditionTuple.deserialize(serialized)
        assert original == deserialized
        assert len(deserialized) == 2  # Should only have column and operator
        assert deserialized[0] == original[0]  # Column name preserved
        assert deserialized[1] == "is_null"    # Operator preserved
        
        # Test that it works in logical operations
        combined = original & c("status", "==", "ACTIVE")
        assert "is_null" in str(combined)
    
    print("Testing is_not_null scenarios...")
    for original in not_null_test_cases:
        # Test serialization format
        serialized = original.serialize()
        expected_format = f"('{original[0]}', 'is_not_null')"
        assert serialized == expected_format, f"Expected {expected_format}, got {serialized}"
        
        # Test round-trip
        deserialized = ConditionTuple.deserialize(serialized)
        assert original == deserialized
        assert len(deserialized) == 2  # Should only have column and operator
        assert deserialized[0] == original[0]      # Column name preserved
        assert deserialized[1] == "is_not_null"   # Operator preserved
        
        # Test that it works in logical operations
        combined = original | c("backup_field", "is_null")
        assert "is_not_null" in str(combined)
    
    # Test mixed null scenarios in complex conditions
    print("Testing mixed null scenarios...")
    mixed_conditions = [
        c("email", "is_null") & c("phone", "is_not_null"),
        c("primary_email", "is_not_null") | c("secondary_email", "is_not_null"),
        ~c("required_field", "is_null"),
        c("optional_field", "is_null") & ~c("backup_field", "is_null"),
    ]
    
    for complex_condition in mixed_conditions:
        # Test that complex conditions with null operators can be serialized/deserialized
        serialized = complex_condition.serialize()
        deserialized = ConditionTuple.deserialize(serialized)
        assert str(complex_condition) == str(deserialized)
        assert type(complex_condition) == type(deserialized)
    
    print("All null scenario tests passed!")


def test_serialization_with_special_values():
    """Test serialization with special Python values."""
    special_cases = [
        c("value", "==", None),
        c("flag", "==", True),
        c("flag", "==", False),
        c("count", "==", 0),
        c("score", "==", -1.5),
        c("data", "==", [1, 2, 3]),
        c("mapping", "==", {"key": "value"}),
        c("range", "between", (0, 100)),
        c("text", "==", ""),  # Empty string
        c("unicode", "contains", "café"),  # Unicode
        c("quotes", "==", 'He said "Hello"'),  # Mixed quotes
        c("path", "starts_with", "/usr/local/bin"),  # Special chars
    ]
    
    for original in special_cases:
        serialized = original.serialize()
        deserialized = ConditionTuple.deserialize(serialized)
        assert original == deserialized
        
        # Verify the types and values are preserved
        for i in range(len(original)):
            assert type(original[i]) == type(deserialized[i])
            assert original[i] == deserialized[i]


if __name__ == "__main__":
    # Run basic tests
    print("Running ConditionTuple serialization tests...")
    
    try:
        test_basic_serialization()
        print("✓ Basic serialization tests passed")
        
        test_basic_deserialization()
        print("✓ Basic deserialization tests passed")
        
        test_round_trip_serialization()
        print("✓ Round trip serialization tests passed")
        
        test_complex_values()
        print("✓ Complex values tests passed")
        
        test_edge_cases()
        print("✓ Edge cases tests passed")
        
        test_deserialization_error_cases()
        print("✓ Error cases tests passed")
        
        test_whitespace_handling()
        print("✓ Whitespace handling tests passed")
        
        test_comparison_with_to_dict()
        print("✓ Comparison with to_dict tests passed")
        
        test_functional_equivalence()
        print("✓ Functional equivalence tests passed")
        
        test_nested_conditions()
        print("✓ Nested conditions tests passed")
        
        test_complex_nested_scenarios()
        print("✓ Complex nested scenarios tests passed")
        
        test_all_operators_serialization()
        print("✓ All operators serialization tests passed")
        
        test_chained_operations_serialization()
        print("✓ Chained operations serialization tests passed")
        
        test_null_scenarios_comprehensive()
        print("✓ Comprehensive null scenarios tests passed")
        
        test_serialization_with_special_values()
        print("✓ Special values serialization tests passed")
        
        print("\n🎉 All tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise