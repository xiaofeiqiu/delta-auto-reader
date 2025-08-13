#!/usr/bin/env python3
"""
Comprehensive example of condition serialization and deserialization.
"""

from feature_store_sdk.filters import c, deserialize_condition


def main():
    print("=== Condition Serialization & Deserialization Examples ===\n")
    
    # Example 1: Simple condition
    print("1. Simple Condition:")
    simple_condition = c("age", ">", 25)._to_condition()
    print(f"   Original: {simple_condition}")
    
    # Serialize to JSON
    json_str = simple_condition.to_json()
    print(f"   JSON: {json_str}")
    
    # Deserialize back
    restored = deserialize_condition(json_str)
    print(f"   Restored: {restored}")
    print(f"   Equal? {str(simple_condition) == str(restored)}\n")
    
    
    # Example 2: Complex AND/OR condition
    print("2. Complex AND/OR Condition:")
    complex_condition = (c("age", ">", 25) & c("country", "==", "US")) | c("status", "!=", "BANNED")
    print(f"   Original: {complex_condition}")
    
    # Serialize
    json_str = complex_condition.to_json()
    print(f"   JSON: {json_str}")
    
    # Deserialize
    restored = deserialize_condition(json_str)
    print(f"   Restored: {restored}")
    print(f"   Equal? {str(complex_condition) == str(restored)}\n")
    
    
    # Example 3: NOT condition
    print("3. NOT Condition:")
    not_condition = ~c("deleted", "==", True)
    print(f"   Original: {not_condition}")
    
    # Serialize
    json_str = not_condition.to_json()
    print(f"   JSON: {json_str}")
    
    # Deserialize
    restored = deserialize_condition(json_str)
    print(f"   Restored: {restored}")
    print(f"   Equal? {str(not_condition) == str(restored)}\n")
    
    
    # Example 4: Null check condition
    print("4. Null Check Condition:")
    null_condition = c("email", "is_not_null")._to_condition()
    print(f"   Original: {null_condition}")
    
    # Serialize
    json_str = null_condition.to_json()
    print(f"   JSON: {json_str}")
    
    # Deserialize
    restored = deserialize_condition(json_str)
    print(f"   Restored: {restored}")
    print(f"   Equal? {str(null_condition) == str(restored)}\n")
    
    
    # Example 5: Very complex nested condition
    print("5. Very Complex Nested Condition:")
    # (age >= 18 AND age <= 65) AND ((country == "US" OR country == "CA") AND status != "BANNED") AND email is_not_null
    complex_nested = (
        (c("age", ">=", 18) & c("age", "<=", 65)) & 
        ((c("country", "==", "US") | c("country", "==", "CA")) & c("status", "!=", "BANNED")) &
        c("email", "is_not_null")
    )
    print(f"   Original: {complex_nested}")
    
    # Serialize
    json_str = complex_nested.to_json()
    print(f"   JSON: {json_str}")
    
    # Deserialize
    restored = deserialize_condition(json_str)
    print(f"   Restored: {restored}")
    print(f"   Equal? {str(complex_nested) == str(restored)}\n")
    
    
    # Example 6: Practical use case - Save and load conditions from file
    print("6. Practical Use Case - Save/Load from File:")
    
    # Create a condition for filtering active users
    user_filter = (
        c("status", "==", "ACTIVE") & 
        c("age", ">=", 21) & 
        ~c("country", "in", ["BANNED_COUNTRY_1", "BANNED_COUNTRY_2"])
    )
    print(f"   User filter: {user_filter}")
    
    # Save to file
    with open("user_filter.json", "w") as f:
        f.write(user_filter.to_json())
    print("   ✓ Saved to user_filter.json")
    
    # Load from file
    with open("user_filter.json", "r") as f:
        loaded_json = f.read()
    
    loaded_condition = deserialize_condition(loaded_json)
    print(f"   Loaded: {loaded_condition}")
    print(f"   Equal? {str(user_filter) == str(loaded_condition)}")
    
    # Clean up
    import os
    os.remove("user_filter.json")
    print("   ✓ Cleaned up file\n")
    
    
    # Example 7: Working with dictionary format
    print("7. Working with Dictionary Format:")
    condition = c("price", "between", [100, 500])._to_condition()
    print(f"   Original: {condition}")
    
    # Get as dictionary
    condition_dict = condition.to_dict()
    print(f"   Dictionary: {condition_dict}")
    
    # Modify the dictionary (e.g., change the price range)
    condition_dict["value"] = [200, 600]
    print(f"   Modified dict: {condition_dict}")
    
    # Create new condition from modified dictionary
    modified_condition = deserialize_condition(condition_dict)
    print(f"   New condition: {modified_condition}\n")
    
    
    print("=== All Examples Completed Successfully! ===")


if __name__ == "__main__":
    main()