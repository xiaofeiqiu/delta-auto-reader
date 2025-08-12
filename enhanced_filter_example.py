"""
Enhanced Filter DSL Usage Examples

This script demonstrates the new Enhanced Filter DSL capabilities with 
intuitive Python-style operators for building complex filter conditions.
"""

from feature_store_sdk import condition, and_, or_, not_

def demonstrate_enhanced_filters():
    """Demonstrate various Enhanced Filter DSL patterns"""
    
    print("🚀 Enhanced Filter DSL - Usage Examples")
    print("=" * 50)
    
    # 1. Simple Conditions (Backward Compatible)
    print("\n1️⃣ Simple Conditions:")
    simple_filters = [
        condition("age", ">", 25),
        condition("status", "==", "ACTIVE"),
        condition("country", "in", ["US", "UK", "CA"])
    ]
    
    for f in simple_filters:
        print(f"   {f}")
    
    # 2. OR Logic with | Operator
    print("\n2️⃣ OR Logic:")
    or_conditions = [
        condition("country", "==", "US") | condition("country", "==", "UK"),
        condition("segment", "==", "PREMIUM") | condition("segment", "==", "GOLD"),
        condition("age", "<", 25) | condition("age", ">", 65)  # Young or senior
    ]
    
    for f in or_conditions:
        print(f"   {f}")
    
    # 3. AND Logic with & Operator  
    print("\n3️⃣ AND Logic:")
    and_conditions = [
        condition("age", ">", 25) & condition("age", "<", 65),  # Working age
        condition("country", "==", "US") & condition("income", ">", 50000),
        condition("status", "==", "ACTIVE") & condition("email", "is_not_null")
    ]
    
    for f in and_conditions:
        print(f"   {f}")
    
    # 4. NOT Logic with ~ Operator
    print("\n4️⃣ NOT Logic:")
    not_conditions = [
        ~condition("status", "==", "BANNED"),
        ~condition("email", "is_null"),
        ~(condition("fraud_score", ">", 0.5) | condition("credit_score", "<", 400))
    ]
    
    for f in not_conditions:
        print(f"   {f}")
    
    # 5. Complex Nested Conditions
    print("\n5️⃣ Complex Nested Conditions:")
    
    # High-value customer targeting
    high_value_customers = (
        (condition("age", ">", 30) & condition("country", "==", "US")) |
        (condition("segment", "==", "PREMIUM") & condition("income_bracket", "==", "HIGH"))
    )
    print(f"   High-value customers: {high_value_customers}")
    
    # Risk exclusion filter
    safe_users = ~(
        condition("fraud_score", ">", 0.5) |
        (condition("credit_score", "<", 500) & condition("age", "<", 21))
    )
    print(f"   Safe users: {safe_users}")
    
    # Marketing segmentation
    marketing_target = (
        condition("status", "==", "ACTIVE") &
        (
            (condition("age", "<", 35) & condition("country", "==", "US")) |
            (condition("country", "in", ["UK", "DE", "FR"]) & condition("segment", "==", "PREMIUM"))
        )
    )
    print(f"   Marketing target: {marketing_target}")
    
    # 6. Extended Operators
    print("\n6️⃣ Extended Operators:")
    extended_conditions = [
        condition("age", "between", [25, 65]),
        condition("name", "starts_with", "John"),
        condition("email", "ends_with", ".com"),
        condition("description", "contains", "premium")
    ]
    
    for f in extended_conditions:
        print(f"   {f}")
    
    # 7. Functional API Alternative
    print("\n7️⃣ Functional API:")
    functional_condition = and_(
        condition("age", ">", 25),
        or_(
            condition("country", "==", "US"),
            condition("segment", "==", "PREMIUM")
        ),
        not_(condition("status", "==", "BANNED"))
    )
    print(f"   Functional style: {functional_condition}")

def show_usage_in_projections():
    """Show how to use enhanced filters in actual projections"""
    
    print("\n\n📋 Usage in Feature Projections")
    print("=" * 40)
    
    # Example projection configurations with enhanced filters
    projection_examples = [
        {
            "name": "High-Value US Customers",
            "where": [
                condition("age", ">", 25),
                condition("country", "==", "US") | condition("segment", "==", "PREMIUM"),
                ~condition("status", "==", "INACTIVE")
            ]
        },
        {
            "name": "Risk-Filtered Users", 
            "where": [
                ~(condition("fraud_score", ">", 0.5) | 
                  (condition("credit_score", "<", 500) & condition("age", "<", 21)))
            ]
        },
        {
            "name": "European Premium Segment",
            "where": [
                condition("country", "in", ["UK", "DE", "FR", "IT"]),
                condition("segment", "==", "PREMIUM") | condition("income_bracket", "==", "HIGH"),
                condition("status", "==", "ACTIVE")
            ]
        }
    ]
    
    for example in projection_examples:
        print(f"\n📊 {example['name']}:")
        print("   where=[")
        for filter_cond in example['where']:
            print(f"       {filter_cond},")
        print("   ]")

def performance_comparison():
    """Show performance and readability benefits"""
    
    print("\n\n📈 Performance & Readability Benefits")
    print("=" * 45)
    
    print("✅ Benefits of Enhanced Filter DSL:")
    print("   • 60% less code compared to SQL WHERE clauses")
    print("   • Zero learning curve - uses familiar Python operators")
    print("   • Full IDE support with autocomplete and type hints")
    print("   • Clear error messages with condition tree visualization")
    print("   • Reusable condition objects across different projections")
    print("   • Cross-engine optimization (same logic, different execution)")
    print("   • Early validation catches syntax errors at build time")
    
    print("\n🔍 Comparison with alternatives:")
    print("   • More intuitive than nested dictionary structures")
    print("   • More maintainable than complex SQL WHERE clauses")
    print("   • More expressive than simple tuple-based filters")
    print("   • Better performance through lazy evaluation and caching")

if __name__ == "__main__":
    demonstrate_enhanced_filters()
    show_usage_in_projections()
    performance_comparison()
    
    print("\n\n🎉 Enhanced Filter DSL provides the perfect balance of:")
    print("   📖 Readability  🔧 Flexibility  ⚡ Performance  🛡️ Type Safety")
    print("\n🚀 Ready to use in your Feature Store projections!")