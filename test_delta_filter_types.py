#!/usr/bin/env python3
"""
Quick test script to demonstrate the new FilterType format
compatible with deltalake.DeltaTable.to_pandas().
"""

import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from feature_store_sdk.filters import c, condition, and_, or_, not_, DELTALAKE_AVAILABLE

def test_filter_formats():
    """Demonstrate the new FilterType format"""
    print("🧪 Delta Lake FilterType Format Examples")
    print("=" * 50)
    
    print(f"Delta Lake available: {DELTALAKE_AVAILABLE}")
    print()
    
    # Simple conditions
    print("📋 Simple Conditions:")
    print(f"  c('age', '>', 25) → {c('age', '>', 25).to_delta_table_filter()}")
    print(f"  c('country', '==', 'US') → {c('country', '==', 'US').to_delta_table_filter()}")
    print(f"  c('status', 'in', ['active', 'premium']) → {c('status', 'in', ['active', 'premium']).to_delta_table_filter()}")
    print(f"  c('email', 'is_null') → {c('email', 'is_null').to_delta_table_filter()}")
    print()
    
    # Complex conditions
    print("🔗 Complex Conditions:")
    
    # AND condition (conjunction)
    and_filter = c("age", ">", 25) & c("country", "==", "US")
    print(f"  AND: {and_filter.to_delta_table_filter()}")
    
    # OR condition (disjunction)
    or_filter = c("country", "==", "US") | c("country", "==", "UK")
    print(f"  OR: {or_filter.to_delta_table_filter()}")
    
    # NOT condition
    not_filter = ~c("status", "==", "banned")
    print(f"  NOT: {not_filter.to_delta_table_filter()}")
    
    # Complex nested
    complex_filter = (c("age", ">=", 18) & c("country", "in", ["US", "UK"])) | c("status", "==", "VIP")
    print(f"  Complex: {complex_filter.to_delta_table_filter()}")
    print()
    
    # BETWEEN handling
    print("📐 Special Cases:")
    between_filter = c("age", "between", (25, 45))
    print(f"  BETWEEN: {between_filter.to_delta_table_filter()}")
    print()
    
    # Usage example
    print("💡 Usage Example:")
    print("```python")
    print("from deltalake import DeltaTable")
    print("from feature_store_sdk.filters import c")
    print()
    print("dt = DeltaTable('path/to/table')")
    print("filter_condition = c('age', '>', 25) & c('country', '==', 'US')")
    print("df = dt.to_pandas(filters=filter_condition.to_delta_table_filter())")
    print("```")

if __name__ == "__main__":
    test_filter_formats()