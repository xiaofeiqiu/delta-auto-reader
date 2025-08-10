#!/usr/bin/env python3
"""
Quick test script to verify Transform functionality works
"""
import sys
import os
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Add the parent directory to Python path to import our SDK
sys.path.append('/workspace')

try:
    from feature_store_sdk import FeatureStore, Transform, projection
    print("✅ All imports successful!")
    
    # Test Transform creation
    print("\n🧪 Testing Transform creation...")
    age_double = Transform("age_doubled", lambda age: age * 2)
    credit_k = Transform("credit_limit_k", lambda credit_limit: credit_limit / 1000)
    print(f"✅ Age Transform: {age_double}")
    print(f"✅ Credit Transform: {credit_k}")
    
    # Test with pandas DataFrame
    print("\n🐼 Testing Transform with Pandas...")
    test_df = pd.DataFrame({
        'age': [25, 35, 45],
        'credit_limit': [10000, 20000, 30000]
    })
    
    age_result = age_double.apply_pandas(test_df)
    print(f"✅ Pandas age transform result: {list(age_result)}")
    
    credit_result = credit_k.apply_pandas(test_df)
    print(f"✅ Pandas credit transform result: {list(credit_result)}")
    
    print("\n🎉 All Transform functionality tests passed!")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
except Exception as e:
    print(f"❌ Test error: {e}")
    import traceback
    traceback.print_exc()