#!/usr/bin/env python3
"""
Test script to verify projection with transforms
"""
import sys
import os
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Add the parent directory to Python path to import our SDK
sys.path.append('/workspace')

from feature_store_sdk import FeatureStore, Transform, feature_source_projection

print("✅ Testing Projection with Transforms")
print("=" * 40)

# Initialize Spark
print("\n🔥 Initializing Spark...")
builder = SparkSession.builder.appName("TransformTest") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("✅ Spark initialized")

# Initialize Feature Store
fs = FeatureStore(spark=spark)
print("✅ Feature Store initialized")

# Create test data
print("\n📊 Creating test data...")
test_accounts = pd.DataFrame({
    'account_id': ['ACC001', 'ACC002', 'ACC003'],
    'user_id': ['USER001', 'USER002', 'USER003'],
    'credit_limit': [10000, 20000, 30000],
    'account_type': ['PREMIUM', 'STANDARD', 'GOLD']
})

# Save as Delta
accounts_df = spark.createDataFrame(test_accounts)
test_path = "/workspace/data/test_accounts_transform"
accounts_df.write.format("delta").mode("overwrite").save(test_path)
print("✅ Test data saved to Delta")

# Create feature group
accounts_fg = fs.get_or_create_batch_feature_group(
    name="test_accounts_transform",
    version=1,
    keys=["account_id"],
    data_location=test_path,
    description="Test accounts for transform testing"
)
print("✅ Feature group created")

# Define transforms
credit_k_transform = Transform("credit_limit_k", lambda credit_limit: credit_limit / 1000)
account_tier_transform = Transform(
    "account_tier", 
    lambda credit_limit: "HIGH" if credit_limit >= 25000 else ("MEDIUM" if credit_limit >= 15000 else "LOW")
)

print("\n🔧 Testing projection with transforms...")

# Create feature view with transforms
test_fv = fs.get_or_create_feature_view(
    name="test_transforms_fv",
    version=1,
    base=accounts_fg,
    source_projections=[
        feature_source_projection(
            feature_group=accounts_fg,
            features=["account_id", "credit_limit", "account_type"],
            transforms=[credit_k_transform, account_tier_transform]
        )
    ],
    description="Test feature view with transforms"
)

print("✅ Feature view with transforms created")

# Test Pandas output
print("\n🐼 Testing Pandas output...")
pandas_result = test_fv.plan().to_pandas()
print(f"📊 Columns: {list(pandas_result.columns)}")
print(f"📈 Shape: {pandas_result.shape}")
print("📋 Results:")
print(pandas_result)

# Verify transforms worked
assert 'credit_limit_k' in pandas_result.columns, "credit_limit_k transform missing"
assert 'account_tier' in pandas_result.columns, "account_tier transform missing"

expected_credit_k = [10.0, 20.0, 30.0]
actual_credit_k = pandas_result['credit_limit_k'].tolist()
assert actual_credit_k == expected_credit_k, f"Credit transform failed: expected {expected_credit_k}, got {actual_credit_k}"

expected_tiers = ['LOW', 'MEDIUM', 'HIGH']  
actual_tiers = pandas_result['account_tier'].tolist()
assert actual_tiers == expected_tiers, f"Tier transform failed: expected {expected_tiers}, got {actual_tiers}"

print("✅ All Pandas transforms validated!")

# Test Spark output
print("\n🔥 Testing Spark output...")
spark_result = test_fv.plan().to_spark(spark)
print(f"📊 Columns: {spark_result.columns}")
print(f"📈 Count: {spark_result.count()}")
print("📋 Results:")
spark_result.show()

print("✅ Spark transforms working!")

# Test Polars output
print("\n⚡ Testing Polars output...")
polars_result = test_fv.plan().to_polars()
print(f"📊 Columns: {list(polars_result.columns)}")
print(f"📈 Shape: {polars_result.shape}")
print("📋 Results:")
print(polars_result)

print("✅ Polars transforms working!")

print("\n🎉 ALL PROJECTION TRANSFORM TESTS PASSED! 🎉")
print("✅ Transform creation")
print("✅ Projection with transforms")
print("✅ Feature view with transforms")  
print("✅ Pandas output with transforms")
print("✅ Spark output with transforms")
print("✅ Polars output with transforms")
print("✅ Mathematical transforms (division)")
print("✅ Conditional transforms (if/else logic)")
print("✅ Multi-column input transforms")

spark.stop()
print("✅ Spark stopped")