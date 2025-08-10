#!/usr/bin/env python3
"""
Test script to validate filter functionality
"""
import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Add feature store SDK to path
sys.path.append('.')
from feature_store_sdk import FeatureStore, projection

def main():
    print("🧪 Testing Filter Functionality")
    print("=" * 32)
    
    # Initialize Spark
    builder = SparkSession.builder.appName("FilterTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark initialized")
    
    # Create sample data
    accounts_data = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004'],
        'status': ['ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE'],
        'credit_limit': [10000, 5000, 15000, 25000]
    })
    
    # Save as Delta
    base_path = "/tmp/test_filter"
    os.makedirs(base_path, exist_ok=True)
    
    accounts_df = spark.createDataFrame(accounts_data)
    accounts_df.write.format("delta").mode("overwrite").save(f"{base_path}/accounts")
    print("✅ Test data saved")
    
    # Initialize Feature Store
    fs = FeatureStore(spark=spark)
    
    accounts_fg = fs.get_or_create_batch_feature_group(
        name="accounts",
        version=1,
        keys=["account_id"],
        data_location=f"{base_path}/accounts"
    )
    print("✅ Feature group created")
    
    # Test filter functionality
    print("\n📋 Test: Single Equality Filter")
    try:
        active_fv = fs.get_or_create_feature_view(
            name="active_accounts",
            version=1,
            base=accounts_fg,
            source_projections=[
                projection(
                    source=accounts_fg,
                    features=["account_id", "status", "credit_limit"],
                    filters=("status", "==", "ACTIVE")  # Tuple format
                )
            ]
        )
        
        result = active_fv.plan().to_pandas()
        print(f"📊 Filtered result count: {len(result)}")
        print(f"📊 Original data count: {len(accounts_data)}")
        print(f"✅ All records have ACTIVE status: {all(result['status'] == 'ACTIVE')}")
        print("\nFiltered data:")
        print(result)
        
        # Test Spark output
        spark_result = active_fv.plan().to_spark(spark)
        print(f"\n🔥 Spark result count: {spark_result.count()}")
        spark_result.show()
        
        print("✅ Filter test PASSED")
        
    except Exception as e:
        print(f"❌ Filter test FAILED: {e}")
        import traceback
        traceback.print_exc()
    
    # Cleanup
    spark.stop()
    print("🧹 Cleanup complete")

if __name__ == "__main__":
    main()