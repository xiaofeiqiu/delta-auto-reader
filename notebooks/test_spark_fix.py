#!/usr/bin/env python3
"""
Test the Spark session fix for BatchFeatureGroup
"""
import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Add the parent directory to Python path to import our SDK
sys.path.append('/workspace')
from feature_store_sdk import FeatureStore, projection

def main():
    print("🔧 Testing Spark Session Fix")
    print("=" * 30)
    
    # Initialize Spark with Delta Lake support
    builder = SparkSession.builder.appName("SparkFixTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark initialized")
    
    # Create sample data
    accounts_data = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004'],
        'status': ['ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE'],
        'account_type': ['PREMIUM', 'STANDARD', 'PREMIUM', 'GOLD']
    })
    
    # Save as Delta
    base_path = "/workspace/data/spark_fix_test"
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
    
    # Test 1: Direct read_data() call
    print("\n📋 Test 1: Direct read_data() call")
    try:
        direct_df = accounts_fg.read_data()
        print(f"✅ Direct read successful: {direct_df.count()} rows")
        direct_df.show(2)
    except Exception as e:
        print(f"❌ Direct read failed: {e}")
        return False
    
    # Test 2: Feature view with filters
    print("\n📋 Test 2: Feature view with filters")
    try:
        active_fv = fs.get_or_create_feature_view(
            name="active_accounts_fix_test",
            version=1,
            base=accounts_fg,
            source_projections=[
                projection(
                    source=accounts_fg,
                    features=["account_id", "status", "account_type"],
                    filters=("status", "==", "ACTIVE")
                )
            ]
        )
        
        # Test pandas output (this was working)
        pandas_result = active_fv.plan().to_pandas()
        print(f"✅ Pandas result: {len(pandas_result)} rows")
        
        # Test Spark output (this was failing)
        spark_result = active_fv.plan().to_spark(spark)
        print(f"✅ Spark result: {spark_result.count()} rows")
        spark_result.show(2)
        
        # Verify both results match
        if len(pandas_result) == spark_result.count():
            print("✅ Pandas and Spark results match!")
        else:
            print("❌ Pandas and Spark results don't match")
            return False
            
    except Exception as e:
        print(f"❌ Feature view test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n🎉 All Spark session tests passed!")
    spark.stop()
    return True

if __name__ == "__main__":
    success = main()
    print(f"\n{'✅ SUCCESS' if success else '❌ FAILURE'}: Spark session fix {'works' if success else 'failed'}!")