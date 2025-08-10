#!/usr/bin/env python3
"""
Test the exact notebook Test 6 scenario with the fix
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
    print("🧪 Test 6: Filter Functionality - Fixed")
    print("=" * 40)
    
    # Initialize Spark with Delta Lake support
    builder = SparkSession.builder.appName("FeatureStoreSDKDemo") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"✅ Spark {spark.version} initialized with Delta Lake support")

    # Create sample business data (exactly like notebook)
    print("📊 Creating sample business data...")

    # Customer accounts data
    accounts_data = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005', 'ACC006'],
        'user_id': ['USER001', 'USER002', 'USER003', 'USER004', 'USER005', 'USER006'],
        'account_type': ['PREMIUM', 'STANDARD', 'PREMIUM', 'GOLD', 'STANDARD', 'GOLD'],
        'status': ['ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE', 'ACTIVE', 'SUSPENDED'],
        'opened_at': ['2023-01-15', '2023-02-20', '2023-03-10', '2023-04-05', '2023-05-12', '2023-06-01'],
        'credit_limit': [10000, 5000, 15000, 25000, 3000, 20000]
    })

    print(f"📋 Created {len(accounts_data)} accounts")

    # Save all data as Delta Lake tables
    base_path = "/workspace/data/feature_store_demo"
    print(f"💾 Saving data to Delta Lake at: {base_path}")

    # Convert to Spark DataFrames and save
    accounts_df = spark.createDataFrame(accounts_data)
    accounts_df.write.format("delta").mode("overwrite").save(f"{base_path}/accounts")
    print("✅ Accounts saved")

    print("\\n🎉 All data successfully saved in Delta Lake format!")

    # Initialize Feature Store SDK
    print("## Initialize Feature Store SDK")

    # Initialize Feature Store
    fs = FeatureStore(spark=spark)
    print("✅ Feature Store initialized")

    # Create feature groups with explicit data locations
    print("\\n📊 Creating feature groups...")

    accounts_fg = fs.get_or_create_batch_feature_group(
        name="accounts", 
        version=1, 
        keys=["account_id"],
        data_location=f"{base_path}/accounts",
        description="Customer account information"
    )
    print(f"✅ {accounts_fg}")

    print("\\n🎯 All feature groups created successfully!")

    # Now run Test 6.1 exactly as in notebook
    print("\\n📋 Test 6.1: Single Equality Filter - Tuple Format")
    active_accounts_fv = fs.get_or_create_feature_view(
        name="active_accounts_only", 
        version=1, 
        base=accounts_fg,
        source_projections=[
            projection(
                source=accounts_fg,
                features=["account_id", "status", "account_type", "credit_limit"],
                filters=("status", "==", "ACTIVE")
            )
        ],
        description="Only active accounts"
    )

    active_result = active_accounts_fv.plan().to_pandas()
    print(f"📊 Original accounts: {len(accounts_data)}")
    print(f"📊 Active accounts only: {len(active_result)}")
    print(f"✅ All accounts are ACTIVE: {all(active_result['status'] == 'ACTIVE')}")
    print(active_result)

    # Test Spark output for active accounts (THIS WAS FAILING BEFORE)
    print("\\n🔥 Testing Spark output for filtered data:")
    try:
        active_spark = active_accounts_fv.plan().to_spark(spark)
        print(f"   Spark DataFrame columns: {active_spark.columns}")
        print(f"   Spark DataFrame count: {active_spark.count()}")
        active_spark.show(3)
        print("✅ SPARK OUTPUT NOW WORKS!")
        
        # Validation check
        expected_active = len([x for x in accounts_data['status'] if x == 'ACTIVE'])
        actual_active = len(active_result)
        spark_count = active_spark.count()
        
        print(f"\\n📊 Validation:")
        print(f"   Expected ACTIVE accounts: {expected_active}")
        print(f"   Pandas result count: {actual_active}")
        print(f"   Spark result count: {spark_count}")
        print(f"   ✅ Test 6.1 PASSED: {expected_active == actual_active == spark_count}")
        
        success = (expected_active == actual_active == spark_count)
    except Exception as e:
        print(f"❌ Spark output still failed: {e}")
        success = False
    
    # DON'T CLEAN UP SPARK YET - LEAVE IT RUNNING FOR THE NOTEBOOK
    # spark.stop()
    # print("🧹 Spark session stopped")
    
    return success

if __name__ == "__main__":
    success = main()
    print(f"\\n{'🎊 SUCCESS' if success else '❌ FAILURE'}: Test 6.1 Filter {'PASSED' if success else 'FAILED'}!")