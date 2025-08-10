#!/usr/bin/env python3
"""
Exact Test 6 reproduction from the notebook
"""
import os
import sys
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Add the parent directory to Python path to import our SDK
sys.path.append('/workspace')
from feature_store_sdk import FeatureStore, projection

def main():
    print("🧪 Test 6: Filter Functionality")
    print("=" * 32)
    
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

    # User profile data
    users_data = pd.DataFrame({
        'user_id': ['USER001', 'USER002', 'USER003', 'USER004', 'USER005', 'USER006'],
        'age': [25, 34, 28, 45, 33, 39],
        'segment': ['PREMIUM', 'STANDARD', 'PREMIUM', 'GOLD', 'STANDARD', 'GOLD'],
        'country': ['US', 'UK', 'CA', 'US', 'DE', 'FR'],
        'city': ['New York', 'London', 'Toronto', 'San Francisco', 'Berlin', 'Paris'],
        'income_bracket': ['HIGH', 'MEDIUM', 'HIGH', 'VERY_HIGH', 'MEDIUM', 'HIGH'],
        'signup_date': ['2022-12-01', '2023-01-15', '2023-02-01', '2022-11-15', '2023-04-01', '2023-05-20']
    })

    # Transaction profile data (aggregated features)
    transactions_data = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005', 'ACC006'],
        'last_txn_ts': ['2024-01-15 10:30:00', '2024-01-14 15:45:00', '2023-12-20 09:15:00', 
                       '2024-01-16 14:20:00', '2024-01-15 11:55:00', '2024-01-13 16:30:00'],
        'avg_ticket': [125.50, 89.75, 245.30, 67.80, 156.25, 301.40],
        'txn_cnt_30d': [8, 5, 1, 12, 7, 15],
        'txn_cnt_90d': [15, 8, 2, 22, 12, 28],
        'total_spend_90d': [1882.5, 718.0, 490.6, 1491.6, 1875.0, 8439.2],
        'distinct_merchants_90d': [8, 5, 2, 12, 7, 16]
    })

    # Risk scores (additional feature group)
    risk_data = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005', 'ACC006'],
        'credit_score': [750, 680, 720, 800, 650, 780],
        'fraud_score': [0.05, 0.12, 0.03, 0.01, 0.08, 0.02],
        'risk_category': ['LOW', 'MEDIUM', 'LOW', 'VERY_LOW', 'MEDIUM', 'LOW'],
        'last_risk_assessment': ['2024-01-10', '2024-01-12', '2023-12-15', '2024-01-14', '2024-01-11', '2024-01-09']
    })

    print(f"📋 Created {len(accounts_data)} accounts")
    print(f"👥 Created {len(users_data)} user profiles") 
    print(f"💳 Created {len(transactions_data)} transaction profiles")
    print(f"⚠️ Created {len(risk_data)} risk assessments")

    # Save all data as Delta Lake tables
    base_path = "/workspace/data/feature_store_demo"
    print(f"💾 Saving data to Delta Lake at: {base_path}")

    # Convert to Spark DataFrames and save
    accounts_df = spark.createDataFrame(accounts_data)
    accounts_df.write.format("delta").mode("overwrite").save(f"{base_path}/accounts")
    print("✅ Accounts saved")

    users_df = spark.createDataFrame(users_data)  
    users_df.write.format("delta").mode("overwrite").save(f"{base_path}/users")
    print("✅ Users saved")

    transactions_df = spark.createDataFrame(transactions_data)
    transactions_df.write.format("delta").mode("overwrite").save(f"{base_path}/transactions_profile")
    print("✅ Transaction profiles saved")

    risk_df = spark.createDataFrame(risk_data)
    risk_df.write.format("delta").mode("overwrite").save(f"{base_path}/risk_scores")
    print("✅ Risk scores saved")

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

    users_fg = fs.get_or_create_batch_feature_group(
        name="users", 
        version=1, 
        keys=["user_id"],
        data_location=f"{base_path}/users",
        description="User demographic and profile data"
    )
    print(f"✅ {users_fg}")

    transactions_fg = fs.get_or_create_batch_feature_group(
        name="transactions_profile", 
        version=1, 
        keys=["account_id"],
        data_location=f"{base_path}/transactions_profile",
        description="Aggregated transaction features per account"
    )
    print(f"✅ {transactions_fg}")

    risk_fg = fs.get_or_create_batch_feature_group(
        name="risk_scores", 
        version=1, 
        keys=["account_id"],
        data_location=f"{base_path}/risk_scores",
        description="Risk assessment scores and categories"
    )
    print(f"✅ {risk_fg}")

    print("\\n🎯 All feature groups created successfully!")

    # Now run Test 6 exactly as in notebook
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

    # Test Spark output for active accounts
    print("\\n🔥 Testing Spark output for filtered data:")
    active_spark = active_accounts_fv.plan().to_spark(spark)
    print(f"   Spark DataFrame columns: {active_spark.columns}")
    print(f"   Spark DataFrame count: {active_spark.count()}")
    active_spark.show(3)

    # Validation check
    expected_active = len([x for x in accounts_data['status'] if x == 'ACTIVE'])
    actual_active = len(active_result)
    spark_count = active_spark.count()
    
    print(f"\\n📊 Validation:")
    print(f"   Expected ACTIVE accounts: {expected_active}")
    print(f"   Pandas result count: {actual_active}")
    print(f"   Spark result count: {spark_count}")
    print(f"   ✅ Test 6.1 PASSED: {expected_active == actual_active == spark_count}")
    
    # Clean up Spark session
    spark.stop()
    print("🧹 Spark session stopped")
    
    return expected_active == actual_active == spark_count

if __name__ == "__main__":
    success = main()
    print(f"\\n🎊 Filter Test 6 {'PASSED' if success else 'FAILED'}! 🎊")