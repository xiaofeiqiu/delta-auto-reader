"""
Simple Feature Store SDK Example (No Prefixes)

This demonstrates the requested API without prefix functionality.
"""

import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Add the current directory to Python path so we can import our SDK
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from feature_store_sdk import FeatureStore, projection


def create_sample_data():
    """Create sample data for testing"""
    
    # Initialize Spark with Delta Lake support
    builder = SparkSession.builder.appName("FeatureStoreSimpleExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    # Create accounts data
    accounts_data = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005'],
        'user_id': ['USER001', 'USER002', 'USER003', 'USER004', 'USER005'],
        'status': ['ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE', 'ACTIVE'],
        'opened_at': ['2023-01-15', '2023-02-20', '2023-03-10', '2023-04-05', '2023-05-12']
    })
    
    # Create users data
    users_data = pd.DataFrame({
        'user_id': ['USER001', 'USER002', 'USER003', 'USER004', 'USER005'],
        'age': [25, 34, 28, 45, 33],
        'segment': ['PREMIUM', 'STANDARD', 'PREMIUM', 'GOLD', 'STANDARD'],
        'country': ['US', 'UK', 'CA', 'US', 'DE']
    })
    
    # Create transaction profile data
    transactions_data = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005'],
        'last_txn_ts': ['2024-01-15 10:30:00', '2024-01-14 15:45:00', '2023-12-20 09:15:00', 
                       '2024-01-16 14:20:00', '2024-01-15 11:55:00'],
        'avg_ticket': [125.50, 89.75, 245.30, 67.80, 156.25],
        'txn_cnt_90d': [15, 8, 2, 22, 12]
    })
    
    # Convert to Spark DataFrames and save as Delta tables
    data_path = "/workspace/data/simple_feature_store"
    
    accounts_df = spark.createDataFrame(accounts_data)
    accounts_df.write.format("delta").mode("overwrite").save(f"{data_path}/accounts")
    
    users_df = spark.createDataFrame(users_data)  
    users_df.write.format("delta").mode("overwrite").save(f"{data_path}/users")
    
    transactions_df = spark.createDataFrame(transactions_data)
    transactions_df.write.format("delta").mode("overwrite").save(f"{data_path}/transactions_profile")
    
    print("✅ Sample data created successfully!")
    return spark


def main():
    """Main example function - exact API as requested"""
    
    print("🚀 Simple Feature Store SDK Example")
    print("=" * 50)
    
    # Create sample data
    spark = create_sample_data()
    
    # Exact API as requested
    fs = FeatureStore()

    accounts_fg = fs.get_or_create_batch_feature_group(
        name="accounts", 
        version=1, 
        keys=["account_id"],
        data_location="/workspace/data/simple_feature_store/accounts"
    )
    
    users_fg = fs.get_or_create_batch_feature_group(
        name="users", 
        version=1, 
        keys=["user_id"],
        data_location="/workspace/data/simple_feature_store/users"
    )
    
    # 如果 transactions 是多行，请先在外部生成唯一画像表再注册；这里假设它已唯一
    transactions_fg = fs.get_or_create_batch_feature_group(
        name="transactions_profile", 
        version=1, 
        keys=["account_id"],
        data_location="/workspace/data/simple_feature_store/transactions_profile"
    )

    # 创建 Feature View（自动等值 join，不需要写 SQL）
    fv = fs.get_or_create_feature_view(
        name="account_features", 
        version=1, 
        base=accounts_fg,
        source_projections=[
            # 基表要暴露的列
            projection(
                source=accounts_fg,
                features=["account_id", "user_id", "status", "opened_at"]
            ),
            # users：用 account.user_id -> users.user_id 连接
            projection(
                source=users_fg,
                features=["age", "segment", "country"],
                keys_map={"user_id": "user_id"},   # 左列=account.user_id, 右列=users.user_id
                join_type="left"
            ),
            # transactions：用 account.account_id -> transactions.account_id 连接
            projection(
                source=transactions_fg,
                features=["last_txn_ts", "avg_ticket", "txn_cnt_90d"],
                keys_map={"account_id": "account_id"},  # 左列=account.account_id, 右列=transactions.account_id
                join_type="left"
            ),
        ],
        description="Accounts base + user profile + transaction profile (no PIT, no agg)"
    )

    # 取数（自动 join）
    df = fv.plan().to_polars()   # 也支持 .to_pandas() / .to_spark()
    
    print("🎉 Result (Polars DataFrame):")
    print("Shape:", df.shape)
    print("Columns:", list(df.columns))
    print(df)
    
    # Clean up
    spark.stop()


if __name__ == "__main__":
    main()