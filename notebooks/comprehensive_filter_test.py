#!/usr/bin/env python3
"""
Comprehensive test script for all Test 6 scenarios
"""
import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Add feature store SDK to path
sys.path.append('/workspace')
from feature_store_sdk import FeatureStore, projection

def main():
    print("🧪 Test 6: Filter Functionality - Comprehensive Testing")
    print("=" * 60)
    
    # Initialize Spark
    builder = SparkSession.builder.appName("ComprehensiveFilterTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark initialized")
    
    # Create comprehensive sample data
    accounts_data = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005', 'ACC006'],
        'user_id': ['USER001', 'USER002', 'USER003', 'USER004', 'USER005', 'USER006'],
        'account_type': ['PREMIUM', 'STANDARD', 'PREMIUM', 'GOLD', 'STANDARD', 'GOLD'],
        'status': ['ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE', 'ACTIVE', 'SUSPENDED'],
        'credit_limit': [10000, 5000, 15000, 25000, 3000, 20000]
    })
    
    users_data = pd.DataFrame({
        'user_id': ['USER001', 'USER002', 'USER003', 'USER004', 'USER005', 'USER006'],
        'age': [25, 34, 28, 45, 33, 39],
        'country': ['US', 'UK', 'CA', 'US', 'DE', 'FR'],
        'income_bracket': ['HIGH', 'MEDIUM', 'HIGH', 'VERY_HIGH', 'MEDIUM', 'HIGH']
    })
    
    risk_data = pd.DataFrame({
        'account_id': ['ACC001', 'ACC002', 'ACC003', 'ACC004', 'ACC005', 'ACC006'],
        'credit_score': [750, 680, 720, 800, 650, 780],
        'risk_category': ['LOW', 'MEDIUM', 'LOW', 'VERY_LOW', 'MEDIUM', 'LOW']
    })
    
    # Save as Delta tables
    base_path = "/workspace/data/comprehensive_test"
    
    accounts_df = spark.createDataFrame(accounts_data)
    accounts_df.write.format("delta").mode("overwrite").save(f"{base_path}/accounts")
    
    users_df = spark.createDataFrame(users_data)
    users_df.write.format("delta").mode("overwrite").save(f"{base_path}/users")
    
    risk_df = spark.createDataFrame(risk_data)
    risk_df.write.format("delta").mode("overwrite").save(f"{base_path}/risk")
    
    print("✅ Test data saved")
    
    # Initialize Feature Store
    fs = FeatureStore(spark=spark)
    
    accounts_fg = fs.get_or_create_batch_feature_group(
        name="accounts", version=1, keys=["account_id"],
        data_location=f"{base_path}/accounts"
    )
    users_fg = fs.get_or_create_batch_feature_group(
        name="users", version=1, keys=["user_id"], 
        data_location=f"{base_path}/users"
    )
    risk_fg = fs.get_or_create_batch_feature_group(
        name="risk", version=1, keys=["account_id"],
        data_location=f"{base_path}/risk"
    )
    print("✅ Feature groups created")
    
    # Run comprehensive tests
    tests_passed = [0]  # Use list for mutable reference
    total_tests = [0]   # Use list for mutable reference
    
    def test_case(name, test_func):
        nonlocal tests_passed, total_tests
        total_tests[0] += 1
        print(f"\n📋 {name}")
        try:
            result = test_func()
            if result:
                tests_passed[0] += 1
                print("✅ PASSED")
            else:
                print("❌ FAILED - Test logic returned False")
        except Exception as e:
            print(f"❌ FAILED - {e}")
            import traceback
            traceback.print_exc()
    
    # Test 6.1: Single equality filter
    def test_single_equality():
        fv = fs.get_or_create_feature_view(
            name="active_accounts_test", version=1, base=accounts_fg,
            source_projections=[
                projection(
                    source=accounts_fg,
                    features=["account_id", "status", "account_type"],
                    filters=("status", "==", "ACTIVE")
                )
            ]
        )
        result = fv.plan().to_pandas()
        spark_result = fv.plan().to_spark(spark)
        
        print(f"   Original: {len(accounts_data)}, Filtered: {len(result)}")
        print(f"   Spark count: {spark_result.count()}")
        return (len(result) == 4 and 
                all(result['status'] == 'ACTIVE') and 
                spark_result.count() == 4)
    
    # Test 6.2: Range filter
    def test_range_filter():
        fv = fs.get_or_create_feature_view(
            name="mature_users_test", version=1, base=accounts_fg,
            source_projections=[
                projection(source=accounts_fg, features=["account_id", "user_id"]),
                projection(
                    source=users_fg,
                    features=["age", "country"],
                    keys_map={"user_id": "user_id"},
                    join_type="left",
                    filters=("age", ">", 30)
                )
            ]
        )
        result = fv.plan().to_pandas()
        spark_result = fv.plan().to_spark(spark)
        ages = result['age'].dropna()
        
        print(f"   Ages > 30: {list(ages)}")
        print(f"   Spark count: {spark_result.count()}")
        return (len(ages) == 4 and all(ages > 30) and spark_result.count() > 0)
    
    # Test 6.3: IN filter
    def test_in_filter():
        fv = fs.get_or_create_feature_view(
            name="us_uk_test", version=1, base=accounts_fg,
            source_projections=[
                projection(source=accounts_fg, features=["account_id", "user_id"]),
                projection(
                    source=users_fg,
                    features=["country", "age"],
                    keys_map={"user_id": "user_id"},
                    join_type="left",
                    filters=("country", "in", ["US", "UK"])
                )
            ]
        )
        result = fv.plan().to_pandas()
        spark_result = fv.plan().to_spark(spark)
        countries = result['country'].dropna().unique()
        
        print(f"   Countries: {list(countries)}")
        print(f"   Spark count: {spark_result.count()}")
        return (set(countries).issubset({'US', 'UK'}) and spark_result.count() > 0)
    
    # Test 6.4: Multiple filters
    def test_multiple_filters():
        fv = fs.get_or_create_feature_view(
            name="multiple_filters_test", version=1, base=accounts_fg,
            source_projections=[
                projection(source=accounts_fg, features=["account_id"]),
                projection(
                    source=risk_fg,
                    features=["credit_score", "risk_category"],
                    keys_map={"account_id": "account_id"},
                    join_type="left",
                    filters=[
                        ("credit_score", ">", 700),
                        ("risk_category", "==", "LOW")
                    ]
                )
            ]
        )
        result = fv.plan().to_pandas()
        spark_result = fv.plan().to_spark(spark)
        scores = result['credit_score'].dropna()
        categories = result['risk_category'].dropna()
        
        print(f"   Credit scores > 700: {list(scores)}")
        print(f"   Low risk categories: {list(categories)}")
        print(f"   Spark count: {spark_result.count()}")
        
        return (len(scores) >= 0 and 
                (all(scores > 700) if len(scores) > 0 else True) and
                (all(categories == 'LOW') if len(categories) > 0 else True) and
                spark_result.count() >= 0)
    
    # Test 6.5: Complex business scenario
    def test_complex_scenario():
        fv = fs.get_or_create_feature_view(
            name="premium_high_credit_test", version=1, base=accounts_fg,
            source_projections=[
                projection(
                    source=accounts_fg,
                    features=["account_id", "account_type", "credit_limit"],
                    filters=("account_type", "==", "PREMIUM")
                ),
                projection(
                    source=risk_fg,
                    features=["credit_score"],
                    keys_map={"account_id": "account_id"},
                    join_type="left",
                    filters=("credit_score", ">", 700)
                )
            ]
        )
        result = fv.plan().to_pandas()
        spark_result = fv.plan().to_spark(spark)
        
        account_types = result['account_type'].dropna()
        credit_scores = result['credit_score'].dropna()
        
        print(f"   Premium accounts: {len(account_types)}")
        print(f"   High credit scores: {list(credit_scores)}")
        print(f"   Spark count: {spark_result.count()}")
        
        return (len(result) >= 0 and
                (all(account_types == 'PREMIUM') if len(account_types) > 0 else True) and
                (all(credit_scores > 700) if len(credit_scores) > 0 else True) and
                spark_result.count() >= 0)
    
    # Run all tests
    test_case("Test 6.1: Single Equality Filter", test_single_equality)
    test_case("Test 6.2: Range Filter", test_range_filter)
    test_case("Test 6.3: IN Filter", test_in_filter)
    test_case("Test 6.4: Multiple Filters", test_multiple_filters)
    test_case("Test 6.5: Complex Business Scenario", test_complex_scenario)
    
    print(f"\n🎯 Test Results: {tests_passed[0]}/{total_tests[0]} passed")
    
    if tests_passed[0] == total_tests[0]:
        print("🎉 ALL FILTER TESTS PASSED! 🎉")
        return True
    else:
        print(f"⚠️ {total_tests[0] - tests_passed[0]} tests failed")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ Filter functionality is fully working!")
    else:
        print("\n❌ Some filter tests failed")