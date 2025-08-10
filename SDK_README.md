# Feature Store SDK

A lightweight, simple Feature Store SDK that supports Delta Lake with Spark 3.4, automatic joins between feature groups, and multiple output formats (Spark, Pandas, Polars).

## 🎯 Key Features

- ✅ **Delta Lake Storage**: All feature data stored in Delta format for ACID transactions and time travel
- ✅ **Automatic Joins**: No SQL writing required - automatic equi-joins between feature groups  
- ✅ **Precise Feature Selection**: Only select the features you need via projections
- ✅ **Multiple Output Formats**: Get results as Spark DataFrames, Pandas DataFrames, or Polars DataFrames
- ✅ **Custom Join Keys**: Flexible join key mapping between different feature groups
- ✅ **Simple API**: Clean, minimal API without over-engineering
- ✅ **Spark 3.4 Compatible**: Works with Spark 3.4.4 and Delta Lake 2.4.0

## 🚀 Quick Start

### Docker Setup

```bash
# Build and start Jupyter notebook
make jupyter

# Or run individual commands
make build       # Build Docker image
make pyspark     # Start PySpark shell  
make bash        # Start bash shell
make test        # Test Delta Lake functionality
```

Access Jupyter at: **http://localhost:8888**

### Basic Usage

```python
from feature_store_sdk import FeatureStore, projection

# Initialize feature store
fs = FeatureStore()

# Create feature groups pointing to Delta tables
accounts_fg = fs.get_or_create_batch_feature_group(
    name="accounts", 
    version=1, 
    keys=["account_id"],
    data_location="/path/to/delta/accounts"
)

users_fg = fs.get_or_create_batch_feature_group(
    name="users", 
    version=1, 
    keys=["user_id"],
    data_location="/path/to/delta/users"
)

transactions_fg = fs.get_or_create_batch_feature_group(
    name="transactions_profile", 
    version=1, 
    keys=["account_id"],
    data_location="/path/to/delta/transactions"
)

# Create feature view with automatic joins
fv = fs.get_or_create_feature_view(
    name="account_features", 
    version=1, 
    base=accounts_fg,
    source_projections=[
        # Base table features
        projection(
            source=accounts_fg,
            features=["account_id", "user_id", "status", "opened_at"]
        ),
        # Join users on user_id
        projection(
            source=users_fg,
            features=["age", "segment", "country"],
            keys_map={"user_id": "user_id"},
            join_type="left"
        ),
        # Join transactions on account_id  
        projection(
            source=transactions_fg,
            features=["last_txn_ts", "avg_ticket", "txn_cnt_90d"],
            keys_map={"account_id": "account_id"},
            join_type="left"
        ),
    ]
)

# Get results in your preferred format
polars_df = fv.plan().to_polars()    # Polars DataFrame
pandas_df = fv.plan().to_pandas()    # Pandas DataFrame  
spark_df = fv.plan().to_spark()      # Spark DataFrame
```

## 📊 Project Structure

```
feature_store_sdk/
├── __init__.py           # Main exports
├── feature_store.py      # FeatureStore class
├── feature_group.py      # BatchFeatureGroup class
├── feature_view.py       # FeatureView with auto-join
├── projection.py         # projection() helper function
└── query_plan.py         # QueryPlan with format conversions

notebooks/
├── feature_store_sdk_demo.ipynb     # Complete SDK demo
└── test-delta-lake.ipynb            # Delta Lake test notebook

examples/
├── simple_example.py               # Basic usage example
└── example_usage.py                # Detailed example

Dockerfile                          # Docker image with Spark 3.4 + Delta
Makefile                            # Build and run commands
Pipfile                             # Python dependencies
```

## 🧪 Comprehensive Demo

The SDK includes a comprehensive Jupyter notebook that demonstrates all features:

- **Basic feature selection** from individual feature groups
- **Multi-table joins** with custom join keys
- **Multiple output formats** (Spark/Pandas/Polars)
- **Advanced ML scenarios** with realistic business data
- **Performance analysis** and query plan inspection

Run the demo:

```bash
make jupyter
# Open notebooks/feature_store_sdk_demo.ipynb
```

## 🔧 API Reference

### FeatureStore

```python
fs = FeatureStore(base_path="/workspace/data/feature_store", spark=None)
```

### BatchFeatureGroup

```python
fg = fs.get_or_create_batch_feature_group(
    name="accounts",
    version=1,
    keys=["account_id"],
    data_location="/path/to/delta/table",  # Optional: explicit Delta table path
    description="Account information"
)
```

### FeatureView with Projections

```python
fv = fs.get_or_create_feature_view(
    name="features",
    version=1,
    base=base_feature_group,
    source_projections=[
        projection(
            source=feature_group,
            features=["col1", "col2", "col3"],     # Only these columns
            keys_map={"left_key": "right_key"},    # Join condition
            join_type="left"                       # left, inner, right, outer
        )
    ]
)
```

### QueryPlan

```python
plan = fv.plan()

# Multiple output formats
spark_df = plan.to_spark()     # pyspark.sql.DataFrame
pandas_df = plan.to_pandas()   # pandas.DataFrame  
polars_df = plan.to_polars()   # polars.DataFrame

# Utilities
plan.count()                   # Row count
plan.show(10)                  # Display first 10 rows
plan.collect()                 # Collect all rows
```

## 🛠 Technical Details

### Dependencies

- **Spark 3.4.4** with Hadoop 3 support
- **Delta Lake 2.4.0** (compatible with Spark 3.4)
- **Python 3.9** with compatible packages:
  - PySpark 3.4.4
  - Pandas 2.1.4  
  - Polars 0.20.31
  - PyArrow 14.0.2

### Docker Environment

The included Docker image provides:
- OpenJDK 11 (required for Spark 3.4)
- Pre-configured Delta Lake extensions
- Jupyter notebook server
- All required Python packages
- Spark UI available at http://localhost:4040

### Storage Format

All feature data is stored in **Delta Lake format**, providing:
- ACID transactions
- Schema evolution
- Time travel capabilities  
- Optimized Parquet storage
- Concurrent read/write support

## ✅ Validation

The SDK has been thoroughly tested with:

- ✅ Feature group creation and Delta Lake storage
- ✅ Precise feature selection via projections  
- ✅ Multi-table automatic joins with custom join keys
- ✅ Left/inner join support
- ✅ Multiple output formats (Spark, Pandas, Polars)
- ✅ Query plan execution and performance analysis
- ✅ Real-world ML feature engineering scenarios

## 🎉 Ready to Use

The Feature Store SDK is production-ready with:

- **No over-engineering** - Simple, clean API focused on your requirements
- **Delta Lake native** - Built for modern data lake architectures  
- **Multi-format support** - Works with your preferred data processing library
- **Automatic joins** - No complex SQL required
- **Feature selection** - Get exactly the features you need
- **Docker ready** - Easy deployment and development

Start using it today with `make jupyter` and explore the demo notebook!