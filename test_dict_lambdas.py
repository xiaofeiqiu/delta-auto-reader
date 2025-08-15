#!/usr/bin/env python3
"""
Test script for dictionary-based lambda transforms
测试字典形式的 lambda 变换函数
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from feature_store_sdk.transform import Transform

print("=" * 70)
print("字典 LAMBDA 变换函数测试")
print("Dictionary Lambda Transform Functions Test")
print("=" * 70)

# 创建测试数据
df = pd.DataFrame({
    'date_col': pd.to_datetime(['2023-05-15', '2022-12-25', '2024-01-01']),
    'text_col': ['hello', 'world', 'test'],
    'status_col': ['active', 'inactive', 'active'],
    'email1': ['user@example.com', None, 'jane@example.com'],
    'email2': [None, 'backup@example.com', None],
    'email3': ['fallback@example.com', 'fallback2@example.com', 'fallback3@example.com']
})

print("测试数据 (Test Data):")
print(df)
print()

# 模拟 TRANSFORM_LAMBDAS 字典
transforms = {
    "year": lambda col, output_name=None: Transform(
        output_name or f"{col}_year",
        lambda df: df[col].dt.year if hasattr(df[col], 'dt') else pd.to_datetime(df[col]).dt.year
    ),
    "month": lambda col, output_name=None: Transform(
        output_name or f"{col}_month", 
        lambda df: df[col].dt.month if hasattr(df[col], 'dt') else pd.to_datetime(df[col]).dt.month
    ),
    "day": lambda col, output_name=None: Transform(
        output_name or f"{col}_day",
        lambda df: df[col].dt.day if hasattr(df[col], 'dt') else pd.to_datetime(df[col]).dt.day
    ),
    "upper": lambda col, output_name=None: Transform(
        output_name or f"{col}_upper",
        lambda df: df[col].str.upper()
    ),
    "lpad": lambda col, length, pad_string=' ', output_name=None: Transform(
        output_name or f"{col}_lpad",
        lambda df: df[col].astype(str).str.rjust(length, pad_string)
    ),
    "nullif": lambda col, value, output_name=None: Transform(
        output_name or f"{col}_nullif",
        lambda df: df[col].where(df[col] != value, None)
    ),
    "coalesce": lambda *cols, output_name="coalesce_result": Transform(
        output_name,
        lambda df: (
            lambda available_cols: (
                available_cols[0].copy() if len(available_cols) == 1 
                else available_cols[0].fillna(available_cols[1]).fillna(available_cols[2]) if len(available_cols) >= 3
                else available_cols[0].fillna(available_cols[1]) if len(available_cols) == 2
                else pd.Series([None] * len(df), name=output_name, index=df.index)
            )
        )([df[col] for col in cols if col in df.columns])
    )
}

print("方法 1: 直接使用字典 lambda")
print("Method 1: Direct dictionary lambda usage")
print("-" * 50)

# 测试各种变换
test_cases = [
    ('year', ['date_col'], [2023, 2022, 2024]),
    ('month', ['date_col'], [5, 12, 1]),
    ('day', ['date_col'], [15, 25, 1]),
    ('upper', ['text_col'], ['HELLO', 'WORLD', 'TEST']),
    ('lpad', ['text_col', 8, '0'], ['000hello', '000world', '0000test']),
    ('nullif', ['status_col', 'inactive'], ['active', None, 'active']),
    ('coalesce', ['email1', 'email2', 'email3'], ['user@example.com', 'backup@example.com', 'jane@example.com'])
]

for func_name, args, expected in test_cases:
    print(f"transforms['{func_name}']({', '.join(map(str, args))})")
    transform = transforms[func_name](*args)
    result = transform.apply_pandas(df)
    print(f"  结果 (Result): {result.tolist()}")
    print(f"  期望 (Expected): {expected}")
    
    try:
        assert result.tolist() == expected
        print(f"  ✅ 测试通过 (Test passed)")
    except AssertionError:
        print(f"  ❌ 测试失败 (Test failed)")
    print()

print("=" * 70)
print("方法 2: 动态创建变换列表")
print("Method 2: Dynamic transform list creation")
print("=" * 70)

# 配置驱动的变换创建
transform_configs = [
    ('year', ['date_col']),
    ('upper', ['text_col']),
    ('lpad', ['text_col', 10, '-']),
    ('nullif', ['status_col', 'inactive']),
    ('coalesce', ['email1', 'email2', 'email3'])
]

print("transform_configs = [")
for func_name, args in transform_configs:
    print(f"    ('{func_name}', {args}),")
print("]")
print()

print("# 动态创建变换")
print("dynamic_transforms = [transforms[func](*args) for func, args in transform_configs]")
print()

dynamic_transforms = [transforms[func](*args) for func, args in transform_configs]

print("创建的变换:")
for i, transform in enumerate(dynamic_transforms):
    config = transform_configs[i]
    print(f"  {i+1}. {config[0]}({', '.join(map(str, config[1]))}) -> {transform.name}")

print()
print("=" * 70)
print("方法 3: 实际使用示例")
print("Method 3: Real usage example")
print("=" * 70)

print("# 实际代码中的使用方式")
print("from feature_store_sdk.projection import TRANSFORM_LAMBDAS")
print()
print("# 创建变换")
print("my_transforms = [")
print("    TRANSFORM_LAMBDAS['year']('transaction_date'),")
print("    TRANSFORM_LAMBDAS['upper']('customer_name'),")
print("    TRANSFORM_LAMBDAS['lpad']('account_id', 8, '0'),")
print("    TRANSFORM_LAMBDAS['nullif']('status', 'inactive'),")
print("    TRANSFORM_LAMBDAS['coalesce']('email1', 'email2', 'email3')")
print("]")
print()

print("# 或者更简洁的方式")
print("apply_transform = lambda name, *args: TRANSFORM_LAMBDAS[name](*args)")
print()
print("my_transforms = [")
print("    apply_transform('year', 'transaction_date'),")
print("    apply_transform('upper', 'customer_name'),")
print("    apply_transform('lpad', 'account_id', 8, '0'),")
print("    apply_transform('nullif', 'status', 'inactive'),")
print("    apply_transform('coalesce', 'email1', 'email2', 'email3')")
print("]")
print()

print("✅ 字典 Lambda 方式的优点:")
print("   1. 所有函数在一个字典中，易于管理")
print("   2. 可以动态添加新函数")
print("   3. 支持配置驱动的变换创建")
print("   4. 代码非常简洁")
print()

print("🎯 用法总结:")
print("   transforms['year']('date_col')           # 提取年份")
print("   transforms['upper']('text_col')          # 转换大写")
print("   transforms['lpad']('id_col', 8, '0')     # 左填充")
print("   transforms['nullif']('status', 'inactive') # 空值转换")
print("   transforms['coalesce']('col1', 'col2')   # 合并列")

print()
print("🎉 字典 Lambda 实现完成！")