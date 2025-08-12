"""
Projection class and helper functions for feature selection and joining
"""
from typing import List, Dict, Optional, Union, Any, Tuple
from .feature_group import BatchFeatureGroup
from .transform import Transform
from .filters import FilterParser, BaseCondition


class FeatureSourceProjection:
    """
    Represents a projection configuration for feature selection and joining
    """
    
    def __init__(
        self,
        feature_group: BatchFeatureGroup,
        features: List[str],
        keys_map: Optional[Dict[str, str]] = None,
        join_type: str = "inner",
        where: Optional[Union[List, Tuple, BaseCondition]] = None,
        transforms: Optional[List[Transform]] = None
    ):
        """
        Initialize FeatureSourceProjection
        
        Args:
            feature_group: Source feature group
            features: List of feature columns to select
            keys_map: Mapping of join keys {left_key: right_key}
            join_type: Type of join (inner, left, right, outer)
            where: Filter conditions to apply to the source data.
                    Supports multiple formats:
                    - Simple: [("age", ">", 25), ("status", "==", "ACTIVE")]
                    - OR logic: [("country", "==", "US") | ("country", "==", "UK")]
                    - Complex: [("age", ">", 25), (("country", "==", "US") | ("segment", "==", "PREMIUM"))]
                    - NOT logic: [~("status", "==", "BANNED")]
                    
                    Supported operators: ==, !=, >, >=, <, <=, in, not_in, is_null, is_not_null, between, starts_with, ends_with, contains
            transforms: List of Transform instances to apply feature transformations
        """
        self.feature_group = feature_group
        self.features = features
        self.keys_map = keys_map or {}
        self.join_type = join_type.lower()
        self.where = FilterParser.parse_where_conditions(where)
        self.transforms = transforms or []
        
        # Validate join type
        valid_joins = ["inner", "left", "right", "outer"]
        if self.join_type not in valid_joins:
            raise ValueError(f"join_type must be one of {valid_joins}")
    
    
    def apply_filters(self, df):
        """
        Apply filters to a DataFrame (works with both Spark and Pandas)
        
        Args:
            df: DataFrame to filter (Spark or Pandas)
            
        Returns:
            Filtered DataFrame
        """
        if not self.where:
            return df
        
        # Detect DataFrame type and apply appropriate filtering
        try:
            # Try Spark DataFrame methods
            from pyspark.sql import DataFrame as SparkDataFrame
            
            if isinstance(df, SparkDataFrame):
                condition = self.where.to_spark_condition(df)
                return df.filter(condition)
        except ImportError:
            pass
        
        # Try Pandas DataFrame
        try:
            import pandas as pd
            if isinstance(df, pd.DataFrame):
                mask = self.where.to_pandas_condition(df)
                return df[mask]
        except ImportError:
            pass
        
        raise ValueError(f"Unsupported DataFrame type: {type(df)}")
    
    
    def apply_filters_polars(self, lf):
        """
        Apply filters to a Polars LazyFrame
        
        Args:
            lf: Polars LazyFrame to filter
            
        Returns:
            Filtered Polars LazyFrame
        """
        if not self.where:
            return lf
        
        condition = self.where.to_polars_condition()
        return lf.filter(condition)
    
    def __repr__(self) -> str:
        filter_str = f", where={len(self.where)}" if self.where else ""
        return f"FeatureSourceProjection(feature_group='{self.feature_group.name}', features={self.features}, join_type='{self.join_type}'{filter_str})"


def feature_source_projection(
    feature_group: BatchFeatureGroup,
    features: List[str],
    keys_map: Optional[Dict[str, str]] = None,
    join_type: str = "inner",
    where: Optional[Union[List, Tuple, BaseCondition]] = None,
    transforms: Optional[List[Transform]] = None
) -> FeatureSourceProjection:
    """
    Helper function to create a FeatureSourceProjection instance
    
    Args:
        feature_group: Source feature group
        features: List of feature columns to select
        keys_map: Mapping of join keys {left_key: right_key}
        join_type: Type of join (inner, left, right, outer)
        where: Filter conditions to apply to the source data.
                Supports multiple formats:
                - Simple: [("age", ">", 25), ("status", "==", "ACTIVE")]
                - OR logic: [("country", "==", "US") | ("country", "==", "UK")]
                - Complex: [("age", ">", 25), (("country", "==", "US") | ("segment", "==", "PREMIUM"))]
                - NOT logic: [~("status", "==", "BANNED")]
                
                Supported operators: ==, !=, >, >=, <, <=, in, not_in, is_null, is_not_null, between, starts_with, ends_with, contains
        transforms: List of Transform instances to apply feature transformations
        
    Returns:
        FeatureSourceProjection instance
    """
    return FeatureSourceProjection(
        feature_group=feature_group,
        features=features,
        keys_map=keys_map,
        join_type=join_type,
        where=where,
        transforms=transforms
    )