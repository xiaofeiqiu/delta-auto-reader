"""
Projection class and helper functions for feature selection and joining
"""
from typing import List, Dict, Optional, Union, Any, Tuple
from .feature_group import BatchFeatureGroup
from .transform import Transform


class Projection:
    """
    Represents a projection configuration for feature selection and joining
    """
    
    def __init__(
        self,
        source: BatchFeatureGroup,
        features: List[str],
        keys_map: Optional[Dict[str, str]] = None,
        join_type: str = "inner",
        filters: Optional[Union[List[Tuple[str, str, Any]], Tuple[str, str, Any]]] = None,
        transform: Optional[List[Transform]] = None
    ):
        """
        Initialize Projection
        
        Args:
            source: Source feature group
            features: List of feature columns to select
            keys_map: Mapping of join keys {left_key: right_key}
            join_type: Type of join (inner, left, right, outer)
            filters: Filter conditions to apply to the source data.
                    Tuple format:
                    - Single: ("status", "==", "ACTIVE")
                    - Multiple: [("age", ">", 25), ("country", "in", ["US", "UK"])]
                    
                    Supported operators: ==, !=, >, >=, <, <=, in, not_in, is_null, is_not_null
            transform: List of Transform instances to apply feature transformations
        """
        self.source = source
        self.features = features
        self.keys_map = keys_map or {}
        self.join_type = join_type.lower()
        self.filters = self._normalize_filters(filters)
        self.transform = transform or []
        
        # Validate join type
        valid_joins = ["inner", "left", "right", "outer"]
        if self.join_type not in valid_joins:
            raise ValueError(f"join_type must be one of {valid_joins}")
    
    def _normalize_filters(self, filters: Optional[Union[List[Tuple[str, str, Any]], Tuple[str, str, Any]]]) -> List[Dict[str, Any]]:
        """
        Normalize filters to a consistent format
        
        Args:
            filters: Single filter tuple, or list of filter tuples
            
        Returns:
            List of filter dictionaries
        """
        if filters is None:
            return []
        
        # Handle single tuple filter
        if isinstance(filters, tuple):
            return [self._tuple_to_dict(filters)]
        
        # Handle list of filters
        if isinstance(filters, list):
            normalized = []
            for filter_item in filters:
                if isinstance(filter_item, tuple):
                    normalized.append(self._tuple_to_dict(filter_item))
                else:
                    raise ValueError(f"Filter item must be tuple, got {type(filter_item)}")
            return normalized
        
        raise ValueError(f"filters must be a tuple or list of tuples, got {type(filters)}")
    
    def _tuple_to_dict(self, filter_tuple: Tuple[str, str, Any]) -> Dict[str, Any]:
        """
        Convert tuple filter format to dictionary format
        
        Args:
            filter_tuple: Tuple in format (column, operator, value)
            
        Returns:
            Dictionary filter format
        """
        if len(filter_tuple) == 2:
            # Handle null operators that don't need values
            column, operator = filter_tuple
            if operator in ["is_null", "is_not_null"]:
                return {"column": column, "operator": operator}
            else:
                raise ValueError(f"Filter tuple for operator '{operator}' requires 3 elements: (column, operator, value)")
        elif len(filter_tuple) == 3:
            column, operator, value = filter_tuple
            return {"column": column, "operator": operator, "value": value}
        else:
            raise ValueError(f"Filter tuple must have 2 or 3 elements, got {len(filter_tuple)}: {filter_tuple}")
    
    def _validate_filter(self, filter_dict: Dict[str, Any]) -> None:
        """
        Validate a single filter dictionary
        
        Args:
            filter_dict: Filter dictionary to validate
        """
        required_keys = ["column", "operator"]
        for key in required_keys:
            if key not in filter_dict:
                raise ValueError(f"Filter missing required key: {key}")
        
        valid_operators = ["==", "!=", ">", ">=", "<", "<=", "in", "not_in", "is_null", "is_not_null"]
        if filter_dict["operator"] not in valid_operators:
            raise ValueError(f"Invalid operator: {filter_dict['operator']}. Must be one of {valid_operators}")
        
        # Value is not required for null checks
        null_ops = ["is_null", "is_not_null"]
        if filter_dict["operator"] not in null_ops and "value" not in filter_dict:
            raise ValueError(f"Filter with operator '{filter_dict['operator']}' requires 'value' key")
    
    def apply_filters(self, df):
        """
        Apply filters to a DataFrame (works with both Spark and Pandas)
        
        Args:
            df: DataFrame to filter (Spark or Pandas)
            
        Returns:
            Filtered DataFrame
        """
        if not self.filters:
            return df
        
        # Validate all filters
        for filter_dict in self.filters:
            self._validate_filter(filter_dict)
        
        # Detect DataFrame type and apply appropriate filtering
        try:
            # Try Spark DataFrame methods
            from pyspark.sql import DataFrame as SparkDataFrame
            from pyspark.sql.functions import col
            
            if isinstance(df, SparkDataFrame):
                return self._apply_spark_filters(df)
        except ImportError:
            pass
        
        # Try Pandas DataFrame
        try:
            import pandas as pd
            if isinstance(df, pd.DataFrame):
                return self._apply_pandas_filters(df)
        except ImportError:
            pass
        
        raise ValueError(f"Unsupported DataFrame type: {type(df)}")
    
    def _apply_spark_filters(self, df):
        """
        Apply filters to a Spark DataFrame
        
        Args:
            df: Spark DataFrame to filter
            
        Returns:
            Filtered Spark DataFrame
        """
        from pyspark.sql.functions import col
        
        for filter_dict in self.filters:
            column_name = filter_dict["column"]
            operator = filter_dict["operator"]
            value = filter_dict.get("value")
            
            if operator == "==":
                df = df.filter(col(column_name) == value)
            elif operator == "!=":
                df = df.filter(col(column_name) != value)
            elif operator == ">":
                df = df.filter(col(column_name) > value)
            elif operator == ">=":
                df = df.filter(col(column_name) >= value)
            elif operator == "<":
                df = df.filter(col(column_name) < value)
            elif operator == "<=":
                df = df.filter(col(column_name) <= value)
            elif operator == "in":
                df = df.filter(col(column_name).isin(value))
            elif operator == "not_in":
                df = df.filter(~col(column_name).isin(value))
            elif operator == "is_null":
                df = df.filter(col(column_name).isNull())
            elif operator == "is_not_null":
                df = df.filter(col(column_name).isNotNull())
        
        return df
    
    def _apply_pandas_filters(self, df):
        """
        Apply filters to a Pandas DataFrame
        
        Args:
            df: Pandas DataFrame to filter
            
        Returns:
            Filtered Pandas DataFrame
        """
        for filter_dict in self.filters:
            column_name = filter_dict["column"]
            operator = filter_dict["operator"]
            value = filter_dict.get("value")
            
            if operator == "==":
                df = df[df[column_name] == value]
            elif operator == "!=":
                df = df[df[column_name] != value]
            elif operator == ">":
                df = df[df[column_name] > value]
            elif operator == ">=":
                df = df[df[column_name] >= value]
            elif operator == "<":
                df = df[df[column_name] < value]
            elif operator == "<=":
                df = df[df[column_name] <= value]
            elif operator == "in":
                df = df[df[column_name].isin(value)]
            elif operator == "not_in":
                df = df[~df[column_name].isin(value)]
            elif operator == "is_null":
                df = df[df[column_name].isna()]
            elif operator == "is_not_null":
                df = df[df[column_name].notna()]
        
        return df
    
    def apply_filters_polars(self, lf):
        """
        Apply filters to a Polars LazyFrame
        
        Args:
            lf: Polars LazyFrame to filter
            
        Returns:
            Filtered Polars LazyFrame
        """
        if not self.filters:
            return lf
        
        # Validate all filters
        for filter_dict in self.filters:
            self._validate_filter(filter_dict)
        
        import polars as pl
        
        for filter_dict in self.filters:
            column_name = filter_dict["column"]
            operator = filter_dict["operator"]
            value = filter_dict.get("value")
            
            if operator == "==":
                lf = lf.filter(pl.col(column_name) == value)
            elif operator == "!=":
                lf = lf.filter(pl.col(column_name) != value)
            elif operator == ">":
                lf = lf.filter(pl.col(column_name) > value)
            elif operator == ">=":
                lf = lf.filter(pl.col(column_name) >= value)
            elif operator == "<":
                lf = lf.filter(pl.col(column_name) < value)
            elif operator == "<=":
                lf = lf.filter(pl.col(column_name) <= value)
            elif operator == "in":
                lf = lf.filter(pl.col(column_name).is_in(value))
            elif operator == "not_in":
                lf = lf.filter(~pl.col(column_name).is_in(value))
            elif operator == "is_null":
                lf = lf.filter(pl.col(column_name).is_null())
            elif operator == "is_not_null":
                lf = lf.filter(pl.col(column_name).is_not_null())
        
        return lf
    
    def __repr__(self) -> str:
        filter_str = f", filters={len(self.filters)}" if self.filters else ""
        return f"Projection(source='{self.source.name}', features={self.features}, join_type='{self.join_type}'{filter_str})"


def projection(
    source: BatchFeatureGroup,
    features: List[str],
    keys_map: Optional[Dict[str, str]] = None,
    join_type: str = "inner",
    filters: Optional[Union[List[Tuple[str, str, Any]], Tuple[str, str, Any]]] = None,
    transform: Optional[List[Transform]] = None
) -> Projection:
    """
    Helper function to create a Projection instance
    
    Args:
        source: Source feature group
        features: List of feature columns to select
        keys_map: Mapping of join keys {left_key: right_key}
        join_type: Type of join (inner, left, right, outer)
        filters: Filter conditions to apply to the source data.
                Tuple format:
                - Single: ("status", "==", "ACTIVE")  
                - Multiple: [("age", ">", 25), ("country", "in", ["US", "UK"])]
                
                Supported operators: ==, !=, >, >=, <, <=, in, not_in, is_null, is_not_null
        transform: List of Transform instances to apply feature transformations
        
    Returns:
        Projection instance
    """
    return Projection(
        source=source,
        features=features,
        keys_map=keys_map,
        join_type=join_type,
        filters=filters,
        transform=transform
    )