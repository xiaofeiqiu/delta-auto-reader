"""
QueryPlan class - Executes feature view queries and converts to different formats
"""
from typing import TYPE_CHECKING, Optional
import pandas as pd
import polars as pl
from pyspark.sql import DataFrame, SparkSession

if TYPE_CHECKING:
    from .feature_view import FeatureView


class QueryPlan:
    """
    Query execution plan that can materialize results in different formats
    """
    
    def __init__(self, feature_view: 'FeatureView'):
        """
        Initialize QueryPlan
        
        Args:
            feature_view: FeatureView to execute
        """
        self.feature_view = feature_view
        self._spark_result_df = None
        self._pandas_result_df = None
        self._polars_result_df = None
    
    def _execute_spark(self) -> DataFrame:
        """
        Execute the query plan using Spark and cache result
        
        Returns:
            Spark DataFrame
        """
        if self._spark_result_df is None:
            self._spark_result_df = self.feature_view._build_query()
        return self._spark_result_df
    
    def _execute_pandas(self) -> pd.DataFrame:
        """
        Execute the query plan and get pandas result (without Spark)
        
        Returns:
            Pandas DataFrame
        """
        if self._pandas_result_df is None:
            self._pandas_result_df = self.feature_view._build_pandas_query()
        return self._pandas_result_df
    
    def _execute_polars(self) -> pl.DataFrame:
        """
        Execute the query plan using Polars lazy evaluation
        
        Returns:
            Polars DataFrame
        """
        if self._polars_result_df is None:
            self._polars_result_df = self.feature_view._build_polars_query()
        return self._polars_result_df
    
    def to_spark(self, spark: Optional[SparkSession] = None) -> DataFrame:
        """
        Get result as Spark DataFrame
        
        Args:
            spark: SparkSession instance (required for Spark output)
        
        Returns:
            Spark DataFrame
        """
        if spark is None:
            raise ValueError("SparkSession must be provided to to_spark() method")
        return self._execute_spark()
    
    def to_pandas(self) -> pd.DataFrame:
        """
        Get result as Pandas DataFrame
        
        Returns:
            Pandas DataFrame
        """
        return self._execute_pandas()
    
    def to_polars(self) -> pl.DataFrame:
        """
        Get result as Polars DataFrame using lazy evaluation
        
        Returns:
            Polars DataFrame
        """
        return self._execute_polars()
    
    def count(self) -> int:
        """
        Get the number of rows in the result
        
        Returns:
            Row count
        """
        return self._execute_spark().count()
    
    def show(self, n: int = 20, truncate: bool = True) -> None:
        """
        Show the first n rows of the result
        
        Args:
            n: Number of rows to show
            truncate: Whether to truncate long strings
        """
        self._execute_spark().show(n, truncate)
    
    def collect(self) -> list:
        """
        Collect all rows as a list of Row objects
        
        Returns:
            List of Row objects
        """
        return self._execute_spark().collect()
    
    def __repr__(self) -> str:
        return f"QueryPlan(feature_view='{self.feature_view.name}')"