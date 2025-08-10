"""
QueryPlan class - Executes feature view queries and converts to different formats
"""
from typing import TYPE_CHECKING
import pandas as pd
import polars as pl
from pyspark.sql import DataFrame

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
        self._result_df = None
    
    def _execute(self) -> DataFrame:
        """
        Execute the query plan and cache result
        
        Returns:
            Spark DataFrame
        """
        if self._result_df is None:
            self._result_df = self.feature_view._build_query()
        return self._result_df
    
    def to_spark(self) -> DataFrame:
        """
        Get result as Spark DataFrame
        
        Returns:
            Spark DataFrame
        """
        return self._execute()
    
    def to_pandas(self) -> pd.DataFrame:
        """
        Get result as Pandas DataFrame
        
        Returns:
            Pandas DataFrame
        """
        spark_df = self._execute()
        return spark_df.toPandas()
    
    def to_polars(self) -> pl.DataFrame:
        """
        Get result as Polars DataFrame
        
        Returns:
            Polars DataFrame
        """
        pandas_df = self.to_pandas()
        return pl.from_pandas(pandas_df)
    
    def count(self) -> int:
        """
        Get the number of rows in the result
        
        Returns:
            Row count
        """
        return self._execute().count()
    
    def show(self, n: int = 20, truncate: bool = True) -> None:
        """
        Show the first n rows of the result
        
        Args:
            n: Number of rows to show
            truncate: Whether to truncate long strings
        """
        self._execute().show(n, truncate)
    
    def collect(self) -> list:
        """
        Collect all rows as a list of Row objects
        
        Returns:
            List of Row objects
        """
        return self._execute().collect()
    
    def __repr__(self) -> str:
        return f"QueryPlan(feature_view='{self.feature_view.name}')"