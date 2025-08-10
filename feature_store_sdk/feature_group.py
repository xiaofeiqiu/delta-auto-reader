"""
BatchFeatureGroup class - Represents a feature group with Delta Lake storage
"""
import os
from typing import List, Optional, Union
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
import polars as pl
from deltalake import DeltaTable


class BatchFeatureGroup:
    """
    Batch Feature Group backed by Delta Lake format
    """
    
    def __init__(
        self, 
        name: str, 
        version: int,
        keys: List[str],
        path: str = None,
        data_location: str = None,
        spark: SparkSession = None,
        description: str = ""
    ):
        """
        Initialize BatchFeatureGroup
        
        Args:
            name: Feature group name
            version: Version number
            keys: Primary key columns
            path: Default path for this feature group (if data_location not provided)
            data_location: Explicit data location (Delta table path)
            spark: Spark session
            description: Optional description
        """
        self.name = name
        self.version = version
        self.keys = keys
        self.description = description
        self._spark = spark
        
        # Use data_location if provided, otherwise use default path
        if data_location:
            self.data_location = data_location
        else:
            self.data_location = path or f"/workspace/data/feature_store/feature_groups/{name}/v{version}"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.data_location), exist_ok=True)
    
    def write_data(self, df: DataFrame, mode: str = "overwrite") -> None:
        """
        Write data to the feature group's Delta table
        
        Args:
            df: Spark DataFrame to write
            mode: Write mode ('overwrite', 'append', etc.)
        """
        if self._spark is None:
            raise ValueError("Spark session is required")
            
        df.write.format("delta").mode(mode).save(self.data_location)
    
    def read_data(self) -> DataFrame:
        """
        Read data from the feature group's Delta table (Spark mode)
        
        Returns:
            Spark DataFrame
        """
        # Try to get active Spark session first, then fall back to stored session
        try:
            spark = SparkSession.getActiveSession()
            if spark is not None:
                return spark.read.format("delta").load(self.data_location)
        except:
            pass
            
        # Fall back to stored Spark session
        if self._spark is None:
            raise ValueError("Spark session is required")
            
        return self._spark.read.format("delta").load(self.data_location)
    
    def read_pandas(self) -> pd.DataFrame:
        """
        Read data from the feature group's Delta table as Pandas DataFrame
        
        Returns:
            Pandas DataFrame
        """
        try:
            dt = DeltaTable(self.data_location)
            return dt.to_pandas()
        except Exception:
            # Fallback to reading via Spark if deltalake package doesn't work
            return self.read_data().toPandas()
    
    def read_polars(self) -> pl.DataFrame:
        """
        Read data from the feature group's Delta table as Polars DataFrame
        
        Returns:
            Polars DataFrame
        """
        return pl.from_pandas(self.read_pandas())
    
    def exists(self) -> bool:
        """
        Check if the feature group data exists
        
        Returns:
            True if Delta table exists
        """
        return os.path.exists(os.path.join(self.data_location, "_delta_log"))
    
    def get_schema(self):
        """
        Get the schema of the feature group
        
        Returns:
            DataFrame schema if exists, None otherwise
        """
        if self.exists():
            return self.read_data().schema
        return None
    
    def __repr__(self) -> str:
        return f"BatchFeatureGroup(name='{self.name}', version={self.version}, keys={self.keys}, location='{self.data_location}')"