"""
FeatureView class - Handles automatic joins between feature groups
"""
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import pandas as pd
import polars as pl

from .feature_group import BatchFeatureGroup
from .projection import Projection
from .query_plan import QueryPlan


# projection function is imported from projection.py


class FeatureView:
    """
    Feature View that automatically joins multiple feature groups
    """
    
    def __init__(
        self,
        name: str,
        version: int,
        base: BatchFeatureGroup,
        source_projections: List[Projection],
        description: str = ""
    ):
        """
        Initialize FeatureView
        
        Args:
            name: Feature view name
            version: Version number
            base: Base feature group
            source_projections: List of projection configurations
            description: Optional description
        """
        self.name = name
        self.version = version
        self.base = base
        self.source_projections = source_projections
        self.description = description
    
    def plan(self) -> QueryPlan:
        """
        Create a query plan that will execute the joins when materialized
        
        Returns:
            QueryPlan instance
        """
        return QueryPlan(self)
    
    def _build_query(self) -> DataFrame:
        """
        Build the joined DataFrame based on projections
        
        Returns:
            Joined Spark DataFrame
        """
        # Start with base feature group
        if not self.base.exists():
            raise ValueError(f"Base feature group {self.base.name} does not exist")
        
        result_df = self.base.read_data()
        
        # Apply filters to base table if specified in base projections
        base_projections = [p for p in self.source_projections if p.source == self.base]
        if base_projections:
            base_projection = base_projections[0]
            result_df = base_projection.apply_filters(result_df)
        
        # Handle joins with other feature groups
        for projection in self.source_projections:
            if projection.source != self.base:
                result_df = self._join_projection(result_df, projection)
        
        # Handle base table projection (select only specified features)
        if base_projections:
            projection = base_projections[0]  # Assume only one base projection
            
            # Build final column selection
            final_columns = []
            
            # Add base table specified features
            for feature in projection.features:
                if feature in result_df.columns:
                    final_columns.append(col(feature))
            
            # Add features from joined tables (non-base projections)
            for p in self.source_projections:
                if p.source != self.base:
                    for feature in p.features:
                        if feature in result_df.columns:
                            final_columns.append(col(feature))
            
            result_df = result_df.select(*final_columns)
        else:
            # If no base projection specified, select all columns
            all_columns = [col(col_name) for col_name in result_df.columns]
            result_df = result_df.select(*all_columns)
        
        return result_df
    
    def _join_projection(self, left_df: DataFrame, projection: Projection) -> DataFrame:
        """
        Join a projection with the current DataFrame
        
        Args:
            left_df: Left DataFrame
            projection: Projection to join
            
        Returns:
            Joined DataFrame
        """
        if not projection.source.exists():
            raise ValueError(f"Feature group {projection.source.name} does not exist")
        
        right_df = projection.source.read_data()
        
        # Apply filters if specified
        right_df = projection.apply_filters(right_df)
        
        # Build join keys list
        if projection.keys_map:
            join_keys = list(projection.keys_map.keys())  # Left side keys
            right_keys = list(projection.keys_map.values())  # Right side keys
        else:
            # Use base keys for join
            join_keys = [key for key in self.base.keys if key in right_df.columns]
            right_keys = join_keys  # Same keys on both sides
        
        if not join_keys:
            raise ValueError(f"No join condition could be built for {projection.source.name}")
        
        # Prepare right DataFrame - select only needed features and join keys
        right_select_cols = []
        
        # Include join keys 
        for key in right_keys:
            if key in right_df.columns:
                right_select_cols.append(col(key))
        
        # Add selected features (only if they don't conflict with join keys)
        for feature in projection.features:
            if feature in right_df.columns:
                right_select_cols.append(col(feature))
        
        right_df_selected = right_df.select(*right_select_cols)
        
        # Perform join using join keys directly (Spark will handle the join condition)
        if len(join_keys) == 1 and join_keys[0] == right_keys[0]:
            # Simple case: same column name on both sides
            joined_df = left_df.join(right_df_selected, join_keys, projection.join_type)
        else:
            # Complex case: different column names or multiple keys
            # We need to use explicit join conditions
            join_conditions = []
            for left_key, right_key in zip(join_keys, right_keys):
                join_conditions.append(left_df[left_key] == right_df_selected[right_key])
            
            join_condition = join_conditions[0]
            for condition in join_conditions[1:]:
                join_condition = join_condition & condition
            
            joined_df = left_df.join(right_df_selected, join_condition, projection.join_type)
            
            # Drop duplicate join keys from right side
            for right_key in right_keys:
                if right_key in left_df.columns and right_key in right_df_selected.columns:
                    # Check if we have duplicates
                    right_key_count = sum(1 for col_name in joined_df.columns if col_name == right_key)
                    if right_key_count > 1:
                        joined_df = joined_df.drop(right_df_selected[right_key])
        
        return joined_df
    
    def _build_pandas_query(self) -> pd.DataFrame:
        """
        Build the joined DataFrame using Pandas operations
        
        Returns:
            Joined Pandas DataFrame
        """
        # Start with base feature group
        if not self.base.exists():
            raise ValueError(f"Base feature group {self.base.name} does not exist")
        
        result_df = self.base.read_pandas()
        
        # Handle base table projection (select only specified features)
        base_projections = [p for p in self.source_projections if p.source == self.base]
        if base_projections:
            projection = base_projections[0]  # Assume only one base projection
            # Apply filters to base table
            result_df = projection.apply_filters(result_df)
            base_features = projection.features.copy()
        else:
            base_features = list(result_df.columns)
        
        # Handle joins with other feature groups
        for projection in self.source_projections:
            if projection.source != self.base:
                result_df = self._pandas_join_projection(result_df, projection)
                base_features.extend(projection.features)
        
        # Select only requested features
        available_features = [f for f in base_features if f in result_df.columns]
        result_df = result_df[available_features]
        
        return result_df
    
    def _pandas_join_projection(self, left_df: pd.DataFrame, projection) -> pd.DataFrame:
        """
        Join a projection using Pandas operations
        
        Args:
            left_df: Left DataFrame
            projection: Projection to join
            
        Returns:
            Joined DataFrame
        """
        if not projection.source.exists():
            raise ValueError(f"Feature group {projection.source.name} does not exist")
        
        right_df = projection.source.read_pandas()
        
        # Apply filters if specified
        right_df = projection.apply_filters(right_df)
        
        # Build join keys
        if projection.keys_map:
            left_keys = list(projection.keys_map.keys())
            right_keys = list(projection.keys_map.values())
        else:
            # Use base keys for join
            left_keys = [key for key in self.base.keys if key in right_df.columns]
            right_keys = left_keys
        
        if not left_keys:
            raise ValueError(f"No join condition could be built for {projection.source.name}")
        
        # Select only needed columns from right DataFrame
        right_select_cols = right_keys + [f for f in projection.features if f not in right_keys]
        right_df_selected = right_df[right_select_cols]
        
        # Perform join
        if projection.join_type == "left":
            how = "left"
        elif projection.join_type == "inner":
            how = "inner"
        else:
            how = "left"  # default
        
        # Handle key mapping for join
        if left_keys == right_keys:
            # Same column names
            joined_df = left_df.merge(right_df_selected, on=left_keys, how=how)
        else:
            # Different column names
            joined_df = left_df.merge(
                right_df_selected, 
                left_on=left_keys, 
                right_on=right_keys, 
                how=how
            )
            # Drop duplicate join columns from right side
            for right_key in right_keys:
                if right_key != left_keys[right_keys.index(right_key)]:
                    joined_df = joined_df.drop(columns=[right_key])
        
        return joined_df
    
    def __repr__(self) -> str:
        return f"FeatureView(name='{self.name}', version={self.version}, base='{self.base.name}', projections={len(self.source_projections)})"