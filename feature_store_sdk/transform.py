"""
Transform class for feature transformations in projections
"""
from typing import Any, Callable, Dict, Optional, Union
import inspect


class Transform:
    """
    Represents a feature transformation to be applied during projection
    """
    
    def __init__(self, name: str, func: Callable):
        """
        Initialize Transform
        
        Args:
            name: Name of the transformed feature (output column name)
            func: Function to apply for transformation
                 Can be:
                 - A lambda function: lambda df: df['age'] * 2
                 - A regular function that takes a DataFrame and returns a Series/Column
                 - A function that takes specific columns as arguments
        """
        self.name = name
        self.func = func
        
        # Analyze the function signature
        self._analyze_function()
    
    def _analyze_function(self):
        """
        Analyze the function signature to understand how to apply it
        """
        try:
            sig = inspect.signature(self.func)
            self.params = list(sig.parameters.keys())
            self.param_count = len(self.params)
        except (ValueError, TypeError):
            # Built-in functions or other cases where signature analysis fails
            self.params = []
            self.param_count = 0
    
    def apply_spark(self, df, features: Dict[str, str] = None):
        """
        Apply transformation to Spark DataFrame
        
        Args:
            df: Spark DataFrame
            features: Dictionary mapping feature names to column names
            
        Returns:
            Transformed Spark Column
        """
        from pyspark.sql.functions import col, udf
        from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType
        
        try:
            # For DataFrame-level transforms, we need to handle them differently for Spark
            if self.param_count == 1 and (not self.params or self.params[0] in ['df', 'dataframe']):
                # This is a DataFrame-level transform - we need to create a UDF that operates on the entire row
                # Instead of converting to pandas, we'll create a UDF that can access all columns
                
                # Get all column names to pass to the UDF
                all_columns = df.columns
                
                # Create a UDF that reconstructs a row-like object for the function
                def dataframe_transform_udf(*row_values):
                    # Create a simple row-like dict for the function to use
                    row_dict = dict(zip(all_columns, row_values))
                    
                    # Create a minimal DataFrame-like object for compatibility
                    class SparkRowDataFrame:
                        def __init__(self, row_data, columns):
                            self.data = row_data
                            self.columns = columns
                            
                        def __getitem__(self, key):
                            return self.data.get(key)
                            
                        def get(self, key, default=None):
                            return self.data.get(key, default)
                    
                    row_df = SparkRowDataFrame(row_dict, all_columns)
                    
                    try:
                        result = self.func(row_df)
                        return result
                    except Exception as e:
                        # If the function doesn't work with our mock DataFrame, 
                        # fallback to passing the row dict directly
                        return self.func(row_dict)
                
                # Determine return type
                return_type = self._infer_return_type()
                spark_udf = udf(dataframe_transform_udf, return_type)
                
                # Apply UDF with all columns
                return spark_udf(*[col(c) for c in all_columns]).alias(self.name)
            else:
                # Function expects individual columns or values
                # Create UDF for complex transformations
                return_type = self._infer_return_type()
                spark_udf = udf(self.func, return_type)
                
                # Apply UDF based on function parameters
                if self.param_count == 0:
                    # No parameters - apply to first available column
                    first_col = df.columns[0]
                    return spark_udf(col(first_col)).alias(self.name)
                elif self.param_count == 1:
                    # Single parameter - try to match by parameter name or use first column
                    param_name = self.params[0] if self.params else None
                    if param_name and param_name in df.columns:
                        return spark_udf(col(param_name)).alias(self.name)
                    else:
                        # Use first available column
                        first_col = df.columns[0]
                        return spark_udf(col(first_col)).alias(self.name)
                else:
                    # Multiple parameters - match by parameter names
                    cols_to_use = []
                    for param in self.params:
                        if param in df.columns:
                            cols_to_use.append(col(param))
                        else:
                            raise ValueError(f"Parameter '{param}' not found in DataFrame columns: {df.columns}")
                    return spark_udf(*cols_to_use).alias(self.name)
                    
        except Exception as e:
            raise ValueError(f"Failed to apply Spark transformation '{self.name}': {e}")
    
    def apply_pandas(self, df):
        """
        Apply transformation to Pandas DataFrame
        
        Args:
            df: Pandas DataFrame
            
        Returns:
            Transformed Pandas Series
        """
        try:
            # Try to apply function directly with DataFrame
            if self.param_count == 1 and (not self.params or self.params[0] in ['df', 'dataframe']):
                # Function expects a DataFrame
                result = self.func(df)
                if hasattr(result, 'name'):  # It's already a Series
                    result.name = self.name
                    return result
                else:
                    # Convert to Series
                    import pandas as pd
                    return pd.Series(result, name=self.name, index=df.index)
            else:
                # Function expects individual columns or values
                if self.param_count == 0:
                    # No parameters - apply to first available column
                    first_col = df.columns[0]
                    result = self.func()
                    if hasattr(result, '__len__') and len(result) == len(df):
                        import pandas as pd
                        return pd.Series(result, name=self.name, index=df.index)
                    else:
                        # Broadcast single value
                        import pandas as pd
                        return pd.Series([result] * len(df), name=self.name, index=df.index)
                elif self.param_count == 1:
                    # Single parameter - try to match by parameter name or use first column
                    param_name = self.params[0] if self.params else None
                    if param_name and param_name in df.columns:
                        # Handle Series operations - apply element-wise
                        series_data = df[param_name]
                        if hasattr(series_data, 'apply'):  # It's a pandas Series
                            result = series_data.apply(self.func)
                        else:
                            result = self.func(series_data)
                    else:
                        # Use first available column
                        first_col = df.columns[0]
                        series_data = df[first_col]
                        if hasattr(series_data, 'apply'):  # It's a pandas Series
                            result = series_data.apply(self.func)
                        else:
                            result = self.func(series_data)
                    
                    if hasattr(result, 'name'):
                        result.name = self.name
                        return result
                    else:
                        import pandas as pd
                        return pd.Series(result, name=self.name, index=df.index)
                else:
                    # Multiple parameters - match by parameter names
                    args = []
                    for param in self.params:
                        if param in df.columns:
                            args.append(df[param])
                        else:
                            raise ValueError(f"Parameter '{param}' not found in DataFrame columns: {df.columns.tolist()}")
                    
                    # For multiple parameters, we need to apply the function element-wise
                    import pandas as pd
                    result = pd.Series(index=df.index, dtype=object, name=self.name)
                    for idx in df.index:
                        row_args = [arg.loc[idx] if hasattr(arg, 'loc') else arg for arg in args]
                        result.loc[idx] = self.func(*row_args)
                    if hasattr(result, 'name'):
                        result.name = self.name
                        return result
                    else:
                        import pandas as pd
                        return pd.Series(result, name=self.name, index=df.index)
                        
        except Exception as e:
            raise ValueError(f"Failed to apply Pandas transformation '{self.name}': {e}")
    
    def apply_polars(self, df):
        """
        Apply transformation to Polars DataFrame
        
        Args:
            df: Polars DataFrame
            
        Returns:
            Transformed Polars Series
        """
        try:
            import polars as pl
            
            # Try to apply function directly with DataFrame
            if self.param_count == 1 and (not self.params or self.params[0] in ['df', 'dataframe']):
                # Function expects a DataFrame
                result = self.func(df)
                if hasattr(result, 'alias'):  # It's already a Polars expression/series
                    return result.alias(self.name)
                else:
                    # Convert to Series
                    return pl.Series(name=self.name, values=result)
            else:
                # Convert to pandas for easier transformation, then back to polars
                pandas_df = df.to_pandas()
                pandas_result = self.apply_pandas(pandas_df)
                return pl.from_pandas(pandas_result)
                        
        except Exception as e:
            raise ValueError(f"Failed to apply Polars transformation '{self.name}': {e}")
    
    def _infer_return_type(self):
        """
        Infer the return type for Spark UDF
        """
        from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, BooleanType
        
        # Default to StringType for safety
        return StringType()
    
    def __repr__(self) -> str:
        return f"Transform(name='{self.name}', func={self.func.__name__ if hasattr(self.func, '__name__') else str(self.func)})"