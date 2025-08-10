"""
FeatureStore class - Main entry point for the feature store SDK
"""
import os
from typing import Dict, Optional
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from .feature_group import BatchFeatureGroup
from .feature_view import FeatureView


class FeatureStore:
    """
    Simple Feature Store implementation using Delta Lake
    """
    
    def __init__(self, base_path: str = "/workspace/data/feature_store", spark: Optional[SparkSession] = None):
        """
        Initialize FeatureStore
        
        Args:
            base_path: Base path for storing feature store data
            spark: Optional existing Spark session
        """
        self.base_path = base_path
        self._feature_groups: Dict[str, BatchFeatureGroup] = {}
        self._feature_views: Dict[str, FeatureView] = {}
        
        if spark is None:
            self._spark = self._create_spark_session()
        else:
            self._spark = spark
            
        # Ensure base directory exists
        os.makedirs(base_path, exist_ok=True)
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Delta Lake support"""
        builder = SparkSession.builder.appName("FeatureStore") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    @property
    def spark(self) -> SparkSession:
        """Get the Spark session"""
        return self._spark
    
    def get_or_create_batch_feature_group(
        self, 
        name: str, 
        version: int = 1, 
        keys: list = None,
        data_location: str = None,
        description: str = ""
    ) -> BatchFeatureGroup:
        """
        Get or create a batch feature group
        
        Args:
            name: Feature group name
            version: Version number
            keys: Primary key columns
            data_location: Explicit data location (Delta table path)
            description: Optional description
            
        Returns:
            BatchFeatureGroup instance
        """
        fg_id = f"{name}_v{version}"
        
        if fg_id not in self._feature_groups:
            fg_path = os.path.join(self.base_path, "feature_groups", name, f"v{version}")
            
            self._feature_groups[fg_id] = BatchFeatureGroup(
                name=name,
                version=version,
                keys=keys or [],
                path=fg_path,
                data_location=data_location,
                spark=self._spark,
                description=description
            )
        
        return self._feature_groups[fg_id]
    
    def get_or_create_feature_view(
        self,
        name: str,
        version: int = 1,
        base: BatchFeatureGroup = None,
        source_projections: list = None,
        description: str = ""
    ) -> FeatureView:
        """
        Get or create a feature view with automatic joins
        
        Args:
            name: Feature view name
            version: Version number
            base: Base feature group for the view
            source_projections: List of projection configurations
            description: Optional description
            
        Returns:
            FeatureView instance
        """
        fv_id = f"{name}_v{version}"
        
        if fv_id not in self._feature_views:
            self._feature_views[fv_id] = FeatureView(
                name=name,
                version=version,
                base=base,
                source_projections=source_projections or [],
                description=description
            )
        
        return self._feature_views[fv_id]