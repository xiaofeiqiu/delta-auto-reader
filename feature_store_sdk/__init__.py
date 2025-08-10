"""
Simple Feature Store SDK

A lightweight feature store SDK that supports Delta Lake with Spark, 
with automatic joins between feature groups.
"""

from .feature_store import FeatureStore
from .feature_group import BatchFeatureGroup
from .feature_view import FeatureView
from .projection import projection
from .query_plan import QueryPlan
from .transform import Transform

__version__ = "0.1.0"
__all__ = [
    "FeatureStore",
    "BatchFeatureGroup", 
    "FeatureView",
    "projection",
    "QueryPlan",
    "Transform"
]