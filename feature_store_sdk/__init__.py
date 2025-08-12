"""
Simple Feature Store SDK

A lightweight feature store SDK that supports Delta Lake with Spark, 
with automatic joins between feature groups.
"""

# Import filters module first (no external dependencies)
from .filters import (
    Condition, AndCondition, OrCondition, NotCondition,
    FilterParser, condition, and_, or_, not_
)

# Import other modules that might have dependencies
try:
    from .feature_store import FeatureStore
    from .feature_group import BatchFeatureGroup
    from .feature_view import FeatureView
    from .projection import feature_source_projection
    from .query_plan import QueryPlan
    from .transform import Transform
    
    __all__ = [
        "FeatureStore",
        "BatchFeatureGroup", 
        "FeatureView",
        "feature_source_projection",
        "QueryPlan",
        "Transform",
        # Filter DSL components
        "Condition", "AndCondition", "OrCondition", "NotCondition",
        "FilterParser", "condition", "and_", "or_", "not_"
    ]
except ImportError as e:
    # If dependencies are missing, only export filter components
    __all__ = [
        "Condition", "AndCondition", "OrCondition", "NotCondition",
        "FilterParser", "condition", "and_", "or_", "not_"
    ]
    import warnings
    warnings.warn(f"Some dependencies missing: {e}. Only filter DSL is available.")

__version__ = "0.1.0"