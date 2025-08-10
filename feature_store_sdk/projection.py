"""
Projection class and helper functions for feature selection and joining
"""
from typing import List, Dict, Optional
from .feature_group import BatchFeatureGroup


class Projection:
    """
    Represents a projection configuration for feature selection and joining
    """
    
    def __init__(
        self,
        source: BatchFeatureGroup,
        features: List[str],
        keys_map: Optional[Dict[str, str]] = None,
        join_type: str = "inner"
    ):
        """
        Initialize Projection
        
        Args:
            source: Source feature group
            features: List of feature columns to select
            keys_map: Mapping of join keys {left_key: right_key}
            join_type: Type of join (inner, left, right, outer)
        """
        self.source = source
        self.features = features
        self.keys_map = keys_map or {}
        self.join_type = join_type.lower()
        
        # Validate join type
        valid_joins = ["inner", "left", "right", "outer"]
        if self.join_type not in valid_joins:
            raise ValueError(f"join_type must be one of {valid_joins}")
    
    def __repr__(self) -> str:
        return f"Projection(source='{self.source.name}', features={self.features}, join_type='{self.join_type}')"


def projection(
    source: BatchFeatureGroup,
    features: List[str],
    keys_map: Optional[Dict[str, str]] = None,
    join_type: str = "inner"
) -> Projection:
    """
    Helper function to create a Projection instance
    
    Args:
        source: Source feature group
        features: List of feature columns to select
        keys_map: Mapping of join keys {left_key: right_key}
        join_type: Type of join (inner, left, right, outer)
        
    Returns:
        Projection instance
    """
    return Projection(
        source=source,
        features=features,
        keys_map=keys_map,
        join_type=join_type
    )