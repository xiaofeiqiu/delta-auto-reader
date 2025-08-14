"""
Enhanced Filter DSL with Python-style operators for intuitive condition building.

This module provides a production-ready way to build complex filter conditions using
familiar Python operators like &, |, and ~.

Examples:
    # Using the c() helper function
    from feature_store_sdk.filters import c
    
    # Simple conditions  
    where = [c("age", ">", 25), c("status", "==", "ACTIVE")]
    
    # OR conditions with operators
    where = [c("country", "==", "US") | c("country", "==", "UK")]
    
    # Complex nested conditions
    where = [
        c("age", ">", 25),
        (c("country", "==", "US") | c("segment", "==", "PREMIUM")),
        ~c("status", "==", "BANNED")
    ]
"""

from typing import Any, List, Union, Dict, Optional, Tuple
from abc import ABC, abstractmethod
import json

# Delta Lake filter types (based on delta-rs FilterType definition)
FilterLiteralType = Tuple[str, str, Any]
FilterConjunctionType = List[FilterLiteralType]  
FilterDNFType = List[FilterConjunctionType]
FilterType = Union[FilterConjunctionType, FilterDNFType]

# Try to import from deltalake if available, otherwise use our own types
try:
    from deltalake import DeltaTable
    from deltalake.table import FilterType as DeltaFilterType
    DELTALAKE_AVAILABLE = True
    # Use the official FilterType if available
    FilterType = DeltaFilterType
except ImportError:
    DELTALAKE_AVAILABLE = False
    # Fallback to our own definition


class BaseCondition(ABC):
    """
    Base class for all filter conditions.
    Provides support for Python-style logical operators.
    """
    
    def __and__(self, other: 'BaseCondition') -> 'AndCondition':
        """Support for & operator (AND logic)"""
        # Flatten nested AND conditions
        left_conditions = [self] if not isinstance(self, AndCondition) else self.conditions
        right_conditions = [other] if not isinstance(other, AndCondition) else other.conditions
        return AndCondition(*(left_conditions + right_conditions))
    
    def __or__(self, other: 'BaseCondition') -> 'OrCondition':
        """Support for | operator (OR logic)"""
        # Flatten nested OR conditions
        left_conditions = [self] if not isinstance(self, OrCondition) else self.conditions
        right_conditions = [other] if not isinstance(other, OrCondition) else other.conditions
        return OrCondition(*(left_conditions + right_conditions))
    
    def __invert__(self) -> 'NotCondition':
        """Support for ~ operator (NOT logic)"""
        return NotCondition(self)
    
    @abstractmethod
    def to_spark_condition(self, df: Any) -> Any:
        """Convert to Spark SQL condition"""
        pass
    
    @abstractmethod
    def to_pandas_condition(self, df: Any) -> Any:
        """Convert to Pandas boolean mask"""
        pass
    
    @abstractmethod
    def to_polars_condition(self) -> Any:
        """Convert to Polars expression"""
        pass
    
    @abstractmethod
    def to_pyarrow_condition(self) -> Any:
        """Convert to PyArrow compute expression"""
        pass
    
    @abstractmethod
    def to_delta_table_filter(self) -> FilterType:
        """Convert to Delta Table filter for DeltaTable.to_pandas()"""
        pass
    
    @abstractmethod
    def __repr__(self) -> str:
        pass
    
    def __str__(self) -> str:
        """String representation for user-friendly display"""
        return self.__repr__()
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Serialize condition to dictionary for JSON serialization"""
        pass
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseCondition':
        """Deserialize condition from dictionary"""
        pass
    
    def to_json(self) -> str:
        """Serialize condition to JSON string"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'BaseCondition':
        """Deserialize condition from JSON string"""
        data = json.loads(json_str)
        return _deserialize_condition(data)


class Condition(BaseCondition):
    """
    Represents a single filter condition.
    
    Args:
        column: Column name to filter on
        operator: Comparison operator (==, !=, >, >=, <, <=, in, not_in, is_null, is_not_null, etc.)
        value: Value to compare against (not needed for null checks)
    """
    
    def __init__(self, column: str, operator: str, value: Any = None) -> None:
        self.column = column
        self.operator = operator
        self.value = value
        self._validate()
    
    def _validate(self):
        """Validate the condition parameters"""
        valid_operators = [
            "==", "!=", ">", ">=", "<", "<=", 
            "in", "not_in", "is_null", "is_not_null",
            "between", "starts_with", "ends_with", "contains"
        ]
        
        if self.operator not in valid_operators:
            raise ValueError(f"Invalid operator '{self.operator}'. Must be one of: {valid_operators}")
        
        # Check if value is required for this operator
        # Note: None is a valid value for == and != operators (NULL comparison)
        null_ops = ["is_null", "is_not_null"]
        if self.operator not in null_ops and self.value is None and self.operator not in ["==", "!="]:
            raise ValueError(f"Operator '{self.operator}' requires a value")
    
    def to_spark_condition(self, df):
        """Convert to Spark SQL condition"""
        from pyspark.sql.functions import col
        
        spark_col = col(self.column)
        
        if self.operator == "==":
            return spark_col == self.value
        elif self.operator == "!=":
            return spark_col != self.value
        elif self.operator == ">":
            return spark_col > self.value
        elif self.operator == ">=":
            return spark_col >= self.value
        elif self.operator == "<":
            return spark_col < self.value
        elif self.operator == "<=":
            return spark_col <= self.value
        elif self.operator == "in":
            return spark_col.isin(self.value)
        elif self.operator == "not_in":
            return ~spark_col.isin(self.value)
        elif self.operator == "is_null":
            return spark_col.isNull()
        elif self.operator == "is_not_null":
            return spark_col.isNotNull()
        elif self.operator == "between":
            min_val, max_val = self.value
            return spark_col.between(min_val, max_val)
        elif self.operator == "starts_with":
            return spark_col.startswith(self.value)
        elif self.operator == "ends_with":
            return spark_col.endswith(self.value)
        elif self.operator == "contains":
            return spark_col.contains(self.value)
        else:
            raise ValueError(f"Unsupported operator for Spark: {self.operator}")
    
    def to_pandas_condition(self, df):
        """Convert to Pandas boolean mask"""
        series = df[self.column]
        
        if self.operator == "==":
            return series == self.value
        elif self.operator == "!=":
            return series != self.value
        elif self.operator == ">":
            return series > self.value
        elif self.operator == ">=":
            return series >= self.value
        elif self.operator == "<":
            return series < self.value
        elif self.operator == "<=":
            return series <= self.value
        elif self.operator == "in":
            return series.isin(self.value)
        elif self.operator == "not_in":
            return ~series.isin(self.value)
        elif self.operator == "is_null":
            return series.isna()
        elif self.operator == "is_not_null":
            return series.notna()
        elif self.operator == "between":
            min_val, max_val = self.value
            return (series >= min_val) & (series <= max_val)
        elif self.operator == "starts_with":
            return series.str.startswith(self.value)
        elif self.operator == "ends_with":
            return series.str.endswith(self.value)
        elif self.operator == "contains":
            return series.str.contains(self.value)
        else:
            raise ValueError(f"Unsupported operator for Pandas: {self.operator}")
    
    def to_polars_condition(self):
        """Convert to Polars expression"""
        import polars as pl
        
        polars_col = pl.col(self.column)
        
        if self.operator == "==":
            return polars_col == self.value
        elif self.operator == "!=":
            return polars_col != self.value
        elif self.operator == ">":
            return polars_col > self.value
        elif self.operator == ">=":
            return polars_col >= self.value
        elif self.operator == "<":
            return polars_col < self.value
        elif self.operator == "<=":
            return polars_col <= self.value
        elif self.operator == "in":
            return polars_col.is_in(self.value)
        elif self.operator == "not_in":
            return ~polars_col.is_in(self.value)
        elif self.operator == "is_null":
            return polars_col.is_null()
        elif self.operator == "is_not_null":
            return polars_col.is_not_null()
        elif self.operator == "between":
            min_val, max_val = self.value
            return polars_col.is_between(min_val, max_val)
        elif self.operator == "starts_with":
            return polars_col.str.starts_with(self.value)
        elif self.operator == "ends_with":
            return polars_col.str.ends_with(self.value)
        elif self.operator == "contains":
            return polars_col.str.contains(self.value)
        else:
            raise ValueError(f"Unsupported operator for Polars: {self.operator}")
    
    def to_pyarrow_condition(self):
        """Convert to PyArrow compute expression"""
        import pyarrow.compute as pc
        
        if self.operator == "==":
            return pc.equal(pc.field(self.column), pc.scalar(self.value))
        elif self.operator == "!=":
            return pc.not_equal(pc.field(self.column), pc.scalar(self.value))
        elif self.operator == ">":
            return pc.greater(pc.field(self.column), pc.scalar(self.value))
        elif self.operator == ">=":
            return pc.greater_equal(pc.field(self.column), pc.scalar(self.value))
        elif self.operator == "<":
            return pc.less(pc.field(self.column), pc.scalar(self.value))
        elif self.operator == "<=":
            return pc.less_equal(pc.field(self.column), pc.scalar(self.value))
        elif self.operator == "in":
            import pyarrow as pa
            return pc.is_in(pc.field(self.column), value_set=pa.array(self.value))
        elif self.operator == "not_in":
            import pyarrow as pa
            return pc.invert(pc.is_in(pc.field(self.column), value_set=pa.array(self.value)))
        elif self.operator == "is_null":
            return pc.is_null(pc.field(self.column))
        elif self.operator == "is_not_null":
            return pc.is_valid(pc.field(self.column))
        elif self.operator == "between":
            min_val, max_val = self.value
            return pc.and_kleene(
                pc.greater_equal(pc.field(self.column), pc.scalar(min_val)),
                pc.less_equal(pc.field(self.column), pc.scalar(max_val))
            )
        elif self.operator == "starts_with":
            return pc.starts_with(pc.field(self.column), pattern=self.value)
        elif self.operator == "ends_with":
            return pc.ends_with(pc.field(self.column), pattern=self.value)
        elif self.operator == "contains":
            return pc.match_substring(pc.field(self.column), pattern=self.value)
        else:
            raise ValueError(f"Unsupported operator for PyArrow: {self.operator}")
    
    def to_delta_table_filter(self) -> FilterType:
        """Convert to Delta Table filter for DeltaTable.to_pandas()"""
        # Delta Lake filter format: (column, operator, value) tuples
        if self.operator == "==":
            return [(self.column, "=", self.value)]
        elif self.operator == "!=":
            return [(self.column, "!=", self.value)]
        elif self.operator == ">":
            return [(self.column, ">", self.value)]
        elif self.operator == ">=":
            return [(self.column, ">=", self.value)]
        elif self.operator == "<":
            return [(self.column, "<", self.value)]
        elif self.operator == "<=":
            return [(self.column, "<=", self.value)]
        elif self.operator == "in":
            return [(self.column, "in", self.value)]
        elif self.operator == "not_in":
            return [(self.column, "not in", self.value)]
        elif self.operator == "is_null":
            return [(self.column, "=", None)]
        elif self.operator == "is_not_null":
            return [(self.column, "!=", None)]
        elif self.operator == "between":
            # Between needs to be represented as two conditions: >= min AND <= max
            min_val, max_val = self.value
            return [(self.column, ">=", min_val), (self.column, "<=", max_val)]
        else:
            # For string operations (starts_with, ends_with, contains), 
            # Delta Lake doesn't support LIKE directly, so we'll fall back to PyArrow
            raise ValueError(f"Operator '{self.operator}' not supported by Delta Lake filters. Use PyArrow filters instead.")
    
    
    def __repr__(self) -> str:
        if self.operator in ["is_null", "is_not_null"]:
            return f"Condition({self.column} {self.operator})"
        return f"Condition({self.column} {self.operator} {self.value})"
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize condition to dictionary"""
        return {
            "type": "Condition",
            "column": self.column,
            "operator": self.operator,
            "value": self.value
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Condition':
        """Deserialize condition from dictionary"""
        if data.get("type") != "Condition":
            raise ValueError(f"Invalid condition type: {data.get('type')}")
        
        return cls(
            column=data["column"],
            operator=data["operator"],
            value=data.get("value")
        )


class AndCondition(BaseCondition):
    """
    Represents an AND combination of conditions.
    """
    
    def __init__(self, *conditions: BaseCondition):
        self.conditions = list(conditions)
    
    def to_spark_condition(self, df):
        """Convert to Spark SQL condition"""
        spark_conditions = [cond.to_spark_condition(df) for cond in self.conditions]
        result = spark_conditions[0]
        for cond in spark_conditions[1:]:
            result = result & cond
        return result
    
    def to_pandas_condition(self, df):
        """Convert to Pandas boolean mask"""
        pandas_conditions = [cond.to_pandas_condition(df) for cond in self.conditions]
        result = pandas_conditions[0]
        for cond in pandas_conditions[1:]:
            result = result & cond
        return result
    
    def to_polars_condition(self):
        """Convert to Polars expression"""
        polars_conditions = [cond.to_polars_condition() for cond in self.conditions]
        result = polars_conditions[0]
        for cond in polars_conditions[1:]:
            result = result & cond
        return result
    
    def to_pyarrow_condition(self):
        """Convert to PyArrow compute expression"""
        import pyarrow.compute as pc
        
        pyarrow_conditions = [cond.to_pyarrow_condition() for cond in self.conditions]
        result = pyarrow_conditions[0]
        for cond in pyarrow_conditions[1:]:
            result = pc.and_kleene(result, cond)
        return result
    
    def to_delta_table_filter(self) -> FilterType:
        """Convert to Delta Table filter for DeltaTable.to_pandas()"""
        # AND conditions: flatten all condition tuples into a single conjunction
        all_filters = []
        for cond in self.conditions:
            cond_filters = cond.to_delta_table_filter()
            if isinstance(cond_filters, list) and len(cond_filters) > 0:
                if isinstance(cond_filters[0], tuple):
                    # This is a FilterConjunctionType (list of tuples)
                    all_filters.extend(cond_filters)
                else:
                    # This is a FilterDNFType (list of lists of tuples) - flatten it
                    for conj in cond_filters:
                        all_filters.extend(conj)
        return all_filters
    
    def __repr__(self) -> str:
        condition_strs = [str(cond) for cond in self.conditions]
        return f"({' AND '.join(condition_strs)})"
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize AND condition to dictionary"""
        serialized_conditions = []
        for cond in self.conditions:
            if hasattr(cond, 'to_dict'):
                serialized_conditions.append(cond.to_dict())
            elif isinstance(cond, ConditionTuple):
                serialized_conditions.append(cond._to_condition().to_dict())
            else:
                raise ValueError(f"Cannot serialize condition of type: {type(cond)}")
        
        return {
            "type": "AndCondition",
            "conditions": serialized_conditions
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AndCondition':
        """Deserialize AND condition from dictionary"""
        if data.get("type") != "AndCondition":
            raise ValueError(f"Invalid condition type: {data.get('type')}")
        
        conditions = []
        for cond_data in data["conditions"]:
            conditions.append(_deserialize_condition(cond_data))
        
        return cls(*conditions)
    
    def serialize(self) -> str:
        """Serialize AndCondition to its string representation."""
        return self.__repr__()
    
    @classmethod
    def deserialize(cls, string_repr: str) -> 'AndCondition':
        """Deserialize AndCondition from its string representation.
        
        Note: This is a complex reconstruction that requires parsing the string
        representation and rebuilding the condition tree.
        """
        # This is complex to implement properly as it would need a full parser
        # For now, raise an informative error
        raise NotImplementedError(
            "Complex condition deserialization is not implemented. "
            "Only individual ConditionTuple objects can be deserialized. "
            "Reconstruct complex conditions by deserializing individual parts."
        )


class OrCondition(BaseCondition):
    """
    Represents an OR combination of conditions.
    """
    
    def __init__(self, *conditions: BaseCondition):
        self.conditions = list(conditions)
    
    def to_spark_condition(self, df):
        """Convert to Spark SQL condition"""
        spark_conditions = [cond.to_spark_condition(df) for cond in self.conditions]
        result = spark_conditions[0]
        for cond in spark_conditions[1:]:
            result = result | cond
        return result
    
    def to_pandas_condition(self, df):
        """Convert to Pandas boolean mask"""
        pandas_conditions = [cond.to_pandas_condition(df) for cond in self.conditions]
        result = pandas_conditions[0]
        for cond in pandas_conditions[1:]:
            result = result | cond
        return result
    
    def to_polars_condition(self):
        """Convert to Polars expression"""
        polars_conditions = [cond.to_polars_condition() for cond in self.conditions]
        result = polars_conditions[0]
        for cond in polars_conditions[1:]:
            result = result | cond
        return result
    
    def to_pyarrow_condition(self):
        """Convert to PyArrow compute expression"""
        import pyarrow.compute as pc
        
        pyarrow_conditions = [cond.to_pyarrow_condition() for cond in self.conditions]
        result = pyarrow_conditions[0]
        for cond in pyarrow_conditions[1:]:
            result = pc.or_kleene(result, cond)
        return result
    
    def to_delta_table_filter(self) -> FilterType:
        """Convert to Delta Table filter for DeltaTable.to_pandas()"""
        # OR conditions: create a DNF (list of conjunctions)
        dnf_filters = []
        for cond in self.conditions:
            cond_filters = cond.to_delta_table_filter()
            if isinstance(cond_filters, list) and len(cond_filters) > 0:
                if isinstance(cond_filters[0], tuple):
                    # This is a FilterConjunctionType (list of tuples)
                    dnf_filters.append(cond_filters)
                else:
                    # This is already a FilterDNFType (list of lists) - extend it
                    dnf_filters.extend(cond_filters)
        return dnf_filters
    
    def __repr__(self) -> str:
        condition_strs = [str(cond) for cond in self.conditions]
        return f"({' OR '.join(condition_strs)})"
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize OR condition to dictionary"""
        serialized_conditions = []
        for cond in self.conditions:
            if hasattr(cond, 'to_dict'):
                serialized_conditions.append(cond.to_dict())
            elif isinstance(cond, ConditionTuple):
                serialized_conditions.append(cond._to_condition().to_dict())
            else:
                raise ValueError(f"Cannot serialize condition of type: {type(cond)}")
        
        return {
            "type": "OrCondition",
            "conditions": serialized_conditions
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OrCondition':
        """Deserialize OR condition from dictionary"""
        if data.get("type") != "OrCondition":
            raise ValueError(f"Invalid condition type: {data.get('type')}")
        
        conditions = []
        for cond_data in data["conditions"]:
            conditions.append(_deserialize_condition(cond_data))
        
        return cls(*conditions)
    
    def serialize(self) -> str:
        """Serialize OrCondition to its string representation."""
        return self.__repr__()
    
    @classmethod
    def deserialize(cls, string_repr: str) -> 'OrCondition':
        """Deserialize OrCondition from its string representation.
        
        Note: This is a complex reconstruction that requires parsing the string
        representation and rebuilding the condition tree.
        """
        # This is complex to implement properly as it would need a full parser
        # For now, raise an informative error
        raise NotImplementedError(
            "Complex condition deserialization is not implemented. "
            "Only individual ConditionTuple objects can be deserialized. "
            "Reconstruct complex conditions by deserializing individual parts."
        )


class NotCondition(BaseCondition):
    """
    Represents a NOT (negation) of a condition.
    """
    
    def __init__(self, condition: BaseCondition):
        self.condition = condition
    
    def to_spark_condition(self, df):
        """Convert to Spark SQL condition"""
        return ~self.condition.to_spark_condition(df)
    
    def to_pandas_condition(self, df):
        """Convert to Pandas boolean mask"""
        return ~self.condition.to_pandas_condition(df)
    
    def to_polars_condition(self):
        """Convert to Polars expression"""
        return ~self.condition.to_polars_condition()
    
    def to_pyarrow_condition(self):
        """Convert to PyArrow compute expression"""
        import pyarrow.compute as pc
        return pc.invert(self.condition.to_pyarrow_condition())
    
    def to_delta_table_filter(self) -> FilterType:
        """Convert to Delta Table filter for DeltaTable.to_pandas()"""
        # NOT conditions are limited in Delta Lake filters
        # We can only handle simple negations by converting operators
        if isinstance(self.condition, Condition):
            column = self.condition.column
            operator = self.condition.operator
            value = self.condition.value
            
            # Convert to opposite operator where possible
            if operator == "==":
                return [(column, "!=", value)]
            elif operator == "!=":
                return [(column, "=", value)]
            elif operator == ">":
                return [(column, "<=", value)]
            elif operator == ">=":
                return [(column, "<", value)]
            elif operator == "<":
                return [(column, ">=", value)]
            elif operator == "<=":
                return [(column, ">", value)]
            elif operator == "in":
                return [(column, "not in", value)]
            elif operator == "not_in":
                return [(column, "in", value)]
            elif operator == "is_null":
                return [(column, "!=", None)]
            elif operator == "is_not_null":
                return [(column, "=", None)]
            else:
                raise ValueError(f"NOT operation not supported for operator '{operator}' in Delta Lake filters")
        else:
            raise ValueError("Complex NOT conditions not supported in Delta Lake filters. Use PyArrow filters instead.")
    
    def __repr__(self) -> str:
        return f"NOT({self.condition})"
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize NOT condition to dictionary"""
        if hasattr(self.condition, 'to_dict'):
            condition_dict = self.condition.to_dict()
        elif isinstance(self.condition, ConditionTuple):
            condition_dict = self.condition._to_condition().to_dict()
        else:
            raise ValueError(f"Cannot serialize condition of type: {type(self.condition)}")
        
        return {
            "type": "NotCondition",
            "condition": condition_dict
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NotCondition':
        """Deserialize NOT condition from dictionary"""
        if data.get("type") != "NotCondition":
            raise ValueError(f"Invalid condition type: {data.get('type')}")
        
        condition = _deserialize_condition(data["condition"])
        return cls(condition)
    
    def serialize(self) -> str:
        """Serialize NotCondition to its string representation."""
        return self.__repr__()
    
    @classmethod
    def deserialize(cls, string_repr: str) -> 'NotCondition':
        """Deserialize NotCondition from its string representation.
        
        Note: This is a complex reconstruction that requires parsing the string
        representation and rebuilding the condition tree.
        """
        # This is complex to implement properly as it would need a full parser
        # For now, raise an informative error
        raise NotImplementedError(
            "Complex condition deserialization is not implemented. "
            "Only individual ConditionTuple objects can be deserialized. "
            "Reconstruct complex conditions by deserializing individual parts."
        )


class FilterParser:
    """
    Parser to convert various filter formats into condition objects.
    """
    
    @staticmethod
    def parse_where_conditions(where: Union['ConditionTuple', BaseCondition, List, None]) -> Union[BaseCondition, None]:
        """
        Parse where conditions from various formats.
        
        Args:
            where: Can be:
                - None: No filtering
                - ConditionTuple: Single ConditionTuple object
                - BaseCondition: Complex condition from combining ConditionTuples with & and | operators
                - List: List of tuples, ConditionTuples, or BaseConditions (combined with AND)
        
        Returns:
            Parsed condition object or None
        """
        if where is None:
            return None
        
        if isinstance(where, ConditionTuple):
            return where._to_condition()
        
        if isinstance(where, BaseCondition):
            return where
        
        if isinstance(where, tuple):
            # Convert regular tuple to ConditionTuple first, then to BaseCondition
            return make_condition(ConditionTuple(*where))
        
        if isinstance(where, list):
            if len(where) == 0:
                return None
            
            # Convert all items in the list to BaseCondition objects
            conditions = []
            for item in where:
                if isinstance(item, BaseCondition):
                    conditions.append(item)
                elif isinstance(item, ConditionTuple):
                    conditions.append(item._to_condition())
                elif isinstance(item, tuple):
                    # Convert regular tuple to ConditionTuple first, then to BaseCondition
                    conditions.append(make_condition(ConditionTuple(*item)))
                else:
                    raise ValueError(f"Unsupported item type in where list: {type(item)}")
            
            # Combine all conditions with AND
            if len(conditions) == 1:
                return conditions[0]
            else:
                return AndCondition(*conditions)
        
        raise ValueError(f"Unsupported where condition type: {type(where)}. Supported types: ConditionTuple, BaseCondition, List, or None.")


def _deserialize_condition(data: Dict[str, Any]) -> BaseCondition:
    """
    Factory function to deserialize any condition type from dictionary.
    
    Args:
        data: Dictionary containing condition data with 'type' field
        
    Returns:
        Appropriate BaseCondition subclass instance
        
    Raises:
        ValueError: If condition type is not recognized
    """
    condition_type = data.get("type")
    
    if condition_type == "Condition":
        return Condition.from_dict(data)
    elif condition_type == "AndCondition":
        return AndCondition.from_dict(data)
    elif condition_type == "OrCondition":
        return OrCondition.from_dict(data)
    elif condition_type == "NotCondition":
        return NotCondition.from_dict(data)
    else:
        raise ValueError(f"Unknown condition type: {condition_type}")


def deserialize_condition_from_string(string_repr: str) -> BaseCondition:
    """
    Unified parser for any condition string representation.
    
    Args:
        string_repr: String representation of a condition
        
    Returns:
        BaseCondition instance
        
    Examples:
        deserialize_condition_from_string("('age', '>', 25)")  # Returns ConditionTuple
        deserialize_condition_from_string("(Condition(age > 25) AND Condition(status == ACTIVE))")  # Returns AndCondition
    """
    string_repr = string_repr.strip()
    
    if not string_repr:
        raise ValueError("Empty string cannot be deserialized")
    
    return _parse_condition(string_repr)


def _parse_condition(string_repr: str) -> BaseCondition:
    """Parse conditions using recursive-descent algorithm."""
    tokens = _tokenize(string_repr)
    
    # Handle empty token list (invalid input)
    if not tokens:
        if not string_repr.startswith('(') or not string_repr.endswith(')'):
            raise ValueError(f"Invalid ConditionTuple string format: {string_repr}")
        else:
            raise ValueError(f"Unable to parse complex condition: {string_repr}")
    
    parser = _RecursiveDescentParser(tokens)
    return parser.parse_expression()


class _Token:
    """Token for condition parser."""
    def __init__(self, type_: str, value: str, position: int = 0):
        self.type = type_
        self.value = value
        self.position = position
    
    def __repr__(self):
        return f"Token({self.type}, {self.value!r})"


class _RecursiveDescentParser:
    """Recursive-descent parser for condition expressions."""
    
    def __init__(self, tokens):
        self.tokens = tokens
        self.position = 0
        self.current_token = self.tokens[0] if tokens else None
    
    def advance(self):
        """Move to the next token."""
        self.position += 1
        if self.position < len(self.tokens):
            self.current_token = self.tokens[self.position]
        else:
            self.current_token = None
    
    def match(self, token_type):
        """Check if current token matches expected type and advance if so."""
        if self.current_token and self.current_token.type == token_type:
            self.advance()
            return True
        return False
    
    def expect(self, token_type):
        """Expect a specific token type and advance, or raise error."""
        if not self.current_token or self.current_token.type != token_type:
            raise ValueError(f"Expected {token_type}, but got {self.current_token.type if self.current_token else 'EOF'}")
        self.advance()
    
    def parse_expression(self):
        """Parse the top-level expression (OR has lowest precedence)."""
        return self.parse_or_expression()
    
    def parse_or_expression(self):
        """Parse OR expressions (lowest precedence)."""
        left = self.parse_and_expression()
        
        while self.current_token and self.current_token.type == 'OR':
            self.advance()  # consume 'OR'
            right = self.parse_and_expression()
            left = OrCondition(left, right)
        
        return left
    
    def parse_and_expression(self):
        """Parse AND expressions (middle precedence)."""
        left = self.parse_not_expression()
        
        while self.current_token and self.current_token.type == 'AND':
            self.advance()  # consume 'AND'
            right = self.parse_not_expression()
            left = AndCondition(left, right)
        
        return left
    
    def parse_not_expression(self):
        """Parse NOT expressions (highest precedence)."""
        if self.current_token and self.current_token.type == 'NOT':
            self.advance()  # consume 'NOT'
            self.expect('LPAREN')  # expect '('
            inner = self.parse_expression()  # recursively parse inner expression
            self.expect('RPAREN')  # expect ')'
            return NotCondition(inner)
        
        return self.parse_primary()
    
    def parse_primary(self):
        """Parse primary expressions (atoms)."""
        if not self.current_token:
            raise ValueError("Unexpected end of input")
        
        # Handle parenthesized expressions
        if self.current_token.type == 'LPAREN':
            self.advance()  # consume '('
            expr = self.parse_expression()  # recursively parse inner expression
            self.expect('RPAREN')  # expect ')'
            return expr
        
        # Handle Condition(...) format
        if self.current_token.type == 'CONDITION':
            return self._parse_condition_token()
        
        # Handle simple tuple format
        if self.current_token.type == 'TUPLE':
            return self._parse_tuple_token()
        
        raise ValueError(f"Unexpected token: {self.current_token}")
    
    def _parse_condition_token(self):
        """Parse a Condition(column operator value) token."""
        import re
        
        condition_str = self.current_token.value
        self.advance()
        
        # Try to match Condition(column operator value) format first
        match = re.match(r'Condition\((.+?)\s+(.+?)\s+(.+)\)', condition_str)
        if match:
            column, operator, value_str = match.groups()
            value = _parse_condition_value(value_str)
            return Condition(column, operator, value)
        
        # Try to match Condition(column operator) format for null operators
        match = re.match(r'Condition\((.+?)\s+(is_null|is_not_null)\)', condition_str)
        if match:
            column, operator = match.groups()
            return Condition(column, operator)
        
        raise ValueError(f"Invalid Condition format: {condition_str}")
    
    def _parse_tuple_token(self):
        """Parse a simple tuple token."""
        import ast
        
        tuple_str = self.current_token.value
        self.advance()
        
        # Remove outer parentheses and parse
        args_str = tuple_str[1:-1]
        try:
            args_tuple = ast.literal_eval(f"({args_str})")
            if not isinstance(args_tuple, tuple):
                args = (args_tuple,)
            else:
                args = args_tuple
            return ConditionTuple(*args)
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Failed to parse ConditionTuple arguments from '{tuple_str}': {e}")


def _tokenize(string_repr: str) -> list[_Token]:
    """Tokenize condition string for Pratt parser."""
    import re
    
    tokens = []
    i = 0
    string_repr = string_repr.strip()
    
    while i < len(string_repr):
        # Skip whitespace
        if string_repr[i].isspace():
            i += 1
            continue
        
        # NOT operator
        if string_repr[i:i+3] == 'NOT':
            tokens.append(_Token('NOT', 'NOT', i))
            i += 3
            continue
        
        # AND operator
        if string_repr[i:i+3] == 'AND':
            tokens.append(_Token('AND', 'AND', i))
            i += 3
            continue
            
        # OR operator  
        if string_repr[i:i+2] == 'OR':
            tokens.append(_Token('OR', 'OR', i))
            i += 2
            continue
        
        # Condition(...) token (must come before '(' check)
        if string_repr[i:].startswith('Condition('):
            # Find the matching closing paren for the Condition
            paren_count = 1
            j = i + 10  # Skip 'Condition('
            while j < len(string_repr) and paren_count > 0:
                if string_repr[j] == '(':
                    paren_count += 1
                elif string_repr[j] == ')':
                    paren_count -= 1
                j += 1
            condition_str = string_repr[i:j]
            tokens.append(_Token('CONDITION', condition_str, i))
            i = j
            continue
        
        # Left parenthesis
        if string_repr[i] == '(':
            # Check if this is a simple tuple (no operators inside)
            # Look ahead to see what's inside this parenthesis
            paren_count = 1
            j = i + 1
            has_operators = False
            while j < len(string_repr) and paren_count > 0:
                if string_repr[j] == '(':
                    paren_count += 1
                elif string_repr[j] == ')':
                    paren_count -= 1
                elif paren_count == 1:  # Only check at top level
                    if (string_repr[j:j+5] == ' AND ' or 
                        string_repr[j:j+4] == ' OR ' or
                        string_repr[j:].startswith('Condition(')):
                        has_operators = True
                        break
                j += 1
            
            if not has_operators and j <= len(string_repr):
                # This is a simple tuple
                tuple_str = string_repr[i:j]
                tokens.append(_Token('TUPLE', tuple_str, i))
                i = j
            else:
                # This is a grouping parenthesis
                tokens.append(_Token('LPAREN', '(', i))
                i += 1
            continue
        
        # Right parenthesis
        if string_repr[i] == ')':
            tokens.append(_Token('RPAREN', ')', i))
            i += 1
            continue
        
        # If we get here, skip the character (shouldn't happen with valid input)
        i += 1
    
    return tokens


def _parse_condition_value(value_str: str):
    """Parse a value from a condition string representation."""
    import ast
    
    if value_str == 'None':
        return None
    
    try:
        return ast.literal_eval(value_str)
    except (ValueError, SyntaxError):
        return value_str


def deserialize_condition(data: Union[str, Dict[str, Any]]) -> BaseCondition:
    """
    Public function to deserialize condition from JSON string or dictionary.
    
    Args:
        data: JSON string or dictionary containing condition data
        
    Returns:
        BaseCondition instance
        
    Examples:
        # From JSON string
        json_str = '{"type": "Condition", "column": "age", "operator": ">", "value": 25}'
        condition = deserialize_condition(json_str)
        
        # From dictionary
        data = {"type": "Condition", "column": "age", "operator": ">", "value": 25}
        condition = deserialize_condition(data)
    """
    if isinstance(data, str):
        data = json.loads(data)
    
    return _deserialize_condition(data)


def condition(column: str, operator: str, value: Any = None) -> Condition:
    """
    Create a Condition object directly.
    
    This is an alternative to using ConditionTuple and c() helper.
    
    Args:
        column: Column name to filter on
        operator: Comparison operator
        value: Value to compare against (not needed for null checks)
    
    Returns:
        Condition object
    
    Examples:
        condition("age", ">", 25)
        condition("status", "==", "ACTIVE")
        condition("email", "is_not_null")
    """
    return Condition(column, operator, value)


def and_(*conditions: BaseCondition) -> AndCondition:
    """
    Create an AND condition from multiple conditions.
    
    Args:
        *conditions: Variable number of condition objects
    
    Returns:
        AndCondition object
    
    Examples:
        and_(condition("age", ">", 25), condition("country", "==", "US"))
    """
    return AndCondition(*conditions)


def or_(*conditions: BaseCondition) -> OrCondition:
    """
    Create an OR condition from multiple conditions.
    
    Args:
        *conditions: Variable number of condition objects
    
    Returns:
        OrCondition object
    
    Examples:
        or_(condition("country", "==", "US"), condition("country", "==", "UK"))
    """
    return OrCondition(*conditions)


def not_(condition: BaseCondition) -> NotCondition:
    """
    Create a NOT condition from a condition.
    
    Args:
        condition: Condition to negate
    
    Returns:
        NotCondition object
    
    Examples:
        not_(condition("status", "==", "BANNED"))
    """
    return NotCondition(condition)


def c(*args) -> 'ConditionTuple':
    """
    Convenient helper function to create ConditionTuple objects.
    
    This is the recommended way to create tuples that support logical operators.
    
    Examples:
        # Simple condition
        c("age", ">", 25)
        
        # Null check
        c("email", "is_not_null")
        
        # Using with operators
        c("age", ">", 25) & c("country", "==", "US")
        c("country", "==", "US") | c("country", "==", "UK")
        ~c("status", "==", "BANNED")
    
    Args:
        *args: Arguments to pass to ConditionTuple constructor
    
    Returns:
        ConditionTuple object that supports &, |, ~ operators
    """
    return ConditionTuple(*args)


# Convert ConditionTuple to Condition automatically
def make_condition(condition_input) -> BaseCondition:
    """
    Convert ConditionTuple to condition objects.
    
    This is a helper function that converts ConditionTuple objects
    to Condition objects so they can use the & | ~ operators.
    """
    if isinstance(condition_input, BaseCondition):
        return condition_input
    elif isinstance(condition_input, ConditionTuple):
        if len(condition_input) == 2 and condition_input[1] in ["is_null", "is_not_null"]:
            column, operator = condition_input
            return Condition(column, operator)
        elif len(condition_input) == 3:
            column, operator, value = condition_input
            return Condition(column, operator, value)
        else:
            raise ValueError(f"Invalid ConditionTuple format: {condition_input}")
    else:
        raise ValueError(f"Cannot convert {type(condition_input)} to condition. Only ConditionTuple is supported.")


# Production-ready tuple class for condition building
class ConditionTuple(tuple):
    """
    A production-ready tuple subclass that supports logical operators for building filter conditions.
    
    This class allows users to write intuitive filter expressions using tuples:
        where = [
            ConditionTuple("age", ">", 25) | ConditionTuple("country", "==", "US"),
            ~ConditionTuple("status", "==", "BANNED")
        ]
    
    Or more commonly, using the c() helper function:
        from feature_store_sdk.filters import c
        where = [
            c("age", ">", 25) | c("country", "==", "US"),
            ~c("status", "==", "BANNED")
        ]
    """
    
    def __new__(cls, *args):
        """Create a new ConditionTuple instance."""
        if len(args) == 1 and isinstance(args[0], (list, tuple)):
            # Handle case where a single tuple/list is passed
            return super().__new__(cls, args[0])
        return super().__new__(cls, args)
    
    def __and__(self, other):
        """Support for & operator (AND logic)."""
        left = self._to_condition()
        right = self._convert_operand(other)
        return left & right
    
    def __or__(self, other):
        """Support for | operator (OR logic)."""
        left = self._to_condition()
        right = self._convert_operand(other)
        return left | right
    
    def __invert__(self):
        """Support for ~ operator (NOT logic)."""
        return ~self._to_condition()
    
    def _to_condition(self) -> BaseCondition:
        """Convert this tuple to a Condition object."""
        return make_condition(self)
    
    def _convert_operand(self, other) -> BaseCondition:
        """Convert the other operand to a BaseCondition."""
        if isinstance(other, BaseCondition):
            return other
        elif isinstance(other, ConditionTuple):
            return make_condition(other)
        elif isinstance(other, tuple):
            # Convert regular tuple to ConditionTuple first, then to BaseCondition
            return make_condition(ConditionTuple(*other))
        else:
            raise TypeError(f"unsupported operand type for logical operation: {type(other)}")
    
    def __repr__(self) -> str:
        """String representation that shows this is a ConditionTuple."""
        if len(self) == 2:
            return f"({self[0]!r}, {self[1]!r})"
        elif len(self) == 3:
            return f"({self[0]!r}, {self[1]!r}, {self[2]!r})"
        else:
            return super().__repr__()
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize ConditionTuple by converting to Condition first."""
        return self._to_condition().to_dict()
    
    def to_json(self) -> str:
        """Serialize ConditionTuple to JSON string."""
        return self._to_condition().to_json()
    
    def to_pyarrow_condition(self):
        """Convert to PyArrow compute expression by first converting to Condition."""
        return self._to_condition().to_pyarrow_condition()
    
    def to_delta_table_filter(self) -> FilterType:
        """Convert to Delta Table filter by first converting to Condition."""
        return self._to_condition().to_delta_table_filter()
    
    def serialize(self) -> str:
        """Serialize ConditionTuple to its string representation."""
        return self.__repr__()
    
    @classmethod
    def deserialize(cls, string_repr: str) -> BaseCondition:
        """Deserialize condition from its string representation.
        
        This method can handle both simple ConditionTuple strings and complex condition strings.
        
        Args:
            string_repr: String representation like "('age', '>', 25)" for simple conditions
                        or complex strings for compound conditions
            
        Returns:
            BaseCondition instance (ConditionTuple for simple conditions, 
                                   AndCondition/OrCondition/NotCondition for complex ones)
            
        Examples:
            ConditionTuple.deserialize("('age', '>', 25)")  # Returns ConditionTuple
            ConditionTuple.deserialize("(Condition(age > 25) AND Condition(status == ACTIVE))")  # Returns AndCondition
        """
        return deserialize_condition_from_string(string_repr)