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

from typing import Any, List, Union, Dict
from abc import ABC, abstractmethod
import json


class BaseCondition(ABC):
    """
    Base class for all filter conditions.
    Provides support for Python-style logical operators.
    """
    
    def __and__(self, other: 'BaseCondition') -> 'AndCondition':
        """Support for & operator (AND logic)"""
        return AndCondition(self, other)
    
    def __or__(self, other: 'BaseCondition') -> 'OrCondition':
        """Support for | operator (OR logic)"""
        return OrCondition(self, other)
    
    def __invert__(self) -> 'NotCondition':
        """Support for ~ operator (NOT logic)"""
        return NotCondition(self)
    
    @abstractmethod
    def to_spark_condition(self, df):
        """Convert to Spark SQL condition"""
        pass
    
    @abstractmethod
    def to_pandas_condition(self, df):
        """Convert to Pandas boolean mask"""
        pass
    
    @abstractmethod
    def to_polars_condition(self):
        """Convert to Polars expression"""
        pass
    
    @abstractmethod
    def to_pyarrow_condition(self):
        """Convert to PyArrow compute expression"""
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
    
    def __init__(self, column: str, operator: str, value: Any = None):
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
        null_ops = ["is_null", "is_not_null"]
        if self.operator not in null_ops and self.value is None:
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
            return f"c({self[0]!r}, {self[1]!r})"
        elif len(self) == 3:
            return f"c({self[0]!r}, {self[1]!r}, {self[2]!r})"
        else:
            return f"c{super().__repr__()}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize ConditionTuple by converting to Condition first."""
        return self._to_condition().to_dict()
    
    def to_json(self) -> str:
        """Serialize ConditionTuple to JSON string."""
        return self._to_condition().to_json()
    
    def to_pyarrow_condition(self):
        """Convert to PyArrow compute expression by first converting to Condition."""
        return self._to_condition().to_pyarrow_condition()