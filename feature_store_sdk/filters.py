"""
Enhanced Filter DSL with Python-style operators for intuitive condition building.

This module provides a production-ready way to build complex filter conditions using
familiar Python operators like &, |, and ~.

Examples:
    # Using the recommended c() helper function
    from feature_store_sdk.filters import c, condition
    
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
    
    # Alternative: Using condition objects directly
    where = [
        condition("age", ">", 25) & condition("country", "==", "US"),
        ~condition("status", "==", "BANNED")
    ]
    
    # Backward compatibility: Legacy tuple syntax still supported
    where = [("age", ">", 25), ("status", "==", "ACTIVE")]
"""

from typing import Any, List, Tuple, Union
from abc import ABC, abstractmethod


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
    def __repr__(self) -> str:
        pass


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
    
    def __repr__(self) -> str:
        if self.operator in ["is_null", "is_not_null"]:
            return f"Condition({self.column} {self.operator})"
        return f"Condition({self.column} {self.operator} {self.value})"


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
    
    def __repr__(self) -> str:
        condition_strs = [str(cond) for cond in self.conditions]
        return f"({' AND '.join(condition_strs)})"


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
    
    def __repr__(self) -> str:
        condition_strs = [str(cond) for cond in self.conditions]
        return f"({' OR '.join(condition_strs)})"


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
    
    def __repr__(self) -> str:
        return f"NOT({self.condition})"


class FilterParser:
    """
    Parser to convert various filter formats into condition objects.
    """
    
    @staticmethod
    def parse_where_conditions(where: Union[List, Tuple, BaseCondition, None]) -> Union[BaseCondition, None]:
        """
        Parse where conditions from various formats.
        
        Args:
            where: Can be:
                - None: No filtering
                - List: List of conditions (AND by default)
                - Tuple: Single condition tuple
                - BaseCondition: Already parsed condition
        
        Returns:
            Parsed condition object or None
        """
        if where is None:
            return None
        
        if isinstance(where, BaseCondition):
            return where
        
        if isinstance(where, tuple):
            return FilterParser._parse_tuple(where)
        
        if isinstance(where, list):
            return FilterParser._parse_list(where)
        
        raise ValueError(f"Unsupported where condition type: {type(where)}")
    
    @staticmethod
    def _parse_tuple(condition_tuple: Tuple) -> Condition:
        """Parse a single condition tuple"""
        if len(condition_tuple) == 2 and condition_tuple[1] in ["is_null", "is_not_null"]:
            column, operator = condition_tuple
            return Condition(column, operator)
        elif len(condition_tuple) == 3:
            column, operator, value = condition_tuple
            return Condition(column, operator, value)
        else:
            raise ValueError(f"Invalid condition tuple format: {condition_tuple}")
    
    @staticmethod
    def _parse_list(condition_list: List) -> BaseCondition:
        """Parse a list of conditions (AND by default)"""
        if not condition_list:
            raise ValueError("Empty condition list")
        
        parsed_conditions = []
        for item in condition_list:
            if isinstance(item, BaseCondition):
                parsed_conditions.append(item)
            elif isinstance(item, tuple):
                parsed_conditions.append(FilterParser._parse_tuple(item))
            else:
                raise ValueError(f"Invalid condition item type: {type(item)}")
        
        if len(parsed_conditions) == 1:
            return parsed_conditions[0]
        else:
            return AndCondition(*parsed_conditions)


# Convenience functions for creating conditions
def condition(column: str, operator: str, value: Any = None) -> Condition:
    """
    Create a single condition.
    
    Args:
        column: Column name
        operator: Comparison operator
        value: Value to compare against
    
    Returns:
        Condition object
    """
    return Condition(column, operator, value)


def and_(*conditions: BaseCondition) -> AndCondition:
    """
    Create an AND combination of conditions.
    
    Args:
        *conditions: Conditions to combine with AND
    
    Returns:
        AndCondition object
    """
    return AndCondition(*conditions)


def or_(*conditions: BaseCondition) -> OrCondition:
    """
    Create an OR combination of conditions.
    
    Args:
        *conditions: Conditions to combine with OR
    
    Returns:
        OrCondition object
    """
    return OrCondition(*conditions)


def not_(condition: BaseCondition) -> NotCondition:
    """
    Create a NOT (negation) of a condition.
    
    Args:
        condition: Condition to negate
    
    Returns:
        NotCondition object
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


# Allow tuple to be converted to Condition automatically
def make_condition(condition_input) -> BaseCondition:
    """
    Convert various inputs into condition objects.
    
    This is a helper function that allows users to use tuples
    and have them automatically converted to Condition objects
    so they can use the & | ~ operators.
    """
    if isinstance(condition_input, BaseCondition):
        return condition_input
    elif isinstance(condition_input, tuple):
        return FilterParser._parse_tuple(condition_input)
    else:
        raise ValueError(f"Cannot convert {type(condition_input)} to condition")


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
        elif isinstance(other, (tuple, ConditionTuple)):
            return make_condition(other)
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