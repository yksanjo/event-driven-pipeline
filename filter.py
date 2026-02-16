"""
Filter module for Event-Driven Pipeline.

Provides filter and predicate utilities for stream processing.
"""

from typing import Any, Callable


class Predicate:
    """Base class for filter predicates."""
    
    def __init__(self, func: Callable[[Any], bool]):
        self.func = func
    
    def __call__(self, value: Any) -> bool:
        return self.func(value)
    
    def and_(self, other: "Predicate") -> "Predicate":
        """Combine with AND."""
        return Predicate(lambda x: self(x) and other(x))
    
    def or_(self, other: "Predicate") -> "Predicate":
        """Combine with OR."""
        return Predicate(lambda x: self(x) or other(x))
    
    def negate(self) -> "Predicate":
        """Negate the predicate."""
        return Predicate(lambda x: not self(x))


class Filter:
    """Filter for stream processing."""
    
    def __init__(self, predicate: Callable[[Any], bool]):
        self.predicate = predicate
    
    def apply(self, data: Any) -> bool:
        """Apply filter."""
        return self.predicate(data)
    
    @staticmethod
    def equals(value: Any) -> Predicate:
        """Filter equals value."""
        return Predicate(lambda x: x == value)
    
    @staticmethod
    def not_equals(value: Any) -> Predicate:
        """Filter not equals value."""
        return Predicate(lambda x: x != value)
    
    @staticmethod
    def greater_than(value: Any) -> Predicate:
        """Filter greater than."""
        return Predicate(lambda x: x > value)
    
    @staticmethod
    def less_than(value: Any) -> Predicate:
        """Filter less than."""
        return Predicate(lambda x: x < value)
    
    @staticmethod
    def contains(substring: str) -> Predicate:
        """Filter contains substring."""
        return Predicate(lambda x: substring in str(x))
    
    @staticmethod
    def matches(pattern: str) -> Predicate:
        """Filter matches regex pattern."""
        import re
        return Predicate(lambda x: bool(re.search(pattern, str(x))))


# Common predicates
class Predicates:
    """Common predicate factory."""
    
    @staticmethod
    def is_none() -> Predicate:
        return Predicate(lambda x: x is None)
    
    @staticmethod
    def is_not_none() -> Predicate:
        return Predicate(lambda x: x is not None)
    
    @staticmethod
    def is_empty() -> Predicate:
        return Predicate(lambda x: not x)
    
    @staticmethod
    def is_true() -> Predicate:
        return Predicate(lambda x: bool(x) is True)
    
    @staticmethod
    def is_numeric() -> Predicate:
        return Predicate(lambda x: isinstance(x, (int, float)))
