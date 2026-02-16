"""
Stream module for Event-Driven Pipeline.

Provides stream processing with sources, transformations, and sinks.
"""

from typing import Any, Callable, List, Optional, AsyncIterator, Iterable
import asyncio


class Stream:
    """
    A stream of data that can be transformed.
    
    Example:
        stream = Stream([1, 2, 3, 4, 5])
        result = await stream.map(lambda x: x * 2).filter(lambda x: x > 5).collect()
    """
    
    def __init__(self, data: Optional[Iterable] = None):
        self._data = data or []
    
    def map(self, func: Callable) -> "Stream":
        """Transform each element."""
        return Stream([func(x) for x in self._data])
    
    def filter(self, predicate: Callable) -> "Stream":
        """Filter elements."""
        return Stream([x for x in self._data if predicate(x)])
    
    def flat_map(self, func: Callable) -> "Stream":
        """Flatten nested results."""
        result = []
        for x in self._data:
            r = func(x)
            if hasattr(r, '__iter__') and not isinstance(r, (str, bytes)):
                result.extend(r)
            else:
                result.append(r)
        return Stream(result)
    
    async def collect(self) -> List[Any]:
        """Collect stream to list."""
        return self._data
    
    def __iter__(self):
        return iter(self._data)


class Source:
    """Data source for streams."""
    
    def __init__(self, name: str = "source"):
        self.name = name
    
    async def produce(self) -> AsyncIterator[Any]:
        """Produce data (override in subclass)."""
        raise NotImplementedError


class Sink:
    """Data sink for streams."""
    
    def __init__(self, name: str = "sink"):
        self.name = name
    
    async def consume(self, data: Any) -> None:
        """Consume data (override in subclass)."""
        raise NotImplementedError


class ListSource(Source):
    """Source that produces from a list."""
    
    def __init__(self, items: List[Any], name: str = "list"):
        super().__init__(name)
        self.items = items
    
    async def produce(self) -> AsyncIterator[Any]:
        for item in self.items:
            yield item


class PrintSink(Sink):
    """Sink that prints to console."""
    
    def __init__(self, prefix: str = ""):
        super().__init__("print")
        self.prefix = prefix
    
    async def consume(self, data: Any) -> None:
        print(f"{self.prefix}{data}")


async def process_stream(
    source: Source,
    *processors: Callable,
    sink: Optional[Sink] = None
) -> List[Any]:
    """Process a stream through processors to a sink."""
    results = []
    
    async for item in source.produce():
        data = item
        for proc in processors:
            if asyncio.iscoroutinefunction(proc):
                data = await proc(data)
            else:
                data = proc(data)
        
        if sink:
            await sink.consume(data)
        
        results.append(data)
    
    return results
