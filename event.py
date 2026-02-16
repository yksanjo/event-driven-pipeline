"""
Event module for Event-Driven Pipeline.

Provides event system with event bus, handlers, and event types.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, List
from datetime import datetime
from enum import Enum
import asyncio


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class Event:
    """
    An event in the pipeline.
    
    Attributes:
        event_type: Type identifier for the event.
        data: Event payload data.
        timestamp: When the event was created.
        source: Origin of the event.
        priority: Event priority.
        metadata: Additional event metadata.
    """
    event_type: str
    data: Any = None
    timestamp: datetime = field(default_factory=datetime.now)
    source: str = ""
    priority: EventPriority = EventPriority.NORMAL
    metadata: dict = field(default_factory=dict)
    
    def __repr__(self) -> str:
        return f"Event(type='{self.event_type}', priority={self.priority.value})"


class EventHandler:
    """Base class for event handlers."""
    
    def __init__(self, name: str = ""):
        self.name = name or self.__class__.__name__
        self._filters: List[Callable[[Event], bool]] = []
    
    async def handle(self, event: Event) -> Any:
        """Handle an event."""
        raise NotImplementedError
    
    def can_handle(self, event: Event) -> bool:
        """Check if handler can process this event."""
        for filter_func in self._filters:
            if not filter_func(event):
                return False
        return True


class FuncEventHandler(EventHandler):
    """Event handler that wraps a function."""
    
    def __init__(self, func: Callable, name: str = ""):
        super().__init__(name or func.__name__)
        self.func = func
    
    async def handle(self, event: Event) -> Any:
        if asyncio.iscoroutinefunction(self.func):
            return await self.func(event)
        return self.func(event)


class EventBus:
    """Central event bus for publishing and subscribing to events."""
    
    def __init__(self):
        self._handlers: dict[str, List[EventHandler]] = {}
        self._running = False
    
    def subscribe(self, event_type: str, handler: Callable) -> None:
        """Subscribe to an event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
    
    async def publish(self, event: Event) -> List[Any]:
        """Publish an event to all subscribers."""
        handlers = self._handlers.get(event.event_type, [])
        results = []
        for handler in handlers:
            try:
                result = handler(event) if not asyncio.iscoroutinefunction(handler) else await handler(event)
                results.append(result)
            except Exception as e:
                print(f"Handler error: {e}")
        return results
    
    def get_handlers(self, event_type: str) -> List[EventHandler]:
        """Get handlers for an event type."""
        return self._handlers.get(event_type, [])
