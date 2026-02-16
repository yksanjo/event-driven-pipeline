"""
Demo examples for the Event-Driven Pipeline.

Demonstrates event bus, pipelines, and stream processing.
"""

import asyncio
from event_driven_pipeline import (
    Event, EventBus, Pipeline, PipelineBuilder,
    Stream, ListSource, PrintSink
)
from event_driven_pipeline.stream import process_stream


# =============================================================================
# Example 1: Event Bus
# =============================================================================

def demo_event_bus():
    """Demonstrate event bus with publish/subscribe."""
    print("\n" + "=" * 60)
    print("Example 1: Event Bus")
    print("=" * 60)
    
    bus = EventBus()
    
    async def on_data(event):
        print(f"Handler 1 received: {event.data}")
        return f"Processed: {event.data}"
    
    async def on_data_2(event):
        print(f"Handler 2 received: {event.data}")
        return "Done"
    
    # Subscribe
    bus.subscribe("data", on_data)
    bus.subscribe("data", on_data_2)
    
    # Publish
    async def run():
        result = await bus.publish(Event("data", data="Hello World"))
        print(f"Results: {result}")
    
    asyncio.run(run())


# =============================================================================
# Example 2: Pipeline
# =============================================================================

def demo_pipeline():
    """Demonstrate pipeline processing."""
    print("\n" + "=" * 60)
    print("Example 2: Pipeline")
    print("=" * 60)
    
    # Define stages
    def validate(data):
        """Validate input."""
        print(f"Validating: {data}")
        return data
    
    def transform(data):
        """Transform data."""
        print(f"Transforming: {data}")
        return data.upper()
    
    def save(data):
        """Save data."""
        print(f"Saving: {data}")
        return f"Saved: {data}"
    
    # Build pipeline
    pipeline = (
        PipelineBuilder("data_processing")
        .stage("validate", validate)
        .stage("transform", transform)
        .stage("save", save)
        .build()
    )
    
    print(f"Pipeline: {pipeline}")
    
    # Execute
    async def run():
        result = await pipeline.execute("hello world")
        print(f"Final result: {result}")
    
    asyncio.run(run())


# =============================================================================
# Example 3: Stream Processing
# =============================================================================

def demo_stream():
    """Demonstrate stream processing."""
    print("\n" + "=" * 60)
    print("Example 3: Stream Processing")
    print("=" * 60)
    
    # Create stream
    stream = Stream([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    
    # Transform and filter
    result = stream.filter(lambda x: x % 2 == 0).map(lambda x: x * 2)
    
    print(f"Original: [1,2,3,4,5,6,7,8,9,10]")
    print(f"Filtered (even) and doubled: {list(result)}")


# =============================================================================
# Example 4: Async Stream
# =============================================================================

def demo_async_stream():
    """Demonstrate async stream processing."""
    print("\n" + "=" * 60)
    print("Example 4: Async Stream")
    print("=" * 60)
    
    # Create source
    source = ListSource([1, 2, 3, 4, 5])
    
    # Define processors
    async def double(x):
        await asyncio.sleep(0.1)  # Simulate async work
        return x * 2
    
    async def run():
        results = await process_stream(source, double, PrintSink(prefix="Result: "))
        print(f"Processed {len(results)} items")
    
    asyncio.run(run())


# =============================================================================
# Example 5: Complex Pipeline
# =============================================================================

def demo_complex_pipeline():
    """Demonstrate complex event-driven pipeline."""
    print("\n" + "=" * 60)
    print("Example 5: Complex Pipeline")
    print("=" * 60)
    
    bus = EventBus()
    pipeline = Pipeline(name="order_processing")
    
    # Add stages
    def receive_order(data):
        print(f"Received order: {data}")
        return data
    
    def validate_order(data):
        print(f"Validating order: {data}")
        if data.get("amount", 0) <= 0:
            raise ValueError("Invalid amount")
        return data
    
    def process_payment(data):
        print(f"Processing payment: ${data.get('amount')}")
        data["payment_status"] = "completed"
        return data
    
    def send_confirmation(data):
        print(f"Sending confirmation for order {data.get('id')}")
        return data
    
    pipeline.add_stage("receive", receive_order)
    pipeline.add_stage("validate", validate_order)
    pipeline.add_stage("payment", process_payment)
    pipeline.add_stage("confirm", send_confirmation)
    
    # Run
    async def run():
        order = {"id": "12345", "amount": 99.99, "customer": "John"}
        try:
            result = await pipeline.execute(order)
            print(f"Order processed successfully: {result}")
        except Exception as e:
            print(f"Error: {e}")
    
    asyncio.run(run())


# =============================================================================
# Run All Demos
# =============================================================================

if __name__ == "__main__":
    demo_event_bus()
    demo_pipeline()
    demo_stream()
    demo_async_stream()
    demo_complex_pipeline()
    
    print("\n" + "=" * 60)
    print("All demos completed!")
    print("=" * 60)
