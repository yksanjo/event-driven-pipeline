"""
Pipeline module for Event-Driven Pipeline.

Provides pipeline construction and execution with stages, transformations, and async support.
"""

from typing import Any, Callable, List, Optional
from dataclasses import dataclass
import asyncio


@dataclass
class PipelineStage:
    """A single stage in the pipeline."""
    name: str
    handler: Callable
    condition: Optional[Callable[[Any], bool]] = None
    
    async def execute(self, data: Any) -> Any:
        """Execute the stage handler."""
        if self.condition and not self.condition(data):
            return data
        
        if asyncio.iscoroutinefunction(self.handler):
            return await self.handler(data)
        return self.handler(data)


class Pipeline:
    """
    A processing pipeline with multiple stages.
    
    Example:
        pipeline = Pipeline()
        pipeline.add_stage("validate", validate_func)
        pipeline.add_stage("transform", transform_func)
        pipeline.add_stage("save", save_func)
        
        result = await pipeline.execute(input_data)
    """
    
    def __init__(self, name: str = "pipeline"):
        self.name = name
        self.stages: List[PipelineStage] = []
    
    def add_stage(
        self,
        name: str,
        handler: Callable,
        condition: Optional[Callable] = None
    ) -> "Pipeline":
        """Add a stage to the pipeline."""
        stage = PipelineStage(name=name, handler=handler, condition=condition)
        self.stages.append(stage)
        return self
    
    async def execute(self, initial_data: Any) -> Any:
        """Execute all stages in sequence."""
        data = initial_data
        for stage in self.stages:
            data = await stage.execute(data)
        return data
    
    def __repr__(self) -> str:
        return f"Pipeline(name='{self.name}', stages={len(self.stages)})"


class PipelineBuilder:
    """Fluent builder for creating pipelines."""
    
    def __init__(self, name: str = "pipeline"):
        self.name = name
        self._stages: List[PipelineStage] = []
    
    def stage(self, name: str, handler: Callable) -> "PipelineBuilder":
        """Add a stage."""
        self._stages.append(PipelineStage(name=name, handler=handler))
        return self
    
    def build(self) -> Pipeline:
        """Build the pipeline."""
        pipeline = Pipeline(name=self.name)
        pipeline.stages = self._stages
        return pipeline
