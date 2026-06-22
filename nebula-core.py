"""
Distributed Task Queue with Auto-scaling Worker Pool
A sophisticated system demonstrating advanced concurrency, distributed processing,
and intelligent resource management.

Author: A Genius Engineer
License: MIT
"""

import asyncio
import json
import logging
import pickle
import socket
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import aiohttp
import aioredis
import psutil
import yaml
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ================ Configuration & Constants ================

class TaskPriority(Enum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4

class TaskStatus(Enum):
    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"

@dataclass
class Task:
    """Represents a unit of work in the distributed system."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    payload: Dict[str, Any]
    priority: TaskPriority = TaskPriority.NORMAL
    created_at: datetime = field(default_factory=datetime.utcnow)
    scheduled_at: Optional[datetime] = None
    timeout: int = 30  # seconds
    max_retries: int = 3
    retry_count: int = 0
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    worker_id: Optional[str] = None
    queue_name: str = "default"
    dependencies: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize task to dictionary for storage."""
        return {
            "id": self.id,
            "name": self.name,
            "payload": self.payload,
            "priority": self.priority.value,
            "created_at": self.created_at.isoformat(),
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "status": self.status.value,
            "result": pickle.dumps(self.result).hex() if self.result else None,
            "error": self.error,
            "worker_id": self.worker_id,
            "queue_name": self.queue_name,
            "dependencies": list(self.dependencies),
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        """Reconstruct task from dictionary."""
        task = cls(
            id=data["id"],
            name=data["name"],
            payload=data["payload"],
            priority=TaskPriority(data["priority"]),
            timeout=data["timeout"],
            max_retries=data["max_retries"],
            retry_count=data["retry_count"],
            queue_name=data["queue_name"],
            dependencies=set(data.get("dependencies", [])),
            metadata=data.get("metadata", {})
        )
        task.created_at = datetime.fromisoformat(data["created_at"])
        task.scheduled_at = datetime.fromisoformat(data["scheduled_at"]) if data.get("scheduled_at") else None
        task.status = TaskStatus(data["status"])
        task.result = pickle.loads(bytes.fromhex(data["result"])) if data.get("result") else None
        task.error = data.get("error")
        task.worker_id = data.get("worker_id")
        return task

# ================ Core Engine ================

class TaskExecutor(ABC):
    """Abstract base class for task execution logic."""
    
    @abstractmethod
    async def execute(self, task: Task) -> Any:
        """Execute the task and return result."""
        pass
    
    @abstractmethod
    async def validate(self, task: Task) -> bool:
        """Validate if task can be executed."""
        pass

class TaskRegistry:
    """Registry for task types and their executors."""
    
    def __init__(self):
        self._executors: Dict[str, TaskExecutor] = {}
        self._middleware: List[Callable] = []
    
    def register(self, name: str, executor: TaskExecutor):
        """Register a task executor."""
        self._executors[name] = executor
    
    def get_executor(self, name: str) -> Optional[TaskExecutor]:
        """Get executor for task type."""
        return self._executors.get(name)
    
    def add_middleware(self, middleware: Callable):
        """Add middleware for task processing pipeline."""
        self._middleware.append(middleware)
    
    async def process_task(self, task: Task) -> Task:
        """Process task through middleware and executor."""
        try:
            # Run middleware chain
            for middleware in self._middleware:
                task = await middleware(task)
                if task.status == TaskStatus.CANCELLED:
                    return task
            
            # Get executor
            executor = self.get_executor(task.name)
            if not executor:
                task.status = TaskStatus.FAILED
                task.error = f"No executor registered for task: {task.name}"
                return task
            
            # Validate
            if not await executor.validate(task):
                task.status = TaskStatus.FAILED
                task.error = "Task validation failed"
                return task
            
            # Execute
            start_time = time.time()
            result = await asyncio.wait_for(
                executor.execute(task),
                timeout=task.timeout
            )
            task.result = result
            task.status = TaskStatus.COMPLETED
            
            # Record metrics
            duration = time.time() - start_time
            TaskMetrics.record_execution(task.name, duration, True)
            
        except asyncio.TimeoutError:
            task.status = TaskStatus.TIMEOUT
            task.error = f"Task exceeded timeout of {task.timeout}s"
            TaskMetrics.record_execution(task.name, task.timeout, False)
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            TaskMetrics.record_execution(task.name, 0, False)
        
        return task

# ================ Storage Backend ================

class StorageBackend(ABC):
    """Abstract storage backend for task persistence."""
    
    @abstractmethod
    async def save_task(self, task: Task):
        pass
    
    @abstractmethod
    async def get_task(self, task_id: str) -> Optional[Task]:
        pass
    
    @abstractmethod
    async def update_task(self, task: Task):
        pass
    
    @abstractmethod
    async def delete_task(self, task_id: str):
        pass
    
    @abstractmethod
    async def get_pending_tasks(self, queue: str = "default", limit: int = 100) -> List[Task]:
        pass

class RedisStorage(StorageBackend):
    """Redis-based storage backend with advanced features."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self._client: Optional[aioredis.Redis] = None
    
    async def initialize(self):
        """Initialize Redis connection with retry logic."""
        retries = 5
        for i in range(retries):
            try:
                self._client = await aioredis.from_url(
                    self.redis_url,
                    decode_responses=False,
                    max_connections=50
                )
                await self._client.ping()
                logging.info("Redis connection established")
                return
            except Exception as e:
                logging.warning(f"Redis connection attempt {i+1} failed: {e}")
                await asyncio.sleep(2 ** i)  # Exponential backoff
        raise ConnectionError("Failed to connect to Redis")
    
    async def save_task(self, task: Task):
        """Save task with TTL and indexing."""
        key = f"task:{task.id}"
        data = json.dumps(task.to_dict())
        await self._client.setex(key, 86400, data)  # 24h TTL
        
        # Add to queue
        await self._client.zadd(
            f"queue:{task.queue_name}",
            {task.id: task.priority.value}
        )
        
        # Index by status
        await self._client.sadd(f"status:{task.status.value}", task.id)
    
    async def get_task(self, task_id: str) -> Optional[Task]:
        key = f"task:{task_id}"
        data = await self._client.get(key)
        if data:
            return Task.from_dict(json.loads(data))
        return None
    
    async def update_task(self, task: Task):
        """Update task with atomic operations."""
        # Use Redis transaction for consistency
        async with self._client.pipeline(transaction=True) as pipe:
            # Update task data
            pipe.setex(f"task:{task.id}", 86400, json.dumps(task.to_dict()))
            
            # Update status index
            pipe.smove("status:previous", f"status:{task.status.value}", task.id)
            
            await pipe.execute()
    
    async def delete_task(self, task_id: str):
        await self._client.delete(f"task:{task_id}")
    
    async def get_pending_tasks(self, queue: str = "default", limit: int = 100) -> List[Task]:
        """Get pending tasks from queue with priority ordering."""
        task_ids = await self._client.zrange(
            f"queue:{queue}",
            0, limit - 1,
            byscore=True
        )
        
        tasks = []
        for task_id in task_ids:
            task = await self.get_task(task_id.decode())
            if task and task.status == TaskStatus.PENDING:
                tasks.append(task)
        
        return tasks

# ================ Auto-Scaling Worker Pool ================

class ScalingStrategy(Enum):
    THRESHOLD = "threshold"
    PREDICTIVE = "predictive"
    HYBRID = "hybrid"

@dataclass
class ScalingConfig:
    """Configuration for auto-scaling."""
    strategy: ScalingStrategy = ScalingStrategy.HYBRID
    min_workers: int = 2
    max_workers: int = 50
    scale_up_threshold: float = 0.7  # 70% queue utilization
    scale_down_threshold: float = 0.3  # 30% queue utilization
    scale_up_interval: int = 10  # seconds
    scale_down_interval: int = 30  # seconds
    cooldown_period: int = 60  # seconds between scale operations
    prediction_window: int = 60  # seconds to look ahead
    cpu_threshold: float = 0.75  # 75% CPU usage triggers scale up
    memory_threshold: float = 0.80  # 80% memory usage triggers scale up

class WorkerManager:
    """Manages worker lifecycle and auto-scaling."""
    
    def __init__(
        self,
        storage: StorageBackend,
        registry: TaskRegistry,
        config: ScalingConfig
    ):
        self.storage = storage
        self.registry = registry
        self.config = config
        self.workers: Dict[str, 'Worker'] = {}
        self._running = False
        self._task_queue: asyncio.Queue = asyncio.Queue()
        self._metrics = defaultdict(lambda: defaultdict(float))
        self._last_scale_time = datetime.utcnow()
        
        # Performance monitoring
        self._cpu_history = []
        self._queue_history = []
        self._throughput_history = []
    
    async def start(self):
        """Start the worker manager and auto-scaling loop."""
        self._running = True
        
        # Start initial workers
        for _ in range(self.config.min_workers):
            await self._create_worker()
        
        # Start background tasks
        asyncio.create_task(self._scaling_loop())
        asyncio.create_task(self._health_check_loop())
        asyncio.create_task(self._metrics_collector())
        
        logging.info(f"Worker manager started with {self.config.min_workers} workers")
    
    async def stop(self):
        """Gracefully stop all workers."""
        self._running = False
        
        # Wait for workers to finish
        for worker in self.workers.values():
            await worker.stop()
        
        self.workers.clear()
        logging.info("All workers stopped")
    
    async def _create_worker(self) -> 'Worker':
        """Create and start a new worker."""
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        worker = Worker(
            worker_id,
            self.storage,
            self.registry,
            self._task_queue
        )
        self.workers[worker_id] = worker
        asyncio.create_task(worker.start())
        return worker
    
    async def _scaling_loop(self):
        """Main auto-scaling decision loop."""
        while self._running:
            await asyncio.sleep(5)  # Check every 5 seconds
            
            # Check if we're in cooldown period
            if (datetime.utcnow() - self._last_scale_time).seconds < self.config.cooldown_period:
                continue
            
            current_workers = len(self.workers)
            queue_size = self._task_queue.qsize()
            max_queue_size = self.config.max_workers * 10  # Assume each worker handles 10 tasks
            
            # Calculate metrics
            queue_utilization = queue_size / max_queue_size if max_queue_size > 0 else 0
            cpu_utilization = psutil.cpu_percent() / 100
            memory_utilization = psutil.virtual_memory().percent / 100
            
            # Decision logic
            should_scale_up = False
            should_scale_down = False
            
            if self.config.strategy == ScalingStrategy.THRESHOLD:
                should_scale_up = queue_utilization > self.config.scale_up_threshold
                should_scale_down = queue_utilization < self.config.scale_down_threshold
            
            elif self.config.strategy == ScalingStrategy.PREDICTIVE:
                predicted_load = self._predict_queue_load()
                should_scale_up = predicted_load > self.config.scale_up_threshold
                should_scale_down = predicted_load < self.config.scale_down_threshold
            
            elif self.config.strategy == ScalingStrategy.HYBRID:
                # Hybrid: consider both current and predicted metrics
                predicted_load = self._predict_queue_load()
                should_scale_up = (
                    queue_utilization > self.config.scale_up_threshold or
                    predicted_load > self.config.scale_up_threshold or
                    cpu_utilization > self.config.cpu_threshold or
                    memory_utilization > self.config.memory_threshold
                )
                should_scale_down = (
                    queue_utilization < self.config.scale_down_threshold and
                    predicted_load < self.config.scale_down_threshold and
                    cpu_utilization < self.config.cpu_threshold * 0.6
                )
            
            # Execute scaling decision
            if should_scale_up and current_workers < self.config.max_workers:
                workers_to_add = min(
                    self.config.max_workers - current_workers,
                    int(current_workers * 0.3) + 1  # Scale up by 30% + 1
                )
                for _ in range(workers_to_add):
                    await self._create_worker()
                
                self._last_scale_time = datetime.utcnow()
                logging.info(f"Scaled up to {len(self.workers)} workers (added {workers_to_add})")
            
            elif should_scale_down and current_workers > self.config.min_workers:
                workers_to_remove = min(
                    current_workers - self.config.min_workers,
                    int(current_workers * 0.2) + 1  # Scale down by 20% + 1
                )
                
                # Remove idle workers
                idle_workers = sorted(
                    [w for w in self.workers.values() if w.is_idle()],
                    key=lambda w: w.get_uptime(),
                    reverse=True
                )
                
                for worker in idle_workers[:workers_to_remove]:
                    await worker.stop()
                    del self.workers[worker.id]
                
                self._last_scale_time = datetime.utcnow()
                logging.info(f"Scaled down to {len(self.workers)} workers (removed {workers_to_remove})")
    
    def _predict_queue_load(self) -> float:
        """Predict future queue load using historical data."""
        if len(self._queue_history) < 10:
            return 0.0
        
        # Simple moving average with trend
        recent = self._queue_history[-10:]
        avg = sum(recent) / len(recent)
        trend = (recent[-1] - recent[0]) / len(recent)
        predicted = avg + trend * 5  # Predict 5 steps ahead
        
        return min(max(predicted / 100, 0), 1.0)
    
    async def _health_check_loop(self):
        """Periodic health check of all workers."""
        while self._running:
            await asyncio.sleep(30)
            
            for worker_id, worker in list(self.workers.items()):
                if not worker.is_healthy():
                    logging.warning(f"Worker {worker_id} is unhealthy, restarting...")
                    await worker.stop()
                    del self.workers[worker_id]
                    await self._create_worker()
    
    async def _metrics_collector(self):
        """Collect and store performance metrics."""
        while self._running:
            await asyncio.sleep(10)
            
            # Update metrics
            self._cpu_history.append(psutil.cpu_percent())
            self._queue_history.append(self._task_queue.qsize())
            self._throughput_history.append(
                self._metrics['throughput']['last_10s']
            )
            
            # Trim history
            max_history = 100
            for hist in [self._cpu_history, self._queue_history, self._throughput_history]:
                if len(hist) > max_history:
                    hist.pop(0)
            
            # Export metrics
            TaskMetrics.workers_gauge.set(len(self.workers))
            TaskMetrics.queue_size_gauge.set(self._task_queue.qsize())

# ================ Worker Implementation ================

class Worker:
    """Individual worker that processes tasks from the queue."""
    
    def __init__(
        self,
        worker_id: str,
        storage: StorageBackend,
        registry: TaskRegistry,
        task_queue: asyncio.Queue
    ):
        self.id = worker_id
        self.storage = storage
        self.registry = registry
        self.task_queue = task_queue
        self._running = False
        self._current_task: Optional[Task] = None
        self._processed_count = 0
        self._error_count = 0
        self._start_time = datetime.utcnow()
        self._last_activity = datetime.utcnow()
    
    async def start(self):
        """Start processing tasks."""
        self._running = True
        while self._running:
            try:
                # Get task with timeout
                try:
                    task = await asyncio.wait_for(
                        self.task_queue.get(),
                        timeout=5
                    )
                except asyncio.TimeoutError:
                    continue
                
                self._current_task = task
                self._last_activity = datetime.utcnow()
                
                # Process task
                task.worker_id = self.id
                task.status = TaskStatus.PROCESSING
                await self.storage.update_task(task)
                
                # Execute
                processed_task = await self.registry.process_task(task)
                
                # Update storage
                await self.storage.update_task(processed_task)
                
                # Update metrics
                self._processed_count += 1
                if processed_task.status == TaskStatus.COMPLETED:
                    TaskMetrics.tasks_completed_counter.inc()
                else:
                    self._error_count += 1
                    TaskMetrics.tasks_failed_counter.inc()
                
                self._current_task = None
                
            except Exception as e:
                logging.error(f"Worker {self.id} error: {e}")
                self._error_count += 1
                await asyncio.sleep(1)
    
    async def stop(self):
        """Stop the worker."""
        self._running = False
        if self._current_task:
            # Requeue current task
            await self.task_queue.put(self._current_task)
    
    def is_idle(self) -> bool:
        """Check if worker is idle."""
        return self._current_task is None
    
    def is_healthy(self) -> bool:
        """Check worker health."""
        # Worker is unhealthy if it hasn't been active for 5 minutes
        idle_time = (datetime.utcnow() - self._last_activity).seconds
        return idle_time < 300
    
    def get_uptime(self) -> float:
        """Get worker uptime in seconds."""
        return (datetime.utcnow() - self._start_time).seconds
    
    def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        return {
            "id": self.id,
            "uptime": self.get_uptime(),
            "processed_count": self._processed_count,
            "error_count": self._error_count,
            "is_idle": self.is_idle(),
            "current_task": self._current_task.id if self._current_task else None
        }

# ================ API & Monitoring ================

class TaskMetrics:
    """Prometheus metrics for task monitoring."""
    
    tasks_total = Counter('tasks_total', 'Total tasks processed', ['queue'])
    tasks_completed_counter = Counter('tasks_completed', 'Completed tasks', ['queue'])
    tasks_failed_counter = Counter('tasks_failed', 'Failed tasks', ['queue'])
    task_duration_histogram = Histogram('task_duration_seconds', 'Task duration', ['task_name'])
    queue_size_gauge = Gauge('queue_size', 'Current queue size')
    workers_gauge = Gauge('workers_total', 'Total number of workers')
    
    @classmethod
    def record_execution(cls, task_name: str, duration: float, success: bool):
        """Record task execution metrics."""
        cls.task_duration_histogram.labels(task_name=task_name).observe(duration)
        if success:
            cls.tasks_completed_counter.inc()
        else:
            cls.tasks_failed_counter.inc()

class TaskQueueAPI:
    """REST API for task management and monitoring."""
    
    def __init__(self, manager: WorkerManager):
        self.manager = manager
        self.app = None
    
    async def start(self, host: str = "0.0.0.0", port: int = 8080):
        """Start the API server."""
        from aiohttp import web
        
        app = web.Application()
        app.router.add_post("/api/tasks", self.submit_task)
        app.router.add_get("/api/tasks/{task_id}", self.get_task)
        app.router.add_get("/api/tasks", self.list_tasks)
        app.router.add_delete("/api/tasks/{task_id}", self.cancel_task)
        app.router.add_get("/api/workers", self.get_workers)
        app.router.add_get("/api/metrics", self.get_metrics)
        app.router.add_post("/api/tasks/batch", self.submit_batch)
        
        self.app = app
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        logging.info(f"API server started on {host}:{port}")
    
    async def submit_task(self, request):
        """Submit a new task."""
        data = await request.json()
        
        task = Task(
            name=data["name"],
            payload=data.get("payload", {}),
            priority=TaskPriority(data.get("priority", 2)),
            timeout=data.get("timeout", 30),
            max_retries=data.get("max_retries", 3),
            queue_name=data.get("queue", "default"),
            scheduled_at=datetime.fromisoformat(data["scheduled_at"]) if data.get("scheduled_at") else None
        )
        
        # Save task
        await self.manager.storage.save_task(task)
        
        # Add to queue if not scheduled
        if not task.scheduled_at or task.scheduled_at <= datetime.utcnow():
            await self.manager._task_queue.put(task)
        
        return web.json_response({"task_id": task.id, "status": "submitted"})
    
    async def submit_batch(self, request):
        """Submit multiple tasks in batch."""
        data = await request.json()
        tasks = data.get("tasks", [])
        
        submitted = []
        for task_data in tasks:
            task = Task(
                name=task_data["name"],
                payload=task_data.get("payload", {}),
                priority=TaskPriority(task_data.get("priority", 2)),
                timeout=task_data.get("timeout", 30)
            )
            await self.manager.storage.save_task(task)
            await self.manager._task_queue.put(task)
            submitted.append(task.id)
        
        return web.json_response({"submitted": len(submitted), "task_ids": submitted})
    
    async def get_task(self, request):
        """Get task details."""
        task_id = request.match_info["task_id"]
        task = await self.manager.storage.get_task(task_id)
        if task:
            return web.json_response(task.to_dict())
        return web.json_response({"error": "Task not found"}, status=404)
    
    async def list_tasks(self, request):
        """List tasks with filtering."""
        queue = request.query.get("queue", "default")
        status = request.query.get("status")
        limit = int(request.query.get("limit", 100))
        
        tasks = await self.manager.storage.get_pending_tasks(queue, limit)
        return web.json_response([t.to_dict() for t in tasks])
    
    async def cancel_task(self, request):
        """Cancel a pending task."""
        task_id = request.match_info["task_id"]
        task = await self.manager.storage.get_task(task_id)
        if task and task.status == TaskStatus.PENDING:
            task.status = TaskStatus.CANCELLED
            await self.manager.storage.update_task(task)
            return web.json_response({"status": "cancelled"})
        return web.json_response({"error": "Task not found or not pending"}, status=404)
    
    async def get_workers(self, request):
        """Get worker statistics."""
        workers = [
            worker.get_stats()
            for worker in self.manager.workers.values()
        ]
        return web.json_response({
            "total": len(workers),
            "workers": workers
        })
    
    async def get_metrics(self, request):
        """Get system metrics."""
        return web.json_response({
            "queue_size": self.manager._task_queue.qsize(),
            "workers": len(self.manager.workers),
            "cpu_usage": psutil.cpu_percent(),
            "memory_usage": psutil.virtual_memory().percent,
            "uptime": (datetime.utcnow() - self.manager._last_scale_time).seconds
        })

# ================ Main Application ================

class DistributedTaskQueue:
    """Main application class orchestrating all components."""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.storage = RedisStorage(self.config.get("redis_url", "redis://localhost:6379"))
        self.registry = TaskRegistry()
        self.scaling_config = ScalingConfig(**self.config.get("scaling", {}))
        self.manager = WorkerManager(self.storage, self.registry, self.scaling_config)
        self.api = TaskQueueAPI(self.manager)
        self._running = False
    
    def _load_config(self, path: str) -> Dict[str, Any]:
        """Load configuration from file."""
        try:
            with open(path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logging.warning(f"Config file {path} not found, using defaults")
            return {}
    
    def register_task(self, name: str, executor: TaskExecutor):
        """Register a task type."""
        self.registry.register(name, executor)
        logging.info(f"Registered task: {name}")
    
    async def start(self):
        """Start the distributed task queue system."""
        self._running = True
        
        # Initialize storage
        await self.storage.initialize()
        
        # Start metrics server
        start_http_server(9090)
        logging.info("Metrics server started on port 9090")
        
        # Start worker manager
        await self.manager.start()
        
        # Start API server
        await self.api.start(port=self.config.get("api_port", 8080))
        
        logging.info("Distributed Task Queue system started")
    
    async def stop(self):
        """Stop the system gracefully."""
        self._running = False
        await self.manager.stop()
        logging.info("System stopped")
    
    async def submit_task(self, task: Task):
        """Submit a task to the queue."""
        await self.storage.save_task(task)
        await self.manager._task_queue.put(task)
        return task.id

# ================ Example Task Executors ================

class ExampleTaskExecutor(TaskExecutor):
    """Example implementation of a task executor."""
    
    async def validate(self, task: Task) -> bool:
        """Validate task payload."""
        required_fields = ["input_data"]
        return all(field in task.payload for field in required_fields)
    
    async def execute(self, task: Task) -> Any:
        """Execute the task."""
        input_data = task.payload["input_data"]
        
        # Simulate work
        await asyncio.sleep(2)
        
        # Process data
        result = {
            "input": input_data,
            "processed": True,
            "timestamp": datetime.utcnow().isoformat(),
            "result": input_data * 2  # Example processing
        }
        
        return result

class DataProcessingExecutor(TaskExecutor):
    """Complex data processing task executor."""
    
    async def validate(self, task: Task) -> bool:
        """Validate complex data processing task."""
        return (
            "data" in task.payload and
            "operation" in task.payload and
            task.payload["operation"] in ["transform", "aggregate", "filter"]
        )
    
    async def execute(self, task: Task) -> Any:
        """Execute data processing with error handling."""
        data = task.payload["data"]
        operation = task.payload["operation"]
        
        # Simulate processing pipeline
        try:
            if operation == "transform":
                result = [item * 2 for item in data]
            elif operation == "aggregate":
                result = sum(data)
            elif operation == "filter":
                threshold = task.payload.get("threshold", 10)
                result = [item for item in data if item > threshold]
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            return {
                "operation": operation,
                "input_size": len(data),
                "output": result,
                "processed_at": datetime.utcnow().isoformat()
            }
        except Exception as e:
            raise RuntimeError(f"Data processing failed: {e}")

# ================ Application Entry Point ================

async def main():
    """Main application entry point."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create application
    app = DistributedTaskQueue("config.yaml")
    
    # Register task executors
    app.register_task("example_task", ExampleTaskExecutor())
    app.register_task("data_processing", DataProcessingExecutor())
    
    try:
        # Start the system
        await app.start()
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
