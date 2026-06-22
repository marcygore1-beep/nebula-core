class TaskDependencyEngine:
    """
    Handles complex task dependencies with intelligent ordering.
    
    Features:
    - DAG validation
    - Parallel execution optimization
    - Dependency resolution
    - Deadlock detection
    """
    
    def __init__(self):
        self.dependency_graph = nx.DiGraph()
        self.execution_order = []
        
    async def add_task(self, task: Task):
        """
        Add task to dependency graph.
        """
        self.dependency_graph.add_node(task.id, task=task)
        
        for dep_id in task.dependencies:
            if dep_id in self.dependency_graph:
                self.dependency_graph.add_edge(dep_id, task.id)
            else:
                raise ValueError(f"Dependency {dep_id} not found")
    
    async def resolve_dependencies(self, task_ids: List[str]) -> List[List[str]]:
        """
        Resolve task dependencies and create execution layers.
        """
        # Build subgraph for given tasks
        subgraph = self.dependency_graph.subgraph(task_ids)
        
        # Check for cycles
        if nx.is_directed_acyclic_graph(subgraph):
            # Topological sort to get execution order
            topo_order = list(nx.topological_sort(subgraph))
            
            # Group into parallel execution layers
            layers = self._group_into_layers(subgraph, topo_order)
            
            return layers
        else:
            # Detect and resolve cycles
            cycles = list(nx.simple_cycles(subgraph))
            if cycles:
                await self._resolve_cycles(cycles)
            return await self.resolve_dependencies(task_ids)
    
    def _group_into_layers(self, graph: nx.DiGraph, topo_order: List[str]) -> List[List[str]]:
        """
        Group tasks into parallel execution layers.
        """
        layers = []
        remaining = set(topo_order)
        
        while remaining:
            layer = []
            for node in list(remaining):
                # Check if all dependencies are in previous layers
                predecessors = set(graph.predecessors(node))
                if predecessors.issubset(set().union(*layers)):
                    layer.append(node)
                    remaining.remove(node)
            
            if layer:
                layers.append(layer)
            else:
                # Handle potential deadlock
                raise RuntimeError("Deadlock detected in dependency graph")
        
        return layers
