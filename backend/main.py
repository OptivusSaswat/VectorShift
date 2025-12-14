import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
from collections import defaultdict

app = FastAPI()


origins = [
    "http://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PipelineRequest(BaseModel):
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]


def is_dag(nodes: List[Dict], edges: List[Dict]) -> bool:
    """
    Check if the graph is a Directed Acyclic Graph (DAG) using DFS.
    Returns True if no cycles are detected, False otherwise.
    """
    if not nodes:
        return True

    graph = defaultdict(list)
    node_ids = {node['id'] for node in nodes}

    for edge in edges:
        source = edge.get('source')
        target = edge.get('target')
        if source and target:
            graph[source].append(target)

    state = {node_id: 0 for node_id in node_ids}

    def has_cycle(node: str) -> bool:
        if state[node] == 1: 
            return True
        if state[node] == 2: 
            return False

        state[node] = 1  

        for neighbor in graph[node]:
            if neighbor in state and has_cycle(neighbor):
                return True

        state[node] = 2  
        return False

    for node_id in node_ids:
        if state[node_id] == 0:
            if has_cycle(node_id):
                return False

    return True


@app.get('/')
def read_root():
    return {'Ping': 'Pong'}


@app.post('/pipelines/parse')
def parse_pipeline(request: PipelineRequest):
    """
    Parse the pipeline and return metrics about nodes, edges, and DAG status.
    """
    num_nodes = len(request.nodes)
    num_edges = len(request.edges)
    dag_status = is_dag(request.nodes, request.edges)

    return {
        'num_nodes': num_nodes,
        'num_edges': num_edges,
        'is_dag': dag_status
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
