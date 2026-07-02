import matplotlib.pyplot as plt
import zlib
import base64
import urllib.request

# Amdahl Graph
nodes = [1, 2, 4, 8, 16, 32, 64]
speedup = [1.0000, 1.7502, 2.7464, 4.1080, 5.0720, 5.4546, 5.2978]
ideal = [1, 2, 4, 8, 16, 32, 64]

plt.figure(figsize=(8, 6))
plt.plot(nodes, speedup, marker='o', linestyle='-', color='b', label='Measured Speedup')
plt.plot(nodes, ideal, linestyle='--', color='r', label='Ideal (Linear) Speedup')
plt.title("Amdahl's Law - 8 WET Files")
plt.xlabel("Number of Nodes")
plt.ylabel("Speedup")
plt.xscale('log', base=2)
plt.yscale('log', base=2)
plt.xticks(nodes, nodes)
plt.yticks(nodes, nodes)
plt.legend()
plt.grid(True, which="both", ls="--", alpha=0.5)
plt.savefig("docs/amdahl_8.png", dpi=300, bbox_inches='tight')

# Sequence Diagram via Kroki
mermaid_code = """sequenceDiagram
    participant Orchestrator
    participant Worker 1 (Map)
    participant Worker 2 (Reduce)
    
    Orchestrator->>Worker 1: POST /load (URLs)
    Worker 1-->>Orchestrator: 200 OK (Downloaded to /tmp)
    
    Orchestrator->>Worker 1: POST /map
    Worker 1-->>Orchestrator: 200 OK (Buckets written locally)
    
    Orchestrator->>Worker 2: POST /reduce (peers: [Worker 1])
    Worker 2->>Worker 1: GET /intermediate?reducer=1
    Worker 1-->>Worker 2: JSONL Bucket
    Worker 2-->>Orchestrator: 200 OK (Reduced)
    
    Orchestrator->>Worker 2: GET /result
    Worker 2-->>Orchestrator: Reduced Key-Values
    Note over Orchestrator: Merge and Sort results"""

compressed = zlib.compress(mermaid_code.encode('utf-8'), 9)
encoded = base64.urlsafe_b64encode(compressed).decode('ascii')
url = f"https://kroki.io/mermaid/png/{encoded}"

urllib.request.urlretrieve(url, "docs/sequence.png")
