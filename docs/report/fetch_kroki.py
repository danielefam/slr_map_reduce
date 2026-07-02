import zlib
import base64
import os

mermaid_code = """sequenceDiagram
    participant O as Orchestrator
    participant W1 as Worker 1
    participant W2 as Worker 2
    
    Note over O, W2: 1. Deploy & Health Check
    O->>W1: GET /health
    O->>W2: GET /health
    
    Note over O, W2: 2. Load Phase
    O->>W1: POST /load (URLs)
    O->>W2: POST /load (URLs)
    W1-->>O: 200 OK
    W2-->>O: 200 OK
    
    Note over O, W2: 3. Map Phase
    O->>W1: POST /map
    O->>W2: POST /map
    W1-->>O: 200 OK
    W2-->>O: 200 OK
    
    Note over O, W2: 4. Shuffle & Reduce Phase
    O->>W1: POST /reduce {peers: [W1, W2]}
    O->>W2: POST /reduce {peers: [W1, W2]}
    
    par Shuffle
        W1->>W2: GET /intermediate?reducer=0
        W2-->>W1: Bucket 0
    and
        W2->>W1: GET /intermediate?reducer=1
        W1-->>W2: Bucket 1
    end
    
    W1-->>O: 200 OK
    W2-->>O: 200 OK
    
    Note over O, W2: 5. Collect Phase
    O->>W1: GET /result
    O->>W2: GET /result
    W1-->>O: Key-Values (Partition 0)
    W2-->>O: Key-Values (Partition 1)
    
    Note over O: Merge and Sort results"""

compressed = zlib.compress(mermaid_code.encode('utf-8'), 9)
encoded = base64.urlsafe_b64encode(compressed).decode('ascii')
url = f"https://kroki.io/mermaid/png/{encoded}"

os.system(f"curl -sL -A 'Mozilla/5.0' -o docs/sequence.png '{url}'")
