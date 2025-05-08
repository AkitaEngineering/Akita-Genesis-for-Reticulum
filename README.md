# Akita Genesis

**Akita Genesis** is a foundational framework for building distributed systems, developed by Akita Engineering (www.akitaengineering.com). It provides core components for node discovery, cluster formation, task management with advanced scheduling, resource monitoring, and inter-node communication, primarily managed via a Command Line Interface (CLI).

**License:** GPLv3

## Features

* **Node Discovery:** Utilizes Reticulum for node discovery and presence announcements within a defined application namespace. Nodes announce their capabilities.
* **Cluster Formation:** Dynamically forms clusters based on discovered nodes sharing a common cluster name. Implements a simple leader election mechanism.
* **Task Management & Advanced Scheduling:**
    * Priority-based task queue system.
    * Leader node delegates tasks to available worker nodes.
    * **Capability Matching:** Tasks can specify required capabilities (e.g., `gpu`), and the leader assigns them to workers possessing those capabilities.
    * **Resource-Aware Scheduling:** Leader considers worker load (task count, CPU usage) when assigning tasks.
* **Fault Tolerance:**
    * Configurable maximum execution attempts for tasks.
    * Timeouts for worker acknowledgment and task processing, triggering re-queues or failures.
* **Resource Monitoring:** Monitors basic CPU, memory, disk, and network usage using `psutil`.
* **Persistent Ledger:** Maintains a ledger of critical system events using SQLite.
* **State Management:** Nodes maintain a local view of the cluster state (nodes, leader, status, capabilities, load). Basic state synchronization via announcements.
* **CLI Control:** Comprehensive Command Line Interface for starting, stopping, monitoring nodes, managing tasks, viewing logs, and inspecting the ledger.
* **API Security:** Node's HTTP control API can be secured using configurable API keys.
* **Configuration:** Flexible configuration system using Pydantic settings, loadable from environment variables or `.env` files.
* **Modular Design:** Organized into distinct Python modules for core functionalities.

## Project Structure

(A brief overview - refer to `docs/architecture.md` for details)

```
akita-genesis/
├── akita_genesis/          # Main application package
│   ├── cli/                # CLI logic
│   ├── core/               # Core node logic (AkitaGenesisNode)
│   ├── modules/            # Functional modules (Comm, Discovery, State, Tasks, etc.)
│   ├── config/             # Configuration (settings.py)
│   └── utils/              # Utilities (logging)
├── docs/                   # Documentation (cli_usage.md, architecture.md)
├── tests/                  # Unit/integration tests (to be developed)
├── .gitignore
├── LICENSE
├── README.md
├── requirements.txt
├── dev-requirements.txt
└── setup.py

```
## Requirements

* Python 3.8+
* Reticulum (`pip install rns`)
* psutil (`pip install psutil`)
* click (`pip install click`)
* pydantic / pydantic-settings (`pip install pydantic pydantic-settings`)
* requests (`pip install requests`)
* rich (`pip install rich`)
* fastapi / uvicorn (`pip install fastapi "uvicorn[standard]"`)
* msgpack (`pip install msgpack`)

(See `requirements.txt` for specific versions).

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/AkitaEngineering/Akita-Genesis-for-Reticulum
    cd akita-genesis
    ```

2.  **Create and activate a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Install Akita Genesis CLI:**
    ```bash
    pip install .
    ```
    Or for development (editable install):
    ```bash
    pip install -e .
    ```

## CLI Usage

After installation, the `akita-genesis` command will be available. See `docs/cli_usage.md` for full details.

**Global Options:**

* `--target-node-api <URL>`: Target node API URL (Env: `AKITA_TARGET_NODE_API`).
* `--api-key <KEY>`: API Key for authentication (Env: `AKITA_API_KEY`). **Required if API keys are configured on the node.**
* `--debug`: Enable CLI debug logging.

**Common Commands:**

* **Start a Node:**
    ```bash
    # Start a node with 'gpu' capability
    akita-genesis start --node-name WorkerGPU --cluster-name MyCluster --capabilities gpu --api-port 8001

    # Start another node
    akita-genesis start --node-name WorkerCPU --cluster-name MyCluster --api-port 8002
    ```

* **Check Node Status:**
    ```bash
    akita-genesis status --target-node-api http://localhost:8001 --api-key <your_key>
    ```

* **Check Cluster Status:**
    ```bash
    akita-genesis cluster status --target-node-api http://localhost:8002 --api-key <your_key>
    ```

* **Submit a Task:**
    ```bash
    # Submit a task requiring 'gpu'
    akita-genesis task submit '{"action":"train","data":"img.dat","required_capabilities":["gpu"]}' --api-key <your_key>

    # Submit a general task
    akita-genesis task submit '{"action":"log","message":"hello"}' --priority 15 --api-key <your_key>
    ```

* **Check Task Status:**
    ```bash
    akita-genesis task status <task_uuid> --api-key <your_key>
    ```

* **View Logs:**
    ```bash
    akita-genesis logs --limit 50 --api-key <your_key>
    ```

* **View Ledger:**
    ```bash
    akita-genesis ledger view --limit 10 --event-type TASK_COMPLETED --api-key <your_key>
    ```

* **Shutdown Node:**
    ```bash
    akita-genesis shutdown --target-node-api http://localhost:8001 --api-key <your_key>
    ```

## Configuration

Configuration parameters are managed in `akita_genesis/config/settings.py` using Pydantic. Settings can be overridden via environment variables (prefixed with `AKITA_`) or a `.env` file in the project root.

Key settings include:

* `LOG_LEVEL`, `SQLITE_DB_FILE`, `DATA_DIR`
* `DEFAULT_NODE_NAME`, `DEFAULT_CLUSTER_NAME`
* `NODE_TIMEOUT_S`, `LEADER_ELECTION_TIMEOUT_S`
* `DISCOVERY_INTERVAL_S`, `ANNOUNCE_INTERVAL_S`
* `DEFAULT_API_HOST`, `DEFAULT_API_PORT`
* **`VALID_API_KEYS`**: Set of allowed API keys (e.g., `AKITA_VALID_API_KEYS='key1,key2'`). If empty, API is unsecured.
* `API_KEY_HEADER_NAME`
* **`MAX_TASK_EXECUTION_ATTEMPTS`**: Max times a task is retried.
* **`WORKER_ACK_TIMEOUT_S`**: Time leader waits for worker ACK.
* **`WORKER_PROCESSING_TIMEOUT_S`**: Time leader waits for worker result after ACK.

## Development

* Install development dependencies: `pip install -r dev-requirements.txt`
* Run linters/formatters: `flake8 .`, `black .`, `isort .`
* Run type checker: `mypy akita_genesis`
* Run tests (once implemented): `pytest`

## Future Improvements

(Refer to `docs/architecture.md` for difficulty assessment)

* **Distributed Ledger:** Implement Raft/Paxos for the ledger for true fault tolerance.
* **Robust State Sync:** Implement CRDTs or version vectors for state synchronization.
* **Advanced Scheduling:** Task stealing, deadline scheduling.
* **Security:** Secure RNS links, role-based access control (RBAC) for API.
* **Fault Tolerance:** Enhanced handling of network partitions, more sophisticated retry strategies.
* **Inter-Cluster Communication:** Define protocols for interaction between distinct Akita Genesis clusters.
* **Web UI:** A web interface for monitoring and management.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues to report bugs or suggest new features. Ensure contributions adhere to the existing coding style (PEP 8, Black, isort) and include tests where appropriate.

---
Akita Engineering
www.akitaengineering.com

