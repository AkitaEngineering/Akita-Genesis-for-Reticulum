# Akita Genesis Architecture

This document provides a high-level overview of the Akita Genesis framework architecture.

## Core Concepts

Akita Genesis is designed as a modular framework for building distributed systems. Key goals include:

* **Decentralization:** Nodes operate peer-to-peer using Reticulum for communication and discovery, although a leader node coordinates task assignment within a cluster.
* **Cluster Formation:** Nodes automatically discover each other and form logical clusters based on a shared cluster name.
* **Task Management:** A distributed task queue where tasks are submitted to the cluster, assigned to worker nodes by a leader based on load and capabilities, and processed.
* **State Management:** Mechanisms for nodes within a cluster to maintain a reasonably consistent view of the cluster's state (nodes, leader).
* **Observability:** Providing status information, logs, and a ledger of events via a CLI and node API.
* **Basic Security:** API access control via API keys.
* **Fault Tolerance:** Task retry mechanisms and node timeout detection.

## Main Components

The system is primarily composed of modules running within each `AkitaGenesisNode` process:

1.  **`core.node.AkitaGenesisNode`:**
    * The central orchestrator class for a single node.
    * Initializes and manages the lifecycle of all other modules.
    * Runs the main asyncio event loop for the node.
    * Hosts the FastAPI server for external API interactions (CLI, UI), **secured with API key authentication**.
    * Handles role-specific logic (leader vs. follower duties).
    * **Leader:** Implements task assignment logic considering worker load, resources, and **task capabilities**. Manages task timeouts and retries.
    * **Worker:** Handles receiving task assignments, acknowledging them, simulating processing, and sending results back to the leader.

2.  **`modules.communication.CommunicationManager`:**
    * Manages all Reticulum (RNS) network interactions.
    * Handles the node's RNS identity.
    * Sets up RNS destinations for different communication aspects (discovery, tasks, state, control).
    * Provides methods for sending unicast messages (e.g., task assignments, results) and initiating periodic announcements (discovery).
    * Deserializes incoming messages and routes them to appropriate handlers.

3.  **`modules.discovery.DiscoveryManager`:**
    * Uses the `CommunicationManager` to periodically announce the node's presence and **capabilities** (`NodeInfo`) via `RNS.Announce`.
    * Listens for announcements from other nodes.
    * Informs the `StateManager` about discovered or updated nodes.

4.  **`modules.state_manager.StateManager`:**
    * Maintains the node's local view of the cluster state (`ClusterState`), including `NodeInfo` (with **capabilities, task load, resources**) and the current leader.
    * Runs a simple leader election algorithm.
    * Performs periodic cleanup of stale/offline nodes.
    * Persists node status information to the database.
    * Provides methods to query cluster state, including `get_available_workers` which **filters by capabilities and sorts by load/resources**.

5.  **`modules.task_manager.TaskManager`:**
    * Manages the lifecycle of tasks.
    * Interacts with the database to store/retrieve task info, including **execution attempts**.
    * Provides methods for submitting tasks, querying status, and updating task states.
    * Includes logic for task re-queuing (e.g., `re_queue_stale_assigned_task`).

6.  **`modules.ledger.Ledger`:**
    * Provides an append-only log for significant system events.
    * Stores events in the database.

7.  **`modules.resources.ResourceMonitor`:**
    * Periodically monitors local system resources.
    * Provides resource info to the `StateManager`.

8.  **`modules.persistence.DatabaseManager`:**
    * Manages interaction with the SQLite database via a dedicated processing queue.
    * Initializes the schema (including new task fields like `execution_attempts`).

9.  **`cli.main`:**
    * The command-line interface (`click`, `rich`).
    * Provides commands to `start` nodes (now including `--capabilities`).
    * Interacts with the node's FastAPI API, **now requiring an `--api-key` option** for most commands if the node is secured.
    * Includes `logs` command to fetch logs via API.

10. **`config.settings`:**
    * Manages configuration, including **`VALID_API_KEYS`**, **`MAX_TASK_EXECUTION_ATTEMPTS`**, and worker timeouts.

## Communication Flow (Example: Task Submission & Execution - Updated)

1.  **Submission:** User runs `akita-genesis task submit '{"data":..., "required_capabilities":["gpu"]}' --api-key <KEY>`.
2.  **API Request:** CLI sends POST request with API key header to the target node's `/tasks/submit` endpoint.
3.  **Authentication:** Node's FastAPI app validates the API key.
4.  **Forwarding (Optional):** If the receiving node is not the leader, it looks up the leader via `StateManager`, finds its RNS address, and sends a `TASK_SUBMIT_TO_LEADER` message via `CommunicationManager`.
5.  **Leader Acceptance:** Leader receives/accepts the task, stores it as `PENDING` via `TaskManager`.
6.  **Assignment (Advanced):** Leader's loop fetches the task. It calls `StateManager.get_available_workers(required_capabilities=["gpu"])`. It selects the best worker based on capabilities, low task count, and low resource usage. It updates task to `ASSIGNED` and sends `TASK_ASSIGN_TO_WORKER` message (containing task details) to the worker via `CommunicationManager`. It tracks the assignment for ACK timeout.
7.  **Worker Acknowledgment:** Worker receives assignment, sends `TASK_ACKNOWLEDGED_BY_WORKER` message back to leader. Leader receives ACK, starts tracking processing timeout.
8.  **Processing:** Worker simulates task processing.
9.  **Result:** Worker sends `TASK_RESULT_FROM_WORKER` message (containing success/failure and result data/error) back to the leader.
10. **Completion:** Leader receives result, updates task to `COMPLETED` or `FAILED` via `TaskManager`, decrements worker task count.
11. **Timeout/Retry (Fault Tolerance):**
    * If worker doesn't ACK in time, leader calls `TaskManager.re_queue_stale_assigned_task`. Task becomes `PENDING` again, attempts incremented.
    * If worker ACKs but doesn't send result in time, leader marks task `FAILED` via `TaskManager`.
    * If task attempts exceed `MAX_TASK_EXECUTION_ATTEMPTS`, leader marks task `FAILED`.
12. **Status Check:** User runs `akita-genesis task status <TASK_ID> --api-key <KEY>`. API is authenticated, status retrieved from DB.

## Future Directions

* **Distributed Ledger:** Replace single SQLite ledger with a consensus-based log (e.g., Raft).
* **Robust State Sync:** Implement more sophisticated state synchronization using CRDTs or version vectors.
* **Advanced Scheduling:** Task stealing, deadline scheduling.
* **Security:** Secure RNS links (investigate RNS options), role-based access control (RBAC) for API.
* **Fault Tolerance:** More nuanced failure handling, network partition detection/recovery strategies.
* **Inter-Cluster Communication:** Define protocols for interaction between separate Akita Genesis clusters.
* **Comprehensive Testing:** Extensive unit and integration tests.
* **Web UI:** A web interface for monitoring and management.

