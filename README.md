# Akita Genesis

Akita Genesis is a foundational framework for building distributed systems. It provides core components for node discovery, cluster formation, task management, resource monitoring, and inter-cluster communication.

## Features

* **Node Discovery:** Utilizes Reticulum for node discovery and presence announcements.
* **Cluster Formation:** Automatically forms clusters based on available nodes and a defined cluster size.
* **Task Management:** Implements a priority-based task queue and task delegation system.
* **Resource Monitoring:** Monitors CPU, memory, disk, and network usage using `psutil`.
* **Ledger:** Maintains a persistent ledger of system events using SQLite.
* **State Management:** Broadcasts and merges cluster state information for consistency.
* **Inter-Cluster Communication:** Enables communication between different clusters.
* **Persistence:** Uses SQLite to store ledger and state data.
* **Modular Design:** Organized into separate modules for easy expansion and maintenance.

## Requirements

* Python 3.6+
* Reticulum
* psutil

## Installation

1.  Clone the repository:

    ```bash
    git clone <repository_url>
    cd akita-genesis
    ```

2.  Install the required packages:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  Run the `core.py` script on multiple machines to start the Akita Genesis nodes:

    ```bash
    python akita_genesis/core.py
    ```

2.  The nodes will automatically discover each other, form clusters, and start processing tasks.

## Configuration

The following configuration parameters can be adjusted in `akita_genesis/config.py`:

* `CLUSTER_SIZE`: The number of nodes in each cluster.
* `DISCOVERY_INTERVAL`: The interval (in seconds) for node discovery and cluster formation.
* `ANNOUNCE_INTERVAL`: The interval (in seconds) for announcing presence.
* `TASK_TIMEOUT`: The timeout (in seconds) for tasks.
* `TASK_CHANNEL`, `STATE_CHANNEL`, `INTER_CLUSTER_CHANNEL`, `LEDGER_CHANNEL`, `RESOURCE_CHANNEL`, `SERVICE_CHANNEL`: Reticulum channel names.

## Architecture

Akita Genesis is designed with a modular architecture, consisting of the following modules:

* **core.py:** Contains the main `AkitaGenesis` class, which manages the overall system.
* **discovery.py:** Handles node discovery and presence announcements.
* **ledger.py:** Manages the system ledger.
* **resources.py:** Monitors system resources.
* **tasks.py:** Handles task management and delegation.
* **state.py:** Manages cluster state information.
* **communication.py:** Enables inter-cluster communication.
* **config.py:** Stores configuration parameters.
* **persistence.py:** Manages data persistence using SQLite.

## Future Improvements

* Implement a distributed consensus algorithm for the ledger.
* Enhance security measures.
* Develop advanced task scheduling and resource management algorithms.
* Add more comprehensive error handling and logging.
* Improve the service discovery mechanism.
* Add more monitoring and reporting tools.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues to report bugs or suggest new features.
