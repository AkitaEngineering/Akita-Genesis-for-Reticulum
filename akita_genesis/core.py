# akita_genesis/core.py
import time
import hashlib
import random
from reticulum import RNS, Identity
from .discovery import NodeDiscovery
from .ledger import Ledger
from .resources import ResourceMonitor
from .tasks import TaskManager
from .state import ClusterState
from .communication import ClusterCommunication
from .config import CLUSTER_SIZE, DISCOVERY_INTERVAL
from .persistence import DataStore

class AkitaGenesis:
    def __init__(self):
        RNS.Transport.start()
        self.my_identity = Identity()
        self.clusters = {}
        self.persistence = DataStore()
        self.discovery = NodeDiscovery(self)
        self.ledger = Ledger(self)
        self.resources = ResourceMonitor(self)
        self.tasks = TaskManager(self)
        self.state = ClusterState(self)
        self.communication = ClusterCommunication(self)
        self.last_discovery = 0

    def elect_controller(self, nodes):
        if not nodes:
            return None
        sorted_nodes = sorted(nodes, key=lambda identity: hashlib.sha256(identity.get_public_hex().encode()).hexdigest())
        return sorted_nodes[0]

    def form_clusters(self):
        available_nodes = list(self.discovery.discovered_nodes.values())
        new_clusters = {}

        while len(available_nodes) >= CLUSTER_SIZE:
            controller = self.elect_controller(available_nodes[:CLUSTER_SIZE])
            nodes = [node for node in available_nodes[:CLUSTER_SIZE] if node != controller]
            new_clusters[controller.get_public_hex()] = {"nodes": nodes, "last_seen": time.time()}
            available_nodes = available_nodes[CLUSTER_SIZE:]

            ledger_entry = {"action": "cluster_formed", "controller": controller.get_public_hex(), "nodes": [n.get_public_hex() for n in nodes], "timestamp": time.time()}
            self.ledger.add_entry(ledger_entry)

        self.clusters = new_clusters
        print("Akita: Clusters formed.")
        self.state.broadcast_state()

    def cleanup_nodes(self):
        current_time = time.time()
        for node_hex, node_identity in list(self.discovery.discovered_nodes.items()):
            if current_time - RNS.Transport.get_last_activity(node_identity) > DISCOVERY_INTERVAL * 2:
                print(f"Akita: Removing inactive node: {node_hex}")
                del self.discovery.discovered_nodes[node_hex]
                ledger_entry = {"action": "node_removed", "node": node_hex, "timestamp": time.time()}
                self.ledger.add_entry(ledger_entry)

    def run(self):
        while True:
            current_time = time.time()
            self.discovery.run()

            if current_time - self.last_discovery > DISCOVERY_INTERVAL:
                self.cleanup_nodes()
                self.form_clusters()
                self.resources.report_usage()

                for controller_hex in self.clusters:
                    if controller_hex in self.tasks.task_queue:
                        self.tasks.process_queue(controller_hex)
                    self.tasks.delegate_task(controller_hex, {"task": "example_task", "data": "some_data"}, priority=random.randint(1, 10))
                    for other_controller_hex in self.clusters:
                        if controller_hex != other_controller_hex:
                            self.communication.send_inter_cluster_message(other_controller_hex, "Hello from another cluster")

                self.last_discovery = current_time

            self.tasks.check_timeouts()
            time.sleep(1)

if __name__ == "__main__":
    akita = AkitaGenesis()
    akita.run()
