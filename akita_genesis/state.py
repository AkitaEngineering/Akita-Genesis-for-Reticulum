# akita_genesis/state.py
from reticulum import Destination, Packet
import json

class ClusterState:
    def __init__(self, core):
        self.core = core
        self.state = self.core.persistence.load_state()
        self.state_destination = Destination(None, Destination.BROADCAST, "akita.state", "v1")
        self.state_destination.set_packet_callback(self.handle_state)

    def broadcast_state(self):
        state_data = {
            "clusters": {
                controller_hex: {"nodes": [n.get_public_hex() for n in cluster["nodes"]]}
                for controller_hex, cluster in self.core.clusters.items()
            },
            "ledger": self.core.ledger.ledger,
            "resource_usage": self.core.resources.resource_usage,
        }
        self.core.persistence.save_state(state_data)
        packet = Packet(self.state_destination, json.dumps(state_data).encode("utf-8"))
        packet.send()
        print("Akita: Cluster state broadcasted.")

    def handle_state(self, packet, packet_receipt):
        try:
            remote_state = json.loads(packet.get_data().decode("utf-8"))
            remote_ledger = remote_state.get("ledger", [])
            for entry in remote_ledger:
                if entry not in self.core.ledger.ledger:
                    self.core.ledger.ledger.append(entry)
                    self.core.persistence.save_ledger(entry)
            self.core.resources.resource_usage.update(remote_state.get("resource_usage", {}))
            print("Akita: Remote state received and merged.")
        except Exception as e:
            print(f"Akita: Error handling state: {e}")
