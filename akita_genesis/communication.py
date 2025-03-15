# akita_genesis/communication.py
from reticulum import Destination, Packet, Identity
import json

class ClusterCommunication:
    def __init__(self, core):
        self.core = core
        self.inter_cluster_destination = Destination(None, Destination.BROADCAST, "akita.inter_cluster", "v1")
        self.inter_cluster_destination.set_packet_callback(self.handle_inter_cluster)

    def send_inter_cluster_message(self, target_controller_hex, message):
        target_identity = Identity.from_public_hex(target_controller_hex)
        packet = Packet(Destination(target_identity, Destination.SINGLE, "akita.inter_cluster", "v1"), message.encode("utf-8"))
        packet.send()
        print(f"Akita: Inter-cluster message sent to {target_controller_hex}")

    def handle_inter_cluster(self, packet, packet_receipt):
        try:
            message = packet.get_data().decode("utf-8")
            print(f"Akita: Inter-cluster message received: {message}")
        except Exception as e:
            print(f"Akita: Error handling inter-cluster message: {e}")
