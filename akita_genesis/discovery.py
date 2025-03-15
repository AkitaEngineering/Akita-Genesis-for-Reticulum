# akita_genesis/discovery.py
from reticulum import RNS, Identity, Destination, Packet, PacketReceipt
from .config import ANNOUNCE_INTERVAL, SERVICE_CHANNEL
import time

class NodeDiscovery:
    def __init__(self, core):
        self.core = core
        self.discovered_nodes = {}
        self.announce_destination = Destination(None, Destination.BROADCAST, "akita.discovery", "v1")
        self.announce_destination.set_packet_callback(self.handle_announcement)
        self.service_destination = Destination(None, Destination.BROADCAST, SERVICE_CHANNEL, "v1")
        self.last_announce = 0

    def announce_presence(self):
        Packet(self.announce_destination, self.core.my_identity.get_public_hex()).send()
        RNS.announce(self.service_destination) #Example of service discovery.
        print(f"Akita: Announced presence: {self.core.my_identity.get_public_hex()}")

    def handle_announcement(self, packet: Packet, packet_receipt: PacketReceipt):
        try:
            node_hex = packet.get_data().decode("utf-8")
            node_identity = Identity.from_public_hex(node_hex)
            self.discovered_nodes[node_hex] = node_identity
            print(f"Akita: Discovered node: {node_hex}")
        except Exception as e:
            print(f"Akita: Error handling announcement: {e}")

    def run(self):
        current_time = time.time()
        if current_time - self.last_announce > ANNOUNCE_INTERVAL:
            self.announce_presence()
            self.last_announce = current_time
