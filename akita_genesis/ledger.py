# akita_genesis/ledger.py
from reticulum import Destination, Packet
import json

class Ledger:
    def __init__(self, core):
        self.core = core
        self.ledger = self.core.persistence.load_ledger()
        self.ledger_destination = Destination(None, Destination.BROADCAST, "akita.ledger", "v1")
        self.ledger_destination.set_packet_callback(self.handle_ledger_entry)

    def add_entry(self, entry):
        self.ledger.append(entry)
        self.core.persistence.save_ledger(entry)
        self.broadcast_entry(entry)

    def broadcast_entry(self, entry):
        packet = Packet(self.ledger_destination, json.dumps(entry).encode("utf-8"))
        packet.send()
        print("Akita: Ledger entry broadcasted.")

    def handle_ledger_entry(self, packet, packet_receipt):
        try:
            entry = json.loads(packet.get_data().decode("utf-8"))
            if entry not in self.ledger:
                self.ledger.append(entry)
                self.core.persistence.save_ledger(entry)
                print("Akita: Ledger entry received and added.")
        except Exception as e:
            print(f"Akita: Error handling ledger entry: {e}")
