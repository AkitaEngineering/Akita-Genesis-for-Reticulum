# akita_genesis/resources.py
from reticulum import Destination, Packet
import json
import psutil

class ResourceMonitor:
    def __init__(self, core):
        self.core = core
        self.resource_usage = {}
        self.resource_destination = Destination(None, Destination.BROADCAST, "akita.resources", "v1")
        self.resource_destination.set_packet_callback(self.handle_resource_report)

    def report_usage(self):
        usage = {
            "cpu": psutil.cpu_percent(interval=1),
            "memory": psutil.virtual_memory().percent,
            "disk": psutil.disk_usage('/').percent,
            "network": psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv,
        }
        self.resource_usage[self.core.my_identity.get_public_hex()] = usage
        packet = Packet(self.resource_destination, json.dumps(self.resource_usage).encode("utf-8"))
        packet.send()
        print(f"Akita: Resource usage reported: {usage}")

    def handle_resource_report(self, packet, packet_receipt):
        try:
            remote_usage = json.loads(packet.get_data().decode("utf-8"))
            self.resource_usage.update(remote_usage)
            print("Akita: Remote resource usage received.")
        except Exception as e:
            print(f"Akita: Error handling resource report: {e}")
