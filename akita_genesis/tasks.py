# akita_genesis/tasks.py
from reticulum import Destination, Packet
import json
import random
import time
from .config import TASK_TIMEOUT
import heapq

class TaskManager:
    def __init__(self, core):
        self.core = core
        self.task_queue = {}
        self.task_results = {}
        self.task_timeouts = {}
        self.task_destination = Destination(None, Destination.BROADCAST, "akita.tasks", "v1")
        self.task_destination.set_packet_callback(self.handle_task)

    def delegate_task(self, controller_hex, task_data, priority=1):
        nodes = self.core.clusters.get(controller_hex, {}).get("nodes", [])
        if not nodes:
            return

        available_nodes = []
        for node_hex in [n.get_public_hex() for n in nodes]:
            resources = self.core.resources.resource_usage.get(node_hex, {"cpu": 0, "memory": 0})
            if resources["cpu"] < 0.8 and resources["memory"] < 0.8:
                available_nodes.append(node_hex)

        if available_nodes:
            target_node_hex = random.choice(available_nodes)
            target_node = self.core.discovery.discovered_nodes[target_node_hex]
            task_id = str(random.randint(100000, 999999))
            task_data["task_id"] = task_id
            packet = Packet(Destination(target_node, Destination.SINGLE, "akita.tasks", "v1"), json.dumps(task_data).encode("utf-8"))
            packet.send()
            print(f"Akita: Task {task_id} delegated to {target_node_hex}")
            self.task_timeouts[task_id] = time.time() + TASK_TIMEOUT
        else:
            heapq.heappush(self.task_queue.setdefault(controller_hex, []), (priority, task_data))
            print("Akita: No available nodes, task queued.")

    def handle_task(self, packet, packet_receipt):
        try:
            task_data = json.loads(packet.get_data().decode("utf-8"))
            task_id = task_data.get("task_id")
            print(f"Akita: Task {task_id} received: {task_data}")
            time.sleep(random.randint(1, 5))
            result = f"Result of task {task_id}"
            self.task_results[task_id] = {"node": self.core.my_identity.get_public_hex(), "result": result}
            print(f"Akita: Task {task_id} completed.")
        except Exception as e:
            print(f"Akita: Error handling task: {e}")

    def check_timeouts(self):
        current_time = time.time()
        timed_out_tasks = [task_id for task_id, timeout in self.task_timeouts.items() if timeout < current_time]
        for task_id in timed_out_tasks:
            print(f"Akita: Task {task_id} timed out.")
            del self.task_timeouts[task_id]
            # Implement task retry or error handling here

    def process_queue(self, controller_hex):
        if controller_hex in self.task_queue and self.task_queue[controller_hex]:
            priority, task_data = heapq.heappop(self.task_queue[controller_hex])
            self.delegate_task(controller_hex, task_data, priority)  
