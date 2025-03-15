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
        self.task_destination = Destination(None, Destination.BROADCAST
