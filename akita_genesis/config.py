# akita_genesis/config.py
CLUSTER_SIZE = 4
DISCOVERY_INTERVAL = 60
ANNOUNCE_INTERVAL = 10
TASK_TIMEOUT = 30  # seconds

TASK_CHANNEL = "akita.tasks"
STATE_CHANNEL = "akita.state"
INTER_CLUSTER_CHANNEL = "akita.inter_cluster"
LEDGER_CHANNEL = "akita.ledger"
RESOURCE_CHANNEL = "akita.resources"
SERVICE_CHANNEL = "akita.services"
