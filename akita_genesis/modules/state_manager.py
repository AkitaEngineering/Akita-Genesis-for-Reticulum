# akita_genesis/modules/state_manager.py
import json
import time
import asyncio
from typing import Dict, Any, Optional, List, Callable, Set, Awaitable # Added Awaitable
from enum import Enum
import pydantic 

# Use absolute imports within the package
from akita_genesis.utils.logger import setup_logger
from akita_genesis.config.settings import settings
from akita_genesis.modules.persistence import db_manager
from akita_genesis.modules.ledger import Ledger, EventType
# Assuming future_to_task helper or asyncio.wrap_future is used for DB calls
# from .ledger import future_to_task 
from akita_genesis.modules.resources import ResourceMonitor 

log = setup_logger(__name__)

class NodeStatus(Enum):
    """Represents the status of a node in the cluster."""
    UNKNOWN = "unknown"
    OFFLINE = "offline"
    ONLINE = "online"
    DEGRADED = "degraded" # e.g., high load, some services failing
    JOINING = "joining"   # Node is in the process of joining
    LEAVING = "leaving"   # Node is gracefully leaving

    def __str__(self):
        return self.value

class NodeInfo(pydantic.BaseModel):
    """
    Represents structured information about a node in the cluster.
    This can be serialized and shared. Uses Pydantic for validation.
    """
    node_id: str # Akita-level unique ID
    node_name: str # Human-readable name
    cluster_name: str # Cluster the node belongs to
    address_hex: Optional[str] = None # Reticulum identity hash (hex)
    last_seen: float = pydantic.Field(default_factory=time.time) # Unix timestamp
    is_leader: bool = False # Whether this node is currently the leader
    status: NodeStatus = NodeStatus.UNKNOWN # Current operational status
    resources: Optional[Dict[str, Any]] = None # Latest resource metrics snapshot
    # Field to track current task load or capability, for more advanced scheduling
    current_task_count: int = 0 # Example: number of tasks currently assigned
    capabilities: List[str] = [] # Example: ['gpu_processing', 'fast_io']

    class Config:
        use_enum_values = True # Serialize enums to their string values (e.g., "ONLINE")

    def update_from(self, other: 'NodeInfo'):
        """
        Updates this NodeInfo instance with data from another NodeInfo instance,
        typically received over the network. Prioritizes fresher data based on 'last_seen'.
        Merges capabilities.
        """
        # Only update if 'other' is indeed fresher or equally fresh
        if other.last_seen >= self.last_seen:
            self.node_name = other.node_name # Name can potentially change
            self.address_hex = other.address_hex # Update RNS address if changed
            self.last_seen = other.last_seen # Update timestamp
            
            # Leader status is primarily determined by the election process.
            # However, if this update is *about this node itself* (node_id matches),
            # trust its reported leader status as it's the most current self-assessment.
            if self.node_id == other.node_id:
                 self.is_leader = other.is_leader
            
            self.status = other.status # Fresher status wins
            # Update resources only if the incoming data has them (avoid overwriting with None)
            if other.resources: 
                self.resources = other.resources
            
            # Update task count if the incoming data is newer or the count differs
            if other.last_seen > self.last_seen or other.current_task_count != self.current_task_count:
                self.current_task_count = other.current_task_count
            
            # Merge capabilities: add any new capabilities from 'other'
            if other.capabilities:
                self.capabilities = sorted(list(set(self.capabilities + other.capabilities)))

class ClusterState:
    """
    Represents the overall state of the cluster as known by the local node.
    Manages a collection of NodeInfo objects and tracks the current leader.
    Provides thread-safe access to the node list and leader information.
    """
    def __init__(self, current_node_id: str, current_cluster_name: str):
        self.current_node_id = current_node_id
        self.current_cluster_name = current_cluster_name
        self.nodes: Dict[str, NodeInfo] = {} # Dictionary mapping node_id -> NodeInfo
        self.leader_id: Optional[str] = None # Akita ID of the current leader node
        self.last_updated: float = time.time() # Timestamp of the last state modification
        self._lock = asyncio.Lock() # Async lock to protect concurrent access to nodes/leader_id

    async def update_node(self, node_info: NodeInfo):
        """Adds a new node or updates an existing node's information in the cluster state."""
        async with self._lock: # Ensure exclusive access for update
            # Ignore nodes that don't belong to this cluster
            if node_info.cluster_name != self.current_cluster_name:
                # Log silently to avoid noise if cross-cluster announcements are possible
                # log.warning(f"Node {node_info.node_id} from different cluster ({node_info.cluster_name}) ignored.")
                return 

            existing_node = self.nodes.get(node_info.node_id)
            if existing_node:
                # Update existing node info using the NodeInfo.update_from logic
                log.debug(f"Updating existing node {node_info.node_id}. Old last_seen: {existing_node.last_seen}, New last_seen: {node_info.last_seen}")
                existing_node.update_from(node_info)
            else:
                # Add new node to the dictionary
                log.debug(f"Adding new node {node_info.node_id} to cluster state.")
                self.nodes[node_info.node_id] = node_info
            
            # Handle potential leader changes based on the update
            # If the updated node is the current leader but now says it's not leader
            if self.leader_id == node_info.node_id and not node_info.is_leader:
                log.info(f"Current leader {node_info.node_id} announced it's no longer leader. Clearing leader.")
                self.leader_id = None # Clear leader, election loop will find a new one
            # Note: An announcement from a non-leader claiming leadership is generally ignored here;
            # the leader election loop is the primary authority for setting the leader.

            # Update the timestamp of the last state modification
            self.last_updated = time.time()

    async def remove_node(self, node_id: str) -> bool:
        """Removes a node from the cluster state dictionary."""
        async with self._lock: # Ensure exclusive access
            if node_id in self.nodes:
                log.debug(f"Removing node {node_id} from cluster state.")
                # Check if the removed node was the leader
                removed_node_was_leader = (self.leader_id == node_id)
                # Remove the node entry
                del self.nodes[node_id]
                # If the leader was removed, clear the leader ID to trigger re-election
                if removed_node_was_leader:
                    log.info(f"Removed leader node {node_id}. Leader is now None.")
                    self.leader_id = None 
                self.last_updated = time.time()
                return True # Indicate node was removed
            return False # Indicate node was not found

    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Retrieves information for a specific node by its Akita ID."""
        async with self._lock:
            # Return a copy to prevent external modification of the cached object
            node = self.nodes.get(node_id)
            # Use Pydantic's model_copy for safe copying
            return node.model_copy(deep=True) if node else None

    async def get_all_nodes(self) -> List[NodeInfo]:
        """Retrieves a list of all nodes currently known in the cluster state."""
        async with self._lock:
            # Return copies of all NodeInfo objects
            return [node.model_copy(deep=True) for node in self.nodes.values()]
    
    async def get_online_nodes(self) -> List[NodeInfo]:
        """Retrieves a list of nodes currently considered ONLINE."""
        async with self._lock:
            return [
                node.model_copy(deep=True) for node in self.nodes.values() 
                if node.status == NodeStatus.ONLINE # Filter for ONLINE status only
            ]
            
    async def get_potential_workers(self) -> List[NodeInfo]:
        """Retrieves a list of nodes suitable for task execution (ONLINE or DEGRADED, and not the leader)."""
        async with self._lock:
            return [
                node.model_copy(deep=True) for node_id, node in self.nodes.items()
                # Exclude the current leader
                if node_id != self.leader_id and \
                # Include nodes that are ONLINE or potentially DEGRADED (might still work)
                   (node.status == NodeStatus.ONLINE or node.status == NodeStatus.DEGRADED) 
            ]

    async def get_leader(self) -> Optional[NodeInfo]:
        """Retrieves the NodeInfo object for the current leader, if one is known."""
        async with self._lock:
            # Check if a leader ID is set and if that node exists in our state
            if self.leader_id and self.leader_id in self.nodes:
                # Return a copy
                return self.nodes[self.leader_id].model_copy(deep=True)
            return None # No leader known or leader not in current state

    async def set_leader(self, node_id: Optional[str]):
        """
        Sets the current leader ID. This should primarily be called by the leader election logic.
        Updates the 'is_leader' flag on the relevant NodeInfo objects in the cache.
        """
        async with self._lock: # Ensure atomic update of leader state
            # Only proceed if the leader is actually changing
            if self.leader_id != node_id:
                log.info(f"Setting new leader via set_leader: {node_id}. Previous: {self.leader_id}")
                old_leader_id = self.leader_id
                self.leader_id = node_id # Update the tracked leader ID
                
                # Update the 'is_leader' flag for the old leader (if exists)
                if old_leader_id and old_leader_id in self.nodes:
                    self.nodes[old_leader_id].is_leader = False
                # Update the 'is_leader' flag for the new leader (if exists)
                if node_id and node_id in self.nodes:
                    self.nodes[node_id].is_leader = True
                
                # Ensure consistency across all nodes in cache (might be redundant but safe)
                # This loop ensures only the current leader_id has is_leader=True
                for nid_loop, node_loop in self.nodes.items():
                    node_loop.is_leader = (nid_loop == self.leader_id)

            self.last_updated = time.time() # Update modification timestamp

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the current cluster state into a dictionary.
        Note: Accessing state under lock synchronously is complex with asyncio. 
              This method returns a snapshot without locking, which might be slightly stale.
              For critical serialization, an async version (`to_dict_async`) would be preferred.
        """
        # Create a snapshot of nodes by dumping each NodeInfo model
        # This avoids locking issues but might capture slightly inconsistent state if updates occur during iteration.
        nodes_dump = {nid: node.model_dump() for nid, node in self.nodes.items()}
        return {
            "current_node_id": self.current_node_id,
            "current_cluster_name": self.current_cluster_name,
            "nodes": nodes_dump, # The dictionary of serialized NodeInfo objects
            "leader_id": self.leader_id,
            "last_updated": self.last_updated,
        }

    @classmethod
    def from_dict(cls, state_dict: Dict[str, Any], current_node_id: str, current_cluster_name: str) -> 'ClusterState':
        """Deserializes cluster state from a dictionary, creating a new ClusterState instance."""
        state = cls(current_node_id, current_cluster_name) # Create new instance
        state.leader_id = state_dict.get("leader_id") # Set leader ID from dict
        state.last_updated = state_dict.get("last_updated", time.time()) # Set timestamp
        
        # Populate nodes dictionary from the 'nodes' key in the input dict
        nodes_data = state_dict.get("nodes", {})
        for nid, node_data_dict in nodes_data.items():
            try:
                # Validate cluster name before adding node to prevent cross-cluster pollution
                if node_data_dict.get("cluster_name") == current_cluster_name:
                    # Use Pydantic model validation to create NodeInfo instance
                    state.nodes[nid] = NodeInfo(**node_data_dict)
                else:
                    log.warning(f"Node {nid} in from_dict has mismatched cluster '{node_data_dict.get('cluster_name')}', expected '{current_cluster_name}'. Skipping.")
            except pydantic.ValidationError as e:
                # Log validation errors if NodeInfo data is malformed
                log.warning(f"Failed to parse NodeInfo for {nid} from received state: {e}. Data: {node_data_dict}")
        return state


class StateManager:
    """
    Manages the cluster state, including node discovery integration, leader election,
    state persistence, node cleanup, and providing worker selection logic.
    """

    def __init__(
        self,
        node_id: str, node_name: str, cluster_name: str,
        ledger: Ledger, resource_monitor: ResourceMonitor,
        communication_manager: Optional[Any] = None # CommManager instance, typically set later
    ):
        """Initializes the StateManager."""
        self.node_id = node_id
        self.node_name = node_name
        self.cluster_name = cluster_name
        self.ledger = ledger
        self.resource_monitor = resource_monitor 
        self.db = db_manager # Use global DB manager instance
        self.communication_manager = communication_manager # Can be set later via setter

        # Holds the local node's view of the cluster state
        self.local_cluster_state = ClusterState(self.node_id, self.cluster_name)
        
        # Callback function to notify other components of significant state changes
        self._on_state_change_callback: Optional[Callable[[ClusterState], Awaitable[None]]] = None 
        # Handles for background asyncio tasks
        self._leader_election_task: Optional[asyncio.Task] = None
        self._node_cleanup_task: Optional[asyncio.Task] = None

        log.info(f"StateManager initialized for Node ID: {self.node_id}, Cluster: {self.cluster_name}")

    # --- Public Methods ---
    def set_communication_manager(self, comm_manager: Any): 
        """Sets the CommunicationManager instance needed for broadcasting state (if implemented)."""
        self.communication_manager = comm_manager
        log.info("CommunicationManager set in StateManager.")

    def set_on_state_change_callback(self, callback: Callable[[ClusterState], Awaitable[None]]): 
        """Registers an async callback function to be called on significant state changes."""
        self._on_state_change_callback = callback

    async def start(self):
        """Starts the StateManager's background tasks (leader election, cleanup) and loads initial state."""
        log.info("StateManager starting background tasks...")
        
        # Wait for DB schema initialization to complete before loading state
        if hasattr(self.db, 'init_db_future') and self.db.init_db_future:
            try:
                log.debug("Waiting for DB schema initialization...")
                # Wait for the future returned by db_manager.init_db()
                await asyncio.wait_for(asyncio.wrap_future(self.db.init_db_future), timeout=15.0) 
                log.debug("DB schema initialization confirmed complete.")
            except asyncio.TimeoutError:
                 log.error("Timeout waiting for DB schema initialization. State loading might be incomplete.")
            except Exception as e:
                log.error(f"Error during DB schema initialization wait: {e}. State loading might fail.")
                # Depending on severity, might need to prevent further startup. Return False?
        
        # Load previously known node states from the database
        await self._load_initial_state_from_db()
        
        # Ensure this node's own information is correctly initialized or updated in the state cache
        my_node_info = await self.local_cluster_state.get_node(self.node_id)
        if not my_node_info: # If not loaded from DB (e.g., first run)
            # Create initial NodeInfo for self
            my_node_info = NodeInfo(
                node_id=self.node_id, node_name=self.node_name, cluster_name=self.cluster_name,
                status=NodeStatus.ONLINE, # Assume ONLINE on start
                last_seen=time.time(),
                # Get RNS address if comms manager is already set and started
                address_hex=self.communication_manager.get_node_identity_hex() if self.communication_manager else None,
                # Get initial resources
                resources=self.resource_monitor.current_resources or self.resource_monitor.get_current_resources() 
                # capabilities will be set by the Node class after init
            )
            # Add self to the local state cache
            await self.local_cluster_state.update_node(my_node_info)
        else: # If loaded from DB, ensure key fields are up-to-date on start
             my_node_info.status = NodeStatus.ONLINE # Mark as ONLINE on start
             my_node_info.last_seen=time.time() # Update timestamp
             # Update RNS address if available from comms manager
             my_node_info.address_hex=self.communication_manager.get_node_identity_hex() if self.communication_manager else my_node_info.address_hex
             # Update resources
             my_node_info.resources = self.resource_monitor.current_resources or self.resource_monitor.get_current_resources() 
             # Update cache with refreshed info
             await self.local_cluster_state.update_node(my_node_info) 

        # Persist the potentially updated self info to the database
        # Need to fetch again as update_node works on copies internally now
        my_node_info_refreshed = await self.local_cluster_state.get_node(self.node_id)
        if my_node_info_refreshed:
             await self._persist_node_info(my_node_info_refreshed) 
        else:
             log.error("Failed to retrieve self node info after update during start.")


        # Start background tasks if they are not already running
        if not self._leader_election_task or self._leader_election_task.done():
            self._leader_election_task = asyncio.create_task(self._leader_election_loop())
        if not self._node_cleanup_task or self._node_cleanup_task.done():
            self._node_cleanup_task = asyncio.create_task(self._node_cleanup_loop())
        log.info("StateManager background tasks started.")

    async def stop(self):
        """Stops the StateManager's background tasks and persists final node state."""
        log.info("StateManager stopping background tasks...")
        # Cancel background tasks first
        tasks_to_cancel = [self._leader_election_task, self._node_cleanup_task]
        for task in tasks_to_cancel:
            if task and not task.done():
                task.cancel()
        # Wait for tasks to finish cancelling, ignoring CancelledError
        await asyncio.gather(*[t for t in tasks_to_cancel if t], return_exceptions=True)

        # Persist the final state of this node as OFFLINE
        current_node = await self.local_cluster_state.get_node(self.node_id)
        if current_node:
            current_node.status = NodeStatus.OFFLINE
            current_node.last_seen = time.time()
            await self._persist_node_info(current_node) # Persist offline status
        log.info("StateManager background tasks stopped.")

    async def handle_node_announcement(self, announced_node_info: NodeInfo):
        """Processes an incoming NodeInfo announcement, updates local state and persists."""
        # Ignore nodes from different clusters
        if announced_node_info.cluster_name != self.cluster_name: return
        
        log.debug(f"Received announcement from node: {announced_node_info.node_id}, status: {announced_node_info.status}, last_seen: {announced_node_info.last_seen}")
        
        # Get existing state before update for comparison
        existing_node = await self.local_cluster_state.get_node(announced_node_info.node_id)
        is_new = existing_node is None
        old_status = existing_node.status if existing_node else None

        # Update the local state cache (this uses NodeInfo.update_from)
        await self.local_cluster_state.update_node(announced_node_info)
        
        # Persist the updated information to the database
        # Fetch the potentially modified node info from cache after update
        updated_node = await self.local_cluster_state.get_node(announced_node_info.node_id) 
        if updated_node: 
            await self._persist_node_info(updated_node) 
        
        # Check if a significant change occurred that warrants callback/re-election
        significant_change = False
        if is_new:
            significant_change = True
            log.info(f"New node {announced_node_info.node_name} ({announced_node_info.node_id}) discovered in cluster {self.cluster_name}.")
            # Record event in ledger
            await self.ledger.record_event(
                EventType.NODE_ONLINE, # Or CLUSTER_JOIN
                details={"node_id": announced_node_info.node_id, "node_name": announced_node_info.node_name, "status": str(announced_node_info.status)},
                source_node_id=announced_node_info.node_id, # Event is about the announced node
                cluster_name=self.cluster_name
            )
        elif old_status != announced_node_info.status:
            # Status change detected
            significant_change = True
            log.info(f"Node {announced_node_info.node_id} status changed: {old_status} -> {announced_node_info.status}")
            # Record specific event (e.g., NODE_ONLINE, NODE_OFFLINE)
            event = EventType.NODE_ONLINE if announced_node_info.status == NodeStatus.ONLINE else \
                    EventType.NODE_OFFLINE if announced_node_info.status == NodeStatus.OFFLINE else \
                    EventType.GENERAL_INFO # Or a specific NODE_STATUS_CHANGE event
            if event != EventType.GENERAL_INFO: # Avoid logging GENERAL_INFO for every minor status flicker
                 await self.ledger.record_event(
                     event, details={"node_id": announced_node_info.node_id, "new_status": str(announced_node_info.status), "old_status": str(old_status)},
                     source_node_id=announced_node_info.node_id, cluster_name=self.cluster_name
                 )

        # Trigger callback and leader re-evaluation if significant change occurred
        if significant_change:
            await self._trigger_state_change_callback()
            # Re-evaluate leader if node status changed (especially if it went offline)
            await self.check_and_elect_leader() 

        # Handle potential leader changes based on announcement (less authoritative than election)
        if (self.local_cluster_state.leader_id == announced_node_info.node_id and not announced_node_info.is_leader) or \
           (announced_node_info.is_leader and (not existing_node or announced_node_info.last_seen > existing_node.last_seen)):
            log.info(f"Potential leader change suggested by announcement from {announced_node_info.node_id}. Triggering election check.")
            await self.check_and_elect_leader()


    async def handle_received_state_broadcast(self, sender_akita_node_id: str, received_cluster_state_dict: Dict[str, Any]):
        """Processes a full or partial cluster state broadcast received from another node."""
        log.debug(f"Received cluster state broadcast from Akita Node {sender_akita_node_id}.")
        
        received_nodes_data = received_cluster_state_dict.get("nodes", {})
        updated_node_ids: Set[str] = set()
        potential_leader_change = False # Flag if broadcast suggests leader change

        # Iterate through nodes in the received state
        for node_id, node_data_dict in received_nodes_data.items():
            # Ignore nodes not belonging to our cluster
            if node_data_dict.get("cluster_name") != self.cluster_name: continue 
            
            try:
                # Parse incoming node data
                incoming_node_info = NodeInfo(**node_data_dict)
                # Get current local state for comparison
                existing_node = await self.local_cluster_state.get_node(node_id)
                
                # Update local state only if incoming data is newer
                if not existing_node or incoming_node_info.last_seen > existing_node.last_seen:
                    log.debug(f"Updating node {node_id} from broadcast (sender: {sender_akita_node_id}, last_seen: {incoming_node_info.last_seen}).")
                    # Update cache
                    await self.local_cluster_state.update_node(incoming_node_info) 
                    # Persist update to DB
                    updated_node = await self.local_cluster_state.get_node(node_id) # Get updated object
                    if updated_node: await self._persist_node_info(updated_node) 
                    updated_node_ids.add(node_id) # Track which nodes were updated

                    # Check if this broadcast changes our view of the leader
                    # Note: Broadcasts are less authoritative than the election loop.
                    if (incoming_node_info.is_leader and self.local_cluster_state.leader_id != node_id) or \
                       (self.local_cluster_state.leader_id == node_id and not incoming_node_info.is_leader):
                        potential_leader_change = True # Flag to re-evaluate leader later

            except pydantic.ValidationError as e:
                log.warning(f"Invalid node data in broadcast from {sender_akita_node_id} for node {node_id}: {e}")
            except Exception as e:
                log.error(f"Error processing node data for {node_id} from broadcast: {e}", exc_info=True)
        
        # If any nodes were updated, trigger callback
        if updated_node_ids:
            log.info(f"Merged state from {sender_akita_node_id}, updated {len(updated_node_ids)} nodes.")
            await self._trigger_state_change_callback()
        
        # If broadcast hinted at leader change, or if we updated nodes and have no leader, re-check election
        if potential_leader_change or (updated_node_ids and not await self.local_cluster_state.get_leader()):
            log.info("Potential leader change detected from broadcast or updates, triggering election check.")
            await self.check_and_elect_leader()


    async def broadcast_local_state(self, full_state: bool = False): # Async now
        """Prepares and broadcasts the current node's view of the cluster state (conceptual)."""
        if not self.communication_manager:
            log.warning("Cannot broadcast state: CommunicationManager not set.")
            return

        log.debug("Preparing to broadcast local state...")
        # Ensure our own node's info is up-to-date before broadcasting
        my_node_info = await self.local_cluster_state.get_node(self.node_id)
        if my_node_info:
            my_node_info.last_seen = time.time() 
            my_node_info.resources = self.resource_monitor.current_resources 
            my_node_info.is_leader = (self.local_cluster_state.leader_id == self.node_id)
            # Update task count if needed
            # my_node_info.current_task_count = ... 
            await self.local_cluster_state.update_node(my_node_info) # Update cache
        
        # Get the current state dictionary
        state_to_send_dict = self.local_cluster_state.to_dict()
        
        # Actual broadcast mechanism needed in CommunicationManager
        # This might involve sending to a GROUP destination or using another strategy.
        if hasattr(self.communication_manager, 'broadcast_on_aspect'): # Check if method exists
             log.info(f"Broadcasting state on {AppAspect.STATE_SYNC.value} with {len(state_to_send_dict.get('nodes', {}))} nodes.")
             # await self.communication_manager.broadcast_on_aspect(
             #     AppAspect.STATE_SYNC,
             #     MessageType.CLUSTER_STATE_BROADCAST,
             #     state_to_send_dict,
             #     source_akita_node_id=self.node_id
             # )
        else:
            log.warning("CommunicationManager does not support broadcast_on_aspect. State not broadcasted.")

    # --- Leader Election Logic ---
    async def _leader_election_loop(self):
        """Background task that periodically runs the leader election check."""
        log.info("Leader election loop started.")
        # Initial delay before first election check
        await asyncio.sleep(max(5, settings.LEADER_ELECTION_TIMEOUT_S / 3)) 
        while not self._shutdown_event.is_set():
            try:
                # Run the election check
                await self.check_and_elect_leader()
                # Wait for the configured interval before the next check
                await asyncio.sleep(settings.LEADER_ELECTION_TIMEOUT_S) 
            except asyncio.CancelledError: 
                log.info("Leader election loop cancelled.")
                break # Exit loop
            except Exception as e: 
                log.error(f"Error in leader election loop: {e}", exc_info=True)
                # Wait shorter interval before retrying after an error
                await asyncio.sleep(settings.LEADER_ELECTION_TIMEOUT_S / 2) 

    async def check_and_elect_leader(self):
        """
        Performs the leader election logic based on the current cluster state.
        Strategy: Elects the ONLINE node with the lexicographically smallest Akita node ID.
        Updates local state and persists changes for the old/new leader.
        """
        current_leader_obj = await self.local_cluster_state.get_leader()
        
        # Get all nodes currently considered ONLINE in this cluster
        eligible_nodes = [ 
            n for n in await self.local_cluster_state.get_all_nodes() 
            if n.cluster_name == self.cluster_name and n.status == NodeStatus.ONLINE
        ]

        # If the current leader is still valid (online and eligible)
        if current_leader_obj and current_leader_obj.node_id in [n.node_id for n in eligible_nodes]:
            log.debug(f"Current leader {current_leader_obj.node_id} is still online and eligible.")
            # Sanity check: If this node thinks it's leader, ensure its NodeInfo reflects that
            if current_leader_obj.node_id == self.node_id and not current_leader_obj.is_leader:
                log.info(f"Correcting leader status for self ({self.node_id}) in local state.")
                await self.local_cluster_state.set_leader(self.node_id) # Update cache
                # Persist the correction (need to fetch the object again as set_leader modified it)
                corrected_self_info = await self.local_cluster_state.get_node(self.node_id)
                if corrected_self_info: await self._persist_node_info(corrected_self_info) 
                await self._trigger_state_change_callback() # Notify of potential state correction
            return # No change needed

        # If no current leader or current leader is ineligible, proceed with election
        log.info(f"Performing leader election. Current leader: {current_leader_obj.node_id if current_leader_obj else 'None'}. Eligible online nodes: {len(eligible_nodes)}")

        if not eligible_nodes:
            # No nodes available to be leader
            log.warning("No eligible (online) nodes found for leader election in this cluster.")
            # If there was a leader before, clear it
            if self.local_cluster_state.leader_id is not None:
                old_leader_id = self.local_cluster_state.leader_id
                await self.local_cluster_state.set_leader(None) # Clear leader in cache
                # Persist the change for the old leader (mark as not leader)
                if current_leader_obj: # Use the object fetched at the start
                     current_leader_obj.is_leader = False # Ensure flag is false
                     await self._persist_node_info(current_leader_obj) 
                await self._trigger_state_change_callback() # Notify about leader loss
            return # No leader can be elected

        # --- Election Strategy: Smallest Node ID Wins ---
        eligible_nodes.sort(key=lambda n: n.node_id) # Sort nodes by their Akita ID
        new_leader_node = eligible_nodes[0] # The node with the smallest ID is the new leader

        # Check if the leader actually changed
        if not current_leader_obj or current_leader_obj.node_id != new_leader_node.node_id:
            log.info(f"New leader elected: {new_leader_node.node_name} ({new_leader_node.node_id})")
            
            old_leader_id = self.local_cluster_state.leader_id # Store old leader ID for logging/persistence
            # Update leader ID in cache (this also updates is_leader flags internally)
            await self.local_cluster_state.set_leader(new_leader_node.node_id) 

            # Persist changes for the old leader (if any) and the new leader
            if old_leader_id:
                # Fetch the updated state of the old leader from cache
                old_leader_obj_updated = await self.local_cluster_state.get_node(old_leader_id) 
                if old_leader_obj_updated: # Should exist
                    # Ensure its is_leader flag is False before persisting
                    old_leader_obj_updated.is_leader = False 
                    await self._persist_node_info(old_leader_obj_updated) 
            
            # Fetch the updated state of the new leader from cache
            new_leader_from_cache = await self.local_cluster_state.get_node(new_leader_node.node_id)
            if new_leader_from_cache: # Should exist
                 # Ensure its is_leader flag is True before persisting
                 new_leader_from_cache.is_leader = True
                 await self._persist_node_info(new_leader_from_cache) 

            # Record the leader election event in the ledger
            await self.ledger.record_event(
                EventType.LEADER_ELECTED,
                details={"new_leader_id": new_leader_node.node_id, "new_leader_name": new_leader_node.node_name, "previous_leader_id": old_leader_id},
                source_node_id=self.node_id, # This node made the decision based on its view
                cluster_name=self.cluster_name
            )
            # Trigger the state change callback to notify other components
            await self._trigger_state_change_callback()
            
        else:
            # Leader remains the same
            log.debug(f"Leader {current_leader_obj.node_id} re-confirmed.")

    # --- Node Cleanup Logic ---
    async def _node_cleanup_loop(self):
        """Background task that periodically checks for stale nodes and marks them OFFLINE."""
        log.info("Node cleanup loop started.")
        while not self._shutdown_event.is_set():
            # Wait for roughly half the node timeout duration before checking
            await asyncio.sleep(settings.NODE_TIMEOUT_S / 2  if settings.NODE_TIMEOUT_S > 20 else 10)
            try:
                # Ensure state manager is still valid
                if not self.local_cluster_state: continue

                all_nodes = await self.local_cluster_state.get_all_nodes()
                now = time.time()
                changed_node_ids: List[str] = [] # Track nodes marked offline in this pass

                # Iterate through all known nodes
                for node_info in all_nodes:
                    # Don't mark self as offline via timeout; handle self shutdown separately
                    if node_info.node_id == self.node_id: 
                        # Keep self alive in DB if status is online
                        if node_info.status == NodeStatus.ONLINE: 
                            node_info.last_seen = now
                            # Persist own updated timestamp (might be frequent)
                            await self._persist_node_info(node_info) 
                        continue

                    # Check nodes that are currently considered ONLINE or DEGRADED
                    if node_info.status == NodeStatus.ONLINE or node_info.status == NodeStatus.DEGRADED:
                        # If node hasn't been seen for longer than the timeout period
                        if (now - node_info.last_seen) > settings.NODE_TIMEOUT_S:
                            log.warning(f"Node {node_info.node_name} ({node_info.node_id}) timed out. Marking as OFFLINE. Last seen: {node_info.last_seen:.2f} (age: {now - node_info.last_seen:.0f}s)")
                            original_status = node_info.status
                            # Update node status in cache
                            node_info.status = NodeStatus.OFFLINE
                            node_info.is_leader = False # Cannot be leader if offline
                            await self.local_cluster_state.update_node(node_info) 
                            
                            # Persist the change to the database
                            await self._persist_node_info(node_info) 
                            changed_node_ids.append(node_info.node_id)

                            # Record the event in the ledger
                            await self.ledger.record_event(
                                EventType.NODE_OFFLINE,
                                details={"node_id": node_info.node_id, "reason": "timeout", "previous_status": str(original_status)},
                                source_node_id=self.node_id, # This node detected the timeout
                                cluster_name=self.cluster_name
                            )
                
                # If any nodes were marked offline, trigger callback and potentially re-election
                if changed_node_ids:
                    log.info(f"Marked {len(changed_node_ids)} nodes as offline due to timeout: {changed_node_ids}")
                    await self._trigger_state_change_callback()
                    
                    # Check if the leader was among the timed-out nodes
                    current_leader = await self.local_cluster_state.get_leader()
                    if not current_leader or current_leader.node_id in changed_node_ids:
                        log.info("Leader may have gone offline due to timeout, triggering election check.")
                        await self.check_and_elect_leader() # Force re-election check
                        
            except asyncio.CancelledError:
                log.info("Node cleanup loop cancelled.")
                break # Exit loop
            except Exception as e:
                log.error(f"Error in node cleanup loop: {e}", exc_info=True)
                # Wait briefly before retrying after an error
                await asyncio.sleep(10) 

    # --- Utility & Accessor Methods ---
    async def _trigger_state_change_callback(self):
        """Invokes the registered state change callback function, if set."""
        if self._on_state_change_callback:
            log.debug("Triggering on_state_change_callback.")
            try:
                # Pass a copy of the state to the callback
                state_copy = self.local_cluster_state # Or create a deep copy if needed
                await self._on_state_change_callback(state_copy)
            except Exception as e:
                log.error(f"Error in on_state_change_callback: {e}", exc_info=True)

    async def get_current_leader_id(self) -> Optional[str]:
        """Returns the Akita ID of the current leader node, or None if no leader."""
        leader = await self.local_cluster_state.get_leader()
        return leader.node_id if leader else None

    async def is_current_node_leader(self) -> bool:
        """Checks if this node is currently the leader."""
        leader_id = await self.get_current_leader_id()
        return leader_id == self.node_id

    async def get_cluster_nodes_info(self) -> List[NodeInfo]:
        """Returns a list of NodeInfo for all known nodes in the cluster."""
        return await self.local_cluster_state.get_all_nodes()

    async def get_cluster_online_nodes_info(self) -> List[NodeInfo]:
        """Returns a list of NodeInfo for all ONLINE nodes in the cluster."""
        return await self.local_cluster_state.get_online_nodes()
        
    async def get_available_workers(self, required_capabilities: Optional[List[str]] = None) -> List[NodeInfo]:
        """
        Gets a list of online or degraded, non-leader nodes suitable for work.
        Filters by capabilities and sorts by load (task count, then CPU).

        Args:
            required_capabilities: Optional list of capability strings the worker must possess.

        Returns:
            A sorted list of suitable worker NodeInfo objects.
        """
        potential_workers = await self.local_cluster_state.get_potential_workers()
        available_workers: List[NodeInfo] = []

        for worker in potential_workers:
            # Basic check: must be ONLINE or DEGRADED (maybe configurable later)
            # Already filtered by get_potential_workers

            # Capability Check
            if required_capabilities:
                # Ensure worker has *all* required capabilities
                if not all(cap in worker.capabilities for cap in required_capabilities):
                    log.debug(f"Worker {worker.node_id} skipped: Missing capabilities {required_capabilities}. Has: {worker.capabilities}")
                    continue # Skip this worker
            
            # If passed checks, add to list
            available_workers.append(worker)
        
        # Sorting Strategy: 
        # 1. Lower current_task_count is better.
        # 2. Lower CPU usage is better (if available).
        # 3. Node ID as tie-breaker for stability.
        def sort_key(w: NodeInfo):
            cpu_usage = 100.0 # Default high if resources unavailable or parsing fails
            try:
                if w.resources and 'cpu' in w.resources and isinstance(w.resources['cpu'], dict) and 'percent_used' in w.resources['cpu']:
                     cpu_val = w.resources['cpu']['percent_used']
                     if isinstance(cpu_val, (int, float)):
                          cpu_usage = float(cpu_val)
            except Exception: pass # Ignore errors parsing resources, use default
            
            return (w.current_task_count, cpu_usage, w.node_id)

        available_workers.sort(key=sort_key)
        
        log.debug(f"Found {len(available_workers)} available workers matching caps {required_capabilities}. Sorted IDs: {[w.node_id for w in available_workers]}")
        return available_workers


    async def update_local_node_resources(self, resources: Dict[str, Any]):
        """Updates the resource information for the current node in the state cache and persists it."""
        # Get the current node's info object
        my_node_info = await self.local_cluster_state.get_node(self.node_id)
        if my_node_info:
            # Update fields directly on the fetched copy
            my_node_info.resources = resources
            my_node_info.last_seen = time.time() 
            # Update the cache with the modified object
            await self.local_cluster_state.update_node(my_node_info) 
            # Persist the changes to the database
            await self._persist_node_info(my_node_info)
            
    async def update_node_task_count(self, node_id: str, change: int):
        """
        Atomically increments or decrements the task count for a specified node
        in the local state cache and persists the change.
        """
        # Get the node info object safely
        node_info = await self.local_cluster_state.get_node(node_id)
        if node_info:
            # Acquire lock for atomic update of the cached object
            async with self.local_cluster_state._lock: 
                # Ensure the object fetched is the one in the cache for direct modification
                cached_node_info = self.local_cluster_state.nodes.get(node_id)
                if cached_node_info: # Check again under lock
                    cached_node_info.current_task_count += change
                    # Prevent task count from going negative
                    if cached_node_info.current_task_count < 0: 
                        log.warning(f"Attempted to decrement task count for {node_id} below zero. Setting to 0.")
                        cached_node_info.current_task_count = 0
                    # Update last_seen as this indicates activity/state change
                    cached_node_info.last_seen = time.time() 
                    # Copy the updated info before releasing lock for persistence
                    info_to_persist = cached_node_info.model_copy(deep=True)
                else:
                    log.warning(f"Node {node_id} disappeared from cache while trying to update task count.")
                    return # Exit if node vanished under lock
            
            # Persist the change outside the lock
            await self._persist_node_info(info_to_persist) 
            log.debug(f"Updated task count for node {node_id} to {info_to_persist.current_task_count}")
        else:
             log.warning(f"Cannot update task count: Node {node_id} not found in local state.")


    # --- Internal Persistence and Loading Methods ---
    async def _load_initial_state_from_db(self):
        """Loads known node states from the database into the local cluster state cache on startup."""
        log.debug("Loading initial state from database...")
        # Define SQL query, including potential new columns for capabilities/task count if persisted
        sql = """SELECT node_id, node_name, cluster_name, address_hex, last_seen, is_leader, 
                        resources, status -- , capabilities, current_task_count -- Add if persisted
                 FROM cluster_nodes WHERE cluster_name = ?"""
        try:
            # Execute query via DB manager
            future = self.db.execute(sql, (self.cluster_name,))
            rows = await asyncio.wrap_future(future) # Await result
            
            nodes_loaded_count = 0
            for row in rows:
                try:
                    # Parse row data into NodeInfo object
                    node_info = NodeInfo(
                        node_id=row["node_id"], node_name=row["node_name"], cluster_name=row["cluster_name"],
                        address_hex=row["address_hex"], last_seen=row["last_seen"], is_leader=bool(row["is_leader"]),
                        status=NodeStatus(row["status"] or "unknown"),
                        resources=json.loads(row["resources"]) if row["resources"] else None,
                        # Load capabilities and task count if they were added to schema/query
                        # capabilities=json.loads(row["capabilities"]) if row.get("capabilities") else [], 
                        # current_task_count=row.get("current_task_count", 0) 
                    )
                    # Update the local state cache with loaded info
                    await self.local_cluster_state.update_node(node_info)
                    nodes_loaded_count +=1
                except (pydantic.ValidationError, json.JSONDecodeError, ValueError, KeyError) as e: 
                    # Handle potential errors during parsing or if columns are missing
                    log.warning(f"Failed to load node {row['node_id']} from DB due to parsing error: {e}")
            log.info(f"Loaded {nodes_loaded_count} nodes from database for cluster {self.cluster_name}.")
        except Exception as e:
            log.error(f"Error loading initial state from database: {e}", exc_info=True)


    async def _persist_node_info(self, node_info: NodeInfo):
        """Queues the persistence of a single NodeInfo object to the database."""
        # Avoid persisting excessively frequently if not needed, but important for status changes.
        log.debug(f"Persisting node info for {node_info.node_id} to DB. Status: {node_info.status}, Leader: {node_info.is_leader}")
        
        # Define SQL for INSERT or REPLACE (upsert)
        # Include capabilities and task count if they are part of the schema
        sql = """
            INSERT OR REPLACE INTO cluster_nodes 
            (node_id, node_name, cluster_name, address_hex, last_seen, is_leader, resources, status --, capabilities, current_task_count -- Add if persisted
            ) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ? --, ?, ? -- Add placeholders if persisted
            ) 
        """ 
        params = (
            node_info.node_id, node_info.node_name, node_info.cluster_name,
            node_info.address_hex, node_info.last_seen,
            1 if node_info.is_leader else 0, # Convert boolean to integer for SQLite
            json.dumps(node_info.resources) if node_info.resources else None, # Serialize resources dict
            str(node_info.status) # Store enum as string
            # json.dumps(node_info.capabilities), # Serialize capabilities list if persisted
            # node_info.current_task_count # Store task count if persisted
        )
        try:
            # Queue the database operation
            future = self.db.execute(sql, params)
            # Await completion to ensure persistence before critical state changes rely on it?
            # Or let it run in the background? For now, await.
            await asyncio.wrap_future(future)
        except Exception as e:
            log.error(f"Failed to persist node info for {node_info.node_id}: {e}", exc_info=True)


    async def _trigger_state_change_callback(self):
        """Invokes the registered state change callback function, if set."""
        if self._on_state_change_callback:
            log.debug("Triggering on_state_change_callback.")
            try:
                # Pass a fresh copy of the state to the callback to avoid race conditions
                state_snapshot = await self.local_cluster_state.get_all_nodes() # Get copies
                leader_snapshot = await self.local_cluster_state.get_leader() # Get copy
                # Create a temporary ClusterState or pass relevant parts if callback expects full object
                # For simplicity, passing the main state object (caller should not modify)
                await self._on_state_change_callback(self.local_cluster_state) 
            except Exception as e:
                log.error(f"Error in on_state_change_callback: {e}", exc_info=True)

