# akita_genesis/modules/discovery.py
import asyncio
import time
from typing import Callable, Any, Optional, Dict, Awaitable # Added Awaitable

import RNS # For RNS.prettyhexrep if needed

# Use absolute imports within the package
from akita_genesis.config.settings import settings
from akita_genesis.utils.logger import setup_logger
from akita_genesis.modules.communication import CommunicationManager, AppAspect, MessageType
# Import specific NodeInfo type from state_manager for clarity
from akita_genesis.modules.state_manager import StateManager, NodeInfo, NodeStatus 

log = setup_logger(__name__)

class DiscoveryManager:
    """
    Handles node discovery using Reticulum announcements.
    It periodically announces this node's presence and listens for announcements 
    from other nodes within the same cluster.
    """

    def __init__(
        self,
        node_id: str, # This node's app-level ID (Akita ID)
        node_name: str, # This node's human-readable name
        cluster_name: str, # The cluster this node belongs to
        communication_manager: CommunicationManager,
        state_manager: StateManager
    ):
        """
        Initializes the DiscoveryManager.

        Args:
            node_id: The Akita-level unique ID for this node.
            node_name: The human-readable name for this node.
            cluster_name: The name of the cluster this node operates within.
            communication_manager: Instance of CommunicationManager for RNS operations.
            state_manager: Instance of StateManager to update with discovered nodes.
        """
        self.node_id = node_id 
        self.node_name = node_name
        self.cluster_name = cluster_name
        self.comm_manager = communication_manager
        self.state_manager = state_manager

        self._announce_task: Optional[asyncio.Task] = None # Task for periodic announcements
        self._is_running = False # Flag indicating if discovery is active
        
        # The NodeInfo object representing *this* node, which will be announced.
        # This is initialized/updated in the start() and _periodic_announce_loop methods.
        self.this_node_info: Optional[NodeInfo] = None

        log.info("DiscoveryManager initialized.")

    async def start(self):
        """
        Starts the discovery process:
        1. Registers a handler to listen for announcements from other nodes.
        2. Prepares this node's own information (`NodeInfo`).
        3. Starts a background task to periodically announce this node's presence.
        """
        if self._is_running:
            log.warning("DiscoveryManager already running.")
            return

        log.info("Starting DiscoveryManager...")
        self._is_running = True

        # Register handler for incoming NODE_ANNOUNCEMENT messages on the DISCOVERY aspect
        # The callback `_handle_node_announcement_message` will process these.
        self.comm_manager.register_message_handler(
            AppAspect.DISCOVERY,
            MessageType.NODE_ANNOUNCEMENT,
            self._handle_node_announcement_message
        )
        
        # Ensure the DISCOVERY destination is set up correctly in CommunicationManager
        # for listening (it should be a GROUP destination).
        # CommunicationManager.start() should handle this, but we double-check.
        if AppAspect.DISCOVERY not in self.comm_manager.destinations:
            log.error(f"Discovery aspect {AppAspect.DISCOVERY.value} not configured in CommunicationManager. Cannot listen for announcements.")
            # Attempt to dynamically set it up as GROUP (though ideally pre-configured)
            await self.comm_manager._setup_destination(AppAspect.DISCOVERY, self.comm_manager._discovery_message_handler, is_single=False) # False for GROUP
            if AppAspect.DISCOVERY not in self.comm_manager.destinations:
                 self._is_running = False
                 log.error("Failed to dynamically set up DISCOVERY aspect. Discovery will not function.")
                 return
        elif self.comm_manager.destinations[AppAspect.DISCOVERY].type != RNS.Destination.GROUP:
             # Log a warning if it's configured but not as GROUP, as it might not receive broadcasts.
             log.warning(f"Discovery aspect {AppAspect.DISCOVERY.value} is not configured as GROUP type. Broadcast discovery might not work correctly.")

        # Prepare this node's information (`NodeInfo`) for announcement.
        # Get RNS identity hash from CommunicationManager (should be available after its start).
        rns_identity_hex = self.comm_manager.get_node_identity_hex()
        if not rns_identity_hex:
            log.error("Cannot start discovery announcements: RNS identity hash is not available from CommunicationManager.")
            self._is_running = False
            return

        # Get the latest status and resources for this node from the StateManager's cache.
        # StateManager.start() should have initialized this node's entry.
        local_node_state = await self.state_manager.local_cluster_state.get_node(self.node_id)
        
        if not local_node_state:
             log.error(f"Cannot prepare initial NodeInfo for announcement: Node {self.node_id} not found in StateManager.")
             # This indicates a problem in the startup sequence.
             # We can create a default one, but it might lack current state.
             local_node_state = NodeInfo( # Create a default if missing
                 node_id=self.node_id, node_name=self.node_name, cluster_name=self.cluster_name,
                 address_hex=rns_identity_hex, status=NodeStatus.ONLINE, # Assume online initially
                 # Attempt to get capabilities from state manager even if full node info missing
                 capabilities=getattr(self.state_manager.local_cluster_state.nodes.get(self.node_id), 'capabilities', []) 
             )

        # Create the initial NodeInfo object for this node to announce
        # Ensure all fields from the NodeInfo model are included
        self.this_node_info = NodeInfo(
            node_id=self.node_id, 
            node_name=self.node_name,
            cluster_name=self.cluster_name,
            address_hex=rns_identity_hex, 
            last_seen=time.time(), 
            is_leader=await self.state_manager.is_current_node_leader(), 
            status=local_node_state.status, 
            resources=local_node_state.resources, 
            capabilities=local_node_state.capabilities, 
            current_task_count=local_node_state.current_task_count 
        )

        # Start the background task for periodic announcements if not already running
        if not self._announce_task or self._announce_task.done():
            self._announce_task = asyncio.create_task(self._periodic_announce_loop())
        
        log.info("DiscoveryManager started. Listening for announcements and will announce self.")

    async def stop(self):
        """Stops the discovery process, cancelling the announcement task."""
        if not self._is_running:
            return
        log.info("Stopping DiscoveryManager...")
        self._is_running = False

        # Cancel the periodic announcement task
        if self._announce_task and not self._announce_task.done():
            self._announce_task.cancel()
            try:
                # Wait for the task to finish cancellation
                await self._announce_task
            except asyncio.CancelledError:
                pass # Expected exception on cancellation
            self._announce_task = None
        
        # Stop the underlying RNS Announce mechanism via CommunicationManager
        self.comm_manager.stop_announce(AppAspect.DISCOVERY)

        # Unregister message handlers? Usually not needed if CommunicationManager handles teardown.
        log.info("DiscoveryManager stopped.")

    async def _periodic_announce_loop(self):
        """Background task that periodically announces this node's presence and state."""
        log.info(f"Starting periodic announcement loop. Interval: {settings.ANNOUNCE_INTERVAL_S}s")
        while self._is_running:
            try:
                # Ensure node info is available before announcing
                if not self.this_node_info:
                    log.warning("Cannot announce: this_node_info not set. Retrying later.")
                    await asyncio.sleep(settings.ANNOUNCE_INTERVAL_S)
                    continue

                # --- Update dynamic fields in this_node_info before announcing ---
                self.this_node_info.last_seen = time.time() # Update timestamp
                # Get latest leader status, operational status, resources, and task count from StateManager
                self.this_node_info.is_leader = await self.state_manager.is_current_node_leader()
                # Access state safely
                my_current_state = await self.state_manager.local_cluster_state.get_node(self.node_id)
                if my_current_state:
                    self.this_node_info.status = my_current_state.status
                    self.this_node_info.resources = my_current_state.resources
                    self.this_node_info.current_task_count = my_current_state.current_task_count
                    # Capabilities usually don't change dynamically, ensure they are set
                    self.this_node_info.capabilities = my_current_state.capabilities 
                else: 
                    # Fallback if state somehow missing, but log warning
                    log.warning(f"Could not retrieve current state for self ({self.node_id}) during announcement prep.")
                    self.this_node_info.status = NodeStatus.UNKNOWN 

                log.debug(f"Announcing self: {self.this_node_info.node_id} ({self.this_node_info.node_name}) "
                          f"Leader: {self.this_node_info.is_leader} Status: {self.this_node_info.status} "
                          f"Tasks: {self.this_node_info.current_task_count} Caps: {self.this_node_info.capabilities}")
                
                # Use CommunicationManager to send the announcement via RNS.Announce
                # This updates the periodic announcement with the latest NodeInfo data.
                await self.comm_manager.announce(
                    aspect_to_announce_on=AppAspect.DISCOVERY, # Announce on the DISCOVERY "channel"
                    message_type=MessageType.NODE_ANNOUNCEMENT, # Identify the message type
                    payload_for_app_data=self.this_node_info.model_dump(), # Send current NodeInfo as payload
                    source_akita_node_id=self.node_id # Include our Akita ID
                )

                # Wait for the configured interval before next announcement
                await asyncio.sleep(settings.ANNOUNCE_INTERVAL_S)

            except asyncio.CancelledError:
                log.info("Periodic announcement loop cancelled.")
                break # Exit loop cleanly
            except Exception as e:
                log.error(f"Error in periodic announcement loop: {e}", exc_info=True)
                # Avoid busy-looping on persistent errors, wait before retrying
                await asyncio.sleep(settings.ANNOUNCE_INTERVAL_S / 2)
        log.info("Periodic announcement loop stopped.")

    async def _handle_node_announcement_message(self, source_rns_hash: bytes, source_akita_node_id: Optional[str], payload: Dict[str, Any]):
        """
        Asynchronous callback executed when a NODE_ANNOUNCEMENT message is received 
        on the DISCOVERY aspect. Parses the payload into NodeInfo and updates the StateManager.
        """
        # Ignore if discovery manager is not running
        if not self._is_running:
            return

        # Log reception with sender details
        source_rns_hex = RNS.prettyhexrep(source_rns_hash) if source_rns_hash else "UnknownRNS"
        log.debug(f"Received NODE_ANNOUNCEMENT from AkitaNode {source_akita_node_id} / RNS {source_rns_hex}. Payload: {str(payload)[:200]}")
        
        # Parse the payload dictionary into a NodeInfo object using Pydantic validation
        try:
            announced_node_info = NodeInfo(**payload)
        except Exception as e: # Catches pydantic.ValidationError and other potential errors
            log.warning(f"Failed to parse NodeInfo from announcement payload: {e}. Payload: {payload}")
            return

        # --- Basic Validation ---
        # Ensure essential fields are present
        if not announced_node_info.node_id or not announced_node_info.cluster_name:
            log.warning(f"Received invalid NodeInfo (missing id or cluster_name): {announced_node_info}")
            return

        # Ignore announcements from self
        if announced_node_info.node_id == self.node_id:
            log.debug("Received own announcement, ignoring.")
            return
            
        # Ignore announcements from nodes belonging to a different cluster
        if announced_node_info.cluster_name != self.cluster_name:
            log.debug(f"Ignoring announcement from node {announced_node_info.node_id} of different cluster '{announced_node_info.cluster_name}'. Expected '{self.cluster_name}'.")
            return

        # --- Cross-check RNS Source ---
        # Ensure the RNS address announced in the payload matches the actual RNS source hash of the packet.
        announced_rns_hex_payload = announced_node_info.address_hex

        if announced_rns_hex_payload and source_rns_hex and announced_rns_hex_payload != source_rns_hex:
            log.warning(f"Mismatch between announced RNS address ({announced_rns_hex_payload}) and packet source RNS hash ({source_rns_hex}) for node {announced_node_info.node_id}. Using packet source hash.")
            # Trust the actual source hash from the packet over the announced one
            announced_node_info.address_hex = source_rns_hex
        elif not announced_rns_hex_payload and source_rns_hex:
             # If payload didn't include address but we know the source, add it.
             log.debug(f"Populating missing address_hex for announced node {announced_node_info.node_id} with source RNS hash {source_rns_hex}")
             announced_node_info.address_hex = source_rns_hex


        # --- Update State Manager ---
        log.info(f"Processing announcement from Node: {announced_node_info.node_name} ({announced_node_info.node_id}), "
                 f"Address: {announced_node_info.address_hex}, Status: {announced_node_info.status}, Leader: {announced_node_info.is_leader}, "
                 f"Tasks: {announced_node_info.current_task_count}, Caps: {announced_node_info.capabilities}")

        # Pass the validated and potentially corrected NodeInfo to the StateManager
        # StateManager will handle merging this information into the local cluster view.
        await self.state_manager.handle_node_announcement(announced_node_info)

    def update_local_node_info_for_announcement(self, field_name: str, value: Any):
        """
        Allows the main Node component to update fields in the NodeInfo object
        that this DiscoveryManager instance announces periodically.
        Useful for reflecting changes in status, leader role, or task load.

        Args:
            field_name: The attribute name on the NodeInfo object to update.
            value: The new value for the attribute.
        """
        if self.this_node_info:
            if hasattr(self.this_node_info, field_name):
                # Update the field on the cached NodeInfo object
                setattr(self.this_node_info, field_name, value)
                # Update the last_seen timestamp whenever info changes, as it implies activity
                self.this_node_info.last_seen = time.time() 
                log.debug(f"Local node info for announcement updated: {field_name} = {value}")
                # The updated info will be included in the next periodic announcement.
            else:
                log.warning(f"Cannot update field '{field_name}' in local NodeInfo: field does not exist.")
        else:
            # This might happen if called before start() fully initializes this_node_info
            log.warning("Cannot update local node info: this_node_info not yet initialized.")

