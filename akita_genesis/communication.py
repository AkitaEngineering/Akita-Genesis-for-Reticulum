# akita_genesis/modules/communication.py
import RNS
import RNS.vendor.umsgpack as msgpack # Using Reticulum's bundled msgpack
import asyncio
import time
from typing import Callable, Any, Optional, Dict, Union, Tuple, Awaitable # Added Awaitable
from enum import Enum

# Use absolute imports within the package
from akita_genesis.config.settings import settings
from akita_genesis.utils.logger import setup_logger
# from .state_manager import NodeInfo # Forward declaration or import carefully if needed for typing

log = setup_logger(__name__)

# Forward declaration for type hint to avoid circular import with state_manager
NodeInfo = Any 

# Define application aspects for Reticulum destinations
# These are appended to the base app name (e.g., akitagen.discovery)
class AppAspect(Enum):
    DISCOVERY = "discovery"       # For node announcements and discovery probes
    STATE_SYNC = "state_sync"     # For broadcasting/exchanging cluster state
    TASK_MANAGEMENT = "task_mgm"  # For task submission, assignment, updates, results
    LEDGER_SYNC = "ledger_sync"   # For ledger synchronization (if implemented)
    HEARTBEAT = "heartbeat"       # For direct node-to-node heartbeats (optional)
    CONTROL = "control"           # For direct control messages between nodes (e.g. remote log fetch)

    def __str__(self):
        return self.value

class MessageType(Enum):
    """Defines the types of messages exchanged between nodes."""
    # Discovery & State
    NODE_ANNOUNCEMENT = "NODE_ANNOUNCEMENT" # Contains NodeInfo
    CLUSTER_STATE_BROADCAST = "CLUSTER_STATE_BROADCAST" # Contains full/partial ClusterState
    
    # Task Lifecycle
    TASK_SUBMIT_TO_LEADER = "TASK_SUBMIT_TO_LEADER" # Client/Node submits task to current leader
    TASK_ASSIGN_TO_WORKER = "TASK_ASSIGN_TO_WORKER" # Leader assigns task to a worker
    TASK_ACKNOWLEDGED_BY_WORKER = "TASK_ACKNOWLEDGED_BY_WORKER" # Worker confirms receipt of task
    TASK_RESULT_FROM_WORKER = "TASK_RESULT_FROM_WORKER" # Worker sends result back to leader
    TASK_UPDATE_REQUEST = "TASK_UPDATE_REQUEST" # e.g. client requests update on a task from leader
    TASK_STATUS_INFO = "TASK_STATUS_INFO" # Leader provides info about a task

    # Heartbeat & Control
    HEARTBEAT_PING = "HEARTBEAT_PING"
    HEARTBEAT_PONG = "HEARTBEAT_PONG"
    DIRECT_MESSAGE = "DIRECT_MESSAGE" # Generic direct message
    FETCH_LOGS_REQUEST = "FETCH_LOGS_REQUEST" # CLI requests logs from node
    FETCH_LOGS_RESPONSE = "FETCH_LOGS_RESPONSE" # Node sends logs to CLI

    def __str__(self):
        return self.value

class CommunicationManager:
    """
    Manages Reticulum network communication for Akita Genesis.
    Handles identity, destinations, sending, and receiving messages.
    """
    def __init__(self, identity_path: str):
        """
        Initializes the CommunicationManager.

        Args:
            identity_path (str): Path to the Reticulum identity file for this node.
        """
        self.identity_path = identity_path
        self.node_rns_identity: Optional[RNS.Identity] = None
        self.rns_transport: Optional[RNS.Transport] = None
        
        # Dictionary to hold configured RNS destinations for various aspects
        self.destinations: Dict[AppAspect, RNS.Destination] = {}
        # Dictionary for link status callbacks (optional)
        self.link_watchers: Dict[bytes, Callable[[RNS.Link], None]] = {} # link_id -> callback

        # Callbacks registry for incoming messages:
        # Format: { AppAspect: { MessageType: async_callback(source_rns_hash, source_akita_node_id, payload) } }
        self._message_callbacks: Dict[AppAspect, Dict[MessageType, Callable[[bytes, Optional[str], Dict[str,Any]], Awaitable[None]]]] = {} 
        # Callbacks registry for link status changes (optional)
        self._link_established_callbacks: Dict[AppAspect, Callable[[RNS.Link], Awaitable[None]]] = {}
        self._link_closed_callbacks: Dict[AppAspect, Callable[[RNS.Link], Awaitable[None]]] = {}

        self._rns_running = False # Flag indicating if RNS transport is active
        log.info(f"CommunicationManager initialized with identity path: {self.identity_path}")

    async def start(self) -> bool:
        """
        Initializes the Reticulum transport and identity.
        Sets up predefined RNS destinations based on AppAspect enum.

        Returns:
            bool: True if initialization was successful, False otherwise.
        """
        log.info("Starting CommunicationManager and Reticulum Transport...")
        try:
            # Check if RNS Transport is already running (potentially globally)
            if RNS.Transport.is_started(): # Corrected method name from is_running
                self.rns_transport = RNS.Transport # Use the existing global instance
                log.info("Reticulum Transport is already running globally.")
            else:
                # If not running globally, attempt to start a local instance for this application.
                # Note: Running multiple independent RNS transports on the same machine can lead to conflicts
                # if not configured carefully (e.g., different ports, interface configs).
                # A single system-wide RNS service is often preferred in production.
                log.info("Reticulum Transport not running globally, attempting to start a local instance.")
                self.rns_transport = RNS.Transport(
                    # Use program_data_path for cache location as per RNS docs
                    program_data_path=settings.DATA_DIR / "reticulum_transport_cache", 
                    listen_port=settings.DEFAULT_RNS_PORT # Allow specifying a listen port via settings
                )
                log.info(f"Reticulum Transport instance created. Cache: {settings.DATA_DIR / 'reticulum_transport_cache'}, Port: {settings.DEFAULT_RNS_PORT or 'default'}")

            # Load or create the Reticulum identity for this node
            self.node_rns_identity = RNS.Identity.from_file(self.identity_path)
            if not self.node_rns_identity:
                log.info(f"No Reticulum identity found at {self.identity_path}, creating new one...")
                self.node_rns_identity = RNS.Identity()
                self.node_rns_identity.to_file(self.identity_path)
                log.info(f"New Reticulum identity created and saved to {self.identity_path}")
            
            # Ensure identity was loaded/created successfully
            if self.node_rns_identity is None: 
                 log.error("Failed to load or create Reticulum identity.")
                 return False

            log.info(f"Reticulum Identity loaded: {RNS.prettyhexrep(self.node_rns_identity.hash)}")

            # Setup predefined RNS destinations for listening
            # DISCOVERY should be GROUP to receive broadcast announcements
            await self._setup_destination(AppAspect.DISCOVERY, self._discovery_message_handler, is_single=False) 
            # STATE_SYNC could be GROUP for broadcast updates or SINGLE for direct sync
            await self._setup_destination(AppAspect.STATE_SYNC, self._state_sync_message_handler, is_single=False) # Assume group for broadcasts
            # TASK_MANAGEMENT is SINGLE for direct leader/worker communication
            await self._setup_destination(AppAspect.TASK_MANAGEMENT, self._task_management_message_handler, is_single=True)
            # CONTROL is SINGLE for direct CLI/external control messages to this node
            await self._setup_destination(AppAspect.CONTROL, self._control_message_handler, is_single=True)

            self._rns_running = True
            log.info("CommunicationManager started successfully.")
            return True

        except Exception as e:
            # Log detailed error during startup
            log.error(f"Failed to start CommunicationManager: {e}", exc_info=True)
            return False

    async def stop(self):
        """Shuts down communication manager components (stops announcements)."""
        log.info("Stopping CommunicationManager...")
        self._rns_running = False # Signal loops/callbacks to stop using RNS
        
        # Stop any active periodic RNS Announce handlers managed by this instance
        if hasattr(self, '_announce_handlers'):
            for aspect in list(self._announce_handlers.keys()): # Iterate over a copy of keys
                self.stop_announce(aspect)
        
        # RNS Destinations are typically managed by the RNS.Transport instance.
        # If this CommunicationManager started its own Transport instance and it's not shared,
        # it *could* be stopped here, but usually RNS Transport lifecycle is managed globally
        # or allowed to terminate naturally when the application exits.
        # Example (if needed): RNS.Transport.stop_propagation()
        log.info("CommunicationManager stopped (RNS destinations will be closed by RNS Transport).")


    def get_node_identity_hash(self) -> Optional[bytes]:
        """Returns the raw bytes of the node's RNS identity hash."""
        return self.node_rns_identity.hash if self.node_rns_identity else None
        
    def get_node_identity_hex(self) -> Optional[str]:
        """Returns the hexadecimal representation of the node's RNS identity hash."""
        ident_hash = self.get_node_identity_hash()
        return RNS.prettyhexrep(ident_hash) if ident_hash else None

    async def _setup_destination(self, aspect: AppAspect, receive_callback: Callable, is_single: bool = True):
        """
        Helper method to configure an RNS Destination for a specific application aspect.

        Args:
            aspect: The AppAspect enum member defining the service.
            receive_callback: The async function to call when a packet is received on this destination.
            is_single: True for a SINGLE destination (direct communication), False for GROUP (broadcast listening).
        """
        if self.node_rns_identity is None:
            log.error(f"Cannot setup destination for {aspect.value}: RNS Identity not loaded.")
            return

        dest_type = RNS.Destination.SINGLE if is_single else RNS.Destination.GROUP
        # Ensure the application name used for RNS is compliant (alphanumeric, <= 10 chars)
        # Use the validated short name from settings.
        compliant_app_name_short = settings.APP_NAME_SHORT[:10]

        # Create the RNS Destination instance
        self.destinations[aspect] = RNS.Destination(
            self.node_rns_identity, # The identity associated with this endpoint
            dest_type,              # SINGLE or GROUP
            compliant_app_name_short, # Base application name part
            aspect.value            # Specific service aspect
        )
        
        # Set the callback for incoming packets.
        # IMPORTANT: RNS callbacks run in RNS's internal threads. To interact safely with
        # asyncio components, the callback must schedule the async handler on the correct event loop.
        self.destinations[aspect].set_packet_callback(
            # Lambda function captures the async callback and schedules it
            lambda data, packet: asyncio.run_coroutine_threadsafe(
                receive_callback(data, packet), # The async handler to run
                asyncio.get_event_loop()        # The loop to run it on
            )
        )

        # Optionally set callbacks for link status changes (if needed for the aspect)
        if aspect in self._link_established_callbacks:
            self.destinations[aspect].set_link_established_callback(
                 lambda link: asyncio.run_coroutine_threadsafe(
                     self._link_established_callbacks[aspect](link), asyncio.get_event_loop()
                 )
            )
        if aspect in self._link_closed_callbacks:
             self.destinations[aspect].set_link_closed_callback(
                 lambda link: asyncio.run_coroutine_threadsafe(
                     self._link_closed_callbacks[aspect](link), asyncio.get_event_loop()
                 )
            )

        log.info(f"Destination {self.destinations[aspect].name} for aspect '{aspect.value}' configured. Type: {'SINGLE' if is_single else 'GROUP'}")


    def register_message_handler(self, aspect: AppAspect, msg_type: MessageType, callback: Callable[[bytes, Optional[str], Dict[str, Any]], Awaitable[None]]):
        """
        Registers an asynchronous callback function to handle specific message types received on a given aspect.

        Args:
            aspect: The AppAspect the message is expected on.
            msg_type: The MessageType to handle.
            callback: An async function: `async def handler(source_rns_hash: bytes, source_akita_node_id: Optional[str], payload: Dict[str, Any])`
        """
        if aspect not in self._message_callbacks:
            self._message_callbacks[aspect] = {}
        self._message_callbacks[aspect][msg_type] = callback
        log.debug(f"Registered handler for MsgType {msg_type.value} on Aspect {aspect.value}")

    # --- Methods for registering link status handlers (optional) ---
    # def register_link_established_handler(self, aspect: AppAspect, callback: Callable[[RNS.Link], Awaitable[None]]): ...
    # def register_link_closed_handler(self, aspect: AppAspect, callback: Callable[[RNS.Link], Awaitable[None]]): ...

    async def _deserialize_message(self, data_bytes: bytes) -> Optional[Tuple[MessageType, Optional[str], Dict[str, Any]]]:
        """
        Deserializes an incoming message using msgpack.
        Expects a dictionary with 'type', 'payload', and optionally 'source_akita_node_id'.

        Returns:
            Tuple containing (MessageType, source_akita_node_id, payload) or None if deserialization fails.
        """
        try:
            unpacked_data = msgpack.unpackb(data_bytes)
            # Validate basic structure
            if not isinstance(unpacked_data, dict) or "type" not in unpacked_data or "payload" not in unpacked_data:
                log.warning(f"Received malformed message (not a dict or missing keys): {unpacked_data!r}")
                return None
            
            msg_type_str = unpacked_data["type"]
            # Include Akita-level sender ID if present in the message
            source_akita_node_id = unpacked_data.get("source_akita_node_id") 

            # Convert message type string to Enum
            try:
                msg_type = MessageType(msg_type_str)
            except ValueError:
                log.warning(f"Received message with unknown type: {msg_type_str}")
                return None
            
            payload = unpacked_data["payload"]
            return msg_type, source_akita_node_id, payload
        except Exception as e:
            # Log error and the problematic data (partially)
            log.error(f"Failed to deserialize message: {e}. Data (first 100 bytes): {data_bytes[:100]!r}")
            return None

    async def _handle_incoming_packet(self, data_bytes: bytes, packet: RNS.Packet, aspect: AppAspect):
        """
        Generic internal handler called by RNS destination callbacks for incoming packets.
        Deserializes the message and dispatches it to the appropriate registered handler.
        """
        # Ignore packets if RNS is shutting down
        if not self._rns_running: return

        # Log packet reception details
        source_rns_hash = packet.link.get_remote_identity_hash() if packet.link else None # Get sender RNS hash if link exists
        source_rns_hex = RNS.prettyhexrep(source_rns_hash) if source_rns_hash else "Unknown (No Link?)"
        log.debug(f"Packet received on aspect {aspect.value} from RNS {source_rns_hex} "
                  f"(RSSI {packet.rssi} dBm, SNR {packet.snr} dB)") # Use packet attributes directly
        
        # Deserialize the message content
        deserialized = await self._deserialize_message(data_bytes)
        if not deserialized:
            # Deserialization failed or message was malformed
            return

        msg_type, source_akita_node_id, payload = deserialized
        
        # Find and call the registered handler for this aspect and message type
        if aspect in self._message_callbacks and msg_type in self._message_callbacks[aspect]:
            try:
                # Call the registered async handler, passing sender info and payload
                await self._message_callbacks[aspect][msg_type](source_rns_hash, source_akita_node_id, payload)
            except Exception as e:
                # Log errors occurring within the application's message handler
                log.error(f"Error in message handler for {aspect.value}/{msg_type.value}: {e}", exc_info=True)
        else:
            # Log if no specific handler was registered
            log.warning(f"No handler registered for message type {msg_type.value} on aspect {aspect.value}")

    # --- Specific callbacks assigned to RNS destinations ---
    # These simply call the generic handler with the correct aspect
    async def _discovery_message_handler(self, data_bytes: bytes, packet: RNS.Packet):
        await self._handle_incoming_packet(data_bytes, packet, AppAspect.DISCOVERY)

    async def _state_sync_message_handler(self, data_bytes: bytes, packet: RNS.Packet):
        await self._handle_incoming_packet(data_bytes, packet, AppAspect.STATE_SYNC)

    async def _task_management_message_handler(self, data_bytes: bytes, packet: RNS.Packet):
        await self._handle_incoming_packet(data_bytes, packet, AppAspect.TASK_MANAGEMENT)
    
    async def _control_message_handler(self, data_bytes: bytes, packet: RNS.Packet):
        await self._handle_incoming_packet(data_bytes, packet, AppAspect.CONTROL)


    async def send_message(
        self,
        destination_rns_hash: bytes, # Target node's RNS identity hash
        aspect: AppAspect,
        message_type: MessageType,
        payload: Dict[str, Any],
        source_akita_node_id: Optional[str] = None, # This node's Akita ID (mandatory)
        timeout_s: int = 10 # Timeout for link establishment attempts
    ) -> bool:
        """
        Sends a unicast message to a specific RNS destination (another node).

        Args:
            destination_rns_hash: The RNS hash of the target identity.
            aspect: The application aspect on the target.
            message_type: The type of message being sent.
            payload: The data payload for the message.
            source_akita_node_id: The Akita-level ID of the sending node (required).
            timeout_s: Timeout in seconds for link establishment attempts.

        Returns:
            True if message was successfully sent (queued by RNS), False otherwise.
        """
        if not self._rns_running or not self.node_rns_identity:
            log.error("Cannot send message: RNS not running or identity not set.")
            return False
        if not source_akita_node_id: # Require Akita-level source ID in messages
            log.error("Cannot send message: source_akita_node_id is required.")
            return False

        try:
            # Ensure app name used for destination is compliant
            compliant_app_name_short = settings.APP_NAME_SHORT[:10]
            # Create an RNS Destination object representing the target
            target_rns_destination = RNS.Destination(
                RNS.Identity(destination_hash=destination_rns_hash), # Use target hash
                RNS.Destination.SINGLE, # Direct messages go to SINGLE destinations
                compliant_app_name_short, 
                aspect.value
            )

            # Prepare the message structure including type, source ID, and payload
            message_data = {
                "type": str(message_type), 
                "source_akita_node_id": source_akita_node_id, # Include sender's Akita ID
                "payload": payload
            }
            # Serialize the message using msgpack
            packed_data = msgpack.packb(message_data)
            
            # Create an RNS Link object for the target destination
            # RNS will manage pathfinding and link establishment attempts.
            link = RNS.Link(target_rns_destination)
            link.set_timeout(timeout_s) # Set how long RNS should try to establish the link

            # Define the potentially blocking send operation to run in a thread
            def blocking_send():
                # Check if link is usable or try to establish it (this can block)
                if not link.is_usable() and not link.establish():
                    log.warning(f"Could not establish link to {RNS.prettyhexrep(destination_rns_hash)} for {message_type.value} on aspect {aspect.value}.")
                    return None # Indicate link failure
                
                # If link is ready, create and send the packet
                packet = RNS.Packet(link, packed_data)
                # packet.send() returns a receipt object (or None on immediate failure)
                return packet.send()

            # Execute the blocking send operation in an executor thread
            receipt = await asyncio.to_thread(blocking_send)

            # Check the receipt status
            if receipt and (receipt.status == RNS.PacketReceipt.SENT or receipt.status == RNS.PacketReceipt.DELIVERED):
                # SENT means RNS accepted it for transmission, DELIVERED means proof was received (if requested)
                log.debug(f"Message {message_type.value} sent to {RNS.prettyhexrep(destination_rns_hash)} on aspect {aspect.value}. Receipt status: {receipt.status_str()}")
                return True
            else:
                # Log failure based on receipt status or link failure
                status_str = receipt.status_str() if receipt else "Link establishment failed or no receipt"
                log.warning(f"Failed to send message {message_type.value} to {RNS.prettyhexrep(destination_rns_hash)}. Status: {status_str}")
                # Optionally tear down the link if sending failed critically? RNS usually handles cleanup.
                # link.teardown() 
                return False

        except RNS.exceptions.LinkCreationFailedException as lcf:
            # Specific exception for link creation issues
            log.error(f"Link creation failed for sending to {RNS.prettyhexrep(destination_rns_hash)} on aspect {aspect.value}: {lcf}")
            return False
        except Exception as e:
            # Catch any other unexpected errors during the send process
            log.error(f"Error sending message to {RNS.prettyhexrep(destination_rns_hash)}: {e}", exc_info=True)
            return False

    async def announce(self, aspect_to_announce_on: AppAspect, message_type: MessageType, payload_for_app_data: Dict[str, Any], source_akita_node_id: str):
        """
        Starts or Updates a periodic RNS Announce for this node's service/presence.
        This makes the node discoverable on the specified aspect. The payload is included
        as application data in the announcement packet.

        Args:
            aspect_to_announce_on: The aspect others will listen on to discover this service/node.
            message_type: The type of this announcement message (e.g., NODE_ANNOUNCEMENT).
            payload_for_app_data: The dictionary payload (e.g., NodeInfo) to include.
            source_akita_node_id: The Akita ID of this node making the announcement.
        """
        if not self._rns_running or not self.node_rns_identity:
            log.error("Cannot send announcement: RNS not running or identity not set.")
            return
        if not source_akita_node_id:
             log.error("Cannot announce: source_akita_node_id is required for app_data.")
             return

        # Ensure app name is compliant
        compliant_app_name_short = settings.APP_NAME_SHORT[:10]
        # Construct the full service name path being announced (e.g., "akitagen.discovery")
        service_name_path = f"{compliant_app_name_short}.{aspect_to_announce_on.value}"

        # Prepare the application data to be included in the announcement packet
        app_data_content = {
            "type": str(message_type),
            "source_akita_node_id": source_akita_node_id, # Identify the Akita node
            "payload": payload_for_app_data
        }
        packed_app_data = msgpack.packb(app_data_content)

        try:
            # Initialize announce handlers dictionary if it doesn't exist
            if not hasattr(self, '_announce_handlers'):
                self._announce_handlers: Dict[AppAspect, RNS.Announce] = {}

            # Check if an announcement for this aspect is already running
            if aspect_to_announce_on in self._announce_handlers and self._announce_handlers[aspect_to_announce_on].is_active():
                current_handler = self._announce_handlers[aspect_to_announce_on]
                # If the data has changed, we need to stop the old announcement and start a new one
                # RNS.Announce doesn't support updating app_data on the fly.
                if current_handler.app_data != packed_app_data:
                    log.debug(f"App data changed for ongoing announcement on {service_name_path}. Restarting announce.")
                    current_handler.stop() # Stop the old one
                    # Continue to create the new one below
                else:
                    # Data hasn't changed, no need to restart the announcement
                    log.debug(f"Announcement for {service_name_path} already active with same data.")
                    return # No action needed

            # Create and start the RNS.Announce instance
            log.debug(f"Starting/Updating RNS Announce for service {service_name_path}. App data payload: {str(payload_for_app_data)[:100]}")
            # The Announce instance automatically starts announcing periodically.
            self._announce_handlers[aspect_to_announce_on] = RNS.Announce(
                owner_identity=self.node_rns_identity, # Our node identity owns this announcement
                name_path=service_name_path, # The "service" name being announced
                app_data=packed_app_data, # The actual data (e.g., NodeInfo)
                announce_interval=settings.ANNOUNCE_INTERVAL_S # Use configured interval
            )
            log.info(f"Periodic RNS Announce started/updated for {service_name_path}.")

        except Exception as e:
            log.error(f"Error during RNS Announce setup for aspect {aspect_to_announce_on.value}: {e}", exc_info=True)

    def stop_announce(self, aspect: AppAspect):
        """Stops the periodic RNS Announce for a given aspect."""
        if hasattr(self, '_announce_handlers') and aspect in self._announce_handlers:
            if self._announce_handlers[aspect].is_active():
                self._announce_handlers[aspect].stop()
                log.info(f"Stopped periodic RNS Announce for aspect {aspect.value}")
            # Remove the handler from our tracking dictionary
            del self._announce_handlers[aspect] 


    async def send_to_all_known_peers(
        self, 
        aspect: AppAspect, 
        message_type: MessageType, 
        payload: Dict[str, Any], 
        peer_list: List[NodeInfo], # Expects list of StateManager.NodeInfo like objects
        source_akita_node_id: str
    ):
        """
        Sends a message concurrently to a list of known peers (nodes).

        Args:
            aspect: The target application aspect on the peers.
            message_type: The type of message to send.
            payload: The message payload.
            peer_list: A list of NodeInfo objects representing the target peers.
            source_akita_node_id: The Akita ID of the sending node.
        """
        if not peer_list:
            log.debug(f"No peers to send message {message_type.value} on aspect {aspect.value}")
            return

        sent_count = 0
        failed_count = 0
        tasks = [] # List to hold asyncio tasks for concurrent sends

        # Create a send task for each peer
        for peer_node_info in peer_list:
            # Ensure peer_node_info has 'address_hex' and 'node_id' (Akita ID)
            peer_rns_hash_hex = getattr(peer_node_info, 'address_hex', None)
            peer_akita_id = getattr(peer_node_info, 'node_id', 'UnknownPeer')

            if peer_rns_hash_hex:
                try:
                    # Convert hex address to bytes
                    dest_rns_hash = RNS.hexrep_to_bytes(peer_rns_hash_hex)
                    # Create an asyncio task for the send_message coroutine
                    tasks.append(
                        self.send_message(dest_rns_hash, aspect, message_type, payload, source_akita_node_id)
                    )
                except Exception as e:
                    # Log error if address conversion or task creation fails
                    log.error(f"Error preparing to send to peer {peer_akita_id} ({peer_rns_hash_hex}): {e}")
                    failed_count +=1
            else:
                # Log warning if peer has no RNS address
                log.warning(f"Peer {peer_akita_id} has no RNS address_hex, cannot send message.")
                failed_count += 1
        
        # Execute all send tasks concurrently and gather results
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Process results
            for result in results:
                if isinstance(result, bool) and result:
                    sent_count += 1 # Increment success count
                else: 
                    failed_count += 1 # Increment failure count (False or Exception)
                    if isinstance(result, Exception):
                        # Log exceptions that occurred during sending
                        log.error(f"Exception during send_to_all_known_peers gather: {result}")
        
        log.info(f"Attempted to send message {message_type.value} to {len(peer_list)} peers: {sent_count} succeeded, {failed_count} failed.")


