# akita_genesis/core/node.py
import asyncio
import signal
import time
import os
from typing import Optional, Dict, Any, List, Set
import uuid 
import platform 
import random # For worker selection if multiple with same load

import RNS # For Reticulum operations and types

# FastAPI and Uvicorn for the control API
from fastapi import FastAPI, HTTPException, Body, Depends, Security # Added Depends, Security
from fastapi.security.api_key import APIKeyHeader # For API Key security
from fastapi.responses import JSONResponse
import uvicorn

from pydantic import SecretStr # For handling API keys securely

# Use absolute imports within the package
from akita_genesis.config.settings import settings
from akita_genesis.utils.logger import setup_logger, log 
from akita_genesis.modules.persistence import db_manager 
from akita_genesis.modules.ledger import Ledger, EventType
from akita_genesis.modules.resources import ResourceMonitor
from akita_genesis.modules.tasks import TaskManager, Task, TaskStatus 
from akita_genesis.modules.state_manager import StateManager, NodeInfo as StateNodeInfo, NodeStatus as StateNodeStatus, ClusterState
from akita_genesis.modules.communication import CommunicationManager, AppAspect, MessageType

# --- API Key Security ---
# Define the API key header based on settings
api_key_header = APIKeyHeader(name=settings.API_KEY_HEADER_NAME, auto_error=False)

async def get_api_key(api_key: str = Security(api_key_header)):
    """
    FastAPI dependency function to validate the API key provided in the request header.
    Compares the provided key against the set of valid keys in the application settings.
    Allows access if no keys are configured (with a warning logged at startup).
    """
    if not settings.VALID_API_KEYS:
        # If no keys are configured, allow access but log it for awareness
        log.debug("API access allowed: No API keys configured.")
        return "unsecured_access" # Indicate unsecured access mode

    # Wrap the provided key in SecretStr for secure comparison
    provided_key = SecretStr(api_key if api_key else "")

    if provided_key in settings.VALID_API_KEYS:
        # Log successful validation (masking most of the key)
        masked_key = f"...{api_key[-4:]}" if api_key and len(api_key) >= 4 else api_key
        log.debug(f"API access granted for key ending in '{masked_key}'.")
        return api_key # Return the valid key, could be used for user identification later
    else:
        # Log failed attempt and raise HTTP 403 Forbidden
        log.warning(f"Forbidden API access attempt: Invalid or missing API key (Header: {settings.API_KEY_HEADER_NAME}).")
        raise HTTPException(
            status_code=403, detail="Invalid or missing API Key"
        )

# --- Node Class ---

class AkitaGenesisNode:
    """
    The main class representing an Akita Genesis node.
    It initializes and manages all core components (communication, state, tasks, etc.) 
    and their interactions within an asyncio event loop. Includes an optional FastAPI server
    for external control and monitoring.
    """

    def __init__(
        self,
        node_name: Optional[str] = None,
        cluster_name: Optional[str] = None,
        identity_path: Optional[str] = None,
        api_host: Optional[str] = None,
        api_port: Optional[int] = None,
        rns_port: Optional[int] = None, 
        run_api_server: bool = True,
        capabilities: Optional[List[str]] = None, # Node capabilities for scheduling
    ):
        """
        Initializes the Akita Genesis Node instance.

        Args:
            node_name: Human-readable name for the node. Defaults to hostname-prefix.
            cluster_name: Name of the cluster to join. Defaults to config setting.
            identity_path: Path to the Reticulum identity file. Defaults to location in data dir.
            api_host: Host address for the control API server. Defaults to config setting.
            api_port: Port for the control API server. Defaults to config setting.
            rns_port: Specific UDP port for Reticulum Transport. Defaults to None (RNS default).
            run_api_server: Whether to start the FastAPI control API server. Defaults to True.
            capabilities: List of strings representing node capabilities (e.g., ['gpu', 'fast_io']).
        """
        # --- Node Identification and Configuration ---
        self.node_id: str = self._generate_node_id() # Unique internal ID for this Akita node
        # Generate a default node name if not provided
        default_hostname = platform.node().split('.')[0] # Use short hostname
        self.node_name: str = node_name or settings.DEFAULT_NODE_NAME or f"{default_hostname}-{self.node_id[:4]}"
        self.cluster_name: str = cluster_name or settings.DEFAULT_CLUSTER_NAME
        # Ensure capabilities are unique and sorted for consistent representation
        self.capabilities: List[str] = sorted(list(set(capabilities or [])))
        
        # Override Reticulum port from config if provided
        if rns_port is not None:
            settings.DEFAULT_RNS_PORT = rns_port
        
        # Determine identity path, ensure directory exists
        self.identity_path: str = identity_path or str(settings.DATA_DIR / f"identity_{self.node_id}.id")
        os.makedirs(os.path.dirname(self.identity_path), exist_ok=True)

        # API server configuration
        self.api_host = api_host or settings.DEFAULT_API_HOST
        self.api_port = api_port or settings.DEFAULT_API_PORT
        self.run_api_server = run_api_server

        # --- Async Control and State ---
        self._shutdown_event = asyncio.Event() # Event to signal graceful shutdown
        self._main_loop_task: Optional[asyncio.Task] = None # Task for the main operational loop
        self._api_server_task: Optional[asyncio.Task] = None # Task for the Uvicorn API server
        self._leader_task_assignment_loop_task: Optional[asyncio.Task] = None # Task for leader's assignment logic
        self._leader_stale_task_check_loop_task: Optional[asyncio.Task] = None # Task for leader's timeout checks
        self.start_time = time.time() # Record node start time for uptime calculation

        # Dictionaries to track tasks assigned by this leader, awaiting worker action
        self._pending_worker_acks: Dict[str, float] = {} # task_id -> assignment_timestamp
        self._pending_worker_results: Dict[str, float] = {} # task_id -> ack_timestamp

        log.info(f"Initializing Akita Genesis Node: {self.node_name} (ID: {self.node_id}) in Cluster: {self.cluster_name} with Caps: {self.capabilities}")
        
        # --- Initialize Core Modules ---
        # Ensure database manager is ready (schema init is handled in StateManager.start)
        if not getattr(db_manager, '_initialized', False): 
            log.warning("DatabaseManager was not pre-initialized. This might indicate an issue.")
            # Attempting init here might be too late if other modules already tried to use it.
            # Relying on the check in StateManager.start() for schema readiness.

        # Initialize functional modules, passing necessary context
        self.ledger = Ledger(node_id=self.node_id, node_name=self.node_name, cluster_name=self.cluster_name)
        self.resource_monitor = ResourceMonitor(ledger=self.ledger, node_id=self.node_id, node_name=self.node_name)
        self.task_manager = TaskManager(ledger=self.ledger, node_id=self.node_id, node_name=self.node_name)
        self.communication_manager = CommunicationManager(identity_path=self.identity_path)
        self.state_manager = StateManager(
            node_id=self.node_id, node_name=self.node_name, cluster_name=self.cluster_name,
            ledger=self.ledger, resource_monitor=self.resource_monitor,
            communication_manager=self.communication_manager # Pass comms manager instance
        )
        self.discovery_manager = DiscoveryManager(
            node_id=self.node_id, node_name=self.node_name, cluster_name=self.cluster_name,
            communication_manager=self.communication_manager, state_manager=self.state_manager
        )
        
        # --- Setup Inter-module Callbacks & API ---
        self.resource_monitor.set_update_callback(self._handle_resource_update)
        self.state_manager.set_on_state_change_callback(self._handle_state_change)
        
        # Initialize FastAPI app with global API key dependency
        self.api_app = FastAPI(
            title="Akita Genesis Node API", version=settings.APP_VERSION,
            description=f"Control and status API for Akita Node {self.node_name} ({self.node_id})",
            dependencies=[Depends(get_api_key)] # Apply security to all routes by default
        )
        self._setup_api_routes() # Define API endpoints
        self._register_message_handlers() # Register handlers for incoming RNS messages

        log.info(f"Node {self.node_name} initialized. RNS Identity Path: {self.identity_path}")
        log.info(f"API server (secured: {bool(settings.VALID_API_KEYS)}) will run on: http://{self.api_host}:{self.api_port}")

    def _generate_node_id(self) -> str:
        """Generates a unique (short) ID for the node instance."""
        # Using a portion of UUID for a relatively unique but readable ID.
        return str(uuid.uuid4().hex[:12]) 

    def _register_message_handlers(self):
        """Registers handlers for different RNS message types with the CommunicationManager."""
        # Task Management Aspect Handlers
        self.communication_manager.register_message_handler( AppAspect.TASK_MANAGEMENT, MessageType.TASK_ASSIGN_TO_WORKER, self._handle_task_assign_to_worker )
        self.communication_manager.register_message_handler( AppAspect.TASK_MANAGEMENT, MessageType.TASK_ACKNOWLEDGED_BY_WORKER, self._handle_task_acknowledged_by_worker )
        self.communication_manager.register_message_handler( AppAspect.TASK_MANAGEMENT, MessageType.TASK_RESULT_FROM_WORKER, self._handle_task_result_from_worker )
        self.communication_manager.register_message_handler( AppAspect.TASK_MANAGEMENT, MessageType.TASK_SUBMIT_TO_LEADER, self._handle_task_submit_to_leader )
        # Control Aspect Handlers (for logs)
        self.communication_manager.register_message_handler( AppAspect.CONTROL, MessageType.FETCH_LOGS_REQUEST, self._handle_fetch_logs_request )
        log.info("RNS message handlers registered.")

    # --- Callback Handlers ---
    async def _handle_resource_update(self, resources: Dict[str, Any]):
        """Callback triggered by ResourceMonitor when local resources are updated."""
        log.debug(f"Local resources updated: CPU {resources.get('cpu',{}).get('percent_used')}%")
        # Update the StateManager with the latest resource info for this node
        await self.state_manager.update_local_node_resources(resources)
        # Update the NodeInfo used for discovery announcements
        if self.discovery_manager.this_node_info: # Check if initialized
            self.discovery_manager.this_node_info.resources = resources

    async def _handle_state_change(self, new_cluster_state: ClusterState):
        """Callback triggered by StateManager on significant cluster state changes."""
        log.info(f"Cluster state changed. Current Leader: {new_cluster_state.leader_id}. Total Nodes: {len(new_cluster_state.nodes)}")
        # Check if this node's leadership role has changed
        is_leader = await self.state_manager.is_current_node_leader()
        
        # Update the node info used for discovery announcements
        if self.discovery_manager.this_node_info: 
            self.discovery_manager.this_node_info.is_leader = is_leader
        
        # Start or stop leader-specific background tasks based on role
        if is_leader:
            # Check if leader duties need to be started (idempotent start)
            if not self._leader_task_assignment_loop_task or self._leader_task_assignment_loop_task.done():
                log.info(f"This node ({self.node_id}) IS NOW THE LEADER. Starting leader duties...")
                await self._start_leader_duties()
            else:
                log.info(f"This node ({self.node_id}) remains LEADER. Duties already running.")
        else:
            # Check if leader duties need to be stopped
            if self._leader_task_assignment_loop_task and not self._leader_task_assignment_loop_task.done():
                 log.info(f"This node ({self.node_id}) is now a FOLLOWER. Stopping leader duties...")
                 await self._stop_leader_duties()
            else:
                 log.info(f"This node ({self.node_id}) remains a FOLLOWER.")


    # --- Role-Specific Duty Management ---
    async def _start_leader_duties(self):
        """Starts background tasks specific to the leader role."""
        # Start task assignment loop if not already running
        if not self._leader_task_assignment_loop_task or self._leader_task_assignment_loop_task.done():
            self._leader_task_assignment_loop_task = asyncio.create_task(self._leader_task_assignment_loop())
        # Start stale task check loop if not already running
        if not self._leader_stale_task_check_loop_task or self._leader_stale_task_check_loop_task.done():
            self._leader_stale_task_check_loop_task = asyncio.create_task(self._leader_stale_task_check_loop())

    async def _stop_leader_duties(self):
        """Stops background tasks specific to the leader role."""
        tasks_to_stop = []
        # Cancel task assignment loop
        if self._leader_task_assignment_loop_task and not self._leader_task_assignment_loop_task.done():
            self._leader_task_assignment_loop_task.cancel()
            tasks_to_stop.append(self._leader_task_assignment_loop_task)
            log.info("Stopping leader task assignment loop.")
        # Cancel stale task check loop
        if self._leader_stale_task_check_loop_task and not self._leader_stale_task_check_loop_task.done():
            self._leader_stale_task_check_loop_task.cancel()
            tasks_to_stop.append(self._leader_stale_task_check_loop_task)
            log.info("Stopping leader stale task check loop.")
        
        # Wait for tasks to finish cancellation
        if tasks_to_stop:
            await asyncio.gather(*tasks_to_stop, return_exceptions=True)
            log.info("Leader duty loops stopped.")


    # --- Leader Logic (Task Assignment & Timeout Handling) ---
    async def _leader_task_assignment_loop(self):
        """Leader's main loop to find pending tasks and assign them to suitable workers."""
        log.info("Leader's task assignment loop started.")
        while not self._shutdown_event.is_set():
            # Ensure still leader before proceeding
            if not await self.state_manager.is_current_node_leader():
                log.info("No longer leader, stopping task assignment loop.")
                break
            
            try:
                # Fetch tasks ready for assignment (PENDING or ACCEPTED)
                tasks_to_assign = await self.task_manager.get_tasks_for_assignment(limit=5)
                if not tasks_to_assign:
                    # No tasks currently, wait before checking again
                    await asyncio.sleep(settings.TASK_PROCESSING_INTERVAL_S)
                    continue

                # Get currently available workers, shuffled for basic load distribution
                all_available_workers = await self.state_manager.get_available_workers()
                # Shuffle to distribute load somewhat if multiple workers are equally suitable
                random.shuffle(all_available_workers) 

                if not all_available_workers:
                    log.debug("No available workers currently.")
                    # Wait longer if no workers are available
                    await asyncio.sleep(settings.TASK_PROCESSING_INTERVAL_S) 
                    continue
                
                log.debug(f"Found {len(tasks_to_assign)} assignable tasks and {len(all_available_workers)} available workers.")
                assigned_in_batch = 0 # Track assignments in this iteration

                for task_obj in tasks_to_assign:
                    # Double-check leadership status and shutdown signal within the loop
                    if self._shutdown_event.is_set(): break
                    if not await self.state_manager.is_current_node_leader(): break 
                    # Avoid assigning more tasks than available workers in one go
                    if assigned_in_batch >= len(all_available_workers): break 

                    # --- Fault Tolerance: Check Max Attempts ---
                    if task_obj.execution_attempts >= settings.MAX_TASK_EXECUTION_ATTEMPTS:
                        log.warning(f"Task {task_obj.id} exceeded max attempts ({task_obj.execution_attempts}). Marking as FAILED.")
                        await self.task_manager.update_task_fields(
                            task_id=task_obj.id, new_status=TaskStatus.FAILED,
                            result={"error": f"Max execution attempts ({settings.MAX_TASK_EXECUTION_ATTEMPTS}) reached"}, 
                            actor_node_id=self.node_id
                        )
                        continue # Move to the next task

                    # --- Advanced Scheduling: Find Suitable Worker ---
                    # Extract required capabilities from task data, if specified
                    required_capabilities = task_obj.data.get("required_capabilities") 
                    
                    chosen_worker_info: Optional[StateNodeInfo] = None
                    # Iterate through shuffled workers to find one meeting criteria
                    # Use the already shuffled list `all_available_workers`
                    workers_to_consider = list(all_available_workers) # Work on a copy for this task
                    
                    while workers_to_consider:
                        worker = workers_to_consider.pop(0) # Get next worker from shuffled list
                        
                        # Check capabilities
                        if required_capabilities:
                            if not all(cap in worker.capabilities for cap in required_capabilities):
                                continue # Worker lacks required capabilities, try next worker

                        # Resource check (example: skip if CPU > 90%) - refine as needed
                        cpu_load = worker.resources.get("cpu", {}).get("percent_used", 0) if worker.resources else 0
                        if cpu_load > 90.0:
                             log.debug(f"Worker {worker.node_id} skipped for task {task_obj.id}: High CPU load ({cpu_load}%)")
                             continue # Try next worker

                        # Found a suitable worker
                        chosen_worker_info = worker
                        break # Assign to this worker

                    if not chosen_worker_info:
                        # No suitable worker found in the current pool for this specific task
                        log.debug(f"No suitable worker currently found for task {task_obj.id} (Caps: {required_capabilities}). Will retry later.")
                        # Mark as ACCEPTED if still PENDING, prevents immediate re-fetch unless requeued
                        if task_obj.status == TaskStatus.PENDING:
                             await self.task_manager.update_task_fields(task_obj.id, new_status=TaskStatus.ACCEPTED, actor_node_id=self.node_id)
                        continue # Try the next task in the batch

                    # --- Assign Task to Chosen Worker ---
                    log.info(f"Leader ({self.node_id}) assigning task {task_obj.id} (attempt {task_obj.execution_attempts + 1}) to worker {chosen_worker_info.node_id} ({chosen_worker_info.node_name})")
                    
                    # Update task state in DB to ASSIGNED, increment attempt count
                    # Pass the chosen worker's Akita ID
                    assign_success = await self.task_manager.update_task_fields(
                        task_id=task_obj.id, new_status=TaskStatus.ASSIGNED,
                        assigned_to_node_id=chosen_worker_info.node_id,
                        increment_attempts=True, 
                        actor_node_id=self.node_id
                    )
                    if not assign_success:
                        log.error(f"Failed to update task {task_obj.id} to ASSIGNED status in DB. Skipping assignment.")
                        continue # Try next task
                    
                    # Update worker's task count in StateManager (for load balancing info)
                    await self.state_manager.update_node_task_count(chosen_worker_info.node_id, 1)

                    # Send the task assignment message via RNS
                    if chosen_worker_info.address_hex:
                        # Send the full task dictionary as payload
                        sent = await self.communication_manager.send_message(
                            destination_rns_hash=RNS.hexrep_to_bytes(chosen_worker_info.address_hex),
                            aspect=AppAspect.TASK_MANAGEMENT, 
                            message_type=MessageType.TASK_ASSIGN_TO_WORKER,
                            payload=task_obj.to_dict(), 
                            source_akita_node_id=self.node_id
                        )
                        if sent:
                            # Track that we're waiting for this worker's ACK
                            self._pending_worker_acks[task_obj.id] = time.time()
                            log.info(f"Task {task_obj.id} assignment message sent to worker {chosen_worker_info.node_id}.")
                            assigned_in_batch += 1
                            # Remove worker from this batch's consideration to distribute load
                            # Note: This modifies the original list `all_available_workers` if not careful
                            # It's better to manage the available pool explicitly per batch.
                            # For simplicity now, assume shuffling helps distribute enough.
                            # A more robust way is needed for fair distribution across many tasks/workers.
                        else:
                            # If sending failed, re-queue the task and revert worker count
                            log.warning(f"Failed to send task {task_obj.id} assignment to worker {chosen_worker_info.node_id}. Re-queuing.")
                            await self.task_manager.re_queue_stale_assigned_task(task_obj.id, reason="failed_to_send_assignment")
                            await self.state_manager.update_node_task_count(chosen_worker_info.node_id, -1) 
                    else:
                        # If worker has no RNS address, re-queue and revert count
                        log.error(f"Worker {chosen_worker_info.node_id} has no RNS address. Cannot assign task {task_obj.id}.")
                        await self.task_manager.re_queue_stale_assigned_task(task_obj.id, reason="worker_no_rns_address")
                        await self.state_manager.update_node_task_count(chosen_worker_info.node_id, -1) 
                
                # Short sleep after processing a batch before fetching again
                await asyncio.sleep(0.2) 

            except asyncio.CancelledError:
                log.info("Leader's task assignment loop cancelled.")
                break # Exit loop cleanly on cancellation
            except Exception as e:
                log.error(f"Error in leader task assignment loop: {e}", exc_info=True)
                # Wait briefly before retrying after an unexpected error
                await asyncio.sleep(settings.TASK_PROCESSING_INTERVAL_S / 2)
        log.info("Leader's task assignment loop stopped.")

    async def _leader_stale_task_check_loop(self):
        """Leader periodically checks for tasks that timed out waiting for worker actions."""
        log.info("Leader's stale task check loop started.")
        while not self._shutdown_event.is_set():
            # Ensure still leader before proceeding
            if not await self.state_manager.is_current_node_leader():
                log.info("No longer leader, stopping stale task check loop.")
                break
            try:
                now = time.time()
                
                # Check for worker ACK timeouts
                # Iterate over a copy of keys to allow modification during iteration
                for task_id, assign_time in list(self._pending_worker_acks.items()):
                    if self._shutdown_event.is_set(): break # Exit early if shutting down
                    if (now - assign_time) > settings.WORKER_ACK_TIMEOUT_S:
                        log.warning(f"Task {task_id} timed out waiting for WORKER_ACK (assigned at {assign_time:.2f}). Re-queuing.")
                        # Fetch task details to get assigned worker ID
                        task_obj = await self.task_manager.get_task_by_id(task_id)
                        # Only re-queue if it's still in ASSIGNED state (hasn't been ACKed/failed otherwise)
                        if task_obj and task_obj.status == TaskStatus.ASSIGNED:
                             worker_id = task_obj.assigned_to_node_id
                             # --- Fault Tolerance: Requeue on Timeout ---
                             await self.task_manager.re_queue_stale_assigned_task(task_id, reason="worker_ack_timeout")
                             # Revert worker's task count if we re-queue
                             if worker_id: await self.state_manager.update_node_task_count(worker_id, -1)
                        # Remove from pending list regardless of whether re-queue succeeded or task state
                        if task_id in self._pending_worker_acks: # Check again in case of race condition
                            del self._pending_worker_acks[task_id] 

                # Check for worker processing timeouts
                # Iterate over a copy of keys
                for task_id, ack_time in list(self._pending_worker_results.items()):
                     if self._shutdown_event.is_set(): break # Exit early
                     if (now - ack_time) > settings.WORKER_PROCESSING_TIMEOUT_S:
                        log.warning(f"Task {task_id} timed out waiting for WORKER_RESULT (ack at {ack_time:.2f}). Marking FAILED.")
                        # Fetch task details
                        task_obj = await self.task_manager.get_task_by_id(task_id)
                        # Only fail if it's still waiting for result (WORKER_ACK or PROCESSING)
                        if task_obj and task_obj.status in [TaskStatus.WORKER_ACK, TaskStatus.PROCESSING]:
                            worker_id = task_obj.assigned_to_node_id
                            # --- Fault Tolerance: Mark Failed on Timeout ---
                            await self.task_manager.update_task_fields(
                                task_id, new_status=TaskStatus.FAILED, 
                                result={"error": f"Worker processing timeout after {settings.WORKER_PROCESSING_TIMEOUT_S}s"}, 
                                actor_node_id=self.node_id
                            )
                            # Revert worker's task count as it failed/timed out
                            if worker_id: await self.state_manager.update_node_task_count(worker_id, -1)
                        # Remove from pending list regardless
                        if task_id in self._pending_worker_results: # Check again
                            del self._pending_worker_results[task_id] 
                
                # Sleep for a reasonable interval before checking again
                await asyncio.sleep(settings.WORKER_ACK_TIMEOUT_S / 3) # Check reasonably often

            except asyncio.CancelledError:
                log.info("Leader's stale task check loop cancelled.")
                break # Exit loop cleanly
            except Exception as e:
                log.error(f"Error in leader stale task check loop: {e}", exc_info=True)
                # Wait briefly before retrying
                await asyncio.sleep(settings.WORKER_ACK_TIMEOUT_S / 2)
        log.info("Leader's stale task check loop stopped.")

    # --- Worker Logic (Task Handling) ---
    async def _handle_task_assign_to_worker(self, source_rns_hash: bytes, leader_akita_node_id: Optional[str], payload: Dict[str, Any]):
        """Handles a task assignment message received from the leader."""
        # Ignore if this node is somehow the leader
        if await self.state_manager.is_current_node_leader():
            log.warning(f"Leader node {self.node_id} received a task assignment meant for a worker. Ignoring.")
            return

        task_dict = payload
        task_id = task_dict.get("id")
        if not task_id:
            log.error(f"Received task assignment from {leader_akita_node_id} with missing task ID.")
            return
            
        log.info(f"Worker {self.node_id} received task assignment {task_id} from leader {leader_akita_node_id}.")

        # Find leader's RNS address to send ACK back
        leader_node_info = await self.state_manager.local_cluster_state.get_node(leader_akita_node_id or "") 
        if not leader_node_info or not leader_node_info.address_hex:
            log.error(f"Cannot ACK task {task_id}: Leader {leader_akita_node_id} RNS address unknown.")
            # Cannot proceed without being able to ACK
            return 

        # Send ACK to leader via RNS
        ack_sent = await self.communication_manager.send_message(
            destination_rns_hash=RNS.hexrep_to_bytes(leader_node_info.address_hex),
            aspect=AppAspect.TASK_MANAGEMENT, 
            message_type=MessageType.TASK_ACKNOWLEDGED_BY_WORKER,
            payload={"task_id": task_id, "worker_node_id": self.node_id, "status": "acknowledged"},
            source_akita_node_id=self.node_id
        )

        if ack_sent:
            log.info(f"Worker {self.node_id} acknowledged task {task_id} to leader {leader_akita_node_id}.")
            # Update local task count via StateManager (which handles persistence)
            # Do this *after* successfully ACKing
            await self.state_manager.update_node_task_count(self.node_id, 1) 
            # Start processing the task asynchronously
            # Pass necessary info: task details, leader ID, leader RNS address
            asyncio.create_task(self._worker_process_task(task_dict, leader_akita_node_id or "", leader_node_info.address_hex)) 
        else:
            log.error(f"Worker {self.node_id} failed to send ACK for task {task_id} to leader {leader_akita_node_id}.")
            # The leader should eventually time out waiting for the ACK and re-queue the task.

    async def _worker_process_task(self, task_dict: Dict[str, Any], leader_akita_id: str, leader_rns_hex: str):
        """Simulates a worker processing a task and sending the result back to the leader."""
        task_id = task_dict.get("id")
        task_data = task_dict.get("data", {})
        log.info(f"Worker {self.node_id} starting to 'process' task {task_id}. Data: {str(task_data)[:100]}")
        
        # Simulate work duration (can be made more realistic based on task data or capabilities)
        simulated_duration = random.uniform(1, 5) 
        await asyncio.sleep(simulated_duration) 

        # Simulate success or failure outcome
        if random.random() > 0.1: # 90% success rate now
            # Task completed successfully
            task_result_payload = {
                "task_id": task_id, 
                "status": str(TaskStatus.COMPLETED), # Report final status
                "result_data": { # Include actual results
                    "message": f"Task {task_id} processed successfully by {self.node_name} in {simulated_duration:.2f}s", 
                    "output": "example_output" # Replace with actual task output
                },
                "worker_node_id": self.node_id # Identify which worker completed it
            }
            log.info(f"Worker {self.node_id} completed task {task_id}.")
        else:
            # Simulate failure
            task_result_payload = {
                "task_id": task_id, 
                "status": str(TaskStatus.FAILED), # Report final status
                "error_message": f"Simulated failure processing task {task_id} on {self.node_name}",
                "worker_node_id": self.node_id
            }
            log.warning(f"Worker {self.node_id} failed task {task_id} (simulated).")

        # Send the result message back to the leader via RNS
        result_sent = await self.communication_manager.send_message(
            destination_rns_hash=RNS.hexrep_to_bytes(leader_rns_hex),
            aspect=AppAspect.TASK_MANAGEMENT, 
            message_type=MessageType.TASK_RESULT_FROM_WORKER,
            payload=task_result_payload,
            source_akita_node_id=self.node_id
        )

        if result_sent:
            log.info(f"Worker {self.node_id} sent result for task {task_id} to leader {leader_akita_id}.")
            # Update local task count via StateManager *after* successfully sending result
            await self.state_manager.update_node_task_count(self.node_id, -1) 
        else:
            # If sending the result fails, the leader should eventually time out the task.
            # The worker's task count might remain inflated temporarily.
            log.error(f"Worker {self.node_id} FAILED to send result for task {task_id} to leader {leader_akita_id}.")

    # --- Leader Message Handlers (Responding to Worker Actions) ---
    async def _handle_task_acknowledged_by_worker(self, source_rns_hash: bytes, worker_akita_node_id: Optional[str], payload: Dict[str, Any]):
        """Handles ACK message from a worker (this node must be the leader)."""
        # Ignore if not leader
        if not await self.state_manager.is_current_node_leader(): return

        task_id = payload.get("task_id")
        if not task_id or not worker_akita_node_id: 
            log.warning(f"Received invalid WORKER_ACK payload: {payload}")
            return

        log.info(f"Leader received WORKER_ACK for task {task_id} from worker {worker_akita_node_id}.")
        
        # Check if we were actually waiting for this ACK
        if task_id in self._pending_worker_acks:
            # Stop tracking ACK timeout, start tracking processing timeout
            del self._pending_worker_acks[task_id] 
            self._pending_worker_results[task_id] = time.time() 
            
            # Update task status in DB to reflect acknowledgment
            await self.task_manager.update_task_fields(
                task_id, 
                new_status=TaskStatus.WORKER_ACK, # Mark as acknowledged
                actor_node_id=self.node_id # Leader is actor recording the ACK
            )
        else:
            # Received an ACK we weren't waiting for
            log.warning(f"Received unexpected WORKER_ACK for task {task_id} from {worker_akita_node_id} (not in pending_acks or already acked/failed).")

    async def _handle_task_result_from_worker(self, source_rns_hash: bytes, worker_akita_node_id: Optional[str], payload: Dict[str, Any]):
        """Handles result message from a worker (this node must be the leader)."""
         # Ignore if not leader
        if not await self.state_manager.is_current_node_leader(): return

        task_id = payload.get("task_id")
        status_str = payload.get("status") # Status reported by worker (COMPLETED or FAILED)
        result_data = payload.get("result_data") # Payload if successful
        error_message = payload.get("error_message") # Error details if failed

        if not task_id or not worker_akita_node_id or not status_str:
            log.warning(f"Received invalid TASK_RESULT payload: {payload}")
            return

        log.info(f"Leader received TASK_RESULT for task {task_id} from worker {worker_akita_node_id}. Reported Status: {status_str}")

        # Check if we were waiting for this result
        if task_id in self._pending_worker_results:
            del self._pending_worker_results[task_id] # Stop tracking processing timeout
        else:
            log.warning(f"Received result for task {task_id} which was not in pending_results (might be late, duplicate, or already failed/timed out). Processing anyway.")

        # Determine final status based on worker report
        final_status = TaskStatus.COMPLETED if status_str == str(TaskStatus.COMPLETED) else TaskStatus.FAILED
        # Prepare final result payload for DB (either success data or error info)
        final_result_payload = result_data if final_status == TaskStatus.COMPLETED else {"error": error_message or "Worker reported failure"}
        
        # Update task status and result in DB
        await self.task_manager.update_task_fields(
            task_id, 
            new_status=final_status, 
            result=final_result_payload, 
            actor_node_id=self.node_id # Leader is actor recording the result
        )
        
        # Decrement worker's task count in StateManager as it finished
        await self.state_manager.update_node_task_count(worker_akita_node_id, -1)

    async def _handle_task_submit_to_leader(self, source_rns_hash: bytes, source_akita_node_id: Optional[str], payload: Dict[str, Any]):
        """Handles tasks submitted directly via RNS to this node (if leader)."""
        # Ignore if not leader
        if not await self.state_manager.is_current_node_leader():
            log.warning(f"Node {self.node_id} received TASK_SUBMIT_TO_LEADER but is not leader. Ignoring task from {source_akita_node_id}.")
            return

        log.info(f"Leader received task submission via RNS from {source_akita_node_id or 'Unknown'}: {str(payload.get('task_data'))[:100]}")
        task_data = payload.get("task_data")
        priority = payload.get("priority", settings.DEFAULT_TASK_PRIORITY)
        task_id_in_payload = payload.get("task_id") # Allow client-provided ID

        if not task_data:
            log.error("Received RNS task submission with no task_data.")
            return

        # Submit to local task manager (will be PENDING)
        await self.task_manager.submit_task_to_system(
            task_data=task_data,
            priority=priority,
            task_id=task_id_in_payload, 
            submitted_by_node_id=source_akita_node_id # Track original submitter
        )

    async def _handle_fetch_logs_request(self, source_rns_hash: bytes, requester_akita_node_id: Optional[str], payload: Dict[str, Any]):
        """Handles a request for logs received via RNS (conceptual)."""
        # This remains a placeholder as direct RNS request-response for CLI is complex.
        log.info(f"Node {self.node_id} received log fetch request via RNS from {requester_akita_node_id} / {RNS.prettyhexrep(source_rns_hash)}.")
        # Prepare dummy logs
        dummy_logs = [ f"{time.time()}: RNS Log Placeholder 1", f"{time.time()}: RNS Log Placeholder 2", ]
        log_payload = {"node_id": self.node_id, "logs": dummy_logs}
        log.warning(f"Log fetching via RNS is conceptual. Would need reply mechanism. Payload: {log_payload}")
        # Cannot easily reply without a Link object from the request or a predefined reply path.

    # --- Node Lifecycle & API (Setup/Teardown) ---
    async def start(self):
        """Starts the Akita Genesis node and all its components."""
        log.info(f"Starting Akita Genesis Node: {self.node_name}...")
        # Record node start event in ledger
        await self.ledger.record_event(EventType.NODE_START, {"version": settings.APP_VERSION, "node_id": self.node_id, "node_name": self.node_name})

        # Start communication layer first
        if not await self.communication_manager.start():
            log.error("Failed to start Communication Manager. Node cannot start.")
            await self.ledger.record_event(EventType.NODE_STOP, {"reason": "Comms failed", "node_id": self.node_id})
            return False
        
        # Start other modules
        await self.resource_monitor.start_monitoring()
        # StateManager start now waits for DB init internally
        await self.state_manager.start() 
        
        # Ensure this node's capabilities are set in its state info after StateManager start
        # This needs to access the potentially updated NodeInfo object within StateManager's cache
        async with self.state_manager.local_cluster_state._lock: # Access state safely
            my_node_info = self.state_manager.local_cluster_state.nodes.get(self.node_id)
            if my_node_info:
                my_node_info.capabilities = self.capabilities
                # Ensure RNS address is populated if available now
                my_node_info.address_hex = self.communication_manager.get_node_identity_hex() or my_node_info.address_hex
                # No need to call update_node again, modify in place under lock
            else:
                # This case should ideally not happen if StateManager.start correctly initializes self
                log.error("Own NodeInfo not found in local_cluster_state after StateManager start. Critical error.")
                # Handle this critical failure? Shutdown?
                return False # Prevent further startup
        # Persist the updated self info outside the lock
        if my_node_info:
             await self.state_manager._persist_node_info(my_node_info) 

        # Update discovery manager's view of self now that state is initialized
        if self.discovery_manager.this_node_info:
            self.discovery_manager.this_node_info.capabilities = self.capabilities
            self.discovery_manager.this_node_info.address_hex = self.communication_manager.get_node_identity_hex()
        else:
            # If discovery manager initializes its_node_info later, it needs to fetch caps
            log.warning("Discovery manager 'this_node_info' not set at node start, capabilities might not be announced initially.")
            # Consider initializing it here if needed:
            # self.discovery_manager.this_node_info = my_node_info.model_copy() if my_node_info else None
            
        await self.discovery_manager.start() # Start announcing and listening

        # Start API server if enabled
        if self.run_api_server:
            uvicorn_log_level = settings.LOG_LEVEL.lower()
            if uvicorn_log_level == "trace": uvicorn_log_level = "debug" 

            config = uvicorn.Config(self.api_app, host=self.api_host, port=self.api_port, log_level=uvicorn_log_level)
            # Store server instance if possible for graceful shutdown
            server = uvicorn.Server(config) 
            # Keep reference for potential shutdown signalling
            self._uvicorn_server_instance = server # type: ignore 
            self._api_server_task = asyncio.create_task(server.serve())
            log.info(f"HTTP API server started on http://{self.api_host}:{self.api_port}")

        log.info(f"Node {self.node_name} (RNS: {self.communication_manager.get_node_identity_hex()}) started successfully.")
        # Start the main operational loop
        self._main_loop_task = asyncio.create_task(self._main_loop())
        return True

    async def _main_loop(self):
        """Main operational loop for periodic checks or actions."""
        try:
            while not self._shutdown_event.is_set():
                # Update discovery info periodically with current state
                # Access state safely
                my_node_info_state = await self.state_manager.local_cluster_state.get_node(self.node_id)
                if my_node_info_state and self.discovery_manager.this_node_info:
                     self.discovery_manager.this_node_info.status = my_node_info_state.status
                     self.discovery_manager.this_node_info.is_leader = my_node_info_state.is_leader
                     self.discovery_manager.this_node_info.current_task_count = my_node_info_state.current_task_count
                # Other periodic checks can go here
                await asyncio.sleep(15) # Interval for checks
        except asyncio.CancelledError:
            log.info("Main node loop cancelled.")
        finally:
            log.info("Main node loop finished.")

    async def stop(self):
        """Gracefully shuts down the Akita Genesis node and its components."""
        if self._shutdown_event.is_set():
            log.info(f"Node {self.node_name} shutdown already in progress or completed.")
            return

        log.info(f"Stopping Akita Genesis Node: {self.node_name}...")
        self._shutdown_event.set() # Signal all loops and tasks to stop

        # Stop leader duties first
        await self._stop_leader_duties() 

        # Cancel main tasks
        tasks_to_cancel = [self._main_loop_task, self._api_server_task]
        
        # Attempt graceful shutdown of Uvicorn server
        if hasattr(self, '_uvicorn_server_instance') and self._uvicorn_server_instance:
            log.info("Attempting graceful shutdown of API server...")
            self._uvicorn_server_instance.should_exit = True # type: ignore
            # Give it a moment before cancelling the task
            await asyncio.sleep(0.5) 
        
        # Cancel tasks
        for task in tasks_to_cancel:
            if task and not task.done():
                task.cancel()
        
        # Wait for main tasks to finish cancellation
        await asyncio.gather(*[t for t in tasks_to_cancel if t], return_exceptions=True)
        log.info("Main loop and API server tasks cancelled/stopped.")

        # Stop modules in reverse order of dependency/start
        # Order: Discovery -> State -> Resources -> Communication
        if self.discovery_manager: await self.discovery_manager.stop()
        if self.state_manager: await self.state_manager.stop() # Persists final state
        if self.resource_monitor: await self.resource_monitor.stop_monitoring()
        if self.communication_manager: await self.communication_manager.stop() # Stops RNS announces

        # Record final stop event
        await self.ledger.record_event(EventType.NODE_STOP, {"reason": "Graceful shutdown command", "node_id": self.node_id})
        
        # Database connection is closed via atexit handler in persistence.py
        log.info(f"Node {self.node_name} stopped successfully.")

    def _setup_api_routes(self):
        """Defines FastAPI routes, applying the API key dependency."""
        api_key_dependency = Depends(get_api_key)

        # Apply dependency to all routes defined below
        # Use unique function names for API route handlers
        @self.api_app.get("/", tags=["General"], dependencies=[api_key_dependency])
        async def get_root_api_route(): 
             return { "message": f"Welcome to Akita Genesis Node: {self.node_name}", "node_id": self.node_id, "node_capabilities": self.capabilities, "cluster_name": self.cluster_name, "rns_identity_hex": self.communication_manager.get_node_identity_hex(), "api_docs": "/docs" }

        @self.api_app.get("/status", tags=["Status"], dependencies=[api_key_dependency])
        async def get_node_status_api_route(): 
            leader_id = await self.state_manager.get_current_leader_id(); is_leader = self.node_id == leader_id; local_node_info = await self.state_manager.local_cluster_state.get_node(self.node_id)
            return { "node_id": self.node_id, "node_name": self.node_name, "cluster_name": self.cluster_name, "capabilities": self.capabilities, "rns_identity_hex": self.communication_manager.get_node_identity_hex(), "status": str(local_node_info.status) if local_node_info else "UNKNOWN", "is_leader": is_leader, "current_leader_id": leader_id, "current_task_count": local_node_info.current_task_count if local_node_info else 0, "uptime_seconds": time.time() - self.start_time, "resources": self.resource_monitor.current_resources, "task_counts": await self.task_manager.count_tasks_by_status(), }

        @self.api_app.get("/cluster/status", tags=["Cluster"], dependencies=[api_key_dependency])
        async def get_cluster_status_api_route():
             nodes_info = await self.state_manager.get_cluster_nodes_info(); return { "cluster_name": self.cluster_name, "current_leader_id": await self.state_manager.get_current_leader_id(), "total_nodes_known": len(nodes_info), "online_nodes_count": len([n for n in nodes_info if n.status == StateNodeStatus.ONLINE]), "nodes": [node.model_dump() for node in nodes_info] }

        @self.api_app.post("/tasks/submit", status_code=202, tags=["Tasks"], dependencies=[api_key_dependency])
        async def submit_task_api_route(task_data: Dict[str, Any] = Body(...), priority: Optional[int] = Body(default=settings.DEFAULT_TASK_PRIORITY)):
            log.info(f"API: Received task submission: {str(task_data)[:100]}, priority: {priority}")
            is_leader = await self.state_manager.is_current_node_leader()
            if not is_leader:
                # Attempt to forward to leader if not leader
                leader_node = await self.state_manager.local_cluster_state.get_leader()
                if leader_node and leader_node.address_hex:
                    log.info(f"Forwarding task to leader {leader_node.node_id} at {leader_node.address_hex}")
                    task_id_for_payload = str(uuid.uuid4()) # Generate ID for tracking
                    payload_to_leader = { "task_id": task_id_for_payload, "task_data": task_data, "priority": priority, "original_submitter_node_id": self.node_id }
                    sent = await self.communication_manager.send_message( destination_rns_hash=RNS.hexrep_to_bytes(leader_node.address_hex), aspect=AppAspect.TASK_MANAGEMENT, message_type=MessageType.TASK_SUBMIT_TO_LEADER, payload=payload_to_leader, source_akita_node_id=self.node_id )
                    if sent: return {"message": "Task forwarded to leader", "task_id": task_id_for_payload, "status": "forwarded_to_leader"}
                    else: log.error("Failed to forward task to leader. Submitting locally as fallback.")
                else: log.warning("No leader found or leader has no RNS address. Submitting task locally.")
            # Local submission if leader or forwarding failed
            task = await self.task_manager.submit_task_to_system( task_data, priority, submitted_by_node_id=self.node_id ) # Submitted via this node's API
            if task: return {"message": "Task submitted to system", "task_id": task.id, "status": str(task.status)}
            else: raise HTTPException(status_code=500, detail="Failed to submit task to system")

        @self.api_app.get("/tasks/{task_id}", tags=["Tasks"], dependencies=[api_key_dependency])
        async def get_task_status_api_route(task_id: str):
             task = await self.task_manager.get_task_by_id(task_id);
             if task: return task.to_dict()
             raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
            
        @self.api_app.get("/tasks", tags=["Tasks"], dependencies=[api_key_dependency])
        async def list_tasks_api_route(status: Optional[str] = None, limit: int = 20):
             # Simplified list logic (as before)
            if status:
                try: task_status_enum = TaskStatus(status.lower()); tasks_dict = await self.task_manager.count_tasks_by_status(task_status_enum); return {"status_filter": str(task_status_enum), "tasks_found_count": tasks_dict.get(str(task_status_enum),0) , "detail": "Listing actual tasks by arbitrary status not fully implemented yet."}
                except ValueError: raise HTTPException(status_code=400, detail=f"Invalid task status: {status}")
            else: 
                # Fetch assignable tasks as an example list
                pending_tasks = await self.task_manager.get_tasks_for_assignment(limit=limit); 
                return {"tasks": [t.to_dict() for t in pending_tasks]}

        @self.api_app.get("/ledger", tags=["Ledger"], dependencies=[api_key_dependency])
        async def get_ledger_entries_api_route(limit: int = 20, offset: int = 0, event_type: Optional[str] = None):
             events = await self.ledger.get_events(limit=limit, offset=offset, event_type=event_type); return {"events": events}

        @self.api_app.get("/logs", tags=["Control"], dependencies=[api_key_dependency]) 
        async def get_node_logs_api_route(limit: int = 100, level: Optional[str] = None):
            # Placeholder log fetching - requires actual implementation
            log.info(f"API: Log fetch request received. Limit: {limit}, Level: {level}")
            # TODO: Implement log fetching from file or buffer
            dummy_logs = [ f"{time.strftime('%Y-%m-%d %H:%M:%S')}: API Log: Entry 1 [INFO]", f"{time.strftime('%Y-%m-%d %H:%M:%S')}: API Log: Entry 2 [DEBUG] (CPU: {self.resource_monitor.current_resources.get('cpu',{}).get('percent_used')}%)", ]
            return {"node_id": self.node_id, "logs": dummy_logs, "count": len(dummy_logs), "filter_level": level}

        @self.api_app.post("/shutdown", tags=["General"], status_code=202, dependencies=[api_key_dependency])
        async def shutdown_node_api_route():
             log.info("API: Received shutdown command."); asyncio.create_task(self.stop()); return {"message": "Node shutdown initiated."}


# --- run_node_async Helper (for starting node with signal handling) ---
async def run_node_async(node_instance: AkitaGenesisNode):
    """Runs the node's start method and handles graceful shutdown signals."""
    loop = asyncio.get_event_loop()
    
    # Function to initiate shutdown
    async def initiate_shutdown(sig):
        # Check if shutdown is already in progress
        if node_instance._shutdown_event.is_set():
            log.info(f"Shutdown already initiated for signal {sig.name}.")
            return
        log.info(f"Received signal {sig.name}. Initiating graceful shutdown...")
        await node_instance.stop()
        # Give tasks a moment to clean up after stop() returns
        await asyncio.sleep(0.5) 

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Ensure signal handler is added only once if run multiple times
        # loop.remove_signal_handler(sig) # Might be needed if loop persists across runs
        try:
            loop.add_signal_handler( sig, lambda s=sig: asyncio.create_task(initiate_shutdown(s)) )
        except ValueError: # Handler might already be added from a previous run in interactive session
            log.debug(f"Signal handler for {sig.name} likely already added.")
        except NotImplementedError: # Windows might not support add_signal_handler for all signals
             log.warning(f"Cannot add signal handler for {sig.name} on this platform.")


    # Start the node
    start_success = await node_instance.start()
    if start_success:
        # Keep running until shutdown event is set
        await node_instance._shutdown_event.wait()
        log.info("Shutdown event received by run_node_async, node should have stopped.")
    else:
        log.error("Node failed to start.")

    # Final cleanup check (stop() should handle most of this)
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if tasks:
        log.info(f"Performing final check for {len(tasks)} outstanding tasks...")
        # Give tasks a brief moment more
        await asyncio.sleep(0.1) 
        remaining_tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
        if remaining_tasks:
             log.warning(f"Force cancelling {len(remaining_tasks)} tasks that did not exit gracefully after shutdown.")
             for task in remaining_tasks:
                 task.cancel()
             # Wait for cancellations to complete
             await asyncio.gather(*remaining_tasks, return_exceptions=True)
             log.info("Outstanding tasks cancelled.")
        else:
             log.info("All tasks appear finished.")

# --- Main Execution Block (for direct testing) ---
if __name__ == '__main__':
    print("Starting Akita Genesis Node directly (for testing)...")
    # Example: Create a node instance with capabilities
    test_node = AkitaGenesisNode( 
        node_name="TestNodeAdv", cluster_name="AdvCluster", 
        identity_path=str(settings.DATA_DIR / "identity_testnode_adv.id"), 
        api_port=8000, 
        capabilities=["general", "fast_cpu"] # Example capabilities
    )
    try:
        # Run the node using the helper function
        asyncio.run(run_node_async(test_node))
    except KeyboardInterrupt:
        # This exception is usually caught by the signal handler now
        log.info("KeyboardInterrupt caught in main block (graceful shutdown should be handled by signal handler).")
    except Exception as e:
         log.critical(f"Unhandled exception in main execution block: {e}", exc_info=True)
    finally:
        log.info("Node process exiting from main block.")

