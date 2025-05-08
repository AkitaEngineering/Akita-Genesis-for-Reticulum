# akita_genesis/modules/tasks.py
import json
import time
import uuid
from enum import Enum
from typing import List, Dict, Any, Optional, Union, Tuple, Awaitable # Added Awaitable
import asyncio # For asyncio.wrap_future

# Use absolute imports within the package
from akita_genesis.utils.logger import setup_logger
from akita_genesis.config.settings import settings
from akita_genesis.modules.persistence import db_manager 
from akita_genesis.modules.ledger import Ledger, EventType 

log = setup_logger(__name__)

class TaskStatus(Enum):
    """Enumeration for task statuses."""
    PENDING = "pending"         # Task submitted, awaiting leader acceptance
    ACCEPTED = "accepted"       # Leader acknowledged, queued for assignment
    ASSIGNED = "assigned"       # Leader assigned task to a specific worker node
    WORKER_ACK = "worker_ack"   # Worker acknowledged receipt of the task assignment
    PROCESSING = "processing"   # Worker node has started processing the task
    COMPLETED = "completed"     # Task finished successfully (result from worker)
    FAILED = "failed"           # Task execution failed (error from worker or leader timeout)
    TIMEOUT = "timeout"         # Task exceeded execution timeout (either leader waiting for worker, or worker itself)
    CANCELLED = "cancelled"     # Task was cancelled

    def __str__(self):
        return self.value

class Task:
    """Represents a single task in the system."""
    def __init__(
        self,
        task_id: str,
        data: Dict[str, Any],
        priority: int = settings.DEFAULT_TASK_PRIORITY,
        status: TaskStatus = TaskStatus.PENDING,
        submit_time: Optional[float] = None,
        assigned_to_node_id: Optional[str] = None, # Akita ID of the assigned worker
        result: Optional[Dict[str, Any]] = None,
        last_updated: Optional[float] = None,
        submitted_by_node_id: Optional[str] = None, # Akita ID of the node that originally submitted
        execution_attempts: int = 0
    ):
        self.id = task_id
        self.data = data
        self.priority = priority
        self.status = status
        self.submit_time = submit_time or time.time()
        self.assigned_to_node_id = assigned_to_node_id
        self.result = result # Can store success data or error details
        self.last_updated = last_updated or time.time()
        self.submitted_by_node_id = submitted_by_node_id
        self.execution_attempts = execution_attempts


    def to_dict(self) -> Dict[str, Any]:
        """Serializes the task object to a dictionary."""
        return {
            "id": self.id,
            "data": self.data,
            "priority": self.priority,
            "status": str(self.status),
            "submit_time": self.submit_time,
            "assigned_to_node_id": self.assigned_to_node_id,
            "result": self.result,
            "last_updated": self.last_updated,
            "submitted_by_node_id": self.submitted_by_node_id,
            "execution_attempts": self.execution_attempts,
        }

    @classmethod
    def from_dict(cls, task_dict: Dict[str, Any]) -> 'Task':
        """Deserializes a task from a dictionary."""
        return cls(
            task_id=task_dict["id"],
            data=task_dict["data"],
            priority=task_dict.get("priority", settings.DEFAULT_TASK_PRIORITY),
            status=TaskStatus(task_dict.get("status", "pending")),
            submit_time=task_dict.get("submit_time"),
            assigned_to_node_id=task_dict.get("assigned_to_node_id"),
            result=task_dict.get("result"),
            last_updated=task_dict.get("last_updated"),
            submitted_by_node_id=task_dict.get("submitted_by_node_id"),
            execution_attempts=task_dict.get("execution_attempts", 0)
        )

    @classmethod
    def from_db_row(cls, row: Any) -> 'Task': # sqlite3.Row or similar
        """Creates a Task object from a database row."""
        # Assuming 'execution_attempts' and 'submitted_by_node_id' columns exist in the tasks table schema
        return cls(
            task_id=row["id"],
            data=json.loads(row["data"]) if row["data"] else {},
            priority=row["priority"],
            status=TaskStatus(row["status"]),
            submit_time=row["submit_time"],
            assigned_to_node_id=row["assigned_to_node_id"],
            result=json.loads(row["result"]) if row["result"] else None,
            last_updated=row["last_updated"],
            submitted_by_node_id=row.get("submitted_by_node_id"), # Use .get for potential missing column
            execution_attempts=row.get("execution_attempts", 0) # Use .get with default
        )


class TaskManager:
    """
    Manages tasks within the Akita Genesis system.
    Handles submission, retrieval, status updates, and persistence of tasks.
    """

    def __init__(self, ledger: Ledger, node_id: Optional[str] = None, node_name: Optional[str] = None):
        """
        Initializes the TaskManager.

        Args:
            ledger: An instance of the Ledger for recording task-related events.
            node_id: The Akita ID of the current node (used for context if tasks are submitted by this node).
            node_name: The name of the current node.
        """
        self.db = db_manager # Use global DB manager instance
        self.ledger = ledger
        self.current_node_id = node_id # Akita ID of this node
        self.current_node_name = node_name
        log.info("TaskManager initialized.")

    def set_node_context(self, node_id: str, node_name: str):
        """Sets the current node context."""
        self.current_node_id = node_id
        self.current_node_name = node_name

    async def submit_task_to_system( # Renamed to reflect it's entering the system via this node
        self,
        task_data: Dict[str, Any],
        priority: int = settings.DEFAULT_TASK_PRIORITY,
        task_id: Optional[str] = None, 
        submitted_by_node_id: Optional[str] = None # Akita ID of original submitter
    ) -> Optional[Task]:
        """
        Submits a new task to the system database with PENDING status.
        A client or node calls this. The leader will later pick it up.

        Args:
            task_data: The payload dictionary for the task.
            priority: The task priority (lower value is higher priority).
            task_id: Optional pre-defined UUID for the task.
            submitted_by_node_id: Akita ID of the node that originally submitted this task.

        Returns:
            The created Task object if successful, None otherwise.
        """
        if not task_id:
            task_id = str(uuid.uuid4()) # Generate a unique ID if not provided
        
        current_time = time.time()
        # Ensure submitted_by_node_id is set, defaults to current node if not provided
        final_submitted_by = submitted_by_node_id or self.current_node_id

        # Create Task object
        task = Task(
            task_id=task_id,
            data=task_data,
            priority=priority,
            status=TaskStatus.PENDING, # Initial status upon submission
            submit_time=current_time,
            last_updated=current_time,
            submitted_by_node_id=final_submitted_by,
            execution_attempts=0 # Starts with zero attempts
        )

        # SQL to insert the new task record
        # Assumes DB schema includes submitted_by_node_id and execution_attempts
        sql = """
            INSERT INTO tasks (id, submit_time, priority, data, status, last_updated, submitted_by_node_id, execution_attempts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            task.id, task.submit_time, task.priority, json.dumps(task.data), # Serialize data to JSON
            str(task.status), task.last_updated, task.submitted_by_node_id, task.execution_attempts
        )

        try:
            # Execute DB insert via the queued DatabaseManager
            future = self.db.execute(sql, params)
            await asyncio.wrap_future(future) # Wait for DB operation to complete
            
            log.info(f"Task {task.id} submitted to system by {final_submitted_by}. Status: PENDING. Data: {str(task_data)[:100]}")

            # Record the submission event in the ledger
            await self.ledger.record_event(
                event_type=EventType.TASK_SUBMITTED,
                details={"task_id": task.id, "priority": priority, "submitted_by": final_submitted_by},
                source_node_id=final_submitted_by # Event source is the submitter
            )
            return task # Return the created Task object
        except Exception as e:
            log.error(f"Failed to submit task {task.id} to DB: {e}", exc_info=True)
            # Record error event if DB submission fails
            await self.ledger.record_event(
                event_type=EventType.ERROR_EVENT,
                details={"error": f"Task DB submission failed for {task.id}: {e}"},
                source_node_id=self.current_node_id # Error occurred on this node
            )
            return None # Indicate failure

    async def get_task_by_id(self, task_id: str) -> Optional[Task]:
        """Retrieves a specific task from the database by its ID."""
        # Ensure all fields defined in Task class are selected
        sql = """SELECT id, submit_time, priority, data, status, assigned_to_node_id, 
                        result, last_updated, submitted_by_node_id, execution_attempts 
                 FROM tasks WHERE id = ?"""
        try:
            future = self.db.execute(sql, (task_id,))
            rows = await asyncio.wrap_future(future) # Await DB result
            # If a row was found, create and return Task object
            if rows and rows[0]:
                return Task.from_db_row(rows[0])
            # Return None if task ID not found
            return None
        except Exception as e:
            log.error(f"Failed to retrieve task {task_id}: {e}", exc_info=True)
            return None

    async def update_task_fields(
        self,
        task_id: str,
        new_status: Optional[TaskStatus] = None,
        assigned_to_node_id: Optional[str] = None, # Use None to clear, or provide Akita ID
        clear_assignment: bool = False, # Explicit flag to set assigned_to_node_id to NULL
        result: Optional[Dict[str, Any]] = None, # Task result (success data or error info)
        increment_attempts: bool = False, # Flag to increment execution_attempts counter
        actor_node_id: Optional[str] = None # Akita ID of the node performing this update
    ) -> bool:
        """
        Updates specific fields of a task in the database. More flexible than just status updates.
        Handles recording relevant ledger events based on status changes.

        Args:
            task_id: The ID of the task to update.
            new_status: The new TaskStatus enum member, if status is changing.
            assigned_to_node_id: The Akita ID of the worker node if assigning.
            clear_assignment: If True, sets assigned_to_node_id to NULL in the DB.
            result: The result dictionary to store (for COMPLETED or FAILED tasks).
            increment_attempts: If True, increments the execution_attempts counter.
            actor_node_id: The Akita ID of the node initiating this update (for ledger).

        Returns:
            True if the update was successful (at least one row affected), False otherwise.
        """
        # First, get the current state of the task to compare status if needed
        task = await self.get_task_by_id(task_id)
        if not task:
            log.warning(f"Task {task_id} not found for update.")
            return False

        current_time = time.time()
        update_clauses: List[str] = ["last_updated = ?"] # Always update last_updated timestamp
        params: List[Any] = [current_time]
        
        original_status = task.status # Store original status for comparison and logging
        final_status = new_status or original_status # Determine the status after update

        # Add clauses and params for fields being updated
        if new_status:
            update_clauses.append("status = ?")
            params.append(str(new_status))
        
        if clear_assignment:
            # Explicitly set assignment to NULL
            update_clauses.append("assigned_to_node_id = NULL")
        elif assigned_to_node_id is not None:
            # Set assignment to the provided worker ID
            update_clauses.append("assigned_to_node_id = ?")
            params.append(assigned_to_node_id)
        
        if result is not None: # Allows storing empty dict {} as result
            # Store result as JSON string
            update_clauses.append("result = ?")
            params.append(json.dumps(result))
        
        if increment_attempts:
            # Increment the attempt counter in the DB
            update_clauses.append("execution_attempts = execution_attempts + 1")
            # Also update the local object's count for ledger event details
            task.execution_attempts += 1 

        # Check if any actual update is being performed besides timestamp
        if len(update_clauses) <= 1: 
            log.debug(f"No fields to update for task {task_id} beyond last_updated.")
            # Optionally, still execute the update for just last_updated if desired
            # return True 

        # Add task ID for the WHERE clause
        params.append(task_id) 
        # Construct the final SQL UPDATE statement
        sql = f"UPDATE tasks SET {', '.join(update_clauses)} WHERE id = ?"
        
        try:
            # Execute the update via DatabaseManager
            future = self.db.execute(sql, tuple(params))
            changed_rows = await asyncio.wrap_future(future) # Await result (row count)

            # Check if the update affected any rows
            if changed_rows > 0:
                log.info(f"Task {task_id} updated. Status: {original_status}->{final_status}, Assigned: {assigned_to_node_id if assigned_to_node_id else task.assigned_to_node_id}, Attempts: {task.execution_attempts}")
                
                # --- Ledger Event Logic ---
                # Map task statuses to corresponding ledger event types
                event_map = {
                    TaskStatus.ACCEPTED: EventType.TASK_ACCEPTED,
                    TaskStatus.ASSIGNED: EventType.TASK_ASSIGNED,
                    TaskStatus.WORKER_ACK: EventType.TASK_STARTED, # Worker ack implies start
                    TaskStatus.PROCESSING: EventType.TASK_STARTED, # Or a more generic status update event
                    TaskStatus.COMPLETED: EventType.TASK_COMPLETED,
                    TaskStatus.FAILED: EventType.TASK_FAILED,
                    TaskStatus.TIMEOUT: EventType.TASK_TIMEOUT, # If status explicitly set to TIMEOUT
                }
                ledger_event_type = event_map.get(final_status)
                
                # Record ledger event only if the status actually changed
                if ledger_event_type and (new_status and new_status != original_status): 
                    ledger_details: Dict[str, Any] = {
                        "task_id": task_id, 
                        "new_status": str(final_status), 
                        "previous_status": str(original_status)
                    }
                    # Add relevant context to the ledger event details
                    if assigned_to_node_id: ledger_details["assigned_to_node_id"] = assigned_to_node_id
                    if clear_assignment: ledger_details["assignment_cleared"] = True
                    if result: ledger_details["result_preview"] = str(result)[:50] # Preview of result
                    if increment_attempts: ledger_details["execution_attempts"] = task.execution_attempts
                    
                    # Record the event
                    await self.ledger.record_event(
                        event_type=ledger_event_type,
                        details=ledger_details,
                        # Source is the node that performed the update action
                        source_node_id=actor_node_id or self.current_node_id 
                    )
                return True # Update successful
            else:
                # Update executed but affected 0 rows (e.g., task_id not found, or values unchanged)
                log.warning(f"Task {task_id} not found or no effective change for update. SQL: {sql}, PARAMS: {params}")
                return False
        except Exception as e:
            # Log errors during DB update
            log.error(f"Failed to update task {task_id}: {e}", exc_info=True)
            return False

    async def get_tasks_for_assignment(self, limit: int = 10) -> List[Task]:
        """
        Retrieves tasks that are ready for a leader to assign (PENDING or ACCEPTED).
        Ordered by priority (ascending) and then by submission time (ascending).

        Args:
            limit: Maximum number of tasks to retrieve.

        Returns:
            A list of Task objects ready for assignment.
        """
        # Select tasks that are either newly submitted (PENDING) or accepted by leader but not yet assigned (ACCEPTED)
        sql = """
            SELECT id, submit_time, priority, data, status, assigned_to_node_id, 
                   result, last_updated, submitted_by_node_id, execution_attempts
            FROM tasks
            WHERE status = ? OR status = ? 
            ORDER BY priority ASC, submit_time ASC
            LIMIT ?
        """
        params = (str(TaskStatus.PENDING), str(TaskStatus.ACCEPTED), limit)
        
        # TODO: Consider adding logic to also fetch tasks that were ASSIGNED but timed out (e.g., worker never ACKed)
        # This would involve checking last_updated time against a timeout threshold.

        try:
            future = self.db.execute(sql, params)
            rows = await asyncio.wrap_future(future)
            # Convert DB rows to Task objects
            return [Task.from_db_row(row) for row in rows]
        except Exception as e:
            log.error(f"Failed to retrieve tasks for assignment: {e}", exc_info=True)
            return []

    async def get_tasks_assigned_to_worker(self, worker_node_id: str, status_list: Optional[List[TaskStatus]] = None, limit: int = 10) -> List[Task]:
        """
        Retrieves tasks currently assigned to a specific worker node, optionally filtered by status.
        Typically used by a worker node to find its current workload.

        Args:
            worker_node_id: The Akita ID of the worker node.
            status_list: Optional list of TaskStatus enums to filter by. Defaults to ASSIGNED and WORKER_ACK.
            limit: Maximum number of tasks to retrieve.

        Returns:
            A list of Task objects assigned to the specified worker.
        """
        # Default statuses represent tasks the worker needs to act upon
        if status_list is None:
            status_list = [TaskStatus.ASSIGNED, TaskStatus.WORKER_ACK] 

        # Build SQL query with placeholders for status list
        status_placeholders = ','.join(['?'] * len(status_list))
        sql = f"""
            SELECT id, submit_time, priority, data, status, assigned_to_node_id, 
                   result, last_updated, submitted_by_node_id, execution_attempts
            FROM tasks
            WHERE assigned_to_node_id = ? AND status IN ({status_placeholders})
            ORDER BY priority ASC, submit_time ASC
            LIMIT ?
        """
        # Combine parameters: worker ID, status strings, limit
        params = [worker_node_id] + [str(s) for s in status_list] + [limit]
        
        try:
            future = self.db.execute(sql, tuple(params))
            rows = await asyncio.wrap_future(future)
            return [Task.from_db_row(row) for row in rows]
        except Exception as e:
            log.error(f"Failed to retrieve tasks for worker {worker_node_id}: {e}", exc_info=True)
            return []

    async def count_tasks_by_status(self, status: Optional[TaskStatus] = None) -> Dict[str, int]:
        """Counts tasks, optionally filtered by a specific status or grouping by all statuses if None."""
        if status:
            # Count tasks with a specific status
            sql = "SELECT COUNT(*) FROM tasks WHERE status = ?"
            params = (str(status),)
            future = self.db.execute(sql, params)
            rows = await asyncio.wrap_future(future)
            # Return count for the specific status
            return {str(status): rows[0][0] if rows and rows[0] else 0}
        else:
            # Count tasks grouped by status
            sql = "SELECT status, COUNT(*) FROM tasks GROUP BY status"
            future = self.db.execute(sql)
            rows = await asyncio.wrap_future(future)
            # Return a dictionary mapping status string to count
            return {row['status']: row['COUNT(*)'] for row in rows} if rows else {}


    async def delete_task(self, task_id: str) -> bool:
        """
        Deletes a task record from the database. Use with caution.
        It's generally better to mark tasks as CANCELLED or FAILED.
        """
        # Optional: Check if task is in a final state before allowing deletion
        task = await self.get_task_by_id(task_id)
        if task and task.status not in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
            log.warning(f"Attempting to delete task {task_id} which is not in a final state ({task.status}). Consider cancelling first.")
            # return False # Uncomment to prevent deletion of non-final tasks

        sql = "DELETE FROM tasks WHERE id = ?"
        try:
            future = self.db.execute(sql, (task_id,))
            changed_rows = await asyncio.wrap_future(future)
            if changed_rows > 0:
                log.info(f"Task {task_id} deleted from database.")
                # Optionally record deletion in ledger
                await self.ledger.record_event(
                    event_type=EventType.GENERAL_INFO, # Or a specific TASK_DELETED event
                    details={"task_id": task_id, "action": "deleted"},
                    source_node_id=self.current_node_id
                )
                return True
            else:
                log.warning(f"Task {task_id} not found for deletion.")
                return False
        except Exception as e:
            log.error(f"Failed to delete task {task_id}: {e}", exc_info=True)
            return False

    async def re_queue_stale_assigned_task(self, task_id: str, reason: str = "stale_assignment") -> bool:
        """
        Resets a task that was assigned but not processed (e.g., worker timed out) 
        back to PENDING status, clears its assignment, and increments its execution attempts.
        Typically called by the leader node.

        Args:
            task_id: The ID of the task to re-queue.
            reason: A string indicating why the task is being re-queued.

        Returns:
            True if the task was successfully updated for re-queuing, False otherwise.
        """
        log.warning(f"Re-queuing task {task_id} due to: {reason}. Resetting to PENDING, clearing assignment, incrementing attempts.")
        
        # Use the generic update method to perform the required changes
        success = await self.update_task_fields(
            task_id=task_id,
            new_status=TaskStatus.PENDING, # Reset status to PENDING
            clear_assignment=True, # Set assigned_to_node_id to NULL
            increment_attempts=True, # Increment the retry counter
            actor_node_id=self.current_node_id # Leader (usually self.current_node_id) performs this
        )
        
        # Record the re-queue event in the ledger if successful
        if success:
            await self.ledger.record_event(
                event_type=EventType.TASK_REQUEUED, # Use specific event type
                details={"task_id": task_id, "reason": reason, "action": "re-queued to PENDING"},
                source_node_id=self.current_node_id
            )
        return success

