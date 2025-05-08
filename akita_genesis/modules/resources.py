# akita_genesis/modules/resources.py
import psutil # Library for retrieving system information
import time
import json
from typing import Dict, Any, Optional, Callable, Awaitable # Added Awaitable
import asyncio

# Use absolute imports within the package
from akita_genesis.utils.logger import setup_logger
from akita_genesis.config.settings import settings
# Import Ledger and EventType for optional logging, handle potential circularity if needed
try:
    from akita_genesis.modules.ledger import Ledger, EventType 
except ImportError:
    Ledger = None # Define as None if import fails (e.g., during setup)
    EventType = None 

log = setup_logger(__name__)

class ResourceMonitor:
    """
    Monitors system resources like CPU, memory, disk, and network usage.
    It can periodically update resource information and invoke a callback.
    """

    def __init__(self, ledger: Optional[Ledger] = None, node_id: Optional[str] = None, node_name: Optional[str] = None):
        """
        Initializes the ResourceMonitor.

        Args:
            ledger: An optional Ledger instance to record resource events (currently unused).
            node_id: The Akita ID of the current node (for context).
            node_name: The name of the current node (for context).
        """
        self.ledger = ledger # Optional ledger instance
        self.node_id = node_id
        self.node_name = node_name
        # Stores the latest fetched resource information
        self.current_resources: Dict[str, Any] = {} 
        # Task handle for the background monitoring loop
        self._monitoring_task: Optional[asyncio.Task] = None
        # Event to signal the monitoring loop to stop
        self._stop_event = asyncio.Event()
        # Optional asynchronous callback function to call when resources are updated
        self._update_callback: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None 

        # Store previous network I/O counters for rate calculation
        try:
            self._last_net_io = psutil.net_io_counters()
        except Exception as e:
            log.warning(f"Could not get initial network I/O counters: {e}. Network rates might be inaccurate initially.")
            self._last_net_io = None # Handle case where initial fetch fails
        self._last_net_io_time = time.time()

        log.info("ResourceMonitor initialized.")

    def set_node_context(self, node_id: str, node_name: str):
        """Sets the node context (ID and name) for resource monitoring and reporting."""
        self.node_id = node_id
        self.node_name = node_name

    def set_update_callback(self, callback: Callable[[Dict[str, Any]], Awaitable[None]]):
        """
        Registers an asynchronous callback function to be invoked when resource metrics are updated.
        The callback will receive the latest resource dictionary as its argument.
        """
        self._update_callback = callback
        log.debug("Resource update callback registered.")

    def get_current_resources(self) -> Dict[str, Any]:
        """
        Synchronously fetches and returns the current system resource information.
        Handles potential errors during data retrieval from psutil.
        """
        resources: Dict[str, Any] = {"timestamp": time.time()} # Start with a timestamp

        try:
            # --- CPU Information ---
            cpu_info: Dict[str, Any] = {}
            try: cpu_info["percent_used"] = psutil.cpu_percent(interval=0.1) # Short non-blocking interval
            except Exception as e: log.warning(f"Could not get cpu_percent: {e}")
            try: cpu_info["logical_cores"] = psutil.cpu_count(logical=True)
            except Exception as e: log.warning(f"Could not get cpu_count(logical): {e}")
            try: cpu_info["physical_cores"] = psutil.cpu_count(logical=False)
            except Exception as e: log.warning(f"Could not get cpu_count(physical): {e}")
            try: 
                cpu_freq = psutil.cpu_freq()
                if cpu_freq:
                    cpu_info["current_frequency_mhz"] = cpu_freq.current
                    cpu_info["max_frequency_mhz"] = cpu_freq.max
            except Exception as e: log.warning(f"Could not get cpu_freq: {e}")
            resources["cpu"] = cpu_info

            # --- Memory Information ---
            mem_info: Dict[str, Any] = {}
            try: 
                virtual_mem = psutil.virtual_memory()
                mem_info["virtual"] = {
                    "total_gb": round(virtual_mem.total / (1024**3), 2),
                    "available_gb": round(virtual_mem.available / (1024**3), 2),
                    "percent_used": virtual_mem.percent,
                    "used_gb": round(virtual_mem.used / (1024**3), 2),
                }
            except Exception as e: log.warning(f"Could not get virtual_memory: {e}")
            try:
                swap_mem = psutil.swap_memory()
                mem_info["swap"] = {
                    "total_gb": round(swap_mem.total / (1024**3), 2),
                    "used_gb": round(swap_mem.used / (1024**3), 2),
                    "percent_used": swap_mem.percent,
                }
            except Exception as e: log.warning(f"Could not get swap_memory: {e}")
            resources["memory"] = mem_info

            # --- Disk Information ---
            disk_info: Dict[str, Any] = {}
            try: 
                # Monitor root partition ('/') by default. Could be made configurable.
                disk_usage_root = psutil.disk_usage('/')
                disk_info["root"] = { 
                    "total_gb": round(disk_usage_root.total / (1024**3), 2),
                    "used_gb": round(disk_usage_root.used / (1024**3), 2),
                    "free_gb": round(disk_usage_root.free / (1024**3), 2),
                    "percent_used": disk_usage_root.percent,
                }
            except Exception as e: log.warning(f"Could not get disk_usage('/'): {e}")
            # Optionally add disk I/O counters (can be system-wide or per-partition)
            # try: disk_io = psutil.disk_io_counters(); disk_info["io"] = {"read_bytes": disk_io.read_bytes, "write_bytes": disk_io.write_bytes}
            # except Exception as e: log.warning(f"Could not get disk_io_counters: {e}")
            resources["disk"] = disk_info

            # --- Network Information (including rates) ---
            net_info: Dict[str, Any] = {}
            try:
                current_net_io = psutil.net_io_counters()
                current_time = time.time()
                time_delta = current_time - self._last_net_io_time
                
                bytes_sent_rate = 0
                bytes_recv_rate = 0
                # Calculate rates only if time has passed and previous data exists
                if time_delta > 0 and self._last_net_io:
                    bytes_sent_delta = current_net_io.bytes_sent - self._last_net_io.bytes_sent
                    bytes_recv_delta = current_net_io.bytes_recv - self._last_net_io.bytes_recv
                    # Ensure rates are non-negative (counters might reset)
                    bytes_sent_rate = max(0, bytes_sent_delta) / time_delta
                    bytes_recv_rate = max(0, bytes_recv_delta) / time_delta
                
                net_info = {
                    "bytes_sent_total": current_net_io.bytes_sent,
                    "bytes_recv_total": current_net_io.bytes_recv,
                    "packets_sent_total": current_net_io.packets_sent,
                    "packets_recv_total": current_net_io.packets_recv,
                    "bytes_sent_rate_bps": bytes_sent_rate * 8, # bits per second
                    "bytes_recv_rate_bps": bytes_recv_rate * 8, # bits per second
                }
                # Update last known values for next calculation
                self._last_net_io = current_net_io
                self._last_net_io_time = current_time
            except Exception as e: 
                log.warning(f"Could not get net_io_counters: {e}")
                # Reset last_net_io if fetching failed to avoid incorrect rates next time
                self._last_net_io = None 
            resources["network"] = net_info

            # --- System Load Average ---
            try:
                # getloadavg() returns tuple (1min, 5min, 15min), might not be available on Windows
                resources["load_average"] = psutil.getloadavg() 
            except AttributeError:
                log.debug("psutil.getloadavg() not available on this platform.")
                resources["load_average"] = None # Indicate unavailability
            except Exception as e: 
                log.warning(f"Could not get load average: {e}")
                resources["load_average"] = None


            # Update the cached resource information
            self.current_resources = resources
            return resources

        except Exception as e:
            # Catch-all for any unexpected error during resource gathering
            log.error(f"Unexpected error getting system resources: {e}", exc_info=True)
            # Return an error structure or last known good data
            return {"error": str(e), "details": "Failed to fetch some resource metrics", "last_known": self.current_resources or {}}

    async def _monitor_loop(self):
        """Internal background task loop to periodically fetch and update resource information."""
        log.info(f"Resource monitoring loop started. Update interval: {settings.RESOURCE_MONITOR_INTERVAL_S}s")
        # Perform an initial fetch immediately
        try:
            initial_resources = self.get_current_resources()
            if self._update_callback and "error" not in initial_resources:
                # Call the callback immediately with the initial data
                await self._update_callback(initial_resources)
        except Exception as e:
             log.error(f"Error during initial resource fetch or callback: {e}")

        # Main monitoring loop
        while not self._stop_event.is_set():
            try:
                # Wait for the configured interval before the next fetch
                await asyncio.sleep(settings.RESOURCE_MONITOR_INTERVAL_S)
                # Check stop event again after sleep, in case stop was called during sleep
                if self._stop_event.is_set():
                    break
                
                log.debug("Fetching updated resource metrics...")
                updated_resources = self.get_current_resources()

                # If fetching was successful (no 'error' key)
                if "error" not in updated_resources:
                    # Optionally log resource updates to the main ledger (can be verbose)
                    # Consider logging only significant changes or summaries.
                    # if self.ledger and self.node_id and EventType:
                    #     summary = { ... } # Create summary dict
                    #     await self.ledger.record_event(EventType.NODE_RESOURCE_UPDATE, details=summary, ...)
                    
                    # If a callback is registered, call it with the updated resources
                    if self._update_callback:
                        try:
                            await self._update_callback(updated_resources)
                        except Exception as cb_e:
                            log.error(f"Error in resource update callback: {cb_e}", exc_info=True)
                else:
                    # Log if fetching resources failed
                    log.warning(f"Could not update resource metrics: {updated_resources.get('error')}")

            except asyncio.CancelledError:
                # Handle task cancellation gracefully
                log.info("Resource monitoring loop cancelled.")
                break
            except Exception as e:
                # Log unexpected errors in the loop
                log.error(f"Exception in resource monitoring loop: {e}", exc_info=True)
                # Avoid busy-looping on persistent errors by waiting before retrying
                await asyncio.sleep(settings.RESOURCE_MONITOR_INTERVAL_S / 2) 

        log.info("Resource monitoring loop stopped.")

    async def start_monitoring(self):
        """Starts the asynchronous resource monitoring loop in the background."""
        # Prevent starting multiple monitoring tasks
        if self._monitoring_task and not self._monitoring_task.done():
            log.warning("Resource monitoring is already running.")
            return
        
        self._stop_event.clear() # Reset stop event
        # Create and start the asyncio task for the monitoring loop
        self._monitoring_task = asyncio.create_task(self._monitor_loop())
        log.info("Resource monitoring task started.")

    async def stop_monitoring(self):
        """Stops the asynchronous resource monitoring loop gracefully."""
        if self._monitoring_task and not self._monitoring_task.done():
            log.info("Stopping resource monitoring...")
            # Signal the loop to stop
            self._stop_event.set()
            try:
                # Wait for the monitoring task to finish (with a timeout)
                await asyncio.wait_for(self._monitoring_task, timeout=5.0)
            except asyncio.TimeoutError:
                # If it doesn't stop gracefully, force cancel it
                log.warning("Resource monitoring task did not stop gracefully in time, cancelling.")
                self._monitoring_task.cancel()
                # Wait for cancellation to complete
                await asyncio.gather(self._monitoring_task, return_exceptions=True)
            except asyncio.CancelledError:
                pass # Expected if already cancelled externally
            self._monitoring_task = None # Clear the task handle
            log.info("Resource monitoring stopped.")
        else:
            log.info("Resource monitoring is not running or already stopped.")


