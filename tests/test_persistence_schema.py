import importlib
import sqlite3
from pathlib import Path

import akita_genesis.modules.persistence as persistence_module
from akita_genesis.config import settings


def test_cluster_nodes_table_has_new_columns(tmp_path):
    # Use a temporary DB file for this test
    test_db = tmp_path / "test_akita.db"

    # Ensure persistence module will initialize against our temp DB
    # Reset singleton and reload module so DatabaseManager uses the new path
    persistence_module.DatabaseManager._instance = None
    settings.SQLITE_DB_FILE = test_db
    persistence = importlib.reload(persistence_module)

    # Wait for schema init to complete (future is a concurrent.futures.Future)
    init_future = persistence.db_manager.init_db_future
    init_future.result(timeout=5)

    # Inspect the SQLite schema for cluster_nodes
    conn = sqlite3.connect(str(test_db))
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(cluster_nodes);")
    cols = [r[1] for r in cur.fetchall()]
    conn.close()

    assert "capabilities" in cols, "capabilities column must exist in cluster_nodes table"
    assert "current_task_count" in cols, "current_task_count column must exist in cluster_nodes table"


def test_can_insert_and_query_capabilities_and_taskcount(tmp_path):
    test_db = tmp_path / "test_akita2.db"
    persistence_module.DatabaseManager._instance = None
    settings.SQLITE_DB_FILE = test_db
    persistence = importlib.reload(persistence_module)
    persistence.db_manager.init_db_future.result(timeout=5)

    # Insert a sample node row using the DB manager
    sql = (
        "INSERT INTO cluster_nodes (node_id, node_name, cluster_name, address_hex, last_seen, is_leader, resources, status, capabilities, current_task_count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    params = (
        "node-x-1", "node-x", "test_cluster", "deadbeef", 1234567890.0, 0,
        '{"cpu": {"percent_used": 1}}', "online", '["gpu","fast_io"]', 7
    )
    fut = persistence.db_manager.execute(sql, params)
    fut.result(timeout=10)

    # Query back
    conn = sqlite3.connect(str(test_db))
    cur = conn.cursor()
    cur.execute("SELECT capabilities, current_task_count FROM cluster_nodes WHERE node_id = ?", ("node-x-1",))
    row = cur.fetchone()
    conn.close()

    assert row is not None
    caps, cnt = row
    assert 'gpu' in caps
    assert int(cnt) == 7
