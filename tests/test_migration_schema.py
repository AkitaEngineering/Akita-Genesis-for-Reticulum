import json
import sqlite3
import importlib
from pathlib import Path

import akita_genesis.modules.persistence as persistence_module
from akita_genesis.config import settings


def test_migration_adds_columns_and_backfills(tmp_path):
    # Create an "old" DB with the older cluster_nodes schema (no new columns)
    old_db = tmp_path / "old_akita.db"
    conn = sqlite3.connect(str(old_db))
    cur = conn.cursor()
    # Old table definition (no capabilities/current_task_count)
    cur.execute('''
        CREATE TABLE cluster_nodes (
            node_id TEXT PRIMARY KEY,
            node_name TEXT NOT NULL,
            cluster_name TEXT NOT NULL,
            address_hex TEXT,
            last_seen REAL NOT NULL,
            is_leader BOOLEAN DEFAULT FALSE,
            resources TEXT,
            status TEXT DEFAULT 'online'
        );
    ''')
    # Insert a row whose resources JSON contains the desired backfill values
    resources_json = json.dumps({"cpu": {"percent_used": 1}, "capabilities": ["gpu","fast_io"], "current_task_count": 5})
    cur.execute("INSERT INTO cluster_nodes (node_id, node_name, cluster_name, address_hex, last_seen, is_leader, resources, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("migr-node-1", "migr-node", "migr_cluster", "deadbeef", 1234567.0, 0, resources_json, "online"))
    conn.commit()
    conn.close()

    # Reload persistence with the old DB path and let migrations run
    persistence_module.DatabaseManager._instance = None
    settings.SQLITE_DB_FILE = old_db
    persistence = importlib.reload(persistence_module)

    # Wait for initial schema init + migration callback to enqueue migration operations
    persistence.db_manager.init_db_future.result(timeout=10)

    # Use the db_manager to SELECT the migrated columns (this waits behind queued migration ops)
    fut = persistence.db_manager.execute("SELECT capabilities, current_task_count FROM cluster_nodes WHERE node_id = ?", ("migr-node-1",))
    rows = fut.result(timeout=10)
    assert rows and rows[0]
    caps_text, cnt = rows[0][0], rows[0][1]

    assert caps_text is not None and 'gpu' in caps_text
    assert int(cnt) == 5
