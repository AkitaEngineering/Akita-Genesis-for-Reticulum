import time
from akita_genesis.utils.logger import get_recent_logs, log


def test_inmemory_log_handler_records_and_filters():
    # Get current entries and note length
    initial = get_recent_logs(limit=1000)
    initial_len = len(initial)

    # Emit messages at different levels
    log.info("TEST-LOG-INFO-1")
    log.error("TEST-LOG-ERROR-1")
    time.sleep(0.01)  # allow timestamp differences

    entries = get_recent_logs(limit=10)
    assert len(entries) >= 2

    # The most recent entries should include our messages
    messages = [e["message"] for e in entries]
    assert any("TEST-LOG-INFO-1" in m for m in messages)
    assert any("TEST-LOG-ERROR-1" in m for m in messages)

    # Filter by level
    error_only = get_recent_logs(limit=10, level="ERROR")
    assert all(e["level"] == "ERROR" for e in error_only)

    # Ensure limiting works
    limited = get_recent_logs(limit=1)
    assert len(limited) == 1


def test_get_recent_logs_returns_new_entries():
    before = get_recent_logs(limit=5)
    log.info("TEST-LOG-NEW-ENTRY")
    after = get_recent_logs(limit=5)
    # There should be at least one new entry after logging
    assert len(after) >= len(before)
    assert any("TEST-LOG-NEW-ENTRY" in e["message"] for e in after)
