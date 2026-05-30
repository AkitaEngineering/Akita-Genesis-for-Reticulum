import asyncio
import importlib

import akita_genesis.modules.persistence as persistence_module
import akita_genesis.modules.tasks as tasks_module
from akita_genesis.config import settings


class DummyLedger:
    async def record_event(self, *args, **kwargs):
        return None


def _reload_task_modules(db_path):
    persistence_module.DatabaseManager._instance = None
    settings.SQLITE_DB_FILE = db_path
    persistence = importlib.reload(persistence_module)
    persistence.db_manager.init_db_future.result(timeout=5)
    return importlib.reload(tasks_module)


def test_task_manager_list_tasks_filters_and_paginates(tmp_path):
    reloaded_tasks = _reload_task_modules(tmp_path / "tasks.db")
    manager = reloaded_tasks.TaskManager(ledger=DummyLedger(), node_id="node-1", node_name="node-1")

    async def scenario():
        first_task = await manager.submit_task_to_system({"job": "first"}, priority=20)
        second_task = await manager.submit_task_to_system({"job": "second"}, priority=10)
        third_task = await manager.submit_task_to_system({"job": "third"}, priority=5)

        assert first_task is not None
        assert second_task is not None
        assert third_task is not None

        await manager.update_task_fields(first_task.id, new_status=reloaded_tasks.TaskStatus.COMPLETED)
        await manager.update_task_fields(second_task.id, new_status=reloaded_tasks.TaskStatus.ASSIGNED)

        pending_tasks = await manager.list_tasks(status=reloaded_tasks.TaskStatus.PENDING, limit=10)
        page_one = await manager.list_tasks(limit=2, offset=0)
        page_two = await manager.list_tasks(limit=2, offset=2)

        return first_task, second_task, third_task, pending_tasks, page_one, page_two

    first_task, second_task, third_task, pending_tasks, page_one, page_two = asyncio.run(scenario())

    assert [task.id for task in pending_tasks] == [third_task.id]
    assert {task.id for task in page_one} | {task.id for task in page_two} == {
        first_task.id,
        second_task.id,
        third_task.id,
    }
    assert len(page_one) == 2
    assert len(page_two) == 1