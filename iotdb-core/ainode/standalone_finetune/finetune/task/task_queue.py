import heapq
import json
import os
import shutil
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.log import Logger
from timecho.ainode.core.finetune.task.task_constants import (
    COMPLETED_DIR,
    MANIFEST,
    PRIORITY_ORDER,
    TASK_FILE_EXTENSION,
    TEMP_FILE_SUFFIX,
    UNFINISHED_DIR,
    TaskStatus,
)
from timecho.ainode.core.finetune.task.task_info import FinetuneTask

logger = Logger()


@dataclass(order=True)
class QueueEntry:
    priority_order: int = field(compare=True)
    created_at: str = field(compare=True)
    task_id: str = field(compare=False)

    @classmethod
    def from_task(cls, task: FinetuneTask) -> "QueueEntry":
        priority_order = PRIORITY_ORDER.get(task.priority, 1)
        return cls(
            priority_order=priority_order,
            created_at=task.created_at,
            task_id=task.task_id,
        )


class FinetuneTaskQueue:
    def __init__(self):
        self._config = AINodeDescriptor().get_config()
        self._exp_dir = self._config.get_ain_exp_dir()
        self._unfinished_dir = os.path.join(self._exp_dir, UNFINISHED_DIR)
        self._completed_dir = os.path.join(self._exp_dir, COMPLETED_DIR)
        self._manifest = os.path.join(self._exp_dir, MANIFEST)

        os.makedirs(self._unfinished_dir, exist_ok=True)
        os.makedirs(self._completed_dir, exist_ok=True)

        self._tasks: Dict[str, FinetuneTask] = {}
        self._heap: List[QueueEntry] = []
        self._active_task_id: Optional[str] = None
        self._lock = threading.RLock()

        self._load_state()

    def _load_state(self) -> None:
        if os.path.exists(self._manifest):
            try:
                with open(self._manifest, "r") as f:
                    manifest = json.load(f)
                self._active_task_id = manifest.get("active_task_id")
            except Exception as e:
                logger.error(f"Failed to load manifest: {e}")

        for filename in os.listdir(self._unfinished_dir):
            if filename.endswith(TASK_FILE_EXTENSION):
                task_path = os.path.join(self._unfinished_dir, filename)
                try:
                    with open(task_path, "r") as f:
                        task_data = json.load(f)
                    task = FinetuneTask.from_dict(task_data)
                    self._tasks[task.task_id] = task

                    if task.status == TaskStatus.PENDING:
                        heapq.heappush(self._heap, QueueEntry.from_task(task))
                except Exception as e:
                    logger.error(f"Failed to load task {filename}: {e}")

    def _save_manifest(self) -> None:
        manifest = {"active_task_id": self._active_task_id}
        tmp_path = self._manifest + TEMP_FILE_SUFFIX
        with open(tmp_path, "w") as f:
            json.dump(manifest, f)
        os.rename(tmp_path, self._manifest)

    def _save_task(self, task: FinetuneTask) -> None:
        task_path = os.path.join(
            self._unfinished_dir, f"{task.task_id}{TASK_FILE_EXTENSION}"
        )
        tmp_path = task_path + TEMP_FILE_SUFFIX
        with open(tmp_path, "w") as f:
            json.dump(task.to_dict(), f, indent=2)
        os.rename(tmp_path, task_path)

    def enqueue(self, task: FinetuneTask) -> None:
        with self._lock:
            self._tasks[task.task_id] = task
            heapq.heappush(self._heap, QueueEntry.from_task(task))
            self._save_task(task)
            self._save_manifest()
            logger.info(f"Enqueued task {task.task_id} with priority {task.priority}")

    def dequeue(self) -> FinetuneTask | None:
        with self._lock:
            while self._heap:
                entry = heapq.heappop(self._heap)
                task = self._tasks.get(entry.task_id)
                if task and task.status == TaskStatus.PENDING:
                    self._active_task_id = task.task_id
                    # manifest is saved in start_task(), no need to persist here
                    return task
            return None

    def start_task(self, task_id: str, pid: int | None = None) -> FinetuneTask | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None

            task.start(pid)
            self._active_task_id = task_id
            self._save_task(task)
            self._save_manifest()

            logger.info(f"Started task {task_id} with pid={pid}")
            return task

    def complete_task(
        self,
        task_id: str,
        success: bool = True,
        error_msg: str = "",
    ) -> FinetuneTask | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return None

            task.complete(success, error_msg)
            self._move_to_completed(task)

            if self._active_task_id == task_id:
                self._active_task_id = None
                self._save_manifest()

            logger.info(f"Completed task {task_id}: success={success}")
            return task

    def cancel_task(self, task_id: str) -> bool:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False

            if task.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
                return False

            task.cancel()
            self._move_to_completed(task)

            if self._active_task_id == task_id:
                self._active_task_id = None
                self._save_manifest()

            logger.info(f"Canceled task {task_id}")
            return True

    def drop_task(self, task_id: str, cleanup: bool = True) -> bool:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return False

            task_path = os.path.join(
                self._unfinished_dir, f"{task_id}{TASK_FILE_EXTENSION}"
            )
            if os.path.exists(task_path):
                os.remove(task_path)

            completed_path = os.path.join(
                self._completed_dir, f"{task_id}{TASK_FILE_EXTENSION}"
            )
            if os.path.exists(completed_path):
                os.remove(completed_path)

            if cleanup and os.path.exists(task.exp_dir):
                shutil.rmtree(task.exp_dir, ignore_errors=True)

            del self._tasks[task_id]

            if self._active_task_id == task_id:
                self._active_task_id = None
                self._save_manifest()

            logger.info(f"Dropped task {task_id}")
            return True

    def get_task(self, task_id: str) -> FinetuneTask | None:
        return self._tasks.get(task_id)

    def list_tasks(self, include_completed: bool = False) -> List[FinetuneTask]:
        with self._lock:
            tasks = list(self._tasks.values())

            if include_completed:
                for filename in os.listdir(self._completed_dir):
                    if filename.endswith(TASK_FILE_EXTENSION):
                        task_id = filename[: -len(TASK_FILE_EXTENSION)]
                        if task_id not in self._tasks:
                            try:
                                with open(
                                    os.path.join(self._completed_dir, filename), "r"
                                ) as f:
                                    task_data = json.load(f)
                                tasks.append(FinetuneTask.from_dict(task_data))
                            except Exception:
                                pass

            tasks.sort(key=lambda t: (PRIORITY_ORDER.get(t.priority, 1), t.created_at))
            return tasks

    def get_active_task(self) -> FinetuneTask | None:
        if self._active_task_id:
            return self._tasks.get(self._active_task_id)
        return None

    def update_progress(self, task_id: str, **kwargs) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if task:
                task.update_progress(**kwargs)
                self._save_task(task)

    def pending_count(self) -> int:
        return sum(1 for t in self._tasks.values() if t.status == TaskStatus.PENDING)

    def recover_stale_tasks(self) -> List[FinetuneTask]:
        """
        Mark tasks stuck in RUNNING state as FAILED.

        Called on startup to recover from crashes where the training process
        was killed without a clean shutdown.

        Returns:
            List of recovered (now FAILED) tasks so the caller can clean up
            associated resources (e.g. model registration).
        """
        with self._lock:
            recovered: List[FinetuneTask] = []
            for task in list(self._tasks.values()):
                if task.status == TaskStatus.RUNNING:
                    task.complete(
                        success=False,
                        error_msg="Interrupted by AINode restart",
                    )
                    self._move_to_completed(task)
                    recovered.append(task)

            if recovered or self._active_task_id is not None:
                self._active_task_id = None
                self._save_manifest()

            return recovered

    def _move_to_completed(self, task: FinetuneTask) -> None:
        src_path = os.path.join(
            self._unfinished_dir, f"{task.task_id}{TASK_FILE_EXTENSION}"
        )
        dst_path = os.path.join(
            self._completed_dir, f"{task.task_id}{TASK_FILE_EXTENSION}"
        )

        with open(dst_path, "w") as f:
            json.dump(task.to_dict(), f, indent=2)

        if os.path.exists(src_path):
            os.remove(src_path)
