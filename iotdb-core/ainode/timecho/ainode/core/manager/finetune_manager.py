import os
import threading
from typing import Any, Dict, List, Optional

import torch.multiprocessing as mp

from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.model_manager import ModelManager
from iotdb.ainode.core.util.decorator import singleton
from timecho.ainode.core.finetune.task.task_constants import TaskStatus
from timecho.ainode.core.finetune.task.task_executor import TaskExecutor
from timecho.ainode.core.finetune.task.task_info import FinetuneTask
from timecho.ainode.core.finetune.task.task_queue import FinetuneTaskQueue
from timecho.ainode.core.hparams.parser import parse_params_into_args

logger = Logger()


@singleton
class FinetuneManager:
    """
    Singleton orchestrator for fine-tuning tasks.

    Core design: serial execution + priority queue + observability.
    - One task runs at a time (GPU resource isolation).
    - Event-driven consumer thread (no busy-polling).
    - Cooperative cancellation via mp.Event shared with training workers.
    - Crash-resilient: stale RUNNING tasks recovered on restart.
    """

    def __init__(self):
        self._model_manager = ModelManager()
        self._task_queue = FinetuneTaskQueue()
        self._executor = TaskExecutor()

        # Consumer thread management
        self._consumer_thread: Optional[threading.Thread] = None
        self._running = False
        self._wakeup = threading.Event()

        # Cancel events for running tasks (task_id -> mp.Event)
        self._cancel_events: Dict[str, mp.Event] = {}
        self._cancel_lock = threading.Lock()

    # ==================== Lifecycle ====================

    def start(self) -> None:
        if self._running:
            return

        # Recover tasks that were RUNNING when the process crashed
        recovered = self._task_queue.recover_stale_tasks()
        for task in recovered:
            try:
                self._model_manager.fail_finetune(task.model_args.model_id)
            except Exception:
                pass
        if recovered:
            logger.info(f"Recovered {len(recovered)} stale task(s) on startup")

        self._running = True
        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            daemon=True,
            name="FinetuneConsumer",
        )
        self._consumer_thread.start()
        logger.info("FinetuneManager consumer started")

    def stop(self) -> None:
        self._running = False
        self._wakeup.set()  # unblock consumer if waiting
        if self._consumer_thread:
            self._consumer_thread.join(timeout=10.0)
        logger.info("FinetuneManager consumer stopped")

    # ==================== Consumer Loop ====================

    def _consumer_loop(self) -> None:
        while self._running:
            # Block efficiently until wakeup or timeout
            self._wakeup.wait(timeout=2.0)
            self._wakeup.clear()

            if not self._running:
                break

            task = self._task_queue.dequeue()
            if task is None:
                continue

            # Create a cancel event for this execution
            cancel_event = mp.Event()
            with self._cancel_lock:
                self._cancel_events[task.task_id] = cancel_event

            try:
                self._task_queue.start_task(task.task_id, pid=os.getpid())
                logger.info(f"Executing task: {task.task_id}")

                result = self._executor.execute(task, cancel_event=cancel_event)

                # Determine outcome
                if cancel_event.is_set():
                    self._task_queue.cancel_task(task.task_id)
                    self._model_manager.fail_finetune(
                        task.model_args.model_id, cleanup=True
                    )
                    logger.info(f"Task {task.task_id} canceled")
                elif result.get("success", False):
                    # Proxy final progress to task
                    self._proxy_progress(task.task_id, result)
                    self._task_queue.complete_task(task.task_id, success=True)
                    self._model_manager.complete_finetune(task.model_args.model_id)
                    logger.info(f"Task {task.task_id} completed successfully")
                else:
                    error = result.get("error", "Unknown error")
                    self._proxy_progress(task.task_id, result)
                    self._task_queue.complete_task(
                        task.task_id, success=False, error_msg=error
                    )
                    self._model_manager.fail_finetune(task.model_args.model_id)
                    logger.error(f"Task {task.task_id} failed: {error}")

            except Exception as e:
                logger.error(f"Error executing task {task.task_id}: {e}")
                self._task_queue.complete_task(
                    task.task_id, success=False, error_msg=str(e)
                )
                try:
                    self._model_manager.fail_finetune(task.model_args.model_id)
                except Exception:
                    pass

            finally:
                with self._cancel_lock:
                    self._cancel_events.pop(task.task_id, None)

    def _proxy_progress(self, task_id: str, result: dict) -> None:
        """Read progress fields from executor result_dict and persist to queue."""
        progress_keys = {
            "current_epoch",
            "total_epochs",
            "current_step",
            "total_steps",
            "train_loss",
            "val_loss",
        }
        updates = {k: result[k] for k in progress_keys if k in result}
        if updates:
            self._task_queue.update_progress(task_id, **updates)

    # ==================== Task API ====================

    def create_task(
        self,
        params: Dict[str, Any],
    ) -> FinetuneTask:
        """
        Register model, parse params, create and enqueue a fine-tuning task.

        Rolls back model registration automatically if param parsing or
        enqueue fails.
        """
        base_model_info = self._model_manager.register_fine_tuned_model(
            model_id=params["model_id"],
            base_model_id=params["base_model_id"],
        )
        try:
            args = parse_params_into_args(params, base_model_info)
            return self._enqueue_new_task(base_model_info, *args)
        except Exception:
            # Rollback: remove the half-registered model
            try:
                self._model_manager.fail_finetune(params["model_id"], cleanup=True)
            except Exception:
                pass
            raise

    def retry_task(self, task_id: str) -> Optional[FinetuneTask]:
        """Re-create a FAILED / CANCELED task with the same configuration."""
        old = self._task_queue.get_task(task_id)
        if old is None:
            return None
        if old.status not in (TaskStatus.FAILED, TaskStatus.CANCELED):
            logger.warning(f"Cannot retry task {task_id} with status {old.status}")
            return None

        # Re-register model for the new attempt
        base_model_info = self._model_manager.register_fine_tuned_model(
            model_id=old.model_args.model_id,
            base_model_id=old.model_args.base_model_id,
        )
        return self._enqueue_new_task(
            base_model_info,
            old.model_args,
            old.data_args,
            old.training_args,
            old.finetune_args,
        )

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a running or pending task."""
        # If running, signal the cancel event (cooperative stop)
        with self._cancel_lock:
            if task_id in self._cancel_events:
                self._cancel_events[task_id].set()
                logger.info(f"Sent cancel signal to running task {task_id}")
                return True
        # If pending, cancel in queue directly
        return self._task_queue.cancel_task(task_id)

    def get_task(self, task_id: str) -> Optional[FinetuneTask]:
        return self._task_queue.get_task(task_id)

    def list_tasks(self, include_completed: bool = False) -> List[FinetuneTask]:
        return self._task_queue.list_tasks(include_completed)

    def drop_task(self, task_id: str, cleanup: bool = True) -> bool:
        return self._task_queue.drop_task(task_id, cleanup)

    def get_active_task(self) -> Optional[FinetuneTask]:
        return self._task_queue.get_active_task()

    def pending_count(self) -> int:
        return self._task_queue.pending_count()

    @property
    def is_running(self) -> bool:
        return self._running

    # ==================== Internal ====================

    def _enqueue_new_task(
        self, base_model_info, model_args, data_args, training_args, finetune_args
    ) -> FinetuneTask:
        """Shared helper: create a FinetuneTask and push it into the queue."""
        task = FinetuneTask.create(
            base_model_info=base_model_info,
            model_args=model_args,
            data_args=data_args,
            training_args=training_args,
            finetune_args=finetune_args,
        )
        self._task_queue.enqueue(task)
        self._wakeup.set()  # wake consumer immediately
        logger.info(f"Enqueued task: {task.task_id}")
        return task
