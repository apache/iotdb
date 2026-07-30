# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import atexit
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Iterable, List, Optional

from iotdb.ainode.core.log import Logger

logger = Logger()


class BatchExecutor:
    """
    Abstract batch concurrent executor.
    - Default fire-and-forget: automatically logs task exceptions (ignores return values)
    - Allows customization of completion logic by overriding callback hooks via inheritance
    - Supports single/batch submission, explicit closure, context management, and exit-time cleanup
    """

    def __init__(
        self,
        max_workers: int = 8,
        thread_name_prefix: str = "batch-exec",
        cancel_on_close: bool = False,
    ):
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=thread_name_prefix
        )
        self._closing = threading.Event()
        self._lock = threading.Lock()
        self._cancel_on_close = cancel_on_close
        atexit.register(self._atexit_close)

    def submit(self, fn: Callable[..., Any], *args, **kwargs) -> Optional[Future]:
        """
        Submit a single task.
        Default fire-and-forget: internally attaches a done callback to automatically log exceptions.
        To take over result handling, inherit and override on_task_done().
        """
        if self._closing.is_set():
            raise RuntimeError("Executor is closing; reject new submissions.")

        fut = self._executor.submit(fn, *args, **kwargs)
        self._attach_done_callback(fut, item=None)
        self.on_task_submitted(item=None, future=fut)
        return fut

    def submit_batch(
        self, items: Iterable[Any], fn: Callable[[Any], Any]
    ) -> List[Future]:
        """
        Submit a batch of tasks. Executes fn(item) for each item.
        """
        if self._closing.is_set():
            raise RuntimeError("Executor is closing; reject new submissions.")

        futures: List[Future] = []
        for item in items:
            fut = self._executor.submit(fn, item)
            self._attach_done_callback(fut, item=item)
            self.on_task_submitted(item=item, future=fut)
            futures.append(fut)
        return futures

    def close(self, wait: bool = True) -> None:
        """
        Close the executor:
          - wait=True: wait for submitted tasks to complete (default; suitable for graceful shutdown)
          - wait=False: return quickly; if cancel_on_close=True, attempts to cancel pending tasks
        """
        with self._lock:
            if self._closing.is_set():
                return
            self._closing.set()
            self._executor.shutdown(
                wait=wait, cancel_futures=(self._cancel_on_close and not wait)
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # Context manager exit: close and wait for tasks to finish
        self.close(wait=True)

    def on_task_submitted(self, item: Any, future: Future) -> None:
        # Triggered when a task is submitted. Default does nothing.
        pass

    def on_task_done(self, item: Any, future: Future) -> None:
        """
        Triggered when a task is done. Default logs exceptions (key for fire-and-forget).
        To customize behavior: inherit and override this method.
        - If you need the result (which may raise), call future.result() here.
        """
        try:
            _ = future.result()
        except Exception as e:
            logger.error(f"Batch task failed (item={item}), because {e}")

    def _attach_done_callback(self, fut: Future, item: Any) -> None:
        def _cb(f: Future, _item=item, self_ref=self):
            try:
                self_ref.on_task_done(_item, f)
            except Exception as e:
                logger.error(
                    f"Error in on_task_done callback (item={_item}), because {e}"
                )

        fut.add_done_callback(_cb)

    def _atexit_close(self) -> None:
        try:
            self.close(wait=False)
        except Exception:
            pass
