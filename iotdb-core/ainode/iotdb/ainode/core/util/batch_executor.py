1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1
1import atexit
1import threading
1from concurrent.futures import Future, ThreadPoolExecutor
1from typing import Any, Callable, Iterable, List, Optional
1
1from iotdb.ainode.core.log import Logger
1
1logger = Logger()
1
1
1class BatchExecutor:
1    """
1    Abstract batch concurrent executor.
1    - Default fire-and-forget: automatically logs task exceptions (ignores return values)
1    - Allows customization of completion logic by overriding callback hooks via inheritance
1    - Supports single/batch submission, explicit closure, context management, and exit-time cleanup
1    """
1
1    def __init__(
1        self,
1        max_workers: int = 8,
1        thread_name_prefix: str = "batch-exec",
1        cancel_on_close: bool = False,
1    ):
1        self._executor = ThreadPoolExecutor(
1            max_workers=max_workers, thread_name_prefix=thread_name_prefix
1        )
1        self._closing = threading.Event()
1        self._lock = threading.Lock()
1        self._cancel_on_close = cancel_on_close
1        atexit.register(self._atexit_close)
1
1    def submit(self, fn: Callable[..., Any], *args, **kwargs) -> Optional[Future]:
1        """
1        Submit a single task.
1        Default fire-and-forget: internally attaches a done callback to automatically log exceptions.
1        To take over result handling, inherit and override on_task_done().
1        """
1        if self._closing.is_set():
1            raise RuntimeError("Executor is closing; reject new submissions.")
1
1        fut = self._executor.submit(fn, *args, **kwargs)
1        self._attach_done_callback(fut, item=None)
1        self.on_task_submitted(item=None, future=fut)
1        return fut
1
1    def submit_batch(
1        self, items: Iterable[Any], fn: Callable[[Any], Any]
1    ) -> List[Future]:
1        """
1        Submit a batch of tasks. Executes fn(item) for each item.
1        """
1        if self._closing.is_set():
1            raise RuntimeError("Executor is closing; reject new submissions.")
1
1        futures: List[Future] = []
1        for item in items:
1            fut = self._executor.submit(fn, item)
1            self._attach_done_callback(fut, item=item)
1            self.on_task_submitted(item=item, future=fut)
1            futures.append(fut)
1        return futures
1
1    def close(self, wait: bool = True) -> None:
1        """
1        Close the executor:
1          - wait=True: wait for submitted tasks to complete (default; suitable for graceful shutdown)
1          - wait=False: return quickly; if cancel_on_close=True, attempts to cancel pending tasks
1        """
1        with self._lock:
1            if self._closing.is_set():
1                return
1            self._closing.set()
1            self._executor.shutdown(
1                wait=wait, cancel_futures=(self._cancel_on_close and not wait)
1            )
1
1    def __enter__(self):
1        return self
1
1    def __exit__(self, exc_type, exc, tb):
1        # Context manager exit: close and wait for tasks to finish
1        self.close(wait=True)
1
1    def on_task_submitted(self, item: Any, future: Future) -> None:
1        # Triggered when a task is submitted. Default does nothing.
1        pass
1
1    def on_task_done(self, item: Any, future: Future) -> None:
1        """
1        Triggered when a task is done. Default logs exceptions (key for fire-and-forget).
1        To customize behavior: inherit and override this method.
1        - If you need the result (which may raise), call future.result() here.
1        """
1        try:
1            _ = future.result()
1        except Exception as e:
1            logger.error(f"Batch task failed (item={item}), because {e}")
1
1    def _attach_done_callback(self, fut: Future, item: Any) -> None:
1        def _cb(f: Future, _item=item, self_ref=self):
1            try:
1                self_ref.on_task_done(_item, f)
1            except Exception as e:
1                logger.error(
1                    f"Error in on_task_done callback (item={_item}), because {e}"
1                )
1
1        fut.add_done_callback(_cb)
1
1    def _atexit_close(self) -> None:
1        try:
1            self.close(wait=False)
1        except Exception:
1            pass
1