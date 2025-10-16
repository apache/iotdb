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
1import os
1
1import psutil
1import torch
1
1from iotdb.ainode.core.inference.request_scheduler.abstract_request_scheduler import (
1    AbstractRequestScheduler,
1)
1from iotdb.ainode.core.log import Logger
1
1logger = Logger()
1
1
1class BasicRequestScheduler(AbstractRequestScheduler):
1    """
1    A simple FIFO request scheduler that selects requests based on memory availability and activation/step size.
1    """
1
1    def __init__(
1        self,
1        waiting_queue,
1        running_queue,
1        finished_queue,
1        pool_id,
1        max_memory_bytes=1 << 30,
1        max_activate_size=10,
1        max_step_size=10,
1    ):
1        super().__init__(waiting_queue, running_queue, finished_queue)
1        self.max_memory_bytes = max_memory_bytes
1        self.max_activate_size = max_activate_size
1        self.max_step_size = max_step_size
1        self.pool_id = pool_id
1        self.device = None
1
1    def memory_is_available(self):
1        if "cuda" in self.device.type:
1            used = torch.cuda.memory_allocated(self.device)
1            reserved = torch.cuda.memory_reserved(self.device)
1        elif "cpu" in self.device.type:
1            process = psutil.Process(os.getpid())
1            used = process.memory_info().rss
1            reserved = used
1        else:
1            used = 0
1            reserved = 0
1            logger.warning(
1                f"[Inference] Unsupported device type: {self.device.type}. Memory checks will not be performed."
1            )
1        logger.debug(
1            f"[Inference][Device-{self.device}][Pool-{self.pool_id}] "
1            f"Memory used: {used/1024**2:.2f} MB, Max memory: {self.max_memory_bytes/1024**2:.2f} MB"
1        )
1        return used < self.max_memory_bytes
1
1    def schedule_activate(self) -> list:
1        requests = []
1        while not self.waiting_queue.empty() and len(requests) < self.max_activate_size:
1            if not self.memory_is_available():
1                break
1            requests.append(self.waiting_queue.get())
1        return requests
1
1    def schedule_step(self) -> list:
1        requests = []
1        while not self.running_queue.empty() and len(requests) < self.max_step_size:
1            if not self.memory_is_available():
1                break
1            requests.append(self.running_queue.get())
1        return requests
1