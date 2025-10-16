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

import os

import psutil
import torch

from iotdb.ainode.core.inference.request_scheduler.abstract_request_scheduler import (
    AbstractRequestScheduler,
)
from iotdb.ainode.core.log import Logger

logger = Logger()


class BasicRequestScheduler(AbstractRequestScheduler):
    """
    A simple FIFO request scheduler that selects requests based on memory availability and activation/step size.
    """

    def __init__(
        self,
        waiting_queue,
        running_queue,
        finished_queue,
        pool_id,
        max_memory_bytes=1 << 30,
        max_activate_size=10,
        max_step_size=10,
    ):
        super().__init__(waiting_queue, running_queue, finished_queue)
        self.max_memory_bytes = max_memory_bytes
        self.max_activate_size = max_activate_size
        self.max_step_size = max_step_size
        self.pool_id = pool_id
        self.device = None

    def memory_is_available(self):
        if "cuda" in self.device.type:
            used = torch.cuda.memory_allocated(self.device)
            reserved = torch.cuda.memory_reserved(self.device)
        elif "cpu" in self.device.type:
            process = psutil.Process(os.getpid())
            used = process.memory_info().rss
            reserved = used
        else:
            used = 0
            reserved = 0
            logger.warning(
                f"[Inference] Unsupported device type: {self.device.type}. Memory checks will not be performed."
            )
        logger.debug(
            f"[Inference][Device-{self.device}][Pool-{self.pool_id}] "
            f"Memory used: {used/1024**2:.2f} MB, Max memory: {self.max_memory_bytes/1024**2:.2f} MB"
        )
        return used < self.max_memory_bytes

    def schedule_activate(self) -> list:
        requests = []
        while not self.waiting_queue.empty() and len(requests) < self.max_activate_size:
            if not self.memory_is_available():
                break
            requests.append(self.waiting_queue.get())
        return requests

    def schedule_step(self) -> list:
        requests = []
        while not self.running_queue.empty() and len(requests) < self.max_step_size:
            if not self.memory_is_available():
                break
            requests.append(self.running_queue.get())
        return requests
