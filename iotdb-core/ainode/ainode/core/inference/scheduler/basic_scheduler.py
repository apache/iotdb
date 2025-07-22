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

import torch

from ainode.core.inference.inference_request import InferenceRequest
from ainode.core.inference.scheduler.abstract_scheduler import AbstractScheduler
from ainode.core.log import Logger

logger = Logger()


class BasicScheduler(AbstractScheduler):

    def __init__(
        self,
        waiting_queue,
        running_queue,
        finished_queue,
        max_memory_bytes=1 << 30,
        max_activate_size=10,
        max_step_size=10,
    ):
        super().__init__(waiting_queue, running_queue, finished_queue)
        self.max_memory_bytes = max_memory_bytes
        self.max_activate_size = max_activate_size
        self.max_step_size = max_step_size

    def memory_is_available(self):
        used = torch.cuda.memory_allocated()  # memory allocated to tensors
        reserved = (
            torch.cuda.memory_reserved()
        )  # memory reserved by the caching allocator
        # logger.debug(f"Memory used: {used} bytes, Max memory: {self.max_memory_bytes} bytes")
        return used < self.max_memory_bytes

    def schedule_activate(self) -> list:
        requests = []
        while not self.waiting_queue.empty() and len(requests) < self.max_activate_size:
            if not self.memory_is_available():
                break
            request = self.waiting_queue.get()
            request.mark_running()
            self.running_queue.put(request)
            requests.append(request)
        return requests

    def schedule_step(self) -> list:
        requests = []
        while not self.running_queue.empty() and len(requests) < self.max_step_size:
            if not self.memory_is_available():
                break
            requests.append(self.running_queue.get())
        return requests
