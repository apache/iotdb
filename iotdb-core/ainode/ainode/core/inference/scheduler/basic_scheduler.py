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

from ainode.core.inference.scheduler.abstract_scheduler import AbstractScheduler
from ainode.core.inference.inference_request import InferenceRequest
from ainode.core.log import Logger
import torch

logger = Logger()

class BasicScheduler(AbstractScheduler):

    def __init__(self, waiting_queue, running_queue, finished_queue, max_memory_bytes, max_batch_size):
        super().__init__(waiting_queue, running_queue, finished_queue)
        self.max_memory_bytes = max_memory_bytes
        self.max_batch_size = max_batch_size

    def memory_is_available(self):
        used = torch.cuda.memory_allocated()
        return used < self.max_memory_bytes

    def schedule_activate(self) -> list:
        # TODO: Check memory size before activating requests
        requests = []
        while not self.waiting_queue.empty():
            if not self.memory_is_available():
                break
            request = self.waiting_queue.get()
            request.mark_running()
            self.running_queue.put(request)
            requests.append(request)
        return requests

    def schedule_step(self) -> list:
        # TODO: Check memory size before executing requests
        requests = []
        while not self.running_queue.empty() and len(requests) < self.max_batch_size:
            if not self.memory_is_available():
                break
            requests.append(self.running_queue.get())
        return requests


