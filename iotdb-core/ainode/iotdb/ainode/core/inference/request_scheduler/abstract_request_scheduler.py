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

from abc import ABC, abstractmethod


class AbstractRequestScheduler(ABC):
    """
    Abstract base class for inference scheduling strategies.

    This class defines the high-level interface for scheduling inference requests.
    A scheduler is responsible for managing the execution order of inference tasks across different
    stages: waiting, running, and finished.

    Subclasses should implement specific scheduling logic.
    """

    def __init__(self, waiting_queue, running_queue, finished_queue):
        """
        Args:
            waiting_queue: Queue containing inference requests that are waiting to be executed.
            running_queue: Queue containing currently running inference tasks.
            finished_queue: Queue containing completed inference tasks.
        """
        self.waiting_queue = waiting_queue
        self.running_queue = running_queue
        self.finished_queue = finished_queue

    @abstractmethod
    def schedule_activate(self) -> list:
        """
        Select one or more inference requests from the waiting queue that are ready to be activated and processed.

        Returns:
            List: A list of inference requests that will be moved to the running queue.
        """
        pass

    @abstractmethod
    def schedule_step(self) -> list:
        """
        Select one or more inference requests from the running queue that are ready to perform the next inference step.

        Returns:
            List: A list of inference requests that are scheduled to run an inference step.
        """
        pass
