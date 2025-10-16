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
1from abc import ABC, abstractmethod
1
1
1class AbstractRequestScheduler(ABC):
1    """
1    Abstract base class for inference scheduling strategies.
1
1    This class defines the high-level interface for scheduling inference requests.
1    A scheduler is responsible for managing the execution order of inference tasks across different
1    stages: waiting, running, and finished.
1
1    Subclasses should implement specific scheduling logic.
1    """
1
1    def __init__(self, waiting_queue, running_queue, finished_queue):
1        """
1        Args:
1            waiting_queue: Queue containing inference requests that are waiting to be executed.
1            running_queue: Queue containing currently running inference tasks.
1            finished_queue: Queue containing completed inference tasks.
1        """
1        self.waiting_queue = waiting_queue
1        self.running_queue = running_queue
1        self.finished_queue = finished_queue
1
1    @abstractmethod
1    def schedule_activate(self) -> list:
1        """
1        Select one or more inference requests from the waiting queue that are ready to be activated and processed.
1
1        Returns:
1            List: A list of inference requests that will be moved to the running queue.
1        """
1        pass
1
1    @abstractmethod
1    def schedule_step(self) -> list:
1        """
1        Select one or more inference requests from the running queue that are ready to perform the next inference step.
1
1        Returns:
1            List: A list of inference requests that are scheduled to run an inference step.
1        """
1        pass
1