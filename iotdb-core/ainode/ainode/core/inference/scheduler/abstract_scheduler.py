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

class AbstractScheduler(ABC):
    """
    This is the abstract scheduler to schedule inference.
    """

    def __init__(self, waiting_queue, running_queue, finished_queue):
        self.waiting_queue = waiting_queue
        self.running_queue = running_queue
        self.finished_queue = finished_queue

    @abstractmethod
    def schedule_activate(self) -> list:
        pass

    @abstractmethod
    def schedule_step(self) -> list:
        pass
