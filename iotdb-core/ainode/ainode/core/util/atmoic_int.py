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

import threading


class AtomicInt:
    def __init__(self, value=0):
        self._value = value
        self._lock = threading.Lock()

    def get(self) -> int:
        with self._lock:
            return self._value

    def incr(self, delta=1):
        with self._lock:
            self._value += delta
            return self._value

    def decr(self, delta=1):
        with self._lock:
            self._value -= delta
            return self._value
