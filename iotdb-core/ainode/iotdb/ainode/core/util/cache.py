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
import sys
from collections import OrderedDict

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.util.decorator import singleton


def _estimate_size_in_byte(obj):
    if isinstance(obj, str):
        return len(obj) + 49
    elif isinstance(obj, int):
        return 28
    elif isinstance(obj, list):
        return 64 + sum(_estimate_size_in_byte(x) for x in obj)
    elif isinstance(obj, dict):
        return 280 + sum(
            _estimate_size_in_byte(k) + _estimate_size_in_byte(v)
            for k, v in obj.items()
        )
    else:
        return sys.getsizeof(obj)


def _get_item_memory(key, value) -> int:
    return _estimate_size_in_byte(key) + _estimate_size_in_byte(value)


@singleton
class MemoryLRUCache:
    def __init__(self):
        self.cache = OrderedDict()
        self.max_memory_bytes = (
            AINodeDescriptor().get_config().get_ain_data_storage_cache_size()
            * 1024
            * 1024
        )
        self.current_memory = 0

    def get(self, key):
        if key not in self.cache:
            return None
        value = self.cache[key]
        self.cache.move_to_end(key)
        return value

    def put(self, key, value):
        item_memory = _get_item_memory(key, value)

        if key in self.cache:
            old_value = self.cache[key]
            old_memory = _get_item_memory(key, old_value)
            self.current_memory -= old_memory
            self.current_memory += item_memory
            self._evict_if_needed()
            self.cache[key] = value
            self.cache.move_to_end(key)
        else:
            self.current_memory += item_memory
            self._evict_if_needed()
            self.cache[key] = value

    def _evict_if_needed(self):
        while self.current_memory > self.max_memory_bytes:
            if not self.cache:
                break
            key, value = self.cache.popitem(last=False)
            removed_memory = _get_item_memory(key, value)
            self.current_memory -= removed_memory

    def get_current_memory_mb(self) -> float:
        return self.current_memory / (1024 * 1024)
