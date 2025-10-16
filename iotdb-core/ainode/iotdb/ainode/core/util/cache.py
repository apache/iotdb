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
1import sys
1from collections import OrderedDict
1
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.util.decorator import singleton
1
1
1def _estimate_size_in_byte(obj):
1    if isinstance(obj, str):
1        return len(obj) + 49
1    elif isinstance(obj, int):
1        return 28
1    elif isinstance(obj, list):
1        return 64 + sum(_estimate_size_in_byte(x) for x in obj)
1    elif isinstance(obj, dict):
1        return 280 + sum(
1            _estimate_size_in_byte(k) + _estimate_size_in_byte(v)
1            for k, v in obj.items()
1        )
1    else:
1        return sys.getsizeof(obj)
1
1
1def _get_item_memory(key, value) -> int:
1    return _estimate_size_in_byte(key) + _estimate_size_in_byte(value)
1
1
1@singleton
1class MemoryLRUCache:
1    def __init__(self):
1        self.cache = OrderedDict()
1        self.max_memory_bytes = (
1            AINodeDescriptor().get_config().get_ain_data_storage_cache_size()
1            * 1024
1            * 1024
1        )
1        self.current_memory = 0
1
1    def get(self, key):
1        if key not in self.cache:
1            return None
1        value = self.cache[key]
1        self.cache.move_to_end(key)
1        return value
1
1    def put(self, key, value):
1        item_memory = _get_item_memory(key, value)
1
1        if key in self.cache:
1            old_value = self.cache[key]
1            old_memory = _get_item_memory(key, old_value)
1            self.current_memory -= old_memory
1            self.current_memory += item_memory
1            self._evict_if_needed()
1            self.cache[key] = value
1            self.cache.move_to_end(key)
1        else:
1            self.current_memory += item_memory
1            self._evict_if_needed()
1            self.cache[key] = value
1
1    def _evict_if_needed(self):
1        while self.current_memory > self.max_memory_bytes:
1            if not self.cache:
1                break
1            key, value = self.cache.popitem(last=False)
1            removed_memory = _get_item_memory(key, value)
1            self.current_memory -= removed_memory
1
1    def get_current_memory_mb(self) -> float:
1        return self.current_memory / (1024 * 1024)
1