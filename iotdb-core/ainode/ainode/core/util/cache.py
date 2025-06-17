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
import time
from collections import OrderedDict

from ainode.core.config import AINodeDescriptor
from ainode.core.log import Logger
from ainode.core.util.decorator import singleton

logger = Logger()


def _estimate_size_in_byte(obj):
    """Estimate object size in bytes with enhanced support for model objects"""
    if isinstance(obj, str):
        return len(obj) + 49
    elif isinstance(obj, int):
        return 28
    elif isinstance(obj, float):
        return 24
    elif isinstance(obj, list):
        return 64 + sum(_estimate_size_in_byte(x) for x in obj)
    elif isinstance(obj, dict):
        return 280 + sum(
            _estimate_size_in_byte(k) + _estimate_size_in_byte(v)
            for k, v in obj.items()
        )
    elif isinstance(obj, tuple):
        return 56 + sum(_estimate_size_in_byte(x) for x in obj)
    else:
        # Enhanced size estimation for complex objects like models
        try:
            return sys.getsizeof(obj)
        except Exception:
            # Fallback for objects that can't be sized
            return 1024  # Conservative estimate


def _get_item_memory(key, value) -> int:
    """Calculate total memory usage for a key-value pair"""
    return _estimate_size_in_byte(key) + _estimate_size_in_byte(value)


@singleton
class MemoryLRUCache:
    """Enhanced LRU cache with better memory management for IoTDB models"""

    def __init__(self):
        self.cache = OrderedDict()
        self.max_memory_bytes = (
            AINodeDescriptor().get_config().get_ain_data_storage_cache_size()
            * 1024
            * 1024
        )
        self.current_memory = 0
        self._cache_stats = {"hits": 0, "misses": 0, "evictions": 0, "total_items": 0}
        self._last_cleanup = time.time()

    def get(self, key):
        """Get item from cache with LRU update"""
        if key not in self.cache:
            self._cache_stats["misses"] += 1
            return None

        value = self.cache[key]
        self.cache.move_to_end(key)
        self._cache_stats["hits"] += 1
        return value

    def put(self, key, value):
        """Put item into cache with memory management"""
        item_memory = _get_item_memory(key, value)

        if key in self.cache:
            # Update existing item
            old_value = self.cache[key]
            old_memory = _get_item_memory(key, old_value)
            self.current_memory -= old_memory
            self.current_memory += item_memory
            self._evict_if_needed()
            self.cache[key] = value
            self.cache.move_to_end(key)
        else:
            # Add new item
            self.current_memory += item_memory
            self._evict_if_needed()
            self.cache[key] = value
            self._cache_stats["total_items"] += 1

    def _evict_if_needed(self):
        """Evict items if memory limit is exceeded"""
        while self.current_memory > self.max_memory_bytes:
            if not self.cache:
                break
            key, value = self.cache.popitem(last=False)
            removed_memory = _get_item_memory(key, value)
            self.current_memory -= removed_memory
            self._cache_stats["evictions"] += 1
            logger.debug(f"Evicted cache item: {key}, freed {removed_memory} bytes")

    def remove(self, key):
        """Manually remove item from cache"""
        if key in self.cache:
            value = self.cache.pop(key)
            removed_memory = _get_item_memory(key, value)
            self.current_memory -= removed_memory
            logger.debug(
                f"Manually removed cache item: {key}, freed {removed_memory} bytes"
            )
            return True
        return False

    def clear(self):
        """Clear all cache items"""
        self.cache.clear()
        self.current_memory = 0
        self._cache_stats["total_items"] = 0
        logger.info("Cache cleared")

    def get_current_memory_mb(self) -> float:
        """Get current memory usage in MB"""
        return self.current_memory / (1024 * 1024)

    def get_cache_stats(self) -> dict:
        """Get cache statistics"""
        total_requests = self._cache_stats["hits"] + self._cache_stats["misses"]
        hit_rate = (
            self._cache_stats["hits"] / total_requests if total_requests > 0 else 0
        )

        return {
            "hits": self._cache_stats["hits"],
            "misses": self._cache_stats["misses"],
            "evictions": self._cache_stats["evictions"],
            "total_items": len(self.cache),
            "hit_rate": hit_rate,
            "memory_usage_mb": self.get_current_memory_mb(),
            "memory_limit_mb": self.max_memory_bytes / (1024 * 1024),
        }

    def cleanup_if_needed(self):
        """Perform periodic cleanup if needed"""
        current_time = time.time()
        if current_time - self._last_cleanup > 3600:  # Cleanup every hour
            # Log cache statistics
            stats = self.get_cache_stats()
            logger.info(f"Cache stats: {stats}")
            self._last_cleanup = current_time
