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
import hashlib
import threading


class ReadWriteLock:
    def __init__(self):
        self._reader_num = 0
        self._read_lock = threading.Lock()
        self._write_lock = threading.Lock()

    def acquire_read(self):
        with self._read_lock:
            if self._reader_num == 0:
                self._write_lock.acquire()
            self._reader_num += 1

    def release_read(self):
        with self._read_lock:
            self._reader_num -= 1
            if self._reader_num == 0:
                self._write_lock.release()

    def acquire_write(self):
        self._write_lock.acquire()

    def release_write(self):
        self._write_lock.release()

    class ReadLockContext:
        def __init__(self, rw_lock):
            self.rw_lock = rw_lock

        def __enter__(self):
            self.rw_lock.acquire_read()

        def __exit__(self, exc_type, exc_value, traceback):
            self.rw_lock.release_read()

    class WriteLockContext:
        def __init__(self, rw_lock):
            self.rw_lock = rw_lock

        def __enter__(self):
            self.rw_lock.acquire_write()

        def __exit__(self, exc_type, exc_value, traceback):
            self.rw_lock.release_write()

    def read_lock(self):
        return self.ReadLockContext(self)

    def write_lock(self):
        return self.WriteLockContext(self)


def hash_model_id(model_id):
    return int(hashlib.md5(str(model_id).encode()).hexdigest(), 16)


class ModelLockPool:
    def __init__(self, pool_size=16):
        self._pool = [ReadWriteLock() for _ in range(pool_size)]
        self._pool_size = pool_size

    def get_lock(self, model_id):
        pool_index = hash_model_id(model_id) % self._pool_size
        return self._pool[pool_index]
