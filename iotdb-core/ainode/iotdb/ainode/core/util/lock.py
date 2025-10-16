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
1import hashlib
1import threading
1
1
1class ReadWriteLock:
1    def __init__(self):
1        self._reader_num = 0
1        self._read_lock = threading.Lock()
1        self._write_lock = threading.Lock()
1
1    def acquire_read(self):
1        with self._read_lock:
1            if self._reader_num == 0:
1                self._write_lock.acquire()
1            self._reader_num += 1
1
1    def release_read(self):
1        with self._read_lock:
1            self._reader_num -= 1
1            if self._reader_num == 0:
1                self._write_lock.release()
1
1    def acquire_write(self):
1        self._write_lock.acquire()
1
1    def release_write(self):
1        self._write_lock.release()
1
1    class ReadLockContext:
1        def __init__(self, rw_lock):
1            self.rw_lock = rw_lock
1
1        def __enter__(self):
1            self.rw_lock.acquire_read()
1
1        def __exit__(self, exc_type, exc_value, traceback):
1            self.rw_lock.release_read()
1
1    class WriteLockContext:
1        def __init__(self, rw_lock):
1            self.rw_lock = rw_lock
1
1        def __enter__(self):
1            self.rw_lock.acquire_write()
1
1        def __exit__(self, exc_type, exc_value, traceback):
1            self.rw_lock.release_write()
1
1    def read_lock(self):
1        return self.ReadLockContext(self)
1
1    def write_lock(self):
1        return self.WriteLockContext(self)
1
1
1def hash_model_id(model_id):
1    return int(hashlib.md5(str(model_id).encode()).hexdigest(), 16)
1
1
1class ModelLockPool:
1    def __init__(self, pool_size=16):
1        self._pool = [ReadWriteLock() for _ in range(pool_size)]
1        self._pool_size = pool_size
1
1    def get_lock(self, model_id):
1        pool_index = hash_model_id(model_id) % self._pool_size
1        return self._pool[pool_index]
1