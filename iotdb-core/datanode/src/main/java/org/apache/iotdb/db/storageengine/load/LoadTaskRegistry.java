/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.i18n.StorageEngineMessages.STORAGE_EXCEPTION_FAILED_TO_CREATE_TSFILEWRITERMANAGER_FOR_UUID_S_BECAUSE_A0D68950;

/**
 * LOAD task registry: owns the uuid -&gt; {@link TsFileWriterManager} lifecycle mapping of one
 * in-progress LOAD task. Single-task operations (get/create/remove) only take the cheap read lock,
 * so concurrent LOAD applies of different tasks never contend; only full-set operations (snapshot
 * inclusion, stop) take the write lock so the task set is stable while every live task is visited.
 */
final class LoadTaskRegistry {

  private final ConcurrentHashMap<String, TsFileWriterManager> tasks = new ConcurrentHashMap<>();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  TsFileWriterManager getOrCreate(String uuid, LoadTaskFactory factory) throws IOException {
    lock.readLock().lock();
    try {
      final AtomicReference<Exception> exception = new AtomicReference<>();
      final TsFileWriterManager writerManager =
          tasks.computeIfAbsent(
              uuid,
              id -> {
                try {
                  return factory.create(id);
                } catch (Exception e) {
                  exception.set(e);
                  return null;
                }
              });
      if (exception.get() != null || writerManager == null) {
        final String message =
            String.format(
                STORAGE_EXCEPTION_FAILED_TO_CREATE_TSFILEWRITERMANAGER_FOR_UUID_S_BECAUSE_A0D68950,
                uuid);
        throw new IOException(message, exception.get());
      }
      return writerManager;
    } finally {
      lock.readLock().unlock();
    }
  }

  Optional<TsFileWriterManager> get(String uuid) {
    return Optional.ofNullable(tasks.get(uuid));
  }

  boolean contains(String uuid) {
    return tasks.containsKey(uuid);
  }

  TsFileWriterManager remove(String uuid) {
    lock.readLock().lock();
    try {
      return tasks.remove(uuid);
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Visits every task under the write lock so creation/removal cannot happen concurrently. */
  void snapshot(LoadTaskVisitor visitor) throws IOException {
    lock.writeLock().lock();
    try {
      for (TsFileWriterManager writerManager : new ArrayList<>(tasks.values())) {
        visitor.visit(writerManager);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  void clear() {
    lock.writeLock().lock();
    try {
      tasks.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @FunctionalInterface
  interface LoadTaskFactory {
    TsFileWriterManager create(String uuid) throws Exception;
  }

  @FunctionalInterface
  interface LoadTaskVisitor {
    void visit(TsFileWriterManager writerManager) throws IOException;
  }
}
