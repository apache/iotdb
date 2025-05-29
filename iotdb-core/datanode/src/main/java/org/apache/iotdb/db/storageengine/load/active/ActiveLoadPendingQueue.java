/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load.active;

import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesNumberMetricsSet;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ActiveLoadPendingQueue {

  private final Set<String> pendingFileSet = new HashSet<>();
  private final Queue<Triple<String, Boolean, Boolean>> pendingFileQueue =
      new ConcurrentLinkedQueue<>();

  private final Set<String> loadingFileSet = new HashSet<>();

  public synchronized boolean enqueue(
      final String file, final boolean isTableModel, final boolean isGeneratedByPipe) {
    if (!loadingFileSet.contains(file) && pendingFileSet.add(file)) {
      pendingFileQueue.offer(new ImmutableTriple<>(file, isGeneratedByPipe, isTableModel));

      ActiveLoadingFilesNumberMetricsSet.getInstance().increaseQueuingFileCounter(1);
      return true;
    }
    return false;
  }

  public synchronized Triple<String, Boolean, Boolean> dequeueFromPending() {
    final Triple<String, Boolean, Boolean> pair = pendingFileQueue.poll();
    if (pair != null) {
      pendingFileSet.remove(pair.getLeft());
      loadingFileSet.add(pair.getLeft());

      ActiveLoadingFilesNumberMetricsSet.getInstance().increaseLoadingFileCounter(1);
      ActiveLoadingFilesNumberMetricsSet.getInstance().increaseQueuingFileCounter(-1);
    }
    return pair;
  }

  public synchronized void removeFromLoading(final String file) {
    loadingFileSet.remove(file);

    ActiveLoadingFilesNumberMetricsSet.getInstance().increaseLoadingFileCounter(-1);
  }

  public synchronized boolean isFilePendingOrLoading(final String file) {
    return loadingFileSet.contains(file) || pendingFileSet.contains(file);
  }

  public int size() {
    return pendingFileQueue.size() + loadingFileSet.size();
  }

  public boolean isEmpty() {
    return pendingFileQueue.isEmpty() && loadingFileSet.isEmpty();
  }
}
