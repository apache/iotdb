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

import org.apache.iotdb.db.storageengine.load.metrics.ActiveLoadingFilesMetricsSet;

import org.apache.tsfile.utils.Pair;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ActiveLoadPendingQueue {

  private final Set<String> pendingFileSet = new HashSet<>();
  private final Queue<Pair<String, Boolean>> pendingFileQueue = new ConcurrentLinkedQueue<>();

  private final Set<String> loadingFileSet = new HashSet<>();

  public synchronized boolean enqueue(final String file, final boolean isGeneratedByPipe) {
    if (!loadingFileSet.contains(file) && pendingFileSet.add(file)) {
      pendingFileQueue.offer(new Pair<>(file, isGeneratedByPipe));

      ActiveLoadingFilesMetricsSet.getInstance().recordQueuingFileCounter(1);
      return true;
    }
    return false;
  }

  public synchronized Pair<String, Boolean> dequeueFromPending() {
    final Pair<String, Boolean> pair = pendingFileQueue.poll();
    if (pair != null) {
      pendingFileSet.remove(pair.left);
      loadingFileSet.add(pair.left);

      ActiveLoadingFilesMetricsSet.getInstance().recordLoadingFileCounter(1);
      ActiveLoadingFilesMetricsSet.getInstance().recordQueuingFileCounter(-1);
    }
    return pair;
  }

  public synchronized void removeFromLoading(final String file) {
    loadingFileSet.remove(file);

    ActiveLoadingFilesMetricsSet.getInstance().recordLoadingFileCounter(-1);
  }

  public int size() {
    return pendingFileQueue.size() + loadingFileSet.size();
  }

  public boolean isEmpty() {
    return pendingFileQueue.isEmpty() && loadingFileSet.isEmpty();
  }
}
