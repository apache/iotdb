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

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ActiveLoadPendingQueue {

  private final Set<String> pendingFileSet = new HashSet<>();
  private final Queue<ActiveLoadEntry> pendingFileQueue = new ConcurrentLinkedQueue<>();

  private final Set<String> loadingFileSet = new HashSet<>();

  public synchronized boolean enqueue(
      final String file,
      final String pendingDir,
      final boolean isGeneratedByPipe,
      final boolean isTableModel) {
    if (!loadingFileSet.contains(file) && pendingFileSet.add(file)) {
      pendingFileQueue.offer(
          new ActiveLoadEntry(file, pendingDir, isGeneratedByPipe, isTableModel));

      ActiveLoadingFilesNumberMetricsSet.getInstance().increaseQueuingFileCounter(1);
      return true;
    }
    return false;
  }

  public synchronized ActiveLoadEntry dequeueFromPending() {
    final ActiveLoadEntry entry = pendingFileQueue.poll();
    if (entry != null) {
      pendingFileSet.remove(entry.getFile());
      loadingFileSet.add(entry.getFile());

      ActiveLoadingFilesNumberMetricsSet.getInstance().increaseLoadingFileCounter(1);
      ActiveLoadingFilesNumberMetricsSet.getInstance().increaseQueuingFileCounter(-1);
    }
    return entry;
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

  public static class ActiveLoadEntry {
    private final String file;
    private final String pendingDir;
    private final boolean isGeneratedByPipe;
    private final boolean isTableModel;

    public ActiveLoadEntry(
        String file, String pendingDir, boolean isGeneratedByPipe, boolean isTableModel) {
      this.file = file;
      this.pendingDir = pendingDir;
      this.isGeneratedByPipe = isGeneratedByPipe;
      this.isTableModel = isTableModel;
    }

    public String getFile() {
      return file;
    }

    public String getPendingDir() {
      return pendingDir;
    }

    public boolean isGeneratedByPipe() {
      return isGeneratedByPipe;
    }

    public boolean isTableModel() {
      return isTableModel;
    }
  }
}
