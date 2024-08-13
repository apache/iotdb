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

package org.apache.iotdb.db.queryengine.execution.load.active;

import org.apache.tsfile.utils.Pair;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ActiveLoadPendingQueue {

  private final Set<String> loadingFileSet = new HashSet<>();
  private final Queue<Pair<String, Boolean>> loadingFileQueue = new ConcurrentLinkedQueue<>();

  public synchronized boolean enqueue(final String file, final boolean isGeneratedByPipe) {
    if (loadingFileSet.add(file)) {
      loadingFileQueue.offer(new Pair<>(file, isGeneratedByPipe));
      return true;
    }
    return false;
  }

  public synchronized Pair<String, Boolean> dequeue() {
    final Pair<String, Boolean> pair = loadingFileQueue.poll();
    if (pair != null) {
      loadingFileSet.remove(pair.left);
    }
    return pair;
  }

  public int size() {
    return loadingFileQueue.size();
  }

  public boolean isEmpty() {
    return loadingFileQueue.isEmpty();
  }
}
