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

package org.apache.iotdb.consensus.multileader.logdispatcher;

import org.apache.iotdb.consensus.multileader.conf.MultiLeaderConsensusConfig;
import org.apache.iotdb.consensus.multileader.logdispatcher.LogDispatcher.LogDispatcherThread;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class SyncStatus {

  private final LogDispatcherThread thread;
  private final List<PendingBatch> pendingBatches = new LinkedList<>();

  public SyncStatus(LogDispatcherThread thread) {
    this.thread = thread;
  }

  public void addNextBatch(PendingBatch batch) throws InterruptedException {
    synchronized (this) {
      while (pendingBatches.size() >= MultiLeaderConsensusConfig.MAX_PENDING_BATCH) {
        wait();
      }
      pendingBatches.add(batch);
    }
  }

  public void removeBatch(PendingBatch batch) {
    synchronized (this) {
      if (pendingBatches.size() > 0 && pendingBatches.get(0).equals(batch)) {
        pendingBatches.remove(0);
        thread.getController().updateAndGet(batch.getEndIndex());
        notify();
      }
      pendingBatches.remove(batch);
    }
  }

  public Optional<Long> getMaxPendingIndex() {
    if (pendingBatches.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(pendingBatches.get(pendingBatches.size() - 1).getEndIndex());
  }
}
