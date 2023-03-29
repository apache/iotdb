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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.LongSupplier;

public class SyncStatus {

  private final IoTConsensusConfig config;
  private final IndexController controller;
  private final LongSupplier supplier;
  private final LinkedList<Batch> pendingBatches = new LinkedList<>();
  private final IoTConsensusMemoryManager iotConsensusMemoryManager =
      IoTConsensusMemoryManager.getInstance();

  public SyncStatus(IndexController controller, IoTConsensusConfig config, LongSupplier supplier) {
    this.controller = controller;
    this.config = config;
    this.supplier = supplier;
  }

  /** we may block here if the synchronization pipeline is full. */
  public void addNextBatch(Batch batch) throws InterruptedException {
    synchronized (this) {
      while (pendingBatches.size() >= config.getReplication().getMaxPendingBatchesNum()
          || !iotConsensusMemoryManager.reserve(batch.getSerializedSize(), false)) {
        wait();
      }
      pendingBatches.add(batch);
    }
  }

  /**
   * We only set a flag if this batch is not the first one. Notice, We need to confirm that the
   * batch in the parameter is actually in pendingBatches, rather than a reference to a different
   * object with equal data, so we do not inherit method equals for Batch
   */
  public void removeBatch(Batch batch) {
    synchronized (this) {
      batch.setSynced(true);
      if (!pendingBatches.isEmpty() && pendingBatches.get(0).equals(batch)) {
        Iterator<Batch> iterator = pendingBatches.iterator();
        Batch current = iterator.next();
        while (current.isSynced()) {
          controller.updateAndGet(
              current.getEndIndex(), supplier.getAsLong() == current.getEndIndex());
          iterator.remove();
          iotConsensusMemoryManager.free(current.getSerializedSize(), false);
          if (iterator.hasNext()) {
            current = iterator.next();
          } else {
            break;
          }
        }
        // wake up logDispatcherThread that might be blocked
        notifyAll();
      }
    }
  }

  public void free() {
    long size = 0;
    for (Batch pendingBatch : pendingBatches) {
      size += pendingBatch.getSerializedSize();
    }
    pendingBatches.clear();
    controller.updateAndGet(0L, true);
    iotConsensusMemoryManager.free(size, false);
  }

  /** Gets the first index that is not currently synchronized. */
  public long getNextSendingIndex() {
    // we do not use ReentrantReadWriteLock because there will be only one thread reading this field
    synchronized (this) {
      return 1
          + (pendingBatches.isEmpty()
              ? controller.getCurrentIndex()
              : pendingBatches.getLast().getEndIndex());
    }
  }

  @TestOnly
  public List<Batch> getPendingBatches() {
    return pendingBatches;
  }
}
