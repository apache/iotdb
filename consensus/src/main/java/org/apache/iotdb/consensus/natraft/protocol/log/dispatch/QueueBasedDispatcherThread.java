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

package org.apache.iotdb.consensus.natraft.protocol.log.dispatch;

import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.natraft.protocol.log.VotingEntry;

import java.util.concurrent.BlockingQueue;

class QueueBasedDispatcherThread extends DispatcherThread {

  private final BlockingQueue<VotingEntry> logBlockingDeque;

  protected QueueBasedDispatcherThread(
      LogDispatcher logDispatcher,
      Peer receiver,
      BlockingQueue<VotingEntry> logBlockingDeque,
      DispatcherGroup group) {
    super(logDispatcher, receiver, group);
    this.logBlockingDeque = logBlockingDeque;
  }

  @Override
  protected boolean fetchLogs() throws InterruptedException {
    if (group.isDelayed()) {
      if (logBlockingDeque.size() < logDispatcher.maxBatchSize
          && System.nanoTime() - lastDispatchTime < 1_000_000_000L) {
        // the follower is being delayed, if there is not enough requests, and it has
        // dispatched recently, wait for a while to get a larger batch
        Thread.sleep(100);
        return false;
      }
    }

    synchronized (logBlockingDeque) {
      VotingEntry poll = logBlockingDeque.poll();
      if (poll != null) {
        currBatch.add(poll);
        logBlockingDeque.drainTo(currBatch, logDispatcher.maxBatchSize - 1);
      } else {
        if (group.getLogDispatcher().getMember().isLeader()) {
          logBlockingDeque.wait(1000);
        } else {
          logBlockingDeque.wait(5000);
        }
        return false;
      }
    }
    return true;
  }
}
