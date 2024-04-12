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

import java.util.ArrayDeque;
import java.util.Queue;

public class QueueBasedDispatcherGroup extends DispatcherGroup {

  private final Queue<VotingEntry> entryQueue;

  public QueueBasedDispatcherGroup(
      Peer peer, LogDispatcher logDispatcher, int maxBindingThreadNum, int minBindingThreadNum) {
    super(peer, logDispatcher, maxBindingThreadNum, minBindingThreadNum);
    this.entryQueue = new ArrayDeque<>(logDispatcher.getConfig().getMaxNumOfLogsInMem());
    init();
  }

  protected DispatcherThread newDispatcherThread(Peer node) {
    return new QueueBasedDispatcherThread(logDispatcher, node, entryQueue, this);
  }

  @Override
  public int compareTo(DispatcherGroup o) {
    if (!(o instanceof QueueBasedDispatcherGroup)) {
      return 0;
    }
    return Long.compare(this.getQueueSize(), ((QueueBasedDispatcherGroup) o).getQueueSize());
  }

  public int getQueueSize() {
    return entryQueue.size();
  }

  public void wakeUp() {
    synchronized (entryQueue) {
      entryQueue.notifyAll();
    }
  }

  @Override
  public String toString() {
    return "{"
        + "rate="
        + rateLimiter.getRate()
        + ", delayed="
        + isDelayed()
        + ", queueSize="
        + entryQueue.size()
        + ", dispatcherNum="
        + dynamicThreadGroup.getThreadCnt().get()
        + "}";
  }

  public boolean add(VotingEntry request) {
    synchronized (entryQueue) {
      boolean added = entryQueue.add(request);
      if (added) {
        entryQueue.notifyAll();
      }
      return added;
    }
  }
}
