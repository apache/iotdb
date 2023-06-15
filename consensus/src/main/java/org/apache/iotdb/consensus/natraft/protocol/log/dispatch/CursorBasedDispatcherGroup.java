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

import java.util.concurrent.atomic.AtomicLong;

public class CursorBasedDispatcherGroup extends DispatcherGroup {
  private AtomicLong cursor = new AtomicLong();

  public CursorBasedDispatcherGroup(
      Peer peer, LogDispatcher logDispatcher, int maxBindingThreadNum, int minBindingThreadNum) {
    super(peer, logDispatcher, maxBindingThreadNum, minBindingThreadNum);
    this.cursor.set(logDispatcher.getMember().getSafeIndex() + 1);
    init();
  }

  protected DispatcherThread newDispatcherThread(Peer node) {
    return new CursorBasedDispatcherThread(logDispatcher, node, cursor, this);
  }

  public int compareTo(DispatcherGroup o) {
    if (!(o instanceof CursorBasedDispatcherGroup)) {
      return 0;
    }
    return -Long.compare(this.cursor.get(), ((CursorBasedDispatcherGroup) o).cursor.get());
  }

  public void wakeUp() {
    synchronized (cursor) {
      cursor.notifyAll();
    }
  }

  @Override
  public String toString() {
    return "{"
        + "rate="
        + rateLimiter.getRate()
        + ", delayed="
        + isDelayed()
        + ", cursor="
        + cursor.get()
        + ", dispatcherNum="
        + dynamicThreadGroup.getThreadCnt().get()
        + "}";
  }

  public boolean add(VotingEntry request) {
    wakeUp();
    return true;
  }
}
