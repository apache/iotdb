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

package org.apache.iotdb.consensus.natraft.protocol;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PeerInfo {
  private AtomicLong matchIndex;
  private AtomicInteger inconsistentHeartbeatNum = new AtomicInteger();
  // lastLogIndex from the last heartbeat
  private long lastHeartBeatIndex;

  public PeerInfo() {
    this.matchIndex = new AtomicLong(-1);
  }

  public long getMatchIndex() {
    return matchIndex.get();
  }

  public void setMatchIndex(long matchIndex) {
    this.matchIndex.set(matchIndex);
  }

  public int incInconsistentHeartbeatNum() {
    return inconsistentHeartbeatNum.incrementAndGet();
  }

  public void resetInconsistentHeartbeatNum() {
    inconsistentHeartbeatNum.set(0);
  }

  public long getLastHeartBeatIndex() {
    return lastHeartBeatIndex;
  }

  public void setLastHeartBeatIndex(long lastHeartBeatIndex) {
    this.lastHeartBeatIndex = lastHeartBeatIndex;
  }
}
