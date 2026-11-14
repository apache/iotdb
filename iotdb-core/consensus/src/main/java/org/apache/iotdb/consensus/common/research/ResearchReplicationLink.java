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

package org.apache.iotdb.consensus.common.research;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

/** Optional benchmark model: a FIFO per-receiver data link; control heartbeats bypass it. */
public final class ResearchReplicationLink {
  private final long bytesPerSecond;
  private long next;

  public ResearchReplicationLink(long bytesPerSecond) {
    if (bytesPerSecond < 0 || (bytesPerSecond > 0 && bytesPerSecond < 65536)) {
      throw new IllegalArgumentException("Replication link must be disabled or at least 64 KiB/s");
    }
    this.bytesPerSecond = bytesPerSecond;
    this.next = System.nanoTime();
  }

  public void receive(long bytes) throws IOException {
    if (bytesPerSecond == 0 || bytes == 0) {
      return;
    }
    long deadline;
    synchronized (this) {
      long now = System.nanoTime();
      next = Math.max(now, next) + (long) Math.ceil(bytes * 1e9 / bytesPerSecond);
      deadline = next;
    }
    long remaining;
    while ((remaining = deadline - System.nanoTime()) > 0) {
      LockSupport.parkNanos(remaining);
      if (Thread.currentThread().isInterrupted()) {
        throw new IOException("Interrupted link model");
      }
    }
    ResearchMetrics.add("raft.replication_link_bytes", bytes);
  }
}
