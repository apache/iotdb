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

package org.apache.iotdb.consensus.traft;

import org.apache.iotdb.consensus.common.Peer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Per-follower background appender thread.
 *
 * <p>Runs an infinite loop that polls every {@code waitingReplicationTimeMs} milliseconds and calls
 * back into {@link TRaftServerImpl#tryReplicateDiskEntriesToFollower} to dispatch any log entries
 * that are on disk but have not yet been sent to the target follower. This handles two scenarios:
 *
 * <ul>
 *   <li>Entries whose partition index was higher than the leader's current inserting partition at
 *       write time (the entry went straight to disk and was deferred).
 *   <li>Entries that could not be sent synchronously because the follower was offline during the
 *       original write, or because the previous partition's quorum commit is still pending.
 * </ul>
 */
class TRaftLogAppender implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(TRaftLogAppender.class);

  private final TRaftServerImpl server;
  private final Peer follower;
  private final long waitingReplicationTimeMs;

  private volatile boolean stopped = false;
  private final CountDownLatch runFinished = new CountDownLatch(1);

  TRaftLogAppender(TRaftServerImpl server, Peer follower, long waitingReplicationTimeMs) {
    this.server = server;
    this.follower = follower;
    this.waitingReplicationTimeMs = waitingReplicationTimeMs;
  }

  /** Signal the appender to stop after the current iteration completes. */
  void stop() {
    stopped = true;
  }

  /**
   * Block until the appender thread has exited, up to {@code timeoutSeconds} seconds.
   *
   * @return {@code true} if the thread exited within the timeout, {@code false} otherwise.
   */
  boolean awaitTermination(long timeoutSeconds) throws InterruptedException {
    return runFinished.await(timeoutSeconds, TimeUnit.SECONDS);
  }

  @Override
  public void run() {
    LOGGER.info("TRaftLogAppender started for follower {}", follower);
    try {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {
          // Attempt to replicate any disk entries that are ready for this follower.
          server.tryReplicateDiskEntriesToFollower(follower);
        } catch (Exception e) {
          LOGGER.warn("Error during disk replication to {}: {}", follower, e.getMessage(), e);
        }
        TimeUnit.MILLISECONDS.sleep(waitingReplicationTimeMs);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      runFinished.countDown();
      LOGGER.info("TRaftLogAppender stopped for follower {}", follower);
    }
  }
}
