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

package org.apache.iotdb.commons.pipe.progress;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/** Used to queue {@link Event}s for one pipe of one region to commit in order. */
public class PipeEventCommitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCommitter.class);

  private final CommitterKey committerKey;

  private final AtomicLong commitIdGenerator = new AtomicLong(0);
  private final AtomicLong lastCommitId = new AtomicLong(0);

  private final PriorityBlockingQueue<EnrichedEvent> commitQueue =
      new PriorityBlockingQueue<>(
          11,
          Comparator.comparing(
              event ->
                  Objects.requireNonNull(event, "committable event cannot be null").getCommitId()));

  PipeEventCommitter(final CommitterKey committerKey) {
    // make it package-private
    this.committerKey = committerKey;
  }

  public synchronized long generateCommitId() {
    return commitIdGenerator.incrementAndGet();
  }

  @SuppressWarnings("java:S899")
  public synchronized void commit(final EnrichedEvent event) {
    commitQueue.offer(event);

    final int commitQueueSizeBeforeCommit = commitQueue.size();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "COMMIT QUEUE OFFER: committer key {}, event commit id {}, last commit id {}, commit queue size {}",
          committerKey,
          event.getCommitId(),
          lastCommitId.get(),
          commitQueueSizeBeforeCommit);
    } else if (commitQueueSizeBeforeCommit != 0 && commitQueueSizeBeforeCommit % 100 == 0) {
      LOGGER.info(
          "COMMIT QUEUE OFFER: committer key {}, event commit id {}, last commit id {}, commit queue size {}",
          committerKey,
          event.getCommitId(),
          lastCommitId.get(),
          commitQueueSizeBeforeCommit);
    }

    while (!commitQueue.isEmpty()) {
      final EnrichedEvent e = commitQueue.peek();

      if (e.getCommitId() <= lastCommitId.get()) {
        LOGGER.warn(
            "commit id must be monotonically increasing, current commit id: {}, last commit id: {}, event: {}, stack trace: {}",
            e.getCommitId(),
            lastCommitId.get(),
            e.coreReportMessage(),
            Thread.currentThread().getStackTrace());
        commitQueue.poll();
        continue;
      }

      if (e.getCommitId() != lastCommitId.get() + 1) {
        break;
      }

      e.onCommitted();
      lastCommitId.incrementAndGet();
      commitQueue.poll();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "COMMIT QUEUE POLL: committer key {}, last commit id {}, commit queue size after commit {}",
            committerKey,
            lastCommitId.get(),
            commitQueue.size());
      }
    }
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public String getPipeName() {
    return committerKey.getPipeName();
  }

  // TODO: waiting called by PipeEventCommitMetrics
  public long getCreationTime() {
    return committerKey.getCreationTime();
  }

  public int getRegionId() {
    return committerKey.getRegionId();
  }

  public long commitQueueSize() {
    return commitQueue.size();
  }

  public long getCurrentCommitId() {
    return commitIdGenerator.get();
  }
}
