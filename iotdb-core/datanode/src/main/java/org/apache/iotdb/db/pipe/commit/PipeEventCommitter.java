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

package org.apache.iotdb.db.pipe.commit;

import org.apache.iotdb.db.pipe.event.EnrichedEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/** Used to queue events for one pipe of one dataRegion to commit in order. */
public class PipeEventCommitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeEventCommitter.class);

  private final AtomicLong commitIdGenerator = new AtomicLong(0);
  private final AtomicLong lastCommitId = new AtomicLong(0);

  private final PriorityBlockingQueue<EnrichedEvent> commitQueue =
      new PriorityBlockingQueue<>(
          11,
          Comparator.comparing(
              event ->
                  Objects.requireNonNull(event, "committable event cannot be null").getCommitId()));

  PipeEventCommitter() {
    // Do nothing but make it package-private.
  }

  public synchronized long generateCommitId() {
    return commitIdGenerator.incrementAndGet();
  }

  public synchronized void commit(EnrichedEvent event) {
    commitQueue.offer(event);

    while (!commitQueue.isEmpty()) {
      final EnrichedEvent e = commitQueue.peek();

      if (e.getCommitId() <= lastCommitId.get()) {
        LOGGER.warn(
            "commit id must be monotonically increasing, lastCommitId: {}, event: {}",
            lastCommitId.get(),
            e);
        commitQueue.poll();
        continue;
      }

      if (e.getCommitId() != lastCommitId.get() + 1) {
        break;
      }

      e.onCommitted();
      lastCommitId.incrementAndGet();
      commitQueue.poll();
    }
  }
}
