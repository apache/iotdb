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

package org.apache.iotdb.db.pipe.connector.protocol;

import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

public abstract class CommittableConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommittableConnector.class);

  protected static final Map<String, AtomicLong> commitIdGeneratorMap = new HashMap<>();
  protected static final Map<String, AtomicLong> lastCommitIdMap = new HashMap<>();
  protected static final Map<String, PriorityQueue<Pair<Long, Runnable>>> commitQueueMap =
      new HashMap<>();

  // Sync sinks are also used inside async sinks for retry. At that case, we should disable its
  // commit.
  protected boolean commitDisabled = false;

  public void disableCommit() {
    commitDisabled = true;
  }

  public void generateCommitId(EnrichedEvent event) {
    if (commitDisabled || event.getCommitId() != EnrichedEvent.NO_COMMIT_ID) {
      return;
    }
    String className = this.getClass().getSimpleName();
    synchronized (commitIdGeneratorMap) {
      if (!commitIdGeneratorMap.containsKey(className)) {
        commitIdGeneratorMap.put(className, new AtomicLong(0));
        lastCommitIdMap.put(className, new AtomicLong(0));
        commitQueueMap.put(className, new PriorityQueue<>(Comparator.comparing(o -> o.left)));
      }
      event.setCommitId(commitIdGeneratorMap.get(className).incrementAndGet());
      // Increase reference count, will decrease in commit()
      event.increaseReferenceCount(CommittableConnector.class.getName());
    }
  }

  /**
   * Commit the event. Decrease the reference count of the event. If the reference count is 0, the
   * progress index of the event will be recalculated and the resources of the event will be
   * released.
   *
   * <p>The synchronization is necessary because the commit order must be the same as the order of
   * the events. Concurrent commit may cause the commit order to be inconsistent with the order of
   * the events.
   *
   * @param enrichedEvent event to commit
   */
  public synchronized void commit(EnrichedEvent enrichedEvent) {
    if (commitDisabled) {
      return;
    }

    String className = this.getClass().getSimpleName();
    PriorityQueue<Pair<Long, Runnable>> commitQueue = commitQueueMap.get(className);
    if (commitQueue == null || enrichedEvent == null) {
      LOGGER.warn("The commit queue is null or the event is null.");
      return;
    }
    if (enrichedEvent.getCommitId() == EnrichedEvent.NO_COMMIT_ID) {
      return;
    }

    commitQueue.offer(
        new Pair<>(
            enrichedEvent.getCommitId(),
            () ->
                enrichedEvent.decreaseReferenceCount(CommittableConnector.class.getName(), true)));

    AtomicLong lastCommitId = lastCommitIdMap.get(className);
    while (!commitQueue.isEmpty()) {
      final Pair<Long, Runnable> committer = commitQueue.peek();
      if (lastCommitId.get() + 1 != committer.left) {
        break;
      }

      committer.right.run();
      lastCommitId.incrementAndGet();

      commitQueue.poll();
    }
  }
}
