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

package org.apache.iotdb.commons.pipe.agent.task.progress;

import org.apache.iotdb.commons.pipe.agent.task.progress.interval.PipeCommitQueue;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.concurrent.atomic.AtomicLong;

/** Used to queue {@link Event}s for one pipe of one region to commit in order. */
public class PipeEventCommitter {

  private final CommitterKey committerKey;

  private final AtomicLong commitIdGenerator = new AtomicLong(0);

  private final PipeCommitQueue commitQueue = new PipeCommitQueue();

  PipeEventCommitter(final CommitterKey committerKey) {
    // make it package-private
    this.committerKey = committerKey;
  }

  public synchronized long generateCommitId() {
    return commitIdGenerator.incrementAndGet();
  }

  @SuppressWarnings("java:S899")
  public synchronized void commit(final EnrichedEvent event) {
    if (event.hasMultipleCommitIds()) {
      for (final EnrichedEvent dummyEvent : event.getDummyEventsForCommitIds()) {
        commitQueue.offer(dummyEvent);
      }
    }
    commitQueue.offer(event);
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
}
