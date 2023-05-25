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

package org.apache.iotdb.db.pipe.core.event;

import org.apache.iotdb.commons.consensus.index.ConsensusIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * EnrichedEvent is an event that can be enriched with additional runtime information. The
 * additional information mainly includes the reference count of the event.
 */
public abstract class EnrichedEvent implements Event {
  private final AtomicInteger referenceCount = new AtomicInteger(0);
  private PipeTaskMeta pipeTaskMeta;

  public EnrichedEvent() {}

  public boolean increaseReferenceCount(String holderMessage) {
    AtomicBoolean success = new AtomicBoolean(true);
    referenceCount.getAndUpdate(
        count -> {
          if (count == 0) {
            success.set(increaseResourceReferenceCount(holderMessage));
          }
          return count + 1;
        });
    return success.get();
  }

  /**
   * Increase the reference count of this event.
   *
   * @param holderMessage the message of the invoker
   * @return true if the reference count is increased successfully, false if the event is not
   *     controlled by the invoker, which means the data stored in the event is not safe to use
   */
  public abstract boolean increaseResourceReferenceCount(String holderMessage);

  public boolean decreaseReferenceCount(String holderMessage) {
    AtomicBoolean success = new AtomicBoolean(true);
    referenceCount.getAndUpdate(
        count -> {
          if (count == 1) {
            success.set(decreaseResourceReferenceCount(holderMessage));
            reportProgress();
          }
          return count - 1;
        });
    return success.get();
  }

  /**
   * Decrease the reference count of this event. If the reference count is decreased to 0, the event
   * can be recycled and the data stored in the event is not safe to use.
   *
   * @param holderMessage the message of the invoker
   * @return true if the reference count is decreased successfully, false otherwise
   */
  public abstract boolean decreaseResourceReferenceCount(String holderMessage);

  private void reportProgress() {
    if (pipeTaskMeta != null) {
      pipeTaskMeta.updateProgressIndex(getConsensusIndex());
    }
  }

  /**
   * Get the reference count of this event.
   *
   * @return the reference count
   */
  public int getReferenceCount() {
    return referenceCount.get();
  }

  public abstract ConsensusIndex getConsensusIndex();

  public void reportProgressIndexToPipeTaskMetaWhenFinish(PipeTaskMeta pipeTaskMeta) {
    this.pipeTaskMeta = pipeTaskMeta;
  }

  public abstract EnrichedEvent shallowCopySelf();
}
