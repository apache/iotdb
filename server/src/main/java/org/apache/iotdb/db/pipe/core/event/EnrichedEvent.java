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

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * EnrichedEvent is an event that can be enriched with additional runtime information. The
 * additional information mainly includes the reference count of the event.
 */
public abstract class EnrichedEvent implements Event {

  private final AtomicInteger referenceCount;

  private final PipeTaskMeta pipeTaskMeta;

  protected String pattern;

  public EnrichedEvent(PipeTaskMeta pipeTaskMeta, String pattern) {
    referenceCount = new AtomicInteger(0);
    this.pipeTaskMeta = pipeTaskMeta;
    this.pattern = pattern;
  }

  /**
   * increase the reference count of this event. when the reference count is positive, the data in
   * the resource of this event should be safe to use.
   *
   * @param holderMessage the message of the invoker
   * @return true if the reference count is increased successfully, false if the event is not
   */
  public final boolean increaseReferenceCount(String holderMessage) {
    boolean isSuccessful = true;
    synchronized (this) {
      if (referenceCount.get() == 0) {
        isSuccessful = increaseResourceReferenceCount(holderMessage);
      }
      referenceCount.incrementAndGet();
    }
    return isSuccessful;
  }

  /**
   * Increase the reference count of the resource of this event.
   *
   * @param holderMessage the message of the invoker
   * @return true if the reference count is increased successfully, false if the event is not
   *     controlled by the invoker, which means the data stored in the event is not safe to use
   */
  public abstract boolean increaseResourceReferenceCount(String holderMessage);

  /**
   * Decrease the reference count of this event. If the reference count is decreased to 0, the event
   * can be recycled and the data stored in the event is not safe to use, the processing progress of
   * the event should be reported to the pipe task meta.
   *
   * @param holderMessage the message of the invoker
   * @return true if the reference count is decreased successfully, false otherwise
   */
  public final boolean decreaseReferenceCount(String holderMessage) {
    boolean isSuccessful = true;
    synchronized (this) {
      if (referenceCount.get() == 1) {
        isSuccessful = decreaseResourceReferenceCount(holderMessage);
        reportProgress();
      }
      referenceCount.decrementAndGet();
    }
    return isSuccessful;
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
      pipeTaskMeta.updateProgressIndex(getProgressIndex());
    }
  }

  public abstract ProgressIndex getProgressIndex();

  /**
   * Get the reference count of this event.
   *
   * @return the reference count
   */
  public int getReferenceCount() {
    return referenceCount.get();
  }

  /** set the pattern of this event. */
  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  /**
   * Get the pattern of this event.
   *
   * @return the pattern
   */
  public String getPattern() {
    return pattern;
  }

  // TODO: ConsensusIndex getConsensusIndex();
  public abstract EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      PipeTaskMeta pipeTaskMeta);
}
