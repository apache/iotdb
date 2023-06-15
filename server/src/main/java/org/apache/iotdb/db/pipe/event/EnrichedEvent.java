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

package org.apache.iotdb.db.pipe.event;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant;
import org.apache.iotdb.pipe.api.event.Event;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * EnrichedEvent is an event that can be enriched with additional runtime information. The
 * additional information mainly includes the reference count of the event.
 */
public abstract class EnrichedEvent implements Event {

  private final AtomicInteger referenceCount;

  private final PipeStaticMeta pipeStaticMeta;
  private final TConsensusGroupId regionId;

  private final String pattern;

  public EnrichedEvent(PipeStaticMeta pipeStaticMeta, TConsensusGroupId regionId, String pattern) {
    this.referenceCount = new AtomicInteger(0);
    this.pipeStaticMeta = pipeStaticMeta;
    this.regionId = regionId;
    this.pattern = pattern;
  }

  /**
   * increase the reference count of this event. when the reference count is positive, the data in
   * the resource of this event should be safe to use.
   *
   * @param holderMessage the message of the invoker
   * @return true if the reference count is increased successfully, false if the event is not
   */
  public boolean increaseReferenceCount(String holderMessage) {
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
  public boolean decreaseReferenceCount(String holderMessage) {
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
    if (pipeStaticMeta != null) {
      PipeAgent.runtime().report(pipeStaticMeta, getProgressIndex());
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

  /**
   * Get the pattern of this event.
   *
   * @return the pattern
   */
  public final String getPattern() {
    return pattern == null ? PipeCollectorConstant.COLLECTOR_PATTERN_DEFAULT_VALUE : pattern;
  }

  public abstract EnrichedEvent shallowCopySelfAndBindPipeStaticMetaForProgressReport(
      PipeStaticMeta pipeStaticMeta, TConsensusGroupId regionId, String pattern);

  public void reportException(PipeRuntimeException pipeRuntimeException) {
    PipeAgent.runtime().report(this.pipeStaticMeta, pipeRuntimeException);
  }
}
