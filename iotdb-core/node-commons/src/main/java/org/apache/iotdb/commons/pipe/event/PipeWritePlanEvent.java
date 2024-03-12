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

package org.apache.iotdb.commons.pipe.event;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public abstract class PipeWritePlanEvent extends EnrichedEvent implements SerializableEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeWritePlanEvent.class);

  protected boolean isGeneratedByPipe;

  protected final AtomicLong referenceCount = new AtomicLong(0);

  protected ProgressIndex progressIndex;

  protected PipeWritePlanEvent(
      String pipeName, PipeTaskMeta pipeTaskMeta, PipePattern pattern, boolean isGeneratedByPipe) {
    super(pipeName, pipeTaskMeta, pattern, Long.MIN_VALUE, Long.MAX_VALUE);
    this.isGeneratedByPipe = isGeneratedByPipe;
  }

  /**
   * This event doesn't share resources with other events, so no need to maintain reference count.
   * We just use a counter to prevent the reference count from being less than 0.
   */
  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    referenceCount.incrementAndGet();
    return true;
  }

  /**
   * This event doesn't share resources with other events, so no need to maintain reference count.
   * We just use a counter to prevent the reference count from being less than 0.
   */
  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    final long count = referenceCount.decrementAndGet();
    if (count < 0) {
      LOGGER.warn("The reference count is less than 0, may need to check the implementation.");
    }
    return true;
  }

  @Override
  public void bindProgressIndex(ProgressIndex progressIndex) {
    this.progressIndex = progressIndex;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    return true;
  }
}
