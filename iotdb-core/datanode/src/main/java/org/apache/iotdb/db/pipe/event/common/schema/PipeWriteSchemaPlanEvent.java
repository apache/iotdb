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

package org.apache.iotdb.db.pipe.event.common.schema;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class PipeWriteSchemaPlanEvent extends EnrichedEvent {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeWriteSchemaPlanEvent.class);

  private final PlanNode planNode;

  private final AtomicLong referenceCount = new AtomicLong(0);
  private final boolean isGeneratedByPipe;

  public PipeWriteSchemaPlanEvent(
      PlanNode planNode,
      boolean isGeneratedByPipe,
      String pipeName,
      PipeTaskMeta pipeTaskMeta,
      String pattern) {
    super(pipeName, pipeTaskMeta, pattern);
    this.planNode = planNode;
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
  public ProgressIndex getProgressIndex() {
    return null;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName, PipeTaskMeta pipeTaskMeta, String pattern) {
    return new PipeWriteSchemaPlanEvent(
        planNode, isGeneratedByPipe, pipeName, pipeTaskMeta, pattern);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }
}
