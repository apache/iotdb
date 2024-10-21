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

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.UserDefinedEvent;

public class UserDefinedEnrichedEvent extends EnrichedEvent {

  private final UserDefinedEvent userDefinedEvent;
  private final EnrichedEvent enrichedEvent;

  public static Event maybeOf(final Event event) {
    return event instanceof UserDefinedEvent
            && ((UserDefinedEvent) event).getSourceEvent() instanceof EnrichedEvent
        ? new UserDefinedEnrichedEvent(
            (UserDefinedEvent) event, (EnrichedEvent) ((UserDefinedEvent) event).getSourceEvent())
        : event;
  }

  private UserDefinedEnrichedEvent(
      final UserDefinedEvent userDefinedEvent, final EnrichedEvent enrichedEvent) {
    super(
        enrichedEvent.getPipeName(),
        enrichedEvent.getCreationTime(),
        enrichedEvent.getPipeTaskMeta(),
        enrichedEvent.getTreePattern(),
        enrichedEvent.getTablePattern(),
        enrichedEvent.getStartTime(),
        enrichedEvent.getEndTime());
    this.userDefinedEvent = userDefinedEvent;
    this.enrichedEvent = enrichedEvent;
  }

  public UserDefinedEvent getUserDefinedEvent() {
    return userDefinedEvent;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    return enrichedEvent.internallyIncreaseResourceReferenceCount(holderMessage);
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    return enrichedEvent.internallyDecreaseResourceReferenceCount(holderMessage);
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return enrichedEvent.getProgressIndex();
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime) {
    return enrichedEvent.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
        pipeName, creationTime, pipeTaskMeta, treePattern, tablePattern, startTime, endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return enrichedEvent.isGeneratedByPipe();
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    return enrichedEvent.mayEventTimeOverlappedWithTimeRange();
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    return enrichedEvent.mayEventPathsOverlappedWithPattern();
  }
}
